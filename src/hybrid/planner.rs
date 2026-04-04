use anyhow::{Context, Result, bail};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeSet, HashMap};

const INTERNAL_TABLE_NAME: &str = "matched_rows";
const SQL_MATCH_LIMIT: usize = 100_000;
pub const SQL_SEMIJOIN_MAX_KEYS: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyntheticColumn {
    Id,
    Score,
}

impl SyntheticColumn {
    const fn name(self) -> &'static str {
        match self {
            Self::Id => "_id",
            Self::Score => "_score",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "_id" => Some(Self::Id),
            "_score" => Some(Self::Score),
            _ => None,
        }
    }

    fn matches_name(self, name: &str) -> bool {
        name == self.name()
    }
}

// ── Semantic Binder ────────────────────────────────────────────────────
// Resolves SQL identifiers into classified categories BEFORE capability
// analysis. This centralises _id/_score detection, alias tracking,
// required-column extraction, and GROUP BY eligibility into one pass
// instead of scattering it across 25+ raw AST walkers.

/// How a SQL identifier was resolved during binding.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BoundIdentifier {
    /// A real index field (e.g. `author`, `price`).
    Source(String),
    /// A synthetic field (`_id` or `_score`).
    Synthetic(SyntheticColumn),
}

/// A bound SELECT projection item — what the planner knows about each
/// item in the SELECT list after binding.
#[derive(Debug, Clone)]
#[allow(dead_code)] // alias fields reserved for future semijoin/binder work
enum BoundSelectItem {
    /// A plain column reference (possibly aliased).
    Column {
        id: BoundIdentifier,
        alias: Option<String>,
    },
    /// An aggregate function like COUNT(*), AVG(price), etc.
    Aggregate { metric: SqlGroupedMetric },
    /// A wildcard (`SELECT *`).
    Wildcard,
    /// An expression the binder cannot fully resolve — passed through
    /// to DataFusion as-is (e.g. `LOWER(author)`, `price * 2`).
    Unresolved {
        /// Source columns referenced inside this expression.
        source_columns: Vec<String>,
        /// Whether `_id` is referenced.
        needs_id: bool,
        /// Whether `_score` is referenced.
        needs_score: bool,
        alias: Option<String>,
    },
}

/// A bound GROUP BY item.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BoundGroupByItem {
    /// A plain source column (eligible for grouped partials).
    Source(String),
    /// An expression or synthetic column (forces expression GROUP BY fallback).
    Expression,
}

/// The result of binding a SELECT statement.
struct BindContext {
    /// Bound SELECT projection items.
    select_items: Vec<BoundSelectItem>,
    /// All SELECT output aliases.
    alias_set: BTreeSet<String>,
    /// Aliases like `author AS author` that are semantically identical to
    /// the underlying source column and must not block source-column
    /// interpretation in WHERE/GROUP BY/required-column extraction.
    identity_source_aliases: BTreeSet<String>,
    /// Bound GROUP BY items.
    group_by_items: Vec<BoundGroupByItem>,
    /// Whether SELECT * is present.
    selects_all_columns: bool,
}

impl BindContext {
    /// Derive `required_columns`, `needs_id`, `needs_score` from bound items
    /// plus ORDER BY and WHERE/HAVING expressions.
    fn derive_columns(
        &self,
        select: &Select,
        order_by: Option<&sqlparser::ast::OrderBy>,
    ) -> (Vec<String>, bool, bool) {
        let mut columns = BTreeSet::new();
        let mut needs_id = false;
        let mut needs_score = false;

        // From SELECT items
        for item in &self.select_items {
            match item {
                BoundSelectItem::Column { id, .. } => match id {
                    BoundIdentifier::Source(field) => {
                        columns.insert(field.clone());
                    }
                    BoundIdentifier::Synthetic(SyntheticColumn::Id) => needs_id = true,
                    BoundIdentifier::Synthetic(SyntheticColumn::Score) => needs_score = true,
                },
                BoundSelectItem::Aggregate { metric } => {
                    if let Some(ref field) = metric.field {
                        columns.insert(field.clone());
                    }
                }
                BoundSelectItem::Wildcard => {
                    // SELECT * — columns come from _source; no explicit columns needed
                }
                BoundSelectItem::Unresolved {
                    source_columns,
                    needs_id: uid,
                    needs_score: usc,
                    ..
                } => {
                    columns.extend(source_columns.iter().cloned());
                    needs_id |= uid;
                    needs_score |= usc;
                }
            }
        }

        // From WHERE clause (residual after pushdowns — walk with alias awareness)
        if let Some(ref selection) = select.selection {
            collect_expr_columns_alias_aware_into(
                selection,
                &self.alias_set,
                &self.identity_source_aliases,
                &mut columns,
                &mut needs_id,
                &mut needs_score,
            );
        }

        // From HAVING clause
        if let Some(ref having) = select.having {
            collect_expr_columns_alias_aware_into(
                having,
                &self.alias_set,
                &self.identity_source_aliases,
                &mut columns,
                &mut needs_id,
                &mut needs_score,
            );
        }

        // From ORDER BY
        if let Some(order_by) = order_by
            && let OrderByKind::Expressions(exprs) = &order_by.kind
        {
            for expr in exprs {
                collect_expr_columns_alias_aware_into(
                    &expr.expr,
                    &self.alias_set,
                    &self.identity_source_aliases,
                    &mut columns,
                    &mut needs_id,
                    &mut needs_score,
                );
            }
        }

        // From GROUP BY expressions (for required_columns — need the fields
        // even if GROUP BY is an expression like LOWER(author))
        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                collect_expr_source_columns(expr, &mut columns);
            }
        }

        // Remove the internal table name if it leaked through
        columns.remove(INTERNAL_TABLE_NAME);

        (columns.into_iter().collect(), needs_id, needs_score)
    }

    /// Derive plain GROUP BY column names and whether expression GROUP BY is present.
    fn derive_group_by(&self) -> (Vec<String>, bool) {
        let mut columns = Vec::new();
        let mut has_expression = false;
        for item in &self.group_by_items {
            match item {
                BoundGroupByItem::Source(name) => columns.push(name.clone()),
                BoundGroupByItem::Expression => has_expression = true,
            }
        }
        (columns, has_expression)
    }
}

/// Bind a parsed SELECT + ORDER BY into a `BindContext`.
fn bind_select(select: &Select) -> BindContext {
    let mut select_items = Vec::with_capacity(select.projection.len());
    let mut alias_set = BTreeSet::new();
    let mut identity_source_aliases = BTreeSet::new();
    let mut selects_all_columns = false;

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                select_items.push(bind_expr_as_select_item(expr, None, &alias_set));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_name = alias.value.clone();
                let bound = bind_expr_as_select_item(expr, Some(alias_name.clone()), &alias_set);
                if let BoundSelectItem::Column {
                    id: BoundIdentifier::Source(source_name),
                    ..
                } = &bound
                    && source_name == &alias_name
                {
                    identity_source_aliases.insert(alias_name.clone());
                }
                alias_set.insert(alias_name);
                select_items.push(bound);
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                selects_all_columns = true;
                select_items.push(BoundSelectItem::Wildcard);
            }
        }
    }

    // Bind GROUP BY (alias-aware — aliases must not be classified as source columns)
    let group_by_items = bind_group_by(&select.group_by, &alias_set, &identity_source_aliases);

    BindContext {
        select_items,
        alias_set,
        identity_source_aliases,
        group_by_items,
        selects_all_columns,
    }
}

/// Bind a single expression as a SELECT item.
fn bind_expr_as_select_item(
    expr: &Expr,
    alias: Option<String>,
    _aliases: &BTreeSet<String>,
) -> BoundSelectItem {
    // Try as aggregate first
    if let Ok(Some(metric)) = parse_grouped_metric(expr, alias.clone()) {
        return BoundSelectItem::Aggregate { metric };
    }

    // Try as plain column
    if let Some(name) = expr_to_field_name(expr) {
        let id = if let Some(syn) = SyntheticColumn::parse(&name) {
            BoundIdentifier::Synthetic(syn)
        } else {
            BoundIdentifier::Source(name)
        };
        return BoundSelectItem::Column { id, alias };
    }

    // Unresolved expression — walk to extract source columns and synthetic flags
    let mut source_columns = BTreeSet::new();
    let mut uid = false;
    let mut usc = false;
    collect_expr_source_columns_with_flags(expr, &mut source_columns, &mut uid, &mut usc);
    BoundSelectItem::Unresolved {
        source_columns: source_columns.into_iter().collect(),
        needs_id: uid,
        needs_score: usc,
        alias,
    }
}

/// Bind GROUP BY items. Plain source columns become `Source`, everything
/// else (expressions, synthetics, aliases) becomes `Expression`.
fn bind_group_by(
    group_by: &sqlparser::ast::GroupByExpr,
    aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> Vec<BoundGroupByItem> {
    let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = group_by else {
        return Vec::new();
    };
    exprs
        .iter()
        .map(|expr| {
            if let Some(name) = expr_to_field_name(expr) {
                if SyntheticColumn::parse(&name).is_some()
                    || alias_blocks_source_name(&name, aliases, identity_source_aliases)
                {
                    BoundGroupByItem::Expression
                } else {
                    BoundGroupByItem::Source(name)
                }
            } else {
                BoundGroupByItem::Expression
            }
        })
        .collect()
}

fn alias_blocks_source_name(
    name: &str,
    aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> bool {
    aliases.contains(name) && !identity_source_aliases.contains(name)
}

/// Walk an expression and collect all source field names (non-synthetic).
fn collect_expr_source_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    let mut uid = false;
    let mut usc = false;
    collect_expr_source_columns_with_flags(expr, columns, &mut uid, &mut usc);
}

/// Walk an expression and collect source columns plus synthetic flags.
fn collect_expr_source_columns_with_flags(
    expr: &Expr,
    columns: &mut BTreeSet<String>,
    needs_id: &mut bool,
    needs_score: &mut bool,
) {
    match expr {
        Expr::Identifier(ident) => {
            let name = &ident.value;
            match SyntheticColumn::parse(name) {
                Some(SyntheticColumn::Id) => *needs_id = true,
                Some(SyntheticColumn::Score) => *needs_score = true,
                None => {
                    columns.insert(name.clone());
                }
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(ident) = idents.last() {
                let name = &ident.value;
                match SyntheticColumn::parse(name) {
                    Some(SyntheticColumn::Id) => *needs_id = true,
                    Some(SyntheticColumn::Score) => *needs_score = true,
                    None => {
                        columns.insert(name.clone());
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_source_columns_with_flags(left, columns, needs_id, needs_score);
            collect_expr_source_columns_with_flags(right, columns, needs_id, needs_score);
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. } => {
            collect_expr_source_columns_with_flags(inner, columns, needs_id, needs_score);
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_expr_source_columns_with_flags(e, columns, needs_id, needs_score);
            collect_expr_source_columns_with_flags(low, columns, needs_id, needs_score);
            collect_expr_source_columns_with_flags(high, columns, needs_id, needs_score);
        }
        Expr::InList { expr: e, list, .. } => {
            collect_expr_source_columns_with_flags(e, columns, needs_id, needs_score);
            for item in list {
                collect_expr_source_columns_with_flags(item, columns, needs_id, needs_score);
            }
        }
        Expr::Function(func) => {
            // For aggregates, extract the field name directly
            if let Ok(Some(metric)) = parse_grouped_metric(expr, None) {
                if let Some(field) = &metric.field {
                    columns.insert(field.clone());
                }
                return;
            }
            if let FunctionArguments::List(list) = &func.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            collect_expr_source_columns_with_flags(
                                e,
                                columns,
                                needs_id,
                                needs_score,
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_expr_source_columns_with_flags(op, columns, needs_id, needs_score);
            }
            for cond in conditions {
                let sqlparser::ast::CaseWhen { condition, result } = cond;
                collect_expr_source_columns_with_flags(condition, columns, needs_id, needs_score);
                collect_expr_source_columns_with_flags(result, columns, needs_id, needs_score);
            }
            if let Some(else_res) = else_result {
                collect_expr_source_columns_with_flags(else_res, columns, needs_id, needs_score);
            }
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            collect_expr_source_columns_with_flags(e, columns, needs_id, needs_score);
            collect_expr_source_columns_with_flags(pattern, columns, needs_id, needs_score);
        }
        _ => {}
    }
}

/// Walk an expression collecting source columns with alias awareness, plus
/// synthetic flags. Unified replacement for separate `collect_expr_columns_alias_aware`
/// + synthetic detection.
fn collect_expr_columns_alias_aware_into(
    expr: &Expr,
    aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
    columns: &mut BTreeSet<String>,
    needs_id: &mut bool,
    needs_score: &mut bool,
) {
    // Aggregates: extract field, skip function name
    if let Ok(Some(metric)) = parse_grouped_metric(expr, None) {
        if let Some(field) = metric.field {
            columns.insert(field);
        }
        return;
    }

    match expr {
        Expr::Identifier(ident) => {
            let name = &ident.value;
            if alias_blocks_source_name(name, aliases, identity_source_aliases) {
                return;
            }
            match SyntheticColumn::parse(name) {
                Some(SyntheticColumn::Id) => *needs_id = true,
                Some(SyntheticColumn::Score) => *needs_score = true,
                None => {
                    columns.insert(name.clone());
                }
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(ident) = idents.last() {
                let name = &ident.value;
                if alias_blocks_source_name(name, aliases, identity_source_aliases) {
                    return;
                }
                match SyntheticColumn::parse(name) {
                    Some(SyntheticColumn::Id) => *needs_id = true,
                    Some(SyntheticColumn::Score) => *needs_score = true,
                    None => {
                        columns.insert(name.clone());
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns_alias_aware_into(
                left,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
            collect_expr_columns_alias_aware_into(
                right,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. } => {
            collect_expr_columns_alias_aware_into(
                inner,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            collect_expr_columns_alias_aware_into(
                e,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
            collect_expr_columns_alias_aware_into(
                low,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
            collect_expr_columns_alias_aware_into(
                high,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
        }
        Expr::InList { expr: e, list, .. } => {
            collect_expr_columns_alias_aware_into(
                e,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
            for item in list {
                collect_expr_columns_alias_aware_into(
                    item,
                    aliases,
                    identity_source_aliases,
                    columns,
                    needs_id,
                    needs_score,
                );
            }
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(list) = &func.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            collect_expr_columns_alias_aware_into(
                                e,
                                aliases,
                                identity_source_aliases,
                                columns,
                                needs_id,
                                needs_score,
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_expr_columns_alias_aware_into(
                    op,
                    aliases,
                    identity_source_aliases,
                    columns,
                    needs_id,
                    needs_score,
                );
            }
            for cond in conditions {
                let sqlparser::ast::CaseWhen { condition, result } = cond;
                collect_expr_columns_alias_aware_into(
                    condition,
                    aliases,
                    identity_source_aliases,
                    columns,
                    needs_id,
                    needs_score,
                );
                collect_expr_columns_alias_aware_into(
                    result,
                    aliases,
                    identity_source_aliases,
                    columns,
                    needs_id,
                    needs_score,
                );
            }
            if let Some(else_res) = else_result {
                collect_expr_columns_alias_aware_into(
                    else_res,
                    aliases,
                    identity_source_aliases,
                    columns,
                    needs_id,
                    needs_score,
                );
            }
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            collect_expr_columns_alias_aware_into(
                e,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
            collect_expr_columns_alias_aware_into(
                pattern,
                aliases,
                identity_source_aliases,
                columns,
                needs_id,
                needs_score,
            );
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextMatchPredicate {
    pub field: String,
    pub query: String,
}

pub(crate) const INTERNAL_SQL_GROUPED_AGG: &str = "__sql_grouped_metrics";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlGroupedMetricFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl SqlGroupedMetricFunction {
    pub(crate) fn to_search_function(&self) -> crate::search::GroupedMetricFunction {
        match self {
            Self::Count => crate::search::GroupedMetricFunction::Count,
            Self::Sum => crate::search::GroupedMetricFunction::Sum,
            Self::Avg => crate::search::GroupedMetricFunction::Avg,
            Self::Min => crate::search::GroupedMetricFunction::Min,
            Self::Max => crate::search::GroupedMetricFunction::Max,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlGroupColumn {
    pub source_name: String,
    pub output_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlGroupedMetric {
    pub output_name: String,
    pub function: SqlGroupedMetricFunction,
    pub field: Option<String>,
    pub projected: bool,
}

#[derive(Debug, Clone)]
pub struct SemijoinPlan {
    pub outer_key: String,
    pub inner_key: String,
    pub inner_plan: Box<QueryPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOrderBy {
    pub output_name: String,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HavingOp {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HavingFilter {
    pub output_name: String,
    pub op: HavingOp,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedSqlPlan {
    pub group_columns: Vec<SqlGroupColumn>,
    pub metrics: Vec<SqlGroupedMetric>,
    pub order_by: Vec<SqlOrderBy>,
    pub having: Vec<HavingFilter>,
}

impl GroupedSqlPlan {
    /// Returns true when this grouped query is eligible for top-K selection
    /// (ORDER BY + LIMIT present, partial sort faster than full sort).
    pub fn uses_top_k(&self, limit: Option<usize>) -> bool {
        !self.order_by.is_empty() && limit.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub index_name: String,
    pub original_sql: String,
    pub rewritten_sql: String,
    pub semijoin: Option<SemijoinPlan>,
    pub text_matches: Vec<TextMatchPredicate>,
    pub pushed_filters: Vec<crate::search::QueryClause>,
    pub required_columns: Vec<String>,
    pub group_by_columns: Vec<String>,
    pub has_residual_predicates: bool,
    pub selects_all_columns: bool,
    pub grouped_sql: Option<GroupedSqlPlan>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub limit_pushed_down: bool,
    /// Whether the SQL query references `_id` in any projection, filter, or ordering.
    pub needs_id: bool,
    /// Whether the SQL query references synthetic `_score` in any projection,
    /// filter, or ordering.
    pub needs_score: bool,
    /// When ORDER BY is a single non-`_score` column, capture (field, descending) for
    /// fast-field sort pushdown into Tantivy's TopDocs collector.
    pub sort_pushdown: Option<(String, bool)>,
    /// True when the SQL has GROUP BY but it's not handled by the
    /// `tantivy_grouped_partials` path (expression GROUP BY, unsupported
    /// aggregates, residual predicates). These queries MUST see all matching
    /// docs for correct results — the default 100K TopDocs cap is wrong here.
    pub has_group_by_fallback: bool,
}

impl QueryPlan {
    pub fn primary_text_match(&self) -> Option<&TextMatchPredicate> {
        self.text_matches.first()
    }

    pub fn uses_grouped_partials(&self) -> bool {
        self.grouped_sql.is_some()
    }

    pub(crate) fn text_matches_json(&self) -> Vec<serde_json::Value> {
        self.text_matches
            .iter()
            .map(|text_match| {
                serde_json::json!({
                    "field": text_match.field,
                    "query": text_match.query,
                })
            })
            .collect()
    }

    /// Returns true if this is a simple `SELECT count(*) FROM ...` with no WHERE,
    /// no GROUP BY, and no non-count aggregates — meaning we can answer it from
    /// `doc_count()` metadata without scanning any documents.
    pub fn is_count_star_only(&self) -> bool {
        if self.semijoin.is_some() {
            return false;
        }
        if !self.text_matches.is_empty() || !self.pushed_filters.is_empty() {
            return false;
        }
        if let Some(grouped) = &self.grouped_sql {
            grouped.group_columns.is_empty()
                && !grouped.metrics.is_empty()
                && grouped.metrics.iter().all(|m| {
                    matches!(m.function, SqlGroupedMetricFunction::Count) && m.field.is_none()
                })
        } else {
            false
        }
    }

    pub fn to_search_request(&self, approximate_top_k: bool) -> crate::search::SearchRequest {
        let query = if self.text_matches.is_empty() && self.pushed_filters.is_empty() {
            crate::search::QueryClause::MatchAll(serde_json::Value::Object(Default::default()))
        } else {
            let mut bool_query = crate::search::BoolQuery::default();

            bool_query
                .must
                .extend(self.text_matches.iter().map(text_match_to_query_clause));

            bool_query.filter.extend(self.pushed_filters.clone());
            crate::search::QueryClause::Bool(bool_query)
        };

        let (size, aggs) = if let Some(grouped_sql) = &self.grouped_sql {
            let metrics = grouped_sql
                .metrics
                .iter()
                .map(|metric| crate::search::GroupedMetricAgg {
                    output_name: metric.output_name.clone(),
                    function: metric.function.to_search_function(),
                    field: metric.field.clone(),
                })
                .collect();

            // Shard-level top-K pruning is opt-in via `sql_approximate_top_k: true`
            // in ferrissearch.yml.  Standard SQL engines compute exact GROUP BY
            // results; this approximation trades correctness for latency on
            // high-cardinality full-scan GROUP BY with ORDER BY + LIMIT.
            //
            // Safety guards (when enabled):
            // - Disabled when HAVING is present (pruned groups could satisfy
            //   HAVING only after cross-shard merge).
            // - Disabled for multi-column ORDER BY (secondary keys not evaluated).
            // - Disabled when ORDER BY references a group column (not a metric).
            let shard_top_k = if approximate_top_k
                && grouped_sql.uses_top_k(self.limit)
                && grouped_sql.having.is_empty()
                && grouped_sql.order_by.len() == 1
                && let Some(first_order) = grouped_sql.order_by.first()
            {
                let matching_metric = grouped_sql
                    .metrics
                    .iter()
                    .find(|m| m.output_name == first_order.output_name);
                if let Some(metric) = matching_metric {
                    let needed = self.offset.unwrap_or(0) + self.limit.unwrap_or(0);
                    Some(crate::search::ShardTopK {
                        limit: needed * 3 + 10,
                        sort_by: first_order.output_name.clone(),
                        sort_function: metric.function.to_search_function(),
                        descending: first_order.desc,
                    })
                } else {
                    None
                }
            } else {
                None
            };

            (
                0,
                HashMap::from([(
                    INTERNAL_SQL_GROUPED_AGG.to_string(),
                    crate::search::AggregationRequest::GroupedMetrics(
                        crate::search::GroupedMetricsAggParams {
                            group_by: grouped_sql
                                .group_columns
                                .iter()
                                .map(|column| column.source_name.clone())
                                .collect(),
                            metrics,
                            shard_top_k,
                        },
                    ),
                )]),
            )
        } else if self.limit_pushed_down {
            let size = self.limit.unwrap_or(SQL_MATCH_LIMIT) + self.offset.unwrap_or(0);
            (size, HashMap::new())
        } else {
            (SQL_MATCH_LIMIT, HashMap::new())
        };

        crate::search::SearchRequest {
            query,
            size,
            from: 0,
            knn: None,
            sort: self.build_sort_clauses(),
            aggs,
        }
    }

    fn build_sort_clauses(&self) -> Vec<crate::search::SortClause> {
        let Some((ref field, desc)) = self.sort_pushdown else {
            return vec![];
        };
        let direction = if desc {
            crate::search::SortDirection::Desc
        } else {
            crate::search::SortDirection::Asc
        };
        vec![crate::search::SortClause::Field(HashMap::from([(
            field.clone(),
            crate::search::SortOrder::Direction(direction),
        )]))]
    }

    /// Build a structured explanation of the query plan for the EXPLAIN API.
    pub fn to_explain_json(&self) -> serde_json::Value {
        let search_request = self.to_search_request(false);

        // Determine which execution strategy would be chosen
        let execution_strategy = if self.grouped_sql.is_some() {
            "tantivy_grouped_partials"
        } else if self.selects_all_columns {
            "materialized_hits_fallback"
        } else {
            "tantivy_fast_fields"
        };

        let strategy_reason = if self.grouped_sql.is_some() {
            "Eligible GROUP BY query can execute as shard-local grouped partial aggregation"
        } else if self.selects_all_columns {
            "SELECT * requires _source materialization; fast-field path unavailable"
        } else if self.has_group_by_fallback {
            "GROUP BY fallback via fast fields. May use bitset streaming (no scan limit) at runtime if score-free and fully fast-field-backed; otherwise falls back to TopDocs with scan limit."
        } else {
            "All projected columns can be read from Tantivy fast fields"
        };

        // Build pipeline stages
        let mut stages = Vec::new();

        if let Some(semijoin) = &self.semijoin {
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "semijoin_key_build",
                "description": "Execute the inner same-index subquery, materialize the qualifying key set at the coordinator, and lower it into the outer search filter",
                "outer_key": semijoin.outer_key,
                "inner_key": semijoin.inner_key,
                "key_limit": SQL_SEMIJOIN_MAX_KEYS,
                "inner_plan": semijoin.inner_plan.to_explain_json(),
            }));
        }

        // Stage 1: Tantivy search
        let mut tantivy_stage = serde_json::json!({
            "stage": 0,
            "name": "tantivy_search",
            "description": "Execute search query in Tantivy to collect matching doc IDs and scores",
            "search_query": search_request.query,
            "max_hits": search_request.size,
            "limit_pushed_down": self.limit_pushed_down,
        });
        let text_matches_json = self.text_matches_json();
        if let Some(tm) = self.primary_text_match() {
            tantivy_stage["text_match"] = serde_json::json!({
                "field": tm.field,
                "query": tm.query,
            });
        }
        if !text_matches_json.is_empty() {
            tantivy_stage["text_matches"] = serde_json::json!(text_matches_json);
        }
        if !self.pushed_filters.is_empty() {
            tantivy_stage["pushed_filters"] = serde_json::json!(self.pushed_filters);
            tantivy_stage["pushed_filter_count"] = serde_json::json!(self.pushed_filters.len());
        }
        stages.push(tantivy_stage);

        // Stage 2: Column extraction
        if let Some(grouped_sql) = &self.grouped_sql {
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "grouped_partial_collect",
                "description": "Compute grouped partial metrics on each shard using Tantivy fast fields",
                "group_by": grouped_sql
                    .group_columns
                    .iter()
                    .map(|column| column.source_name.clone())
                    .collect::<Vec<_>>(),
                "metrics": grouped_sql
                    .metrics
                    .iter()
                    .map(|metric| serde_json::json!({
                        "output": metric.output_name,
                        "function": format!("{:?}", metric.function).to_lowercase(),
                        "field": metric.field,
                        "hidden": !metric.projected,
                    }))
                    .collect::<Vec<_>>(),
            }));
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "grouped_partial_merge",
                "description": "Merge compact grouped partial states at the coordinator",
            }));
            let mut final_stage = serde_json::json!({
                "stage": 0,
                "name": "final_grouped_sql_shape",
                "description": "Apply final projection and ordering to merged grouped results",
            });
            if let Some(limit) = self.limit {
                final_stage["limit"] = serde_json::json!(limit);
            }
            if let Some(offset) = self.offset {
                final_stage["offset"] = serde_json::json!(offset);
            }
            if !grouped_sql.having.is_empty() {
                final_stage["having"] = serde_json::json!(
                    grouped_sql
                        .having
                        .iter()
                        .map(|h| format!("{} {:?} {}", h.output_name, h.op, h.value))
                        .collect::<Vec<_>>()
                );
            }
            if grouped_sql.uses_top_k(self.limit) {
                final_stage["top_k_selection"] = serde_json::json!(true);
            }
            stages.push(final_stage);
        } else if !self.selects_all_columns {
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "fast_field_read",
                "description": "Read required columns directly from Tantivy fast fields (columnar storage)",
                "columns": self.required_columns,
            }));
        } else {
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "source_materialization",
                "description": "Materialize full _source documents for SELECT * projection",
            }));
        }

        if self.grouped_sql.is_none() {
            // Stage 3: Arrow batch building
            stages.push(serde_json::json!({
                "stage": 0,
                "name": "arrow_batch",
                "description": "Build Arrow RecordBatch from extracted columns with score column",
            }));

            // Stage 4: DataFusion SQL execution
            let mut datafusion_stage = serde_json::json!({
                "stage": 0,
                "name": "datafusion_sql",
                "description": "Execute rewritten SQL over Arrow batches for projection, sorting, and aggregation",
                "rewritten_sql": self.rewritten_sql,
            });
            if self.has_residual_predicates {
                datafusion_stage["residual_predicates"] = serde_json::json!(true);
                datafusion_stage["residual_note"] = serde_json::json!(
                    "Some predicates could not be pushed into Tantivy and will be evaluated by DataFusion"
                );
            }
            if !self.group_by_columns.is_empty() {
                datafusion_stage["group_by"] = serde_json::json!(self.group_by_columns);
            }
            stages.push(datafusion_stage);
        }

        for (index, stage) in stages.iter_mut().enumerate() {
            stage["stage"] = serde_json::json!(index + 1);
        }

        let mut explain = serde_json::json!({
            "original_sql": self.original_sql,
            "index": self.index_name,
            "execution_strategy": execution_strategy,
            "strategy_reason": strategy_reason,
            "pushdown_summary": {
                "text_match": self.primary_text_match().map(|tm| serde_json::json!({
                    "field": tm.field,
                    "query": tm.query,
                })),
                "text_matches": text_matches_json,
                "pushed_filters": self.pushed_filters,
                "pushed_filter_count": self.pushed_filters.len(),
                "has_residual_predicates": self.has_residual_predicates,
            },
            "columns": {
                "required": self.required_columns,
                "group_by": self.group_by_columns,
                "selects_all": self.selects_all_columns,
                "uses_grouped_partials": self.grouped_sql.is_some(),
                "limit": self.limit,
                "offset": self.offset,
                "limit_pushed_down": self.limit_pushed_down,
            },
            "rewritten_sql": self.rewritten_sql,
            "pipeline": stages,
        });

        if let Some(semijoin) = &self.semijoin {
            explain["semijoin"] = serde_json::json!({
                "outer_key": semijoin.outer_key,
                "inner_key": semijoin.inner_key,
                "key_limit": SQL_SEMIJOIN_MAX_KEYS,
                "inner_plan": semijoin.inner_plan.to_explain_json(),
            });
        }

        explain
    }
}

pub fn plan_sql(index_name: &str, sql: &str) -> Result<QueryPlan> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .with_context(|| format!("Failed to parse SQL query: {sql}"))?;

    if statements.len() != 1 {
        bail!("Exactly one SQL statement is supported");
    }

    let mut statement = statements.remove(0);
    let query = match &mut statement {
        Statement::Query(query) => query,
        _ => bail!("Only SELECT statements are supported"),
    };

    // ── Early shape validation ─────────────────────────────────────────
    // Reject unsupported query shapes with clear errors before any
    // planning work begins. This prevents opaque DataFusion table-
    // resolution failures from leaking to the user.
    validate_query_shape(query)?;

    let order_by = query.order_by.clone();
    let (limit, offset) = extract_limit_offset(&query.limit_clause);

    let select = match query.body.as_mut() {
        SetExpr::Select(select) => select,
        _ => bail!("Only simple SELECT queries are supported"),
    };

    let (source_table, source_alias) = extract_single_table_info(select)?;
    if source_table != index_name {
        bail!(
            "SQL FROM [{}] must match request index [{}]",
            source_table,
            index_name
        );
    }

    // ── Binding ──────────────────────────────────────────────────────
    // Bind the SELECT before pushdown extraction so we can collect
    // aliases, then use the binder's alias set for pushdown guards,
    // and finally derive required_columns / GROUP BY from bound items.
    let bind_ctx = bind_select(select);
    let selects_all_columns = bind_ctx.selects_all_columns;

    let semijoin = extract_semijoin_predicate(
        &mut select.selection,
        index_name,
        source_alias.as_deref(),
        &bind_ctx.alias_set,
        &bind_ctx.identity_source_aliases,
    )?;

    let (text_matches, pushed_filters) = extract_pushdowns(
        &mut select.selection,
        &bind_ctx.alias_set,
        &bind_ctx.identity_source_aliases,
    )?;
    let has_residual_predicates = select.selection.is_some();
    rewrite_table_name(select);

    // Derive columns and GROUP BY from the binder context
    let (required_columns, needs_id, needs_score) =
        bind_ctx.derive_columns(select, order_by.as_ref());
    let (group_by_columns, has_expression_group_by) = bind_ctx.derive_group_by();
    let grouped_sql = extract_grouped_sql_plan(
        select,
        order_by.as_ref(),
        has_residual_predicates,
        selects_all_columns,
        has_expression_group_by,
        limit,
        offset,
        &group_by_columns,
    )?;

    let sort_pushdown = extract_sort_pushdown(order_by.as_ref());

    let limit_pushed_down = limit.is_some()
        && !has_residual_predicates
        && grouped_sql.is_none()
        && group_by_columns.is_empty()
        && (is_order_by_score_only(order_by.as_ref()) || sort_pushdown.is_some());

    let has_group_by_fallback =
        (!group_by_columns.is_empty() || has_expression_group_by) && grouped_sql.is_none();

    Ok(QueryPlan {
        index_name: index_name.to_string(),
        original_sql: sql.to_string(),
        rewritten_sql: statement.to_string(),
        semijoin,
        text_matches,
        pushed_filters,
        required_columns,
        group_by_columns,
        has_residual_predicates,
        selects_all_columns,
        grouped_sql,
        limit,
        offset,
        limit_pushed_down,
        needs_id,
        needs_score,
        sort_pushdown,
        has_group_by_fallback,
    })
}

#[allow(clippy::too_many_arguments)]
fn extract_grouped_sql_plan(
    select: &Select,
    order_by: Option<&sqlparser::ast::OrderBy>,
    has_residual_predicates: bool,
    selects_all_columns: bool,
    has_expression_group_by: bool,
    _limit: Option<usize>,
    _offset: Option<usize>,
    group_by_columns: &[String],
) -> Result<Option<GroupedSqlPlan>> {
    // Expression-based GROUP BY (e.g., LOWER(author), year / 10) cannot use
    // the grouped partials path — it would group by the raw column instead of
    // the expression result. Fall through to DataFusion.
    if has_residual_predicates || selects_all_columns || has_expression_group_by {
        return Ok(None);
    }

    let mut group_columns = Vec::new();
    let mut metrics = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(source_name) = expr_to_field_name(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name: source_name.clone(),
                        output_name: source_name,
                    });
                    continue;
                }

                if let Some(metric) = parse_grouped_metric(expr, None)? {
                    ensure_grouped_metric(&mut metrics, metric, true);
                    continue;
                }
                return Ok(None);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(source_name) = expr_to_field_name(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name,
                        output_name: alias.value.clone(),
                    });
                    continue;
                }

                if let Some(metric) = parse_grouped_metric(expr, Some(alias.value.clone()))? {
                    ensure_grouped_metric(&mut metrics, metric, true);
                    continue;
                }
                return Ok(None);
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
        }
    }

    if let Some(order_by) = order_by {
        register_hidden_grouped_metrics_from_order_by(order_by, &mut metrics)?;
    }
    if let Some(having_expr) = &select.having {
        register_hidden_grouped_metrics_from_having(having_expr, &mut metrics)?;
    }

    if metrics.is_empty() {
        return Ok(None);
    }

    let mut valid_names: HashMap<String, String> = HashMap::new();
    for column in &group_columns {
        valid_names.insert(column.source_name.clone(), column.output_name.clone());
        valid_names.insert(column.output_name.clone(), column.output_name.clone());
    }
    for metric in &metrics {
        valid_names.insert(metric.output_name.clone(), metric.output_name.clone());
    }

    let mut order_items = Vec::new();
    if let Some(order_by) = order_by {
        match &order_by.kind {
            OrderByKind::Expressions(exprs) => {
                for expr in exprs {
                    let Some(output_name) =
                        resolve_grouped_output_name(&expr.expr, &valid_names, &metrics)
                    else {
                        return Ok(None);
                    };
                    order_items.push(SqlOrderBy {
                        output_name,
                        desc: expr.options.asc == Some(false),
                    });
                }
            }
            OrderByKind::All(_) => return Ok(None),
        }
    }

    // Parse HAVING clause into post-merge filters
    let mut having_filters = Vec::new();
    if let Some(having_expr) = &select.having
        && !parse_having_filters(having_expr, &valid_names, &metrics, &mut having_filters)
    {
        return Ok(None);
    }

    // Reject duplicate output aliases — HAVING/ORDER BY resolution would be
    // ambiguous (same behavior as PostgreSQL "column reference is ambiguous").
    {
        let mut seen = std::collections::HashSet::new();
        for col in &group_columns {
            if !seen.insert(&col.output_name) {
                return Err(anyhow::anyhow!(
                    "ambiguous column reference '{}' in GROUP BY output",
                    col.output_name
                ));
            }
        }
        for metric in &metrics {
            if !seen.insert(&metric.output_name) {
                return Err(anyhow::anyhow!(
                    "ambiguous column reference '{}' in GROUP BY output",
                    metric.output_name
                ));
            }
        }
    }

    Ok(Some(GroupedSqlPlan {
        group_columns,
        metrics,
        order_by: order_items,
        having: having_filters,
    }))
}

/// Parse a HAVING expression into simple comparison filters.
/// Returns false if the HAVING clause cannot be fully handled (bail to fallback).
fn parse_having_filters(
    expr: &Expr,
    valid_names: &HashMap<String, String>,
    metrics: &[SqlGroupedMetric],
    filters: &mut Vec<HavingFilter>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            parse_having_filters(left, valid_names, metrics, filters)
                && parse_having_filters(right, valid_names, metrics, filters)
        }
        Expr::BinaryOp { left, op, right } => {
            let having_op = match op {
                BinaryOperator::Gt => HavingOp::Gt,
                BinaryOperator::GtEq => HavingOp::GtEq,
                BinaryOperator::Lt => HavingOp::Lt,
                BinaryOperator::LtEq => HavingOp::LtEq,
                BinaryOperator::Eq => HavingOp::Eq,
                _ => return false,
            };
            // Try field/aggregate op value
            if let (Some(name), Some(val)) = (
                resolve_grouped_output_name(left, valid_names, metrics),
                expr_to_f64(right),
            ) {
                filters.push(HavingFilter {
                    output_name: name,
                    op: having_op,
                    value: val,
                });
                return true;
            }
            // Try value op field/aggregate (flipped)
            if let (Some(val), Some(name)) = (
                expr_to_f64(left),
                resolve_grouped_output_name(right, valid_names, metrics),
            ) {
                let flipped_op = match having_op {
                    HavingOp::Gt => HavingOp::Lt,
                    HavingOp::GtEq => HavingOp::LtEq,
                    HavingOp::Lt => HavingOp::Gt,
                    HavingOp::LtEq => HavingOp::GtEq,
                    HavingOp::Eq => HavingOp::Eq,
                };
                filters.push(HavingFilter {
                    output_name: name,
                    op: flipped_op,
                    value: val,
                });
                return true;
            }
            false
        }
        Expr::Nested(inner) => parse_having_filters(inner, valid_names, metrics, filters),
        _ => false,
    }
}

/// Resolve a grouped-query reference to its output name.
/// Tries identifier lookup first, then aggregate function matching.
fn resolve_grouped_output_name(
    expr: &Expr,
    valid_names: &HashMap<String, String>,
    metrics: &[SqlGroupedMetric],
) -> Option<String> {
    // 1. Try simple identifier: HAVING posts > 10
    if let Some(name) = expr_to_field_name(expr)
        && let Some(output_name) = valid_names.get(&name)
    {
        return Some(output_name.clone());
    }
    // 2. Try aggregate function: HAVING COUNT(*) > 10
    if let Ok(Some(parsed)) = parse_grouped_metric(expr, None) {
        for metric in metrics {
            if metric.function == parsed.function && metric.field == parsed.field {
                return Some(metric.output_name.clone());
            }
        }
    }
    None
}

fn expr_to_f64(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(n, _) => n.parse::<f64>().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => expr_to_f64(inner).map(|v| -v),
        _ => None,
    }
}

fn parse_grouped_metric(expr: &Expr, alias: Option<String>) -> Result<Option<SqlGroupedMetric>> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };

    let args = match &function.args {
        FunctionArguments::List(list) => &list.args,
        _ => return Ok(None),
    };
    let function_name = function.name.to_string();
    let normalized = function_name.to_ascii_lowercase();

    let (function, field, default_name) = match normalized.as_str() {
        "count" => {
            if args.len() != 1 {
                return Ok(None);
            }
            match &args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                    (SqlGroupedMetricFunction::Count, None, "count".to_string())
                }
                FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {
                    return Ok(None);
                }
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                    let Some(field) = expr_to_field_name(expr) else {
                        return Ok(None);
                    };
                    (
                        SqlGroupedMetricFunction::Count,
                        Some(field.clone()),
                        format!("count_{field}"),
                    )
                }
                _ => return Ok(None),
            }
        }
        "sum" | "avg" | "min" | "max" => {
            if args.len() != 1 {
                return Ok(None);
            }
            let field = match &args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr_to_field_name(expr),
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    if let FunctionArgExpr::Expr(expr) = arg {
                        expr_to_field_name(expr)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            let Some(field) = field else {
                return Ok(None);
            };
            let function = match normalized.as_str() {
                "sum" => SqlGroupedMetricFunction::Sum,
                "avg" => SqlGroupedMetricFunction::Avg,
                "min" => SqlGroupedMetricFunction::Min,
                "max" => SqlGroupedMetricFunction::Max,
                _ => unreachable!(),
            };
            (
                function,
                Some(field.clone()),
                format!("{}_{}", normalized, field),
            )
        }
        _ => return Ok(None),
    };

    Ok(Some(SqlGroupedMetric {
        output_name: alias.unwrap_or(default_name),
        function,
        field,
        projected: true,
    }))
}

fn ensure_grouped_metric(
    metrics: &mut Vec<SqlGroupedMetric>,
    parsed: SqlGroupedMetric,
    projected: bool,
) -> String {
    if let Some(existing) = metrics
        .iter_mut()
        .find(|metric| metric.function == parsed.function && metric.field == parsed.field)
    {
        if projected {
            existing.projected = true;
            existing.output_name = parsed.output_name.clone();
        }
        return existing.output_name.clone();
    }

    let output_name = if projected {
        parsed.output_name.clone()
    } else {
        format!("__hidden_grouped_metric_{}", metrics.len())
    };
    metrics.push(SqlGroupedMetric {
        output_name: output_name.clone(),
        function: parsed.function,
        field: parsed.field,
        projected,
    });
    output_name
}

fn register_hidden_grouped_metrics_from_order_by(
    order_by: &sqlparser::ast::OrderBy,
    metrics: &mut Vec<SqlGroupedMetric>,
) -> Result<()> {
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return Ok(());
    };

    for expr in exprs {
        register_hidden_grouped_metric(&expr.expr, metrics)?;
    }
    Ok(())
}

fn register_hidden_grouped_metrics_from_having(
    expr: &Expr,
    metrics: &mut Vec<SqlGroupedMetric>,
) -> Result<()> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            register_hidden_grouped_metrics_from_having(left, metrics)?;
            register_hidden_grouped_metrics_from_having(right, metrics)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            register_hidden_grouped_metric(left, metrics)?;
            register_hidden_grouped_metric(right, metrics)?;
        }
        Expr::Nested(inner) => register_hidden_grouped_metrics_from_having(inner, metrics)?,
        _ => {}
    }
    Ok(())
}

fn register_hidden_grouped_metric(expr: &Expr, metrics: &mut Vec<SqlGroupedMetric>) -> Result<()> {
    if let Some(metric) = parse_grouped_metric(expr, None)? {
        ensure_grouped_metric(metrics, metric, false);
    }
    Ok(())
}

fn extract_limit_offset(
    clause: &Option<sqlparser::ast::LimitClause>,
) -> (Option<usize>, Option<usize>) {
    let Some(clause) = clause else {
        return (None, None);
    };
    match clause {
        sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } => {
            let l = limit.as_ref().and_then(parse_limit_expr);
            let o = offset
                .as_ref()
                .and_then(|off| parse_offset_value(&off.value));
            (l, o)
        }
        sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
            let l = parse_limit_expr(limit);
            let o = parse_offset_value(offset);
            (l, o)
        }
    }
}

fn parse_limit_expr(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn parse_offset_value(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn is_order_by_score_only(order_by: Option<&sqlparser::ast::OrderBy>) -> bool {
    let Some(order_by) = order_by else {
        return true;
    };
    match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs.iter().all(|expr| {
            expr_to_field_name(&expr.expr)
                .map(|name| SyntheticColumn::Score.matches_name(&name))
                .unwrap_or(false)
        }),
        OrderByKind::All(_) => false,
    }
}

/// Extract a single-column ORDER BY on a non-`_score` field for fast-field sort pushdown.
/// Returns `Some((field_name, is_desc))` when the ORDER BY is a single data column
/// (not synthetic `_score`), enabling Tantivy's `TopDocs::order_by_fast_field()`.
fn extract_sort_pushdown(order_by: Option<&sqlparser::ast::OrderBy>) -> Option<(String, bool)> {
    let order_by = order_by?;
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return None;
    };
    if exprs.len() != 1 {
        return None;
    }
    let expr = &exprs[0];
    let name = expr_to_field_name(&expr.expr)?;
    if SyntheticColumn::Score.matches_name(&name) {
        return None;
    }
    let desc = expr.options.asc == Some(false);
    Some((name, desc))
}

// ── Query-shape validation ─────────────────────────────────────────────
// These functions run early in `plan_sql()` to reject unsupported SQL
// shapes with clear, actionable error messages instead of letting them
// leak through to DataFusion as opaque table-resolution failures.

/// Top-level shape validation. Called immediately after parsing the Query
/// AST and before any planning work begins.
fn validate_query_shape(query: &sqlparser::ast::Query) -> Result<()> {
    // CTEs (WITH ... AS ...)
    if query.with.is_some() {
        bail!("CTEs (WITH ... AS ...) are not supported. Rewrite as separate queries.");
    }

    // UNION / EXCEPT / INTERSECT
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            // Check FROM clause for subqueries and JOINs
            validate_from_clause(select)?;
            // Check WHERE clause for subqueries
            if let Some(ref selection) = select.selection {
                validate_where_clause(selection, true)?;
            }
            // Check HAVING for subqueries
            if let Some(ref having) = select.having
                && expr_contains_subquery(having)
            {
                bail!(
                    "Subqueries in HAVING are not supported. \
                     Rewrite as separate queries."
                );
            }
            // Check SELECT projection for subqueries
            validate_projection(select)?;
        }
        SetExpr::SetOperation { op, .. } => {
            bail!("{op} is not supported. Only simple SELECT queries are supported.");
        }
        _ => {
            bail!("Only simple SELECT queries are supported");
        }
    }

    // Check ORDER BY for subqueries
    if let Some(ref order_by) = query.order_by
        && let OrderByKind::Expressions(exprs) = &order_by.kind
    {
        for expr in exprs {
            if expr_contains_subquery(&expr.expr) {
                bail!(
                    "Subqueries in ORDER BY are not supported. \
                         Rewrite as separate queries."
                );
            }
        }
    }

    Ok(())
}

/// Validate FROM clause: reject subqueries and JOINs.
fn validate_from_clause(select: &Select) -> Result<()> {
    for table_with_joins in &select.from {
        match &table_with_joins.relation {
            TableFactor::Derived { .. } => {
                bail!(
                    "Subqueries in FROM (derived tables) are not supported. \
                     Rewrite as separate queries."
                );
            }
            TableFactor::NestedJoin { .. } => {
                bail!("JOINs are not supported. Rewrite as separate queries.");
            }
            _ => {}
        }
        if !table_with_joins.joins.is_empty() {
            bail!("JOINs are not supported. Rewrite as separate queries.");
        }
    }
    if select.from.len() > 1 {
        bail!(
            "Multi-table FROM (implicit join) is not supported. \
             Use a single table per query."
        );
    }
    Ok(())
}

/// Validate SELECT projection: reject subqueries in any projection expression.
fn validate_projection(select: &Select) -> Result<()> {
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => continue,
        };
        if expr_contains_subquery(expr) {
            bail!(
                "Subqueries in SELECT expressions are not supported. \
                 Rewrite as separate queries."
            );
        }
    }
    Ok(())
}

/// Validate WHERE clause: reject subquery expressions.
fn validate_where_clause(expr: &Expr, semijoin_leaf_allowed: bool) -> Result<()> {
    match expr {
        Expr::InSubquery {
            expr: inner_expr,
            subquery,
            negated,
        } => {
            if *negated {
                bail!(
                    "NOT IN subqueries are not supported. \
                     Rewrite as separate queries."
                );
            }
            if !semijoin_leaf_allowed {
                bail!(
                    "Subqueries (IN (SELECT ...)) are only supported as top-level AND predicates. \
                     Rewrite the expression or run the subquery separately."
                );
            }
            validate_where_clause(inner_expr, false)?;
            validate_query_shape(subquery)?;
        }
        Expr::Exists { .. } => {
            bail!(
                "EXISTS subqueries are not supported. \
                 Rewrite as separate queries."
            );
        }
        Expr::Subquery(_) => {
            bail!(
                "Scalar subqueries are not supported. \
                 Rewrite as separate queries."
            );
        }
        // Recurse into compound expressions
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            validate_where_clause(left, semijoin_leaf_allowed)?;
            validate_where_clause(right, semijoin_leaf_allowed)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_where_clause(left, false)?;
            validate_where_clause(right, false)?;
        }
        Expr::UnaryOp { expr: inner, .. } => {
            validate_where_clause(inner, false)?;
        }
        Expr::Nested(inner) => {
            validate_where_clause(inner, semijoin_leaf_allowed)?;
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            validate_where_clause(e, false)?;
            validate_where_clause(low, false)?;
            validate_where_clause(high, false)?;
        }
        Expr::InList { expr: e, list, .. } => {
            validate_where_clause(e, false)?;
            for item in list {
                validate_where_clause(item, false)?;
            }
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            validate_where_clause(e, false)?;
            validate_where_clause(pattern, false)?;
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            validate_where_clause(inner, false)?;
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                validate_where_clause(op, false)?;
            }
            for cond in conditions {
                let sqlparser::ast::CaseWhen { condition, result } = cond;
                validate_where_clause(condition, false)?;
                validate_where_clause(result, false)?;
            }
            if let Some(else_res) = else_result {
                validate_where_clause(else_res, false)?;
            }
        }
        Expr::Function(func) => {
            if let FunctionArguments::Subquery(_) = &func.args {
                bail!(
                    "Subqueries as function arguments are not supported. \
                     Rewrite as separate queries."
                );
            }
            if let FunctionArguments::List(list) = &func.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            validate_where_clause(e, false)?;
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Cast { expr: inner, .. } => {
            validate_where_clause(inner, false)?;
        }
        // Leaf expressions (literals, identifiers, etc.) — no subqueries possible
        _ => {}
    }
    Ok(())
}

/// Returns true if the expression contains any subquery node.
/// Used for HAVING clause checking where we want to detect but not produce
/// a specific error message for each variant.
fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. } => expr_contains_subquery(inner),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            expr_contains_subquery(e) || expr_contains_subquery(low) || expr_contains_subquery(high)
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => expr_contains_subquery(e) || expr_contains_subquery(pattern),
        Expr::InList { expr: e, list, .. } => {
            expr_contains_subquery(e) || list.iter().any(expr_contains_subquery)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(expr_contains_subquery)
                || conditions.iter().any(|case_when| {
                    expr_contains_subquery(&case_when.condition)
                        || expr_contains_subquery(&case_when.result)
                })
                || else_result.as_deref().is_some_and(expr_contains_subquery)
        }
        Expr::Function(func) => {
            matches!(&func.args, FunctionArguments::Subquery(_))
                || if let FunctionArguments::List(list) = &func.args {
                    list.args.iter().any(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => expr_contains_subquery(e),
                        _ => false,
                    })
                } else {
                    false
                }
        }
        _ => false,
    }
}

fn extract_single_table_info(select: &Select) -> Result<(String, Option<String>)> {
    if select.from.len() != 1 {
        bail!("Exactly one FROM table is supported");
    }

    let table = &select.from[0];
    if !table.joins.is_empty() {
        bail!("JOIN is not supported in the SQL search MVP");
    }

    match &table.relation {
        TableFactor::Table { name, alias, .. } => {
            if name.0.len() != 1 {
                bail!("Qualified table names are not supported in the SQL search MVP");
            }
            match &name.0[0] {
                ObjectNamePart::Identifier(ident) => Ok((
                    ident.value.clone(),
                    alias.as_ref().map(|alias| alias.name.value.clone()),
                )),
                _ => bail!("Unsupported table name syntax"),
            }
        }
        _ => bail!("Unsupported FROM clause"),
    }
}

fn rewrite_table_name(select: &mut Select) {
    if let Some(table) = select.from.first_mut()
        && let TableFactor::Table { name, .. } = &mut table.relation
    {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            INTERNAL_TABLE_NAME,
        ))]);
    }
}

fn extract_pushdowns(
    selection: &mut Option<Expr>,
    select_aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> Result<(Vec<TextMatchPredicate>, Vec<crate::search::QueryClause>)> {
    let Some(expr) = selection.take() else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut predicates = Vec::new();
    split_conjunction(expr, &mut predicates);

    let mut text_matches = Vec::new();
    let mut pushed_filters = Vec::new();
    let mut residual = Vec::new();
    for predicate in predicates {
        // Never push down predicates that reference SELECT aliases (e.g.,
        // `WHERE posts > 10` when `posts` is an alias for `count(*)`).
        // These are aggregate-derived values, not physical fields.
        if predicate_references_alias(&predicate, select_aliases, identity_source_aliases) {
            residual.push(predicate);
            continue;
        }
        match parse_text_match(&predicate)? {
            Some(found) => {
                text_matches.push(found);
            }
            None => {
                if let Some(filter) = parse_pushdown_predicate(&predicate)? {
                    pushed_filters.push(filter);
                } else {
                    residual.push(predicate);
                }
            }
        }
    }

    *selection = combine_conjunctions(residual);

    // Safety check: if text_match() survived in residual predicates (e.g.,
    // `text_match(title, 'rust') OR author = 'pg'`), DataFusion will crash
    // because text_match is not a registered UDF. Bail with a clear error.
    if let Some(sel) = selection.as_ref()
        && expr_contains_text_match(sel)
    {
        bail!(
            "text_match() cannot be used inside OR or complex expressions; \
             it must appear as a top-level AND predicate"
        );
    }

    Ok((text_matches, pushed_filters))
}

fn extract_semijoin_predicate(
    selection: &mut Option<Expr>,
    index_name: &str,
    outer_source_alias: Option<&str>,
    select_aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> Result<Option<SemijoinPlan>> {
    let Some(expr) = selection.take() else {
        return Ok(None);
    };

    let mut predicates = Vec::new();
    split_conjunction(expr, &mut predicates);

    let mut semijoins = Vec::new();
    let mut residual = Vec::new();

    for predicate in predicates {
        match parse_semijoin_predicate(
            &predicate,
            index_name,
            outer_source_alias,
            select_aliases,
            identity_source_aliases,
        )? {
            Some(semijoin) => semijoins.push(semijoin),
            None => residual.push(predicate),
        }
    }

    *selection = combine_conjunctions(residual);

    match semijoins.len() {
        0 => Ok(None),
        1 => Ok(semijoins.into_iter().next()),
        _ => bail!("Only one IN (SELECT ...) semijoin predicate is supported per query in the MVP"),
    }
}

fn parse_semijoin_predicate(
    predicate: &Expr,
    index_name: &str,
    outer_source_alias: Option<&str>,
    select_aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> Result<Option<SemijoinPlan>> {
    let Expr::InSubquery {
        expr,
        subquery,
        negated: false,
    } = predicate
    else {
        return Ok(None);
    };

    let outer_key = resolve_semijoin_outer_key(expr, select_aliases, identity_source_aliases)?;
    let inner_key = extract_semijoin_inner_key(subquery)?;

    if let Some(outer_alias) = outer_source_alias
        && query_references_outer_alias(subquery, outer_alias)
    {
        bail!(
            "Correlated semijoin subqueries are not supported in the MVP. \
             Remove the outer alias reference [{}] from the inner query.",
            outer_alias
        );
    }

    let inner_sql = subquery.to_string();
    let inner_plan = plan_sql(index_name, &inner_sql)?;
    if inner_plan.semijoin.is_some() {
        bail!("Nested semijoin subqueries are not supported in the MVP");
    }

    Ok(Some(SemijoinPlan {
        outer_key,
        inner_key,
        inner_plan: Box::new(inner_plan),
    }))
}

fn resolve_semijoin_outer_key(
    expr: &Expr,
    select_aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> Result<String> {
    let Some(name) = expr_to_field_name(expr) else {
        bail!(
            "Semijoin outer key must be a plain source column. \
             Expressions like LOWER(author) are not supported in the MVP."
        );
    };
    if alias_blocks_source_name(&name, select_aliases, identity_source_aliases) {
        bail!(
            "Semijoin outer key [{}] cannot reference a SELECT alias. \
             Use the underlying source column instead.",
            name
        );
    }
    if SyntheticColumn::parse(&name).is_some() {
        bail!(
            "Semijoin outer key [{}] cannot use synthetic columns like _id or _score",
            name
        );
    }
    Ok(name)
}

fn extract_semijoin_inner_key(subquery: &Query) -> Result<String> {
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        bail!("Semijoin subqueries must be simple SELECT queries");
    };

    if select.projection.len() != 1 {
        bail!("Semijoin subqueries must project exactly one key column in the MVP");
    }

    match &select.projection[0] {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            let Some(name) = expr_to_field_name(expr) else {
                bail!(
                    "Semijoin subquery keys must be plain source columns. \
                     Computed expressions are not supported in the MVP."
                );
            };
            if SyntheticColumn::parse(&name).is_some() {
                bail!("Semijoin subquery keys cannot use synthetic columns like _id or _score");
            }
            Ok(name)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            bail!("Semijoin subqueries cannot use SELECT * in the MVP")
        }
    }
}

fn query_references_outer_alias(query: &Query, outer_alias: &str) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };

    if select
        .from
        .iter()
        .filter_map(|table_with_joins| match &table_with_joins.relation {
            TableFactor::Table { alias, .. } => {
                alias.as_ref().map(|alias| alias.name.value.as_str())
            }
            _ => None,
        })
        .any(|alias| alias.eq_ignore_ascii_case(outer_alias))
    {
        return false;
    }

    select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_qualified_prefix(expr, outer_alias)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }) || select
        .selection
        .as_ref()
        .is_some_and(|expr| expr_references_qualified_prefix(expr, outer_alias))
        || select
            .having
            .as_ref()
            .is_some_and(|expr| expr_references_qualified_prefix(expr, outer_alias))
        || match &select.group_by {
            sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs
                .iter()
                .any(|expr| expr_references_qualified_prefix(expr, outer_alias)),
            _ => false,
        }
        || query
            .order_by
            .as_ref()
            .is_some_and(|order_by| match &order_by.kind {
                OrderByKind::Expressions(exprs) => exprs
                    .iter()
                    .any(|expr| expr_references_qualified_prefix(&expr.expr, outer_alias)),
                OrderByKind::All(_) => false,
            })
}

fn expr_references_qualified_prefix(expr: &Expr, prefix: &str) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) => parts
            .first()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case(prefix)),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_qualified_prefix(left, prefix)
                || expr_references_qualified_prefix(right, prefix)
        }
        Expr::UnaryOp { expr: inner, .. }
        | Expr::Nested(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. } => expr_references_qualified_prefix(inner, prefix),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            expr_references_qualified_prefix(e, prefix)
                || expr_references_qualified_prefix(low, prefix)
                || expr_references_qualified_prefix(high, prefix)
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            expr_references_qualified_prefix(e, prefix)
                || expr_references_qualified_prefix(pattern, prefix)
        }
        Expr::InList { expr: e, list, .. } => {
            expr_references_qualified_prefix(e, prefix)
                || list
                    .iter()
                    .any(|item| expr_references_qualified_prefix(item, prefix))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|expr| expr_references_qualified_prefix(expr, prefix))
                || conditions.iter().any(|case_when| {
                    expr_references_qualified_prefix(&case_when.condition, prefix)
                        || expr_references_qualified_prefix(&case_when.result, prefix)
                })
                || else_result
                    .as_deref()
                    .is_some_and(|expr| expr_references_qualified_prefix(expr, prefix))
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(list) = &func.args {
                list.args.iter().any(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => expr_references_qualified_prefix(expr, prefix),
                    _ => false,
                })
            } else {
                false
            }
        }
        _ => false,
    }
}

fn text_match_to_query_clause(predicate: &TextMatchPredicate) -> crate::search::QueryClause {
    crate::search::QueryClause::Match(HashMap::from([(
        predicate.field.clone(),
        serde_json::Value::String(predicate.query.clone()),
    )]))
}

/// Returns true if the expression tree contains a `text_match(...)` function call.
/// Walks all expression shapes that could survive as residual predicates.
fn expr_contains_text_match(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            if f.name.to_string().eq_ignore_ascii_case("text_match") {
                return true;
            }
            // Recurse into function arguments (e.g., COALESCE(text_match(...), false))
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            if expr_contains_text_match(e) {
                                return true;
                            }
                        }
                        FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            if expr_contains_text_match(e) {
                                return true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            false
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_text_match(left) || expr_contains_text_match(right)
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Cast { expr: inner, .. } => expr_contains_text_match(inner),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand
                && expr_contains_text_match(op)
            {
                return true;
            }
            for case_when in conditions {
                if expr_contains_text_match(&case_when.condition)
                    || expr_contains_text_match(&case_when.result)
                {
                    return true;
                }
            }
            if let Some(el) = else_result
                && expr_contains_text_match(el)
            {
                return true;
            }
            false
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            expr_contains_text_match(e)
                || expr_contains_text_match(low)
                || expr_contains_text_match(high)
        }
        Expr::InList { expr: e, list, .. } => {
            expr_contains_text_match(e) || list.iter().any(expr_contains_text_match)
        }
        _ => false,
    }
}

/// Returns true if the predicate references any SELECT alias.
/// For example, `posts > 10` when `posts` is an alias for `count(*)`.
fn predicate_references_alias(
    expr: &Expr,
    aliases: &BTreeSet<String>,
    identity_source_aliases: &BTreeSet<String>,
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            alias_blocks_source_name(&ident.value, aliases, identity_source_aliases)
        }
        Expr::CompoundIdentifier(idents) => idents.last().is_some_and(|ident| {
            alias_blocks_source_name(&ident.value, aliases, identity_source_aliases)
        }),
        Expr::BinaryOp { left, right, .. } => {
            predicate_references_alias(left, aliases, identity_source_aliases)
                || predicate_references_alias(right, aliases, identity_source_aliases)
        }
        Expr::Between {
            expr: between_expr,
            low,
            high,
            ..
        } => {
            predicate_references_alias(between_expr, aliases, identity_source_aliases)
                || predicate_references_alias(low, aliases, identity_source_aliases)
                || predicate_references_alias(high, aliases, identity_source_aliases)
        }
        Expr::InList {
            expr: in_expr,
            list,
            ..
        } => {
            predicate_references_alias(in_expr, aliases, identity_source_aliases)
                || list
                    .iter()
                    .any(|item| predicate_references_alias(item, aliases, identity_source_aliases))
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            predicate_references_alias(expr, aliases, identity_source_aliases)
        }
        Expr::Like {
            expr: e, pattern, ..
        }
        | Expr::ILike {
            expr: e, pattern, ..
        } => {
            predicate_references_alias(e, aliases, identity_source_aliases)
                || predicate_references_alias(pattern, aliases, identity_source_aliases)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_some_and(|expr| {
                predicate_references_alias(expr, aliases, identity_source_aliases)
            }) || conditions.iter().any(|case_when| {
                predicate_references_alias(&case_when.condition, aliases, identity_source_aliases)
                    || predicate_references_alias(
                        &case_when.result,
                        aliases,
                        identity_source_aliases,
                    )
            }) || else_result.as_deref().is_some_and(|expr| {
                predicate_references_alias(expr, aliases, identity_source_aliases)
            })
        }
        _ => false,
    }
}

fn split_conjunction(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_conjunction(*left, out);
            split_conjunction(*right, out);
        }
        other => out.push(other),
    }
}

fn combine_conjunctions(predicates: Vec<Expr>) -> Option<Expr> {
    let mut iter = predicates.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn parse_text_match(expr: &Expr) -> Result<Option<TextMatchPredicate>> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };

    if !function.name.to_string().eq_ignore_ascii_case("text_match") {
        return Ok(None);
    }

    let args = match &function.args {
        FunctionArguments::List(list) => &list.args,
        _ => bail!("text_match(field, query) must use a normal argument list"),
    };
    if args.len() != 2 {
        bail!("text_match(field, query) expects exactly two arguments");
    }

    let field = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => ident.value.clone(),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(idents))) => idents
            .last()
            .map(|ident| ident.value.clone())
            .context("Invalid text_match field argument")?,
        _ => bail!("text_match first argument must be a field identifier"),
    };

    let query = match &args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))
            if matches!(value.value, SqlValue::SingleQuotedString(_)) =>
        {
            match &value.value {
                SqlValue::SingleQuotedString(value) => value.clone(),
                _ => unreachable!(),
            }
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))
            if matches!(value.value, SqlValue::DoubleQuotedString(_)) =>
        {
            match &value.value {
                SqlValue::DoubleQuotedString(value) => value.clone(),
                _ => unreachable!(),
            }
        }
        _ => bail!("text_match second argument must be a string literal"),
    };

    Ok(Some(TextMatchPredicate { field, query }))
}

fn collect_or_branches<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            collect_or_branches(left, out);
            collect_or_branches(right, out);
        }
        Expr::Nested(inner) => collect_or_branches(inner, out),
        other => out.push(other),
    }
}

fn parse_pushdown_predicate(expr: &Expr) -> Result<Option<crate::search::QueryClause>> {
    // Unwrap parenthesized expressions
    if let Expr::Nested(inner) = expr {
        return parse_pushdown_predicate(inner);
    }

    // OR disjunction: push down if ALL branches are individually pushable.
    // `author = 'pg' OR author = 'sama'` → Bool { should: [Term(pg), Term(sama)] }
    if matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::Or,
            ..
        }
    ) {
        let mut branches = Vec::new();
        collect_or_branches(expr, &mut branches);
        let mut clauses = Vec::new();
        for branch in branches {
            if let Some(clause) = parse_pushdown_predicate(branch)? {
                clauses.push(clause);
            } else {
                return Ok(None);
            }
        }
        if clauses.is_empty() {
            return Ok(None);
        }
        return Ok(Some(crate::search::QueryClause::Bool(
            crate::search::BoolQuery {
                should: clauses,
                ..Default::default()
            },
        )));
    }

    // BETWEEN field AND low AND high → Range { gte: low, lte: high }
    if let Expr::Between {
        expr: between_expr,
        negated: false,
        low,
        high,
    } = expr
    {
        let Some(field) = expr_to_field_name(between_expr) else {
            return Ok(None);
        };
        if SyntheticColumn::parse(&field).is_some() {
            return Ok(None);
        }
        let (Some(low_val), Some(high_val)) = (expr_to_json_value(low)?, expr_to_json_value(high)?)
        else {
            return Ok(None);
        };
        return Ok(Some(crate::search::QueryClause::Range(HashMap::from([(
            field,
            crate::search::RangeCondition {
                gte: Some(low_val),
                lte: Some(high_val),
                ..Default::default()
            },
        )]))));
    }

    // IN (a, b, c) → Bool { should: [Term(a), Term(b), Term(c)] }
    if let Expr::InList {
        expr: in_expr,
        list,
        negated: false,
    } = expr
    {
        let Some(field) = expr_to_field_name(in_expr) else {
            return Ok(None);
        };
        if SyntheticColumn::parse(&field).is_some() {
            return Ok(None);
        }
        let mut terms = Vec::new();
        for item in list {
            let Some(value) = expr_to_json_value(item)? else {
                return Ok(None);
            };
            terms.push(crate::search::QueryClause::Term(HashMap::from([(
                field.clone(),
                value,
            )])));
        }
        if terms.is_empty() {
            return Ok(None);
        }
        return Ok(Some(crate::search::QueryClause::Bool(
            crate::search::BoolQuery {
                should: terms,
                ..Default::default()
            },
        )));
    }

    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };

    let Some((field, value, flipped)) = extract_comparison_parts(left, right)? else {
        return Ok(None);
    };

    if SyntheticColumn::parse(&field).is_some() {
        return Ok(None);
    }

    let normalized_op = if flipped {
        flip_binary_operator(op)
    } else {
        op.clone()
    };

    let clause = match normalized_op {
        BinaryOperator::Eq => crate::search::QueryClause::Term(HashMap::from([(field, value)])),
        BinaryOperator::Gt => crate::search::QueryClause::Range(HashMap::from([(
            field,
            crate::search::RangeCondition {
                gt: Some(value),
                ..Default::default()
            },
        )])),
        BinaryOperator::GtEq => crate::search::QueryClause::Range(HashMap::from([(
            field,
            crate::search::RangeCondition {
                gte: Some(value),
                ..Default::default()
            },
        )])),
        BinaryOperator::Lt => crate::search::QueryClause::Range(HashMap::from([(
            field,
            crate::search::RangeCondition {
                lt: Some(value),
                ..Default::default()
            },
        )])),
        BinaryOperator::LtEq => crate::search::QueryClause::Range(HashMap::from([(
            field,
            crate::search::RangeCondition {
                lte: Some(value),
                ..Default::default()
            },
        )])),
        _ => return Ok(None),
    };

    Ok(Some(clause))
}

fn extract_comparison_parts(
    left: &Expr,
    right: &Expr,
) -> Result<Option<(String, serde_json::Value, bool)>> {
    if let (Some(field), Some(value)) = (expr_to_field_name(left), expr_to_json_value(right)?) {
        return Ok(Some((field, value, false)));
    }
    if let (Some(field), Some(value)) = (expr_to_field_name(right), expr_to_json_value(left)?) {
        return Ok(Some((field, value, true)));
    }
    Ok(None)
}

fn expr_to_field_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        _ => None,
    }
}

fn expr_to_json_value(expr: &Expr) -> Result<Option<serde_json::Value>> {
    match expr {
        Expr::Value(value) => Ok(sql_value_to_json(&value.value)),
        Expr::UnaryOp { op, expr } if *op == UnaryOperator::Minus => {
            let Some(value) = expr_to_json_value(expr)? else {
                return Ok(None);
            };
            Ok(match value {
                serde_json::Value::Number(number) => {
                    if let Some(int_value) = number.as_i64() {
                        Some(serde_json::Value::from(-int_value))
                    } else {
                        number
                            .as_f64()
                            .map(|float_value| serde_json::Value::from(-float_value))
                    }
                }
                _ => None,
            })
        }
        _ => Ok(None),
    }
}

fn sql_value_to_json(value: &SqlValue) -> Option<serde_json::Value> {
    match value {
        SqlValue::Number(number, _) => number
            .parse::<i64>()
            .map(serde_json::Value::from)
            .ok()
            .or_else(|| number.parse::<f64>().ok().map(serde_json::Value::from)),
        SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text) => {
            Some(serde_json::Value::String(text.clone()))
        }
        SqlValue::Boolean(value) => Some(serde_json::Value::Bool(*value)),
        SqlValue::Null => Some(serde_json::Value::Null),
        _ => None,
    }
}

fn flip_binary_operator(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_column_helper_identifies_internal_fields() {
        assert_eq!(SyntheticColumn::parse("_id"), Some(SyntheticColumn::Id));
        assert_eq!(
            SyntheticColumn::parse("_score"),
            Some(SyntheticColumn::Score)
        );
        assert_eq!(SyntheticColumn::parse("score"), None);
        assert_eq!(SyntheticColumn::parse("title"), None);
    }

    #[test]
    fn count_star_only_simple() {
        let plan = plan_sql("idx", "SELECT count(*) AS total FROM \"idx\"").unwrap();
        assert!(plan.is_count_star_only());
        assert!(plan.uses_grouped_partials());
    }

    #[test]
    fn count_star_only_multiple_counts() {
        let plan = plan_sql("idx", "SELECT count(*) AS a, count(*) AS b FROM \"idx\"").unwrap();
        assert!(plan.is_count_star_only());
    }

    #[test]
    fn count_star_with_where_not_fast() {
        let plan = plan_sql(
            "idx",
            "SELECT count(*) AS total FROM \"idx\" WHERE price > 10",
        )
        .unwrap();
        assert!(!plan.is_count_star_only());
    }

    #[test]
    fn count_with_group_by_not_fast() {
        let plan = plan_sql(
            "idx",
            "SELECT brand, count(*) AS total FROM \"idx\" GROUP BY brand",
        )
        .unwrap();
        assert!(!plan.is_count_star_only());
    }

    #[test]
    fn avg_not_count_star() {
        let plan = plan_sql("idx", "SELECT avg(price) AS avg_p FROM \"idx\"").unwrap();
        assert!(!plan.is_count_star_only());
    }

    #[test]
    fn count_field_not_count_star() {
        let plan = plan_sql("idx", "SELECT count(price) AS c FROM \"idx\"").unwrap();
        assert!(!plan.is_count_star_only());
    }

    #[test]
    fn select_star_limit_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" LIMIT 10").unwrap();
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(10));
        let req = plan.to_search_request(false);
        assert_eq!(req.size, 10);
    }

    #[test]
    fn select_star_limit_with_offset_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" LIMIT 5 OFFSET 20").unwrap();
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        let req = plan.to_search_request(false);
        assert_eq!(req.size, 25); // limit + offset
    }

    #[test]
    fn select_star_limit_order_by_underscore_score_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" ORDER BY _score DESC LIMIT 4").unwrap();
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(4));
    }

    #[test]
    fn select_star_limit_order_by_fast_field_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" ORDER BY price DESC LIMIT 4").unwrap();
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        assert!(plan.sort_pushdown.is_some());
    }

    #[test]
    fn select_star_without_limit_no_pushdown() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\"").unwrap();
        assert!(plan.selects_all_columns);
        assert!(!plan.limit_pushed_down);
    }

    #[test]
    fn group_by_with_limit_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 20",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(20));
        assert_eq!(plan.offset, None);
    }

    #[test]
    fn group_by_with_limit_and_offset_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC LIMIT 10 OFFSET 5",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.offset, Some(5));
    }

    #[test]
    fn group_by_with_limit_no_order_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT category, count(*) AS cnt FROM idx GROUP BY category LIMIT 5",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(5));
    }

    // ── Alias pushdown guard tests ──────────────────────────────────────

    #[test]
    fn where_on_select_alias_not_pushed_down() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx WHERE posts > 10 GROUP BY author",
        )
        .unwrap();
        // "posts" is a SELECT alias — must NOT be pushed to Tantivy
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn where_on_real_field_still_pushed_with_alias_present() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx WHERE upvotes > 10 GROUP BY author",
        )
        .unwrap();
        // "upvotes" is a real field, not an alias — should be pushed
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn where_on_alias_and_real_field_only_pushes_real_field() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx WHERE upvotes > 10 AND posts > 5 GROUP BY author",
        )
        .unwrap();
        // "upvotes" pushed; "posts" stays as residual
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn alias_shadowing_real_column_preserves_required_column() {
        // SELECT LOWER(author) AS author — alias "author" shadows the real
        // column "author" that LOWER() needs. The planner must NOT strip it.
        let plan = plan_sql(
            "idx",
            "SELECT LOWER(author) AS author, count(*) AS posts, avg(upvotes) AS avg_upvotes \
             FROM idx WHERE text_match(title, 'rust') \
             GROUP BY LOWER(author) ORDER BY avg_upvotes DESC LIMIT 20",
        )
        .unwrap();
        assert!(
            plan.required_columns.contains(&"author".to_string()),
            "author must survive alias stripping: {:?}",
            plan.required_columns
        );
        assert!(plan.required_columns.contains(&"upvotes".to_string()));
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn pure_computed_alias_still_stripped() {
        // count(*) AS total — "total" is purely computed, not a real column.
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS total FROM idx GROUP BY author ORDER BY total DESC",
        )
        .unwrap();
        assert!(
            !plan.required_columns.contains(&"total".to_string()),
            "pure alias 'total' should not be in required_columns: {:?}",
            plan.required_columns
        );
        assert!(plan.required_columns.contains(&"author".to_string()));
    }

    #[test]
    fn having_aggregate_source_field_survives_alias_shadowing() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS price FROM idx GROUP BY author HAVING AVG(price) > 10 ORDER BY author",
        )
        .unwrap();
        assert!(
            plan.required_columns.contains(&"price".to_string()),
            "price must survive alias stripping when only HAVING aggregate expressions reference the real field: {:?}",
            plan.required_columns
        );
        assert!(plan.required_columns.contains(&"author".to_string()));
    }

    #[test]
    fn wrapped_having_aggregate_source_field_survives_alias_shadowing() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS price FROM idx GROUP BY author HAVING COALESCE(AVG(price), 0) > 10 ORDER BY author",
        )
        .unwrap();
        assert!(
            plan.required_columns.contains(&"price".to_string()),
            "price must survive alias stripping when wrapped HAVING aggregate expressions reference the real field: {:?}",
            plan.required_columns
        );
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn wrapped_order_by_alias_still_stripped() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS total FROM idx GROUP BY author ORDER BY COALESCE(total, 0) DESC",
        )
        .unwrap();
        assert!(
            !plan.required_columns.contains(&"total".to_string()),
            "wrapped alias 'total' should not be in required_columns: {:?}",
            plan.required_columns
        );
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(plan.has_group_by_fallback);
    }

    // ── HAVING support tests ────────────────────────────────────────────

    #[test]
    fn having_simple_comparison_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author HAVING posts > 10 ORDER BY posts DESC",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.having[0].output_name, "posts");
        assert_eq!(grouped.having[0].value, 10.0);
        assert!(matches!(grouped.having[0].op, super::HavingOp::Gt));
    }

    #[test]
    fn having_multiple_conditions() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts, avg(upvotes) AS avg_up FROM idx GROUP BY author HAVING posts > 10 AND avg_up > 5.0",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 2);
    }

    #[test]
    fn having_with_limit_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author HAVING posts > 10 ORDER BY posts DESC LIMIT 20",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert_eq!(plan.limit, Some(20));
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
    }

    #[test]
    fn having_aggregate_expr_count_star() {
        // HAVING COUNT(*) > 20 — uses aggregate expression, not alias
        let plan = plan_sql(
            "idx",
            "SELECT author, COUNT(*) AS posts FROM idx GROUP BY author HAVING COUNT(*) > 20",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.having[0].output_name, "posts");
        assert_eq!(grouped.having[0].value, 20.0);
        assert!(matches!(grouped.having[0].op, super::HavingOp::Gt));
    }

    #[test]
    fn having_aggregate_expr_avg() {
        // HAVING AVG(upvotes) > 50 — aggregate expression, not alias
        let plan = plan_sql(
            "idx",
            "SELECT author, AVG(upvotes) AS avg_up FROM idx GROUP BY author HAVING AVG(upvotes) > 50",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.having[0].output_name, "avg_up");
        assert_eq!(grouped.having[0].value, 50.0);
    }

    #[test]
    fn having_aggregate_expr_with_where_and_order() {
        // Full query from the bug report
        let plan = plan_sql(
            "hackernews",
            "SELECT author, COUNT(*) AS posts, AVG(comments) AS avg_comments, AVG(upvotes) AS avg_upvotes FROM hackernews WHERE upvotes > 50 GROUP BY author HAVING COUNT(*) > 20 ORDER BY avg_comments DESC LIMIT 10",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.having[0].output_name, "posts");
        assert_eq!(grouped.having[0].value, 20.0);
        assert_eq!(grouped.order_by.len(), 1);
        assert_eq!(grouped.order_by[0].output_name, "avg_comments");
        assert!(grouped.order_by[0].desc);
        assert_eq!(plan.limit, Some(10));
        assert!(!plan.pushed_filters.is_empty()); // upvotes > 50 pushed to tantivy
    }

    #[test]
    fn having_mixed_alias_and_aggregate_expr() {
        // Mix: one condition uses alias, another uses aggregate expression
        let plan = plan_sql(
            "idx",
            "SELECT author, COUNT(*) AS posts, AVG(upvotes) AS avg_up FROM idx GROUP BY author HAVING posts > 10 AND AVG(upvotes) > 5.0",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 2);
    }

    #[test]
    fn having_aggregate_expr_flipped() {
        // 20 < COUNT(*) — value on the left
        let plan = plan_sql(
            "idx",
            "SELECT author, COUNT(*) AS posts FROM idx GROUP BY author HAVING 20 < COUNT(*)",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.having[0].output_name, "posts");
        assert_eq!(grouped.having[0].value, 20.0);
        // 20 < COUNT(*) flips to COUNT(*) > 20
        assert!(matches!(grouped.having[0].op, super::HavingOp::Gt));
    }

    #[test]
    fn order_by_aggregate_expr_without_alias_uses_grouped_partials() {
        let plan = plan_sql(
            "hackernews",
            "SELECT author, SUM(upvotes) FROM hackernews WHERE upvotes > 100 GROUP BY author ORDER BY SUM(upvotes) DESC LIMIT 5",
        )
        .unwrap();

        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.metrics.len(), 1);
        assert_eq!(grouped.metrics[0].field.as_deref(), Some("upvotes"));
        assert_eq!(grouped.order_by.len(), 1);
        assert_eq!(
            grouped.order_by[0].output_name,
            grouped.metrics[0].output_name
        );
        assert!(grouped.order_by[0].desc);
    }

    #[test]
    fn having_hidden_metric_without_projection_stays_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author FROM idx GROUP BY author HAVING COUNT(*) > 20 ORDER BY author ASC",
        )
        .unwrap();

        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.group_columns.len(), 1);
        assert_eq!(grouped.metrics.len(), 1);
        assert!(!grouped.metrics[0].projected);
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(
            grouped.having[0].output_name,
            grouped.metrics[0].output_name
        );
    }

    #[test]
    fn order_by_hidden_metric_without_projection_stays_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author FROM idx GROUP BY author ORDER BY SUM(upvotes) DESC LIMIT 5",
        )
        .unwrap();

        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.metrics.len(), 1);
        assert!(!grouped.metrics[0].projected);
        assert_eq!(grouped.order_by.len(), 1);
        assert_eq!(
            grouped.order_by[0].output_name,
            grouped.metrics[0].output_name
        );
    }

    // ── OR disjunction pushdown tests ──────────────────────────────────

    #[test]
    fn or_equality_pushed_as_bool_should() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx WHERE author = 'pg' OR author = 'sama' GROUP BY author",
        )
        .unwrap();
        // OR of two equality predicates → pushed as Bool { should: [Term, Term] }
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        assert!(plan.uses_grouped_partials());
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bq) => {
                assert_eq!(bq.should.len(), 2);
                assert!(bq.must.is_empty());
            }
            other => panic!("Expected Bool query, got: {:?}", other),
        }
    }

    #[test]
    fn or_three_way_pushed_flat() {
        let plan = plan_sql(
            "idx",
            "SELECT category FROM idx WHERE category = 'rust' OR category = 'python' OR category = 'ml'",
        )
        .unwrap();
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bq) => {
                assert_eq!(bq.should.len(), 3);
            }
            other => panic!("Expected Bool query, got: {:?}", other),
        }
    }

    #[test]
    fn or_mixed_with_and_pushdown() {
        // upvotes > 100 AND (author = 'pg' OR author = 'sama')
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx WHERE upvotes > 100 AND (author = 'pg' OR author = 'sama') GROUP BY author",
        )
        .unwrap();
        // Both predicates pushed: Range + Bool { should }
        assert_eq!(plan.pushed_filters.len(), 2);
        assert!(!plan.has_residual_predicates);
        assert!(plan.uses_grouped_partials());
    }

    #[test]
    fn or_with_range_pushed() {
        let plan = plan_sql(
            "idx",
            "SELECT price FROM idx WHERE price < 10 OR price > 1000",
        )
        .unwrap();
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bq) => {
                assert_eq!(bq.should.len(), 2);
            }
            other => panic!("Expected Bool query, got: {:?}", other),
        }
    }

    #[test]
    fn or_with_unpushable_branch_stays_residual() {
        // text_match inside OR now gives a clear error instead of crashing DataFusion
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') OR author = 'pg'",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("text_match()"));
    }

    #[test]
    fn or_on_underscore_score_not_pushed() {
        let plan = plan_sql(
            "idx",
            "SELECT author FROM idx WHERE _score > 0.5 OR author = 'pg'",
        )
        .unwrap();
        // _score branch can't be pushed → whole OR stays residual
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn real_score_field_is_not_treated_as_synthetic() {
        let plan = plan_sql(
            "idx",
            "SELECT title, score FROM idx WHERE score > 10 ORDER BY score DESC LIMIT 5",
        )
        .unwrap();

        assert!(plan.required_columns.contains(&"score".to_string()));
        assert!(!plan.needs_score);
        assert_eq!(plan.pushed_filters.len(), 1);
        assert_eq!(plan.sort_pushdown, Some(("score".to_string(), true)));
        assert!(plan.limit_pushed_down);
    }

    #[test]
    fn computed_expression_falls_to_fast_fields() {
        // SUM(upvotes) * 1.0 / COUNT(*) is a computed expression, not a simple aggregate.
        // The grouped partials path cannot handle it — falls to tantivy_fast_fields + DataFusion.
        let plan = plan_sql(
            "hackernews",
            "SELECT author, COUNT(*) AS posts, SUM(upvotes) * 1.0 / COUNT(*) AS upvotes_per_post FROM hackernews GROUP BY author HAVING posts >= 100 ORDER BY upvotes_per_post DESC LIMIT 20",
        ).unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(!plan.selects_all_columns);
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(plan.required_columns.contains(&"upvotes".to_string()));
    }

    #[test]
    fn computed_expression_with_group_by_no_limit_pushdown() {
        // GROUP BY + computed aggregate + LIMIT must NOT push LIMIT to Tantivy.
        // DataFusion needs all matching docs to compute correct GROUP BY results.
        let plan = plan_sql(
            "idx",
            "SELECT author, SUM(price) * 1.0 / COUNT(*) AS avg_p FROM idx GROUP BY author ORDER BY avg_p DESC LIMIT 5",
        ).unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(5));
        assert!(!plan.group_by_columns.is_empty());
    }

    #[test]
    fn no_group_by_computed_agg_limit_still_pushes() {
        // Ungrouped computed expression WITHOUT GROUP BY — LIMIT can push
        // because there's no grouping that requires all rows.
        let plan = plan_sql("idx", "SELECT price FROM idx ORDER BY price DESC LIMIT 5").unwrap();
        assert!(plan.limit_pushed_down);
    }

    #[test]
    fn max_minus_min_group_by_no_limit_pushdown() {
        // max(x) - min(x) is a computed expression with GROUP BY.
        // Must NOT push LIMIT down.
        let plan = plan_sql(
            "idx",
            "SELECT author, MAX(price) - MIN(price) AS spread FROM idx GROUP BY author ORDER BY spread DESC LIMIT 3",
        ).unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
        assert_eq!(plan.limit, Some(3));
    }

    // ── Bug fix: _score not excluded from pushdown ──────────────────

    #[test]
    fn underscore_score_not_pushed_equality() {
        let plan = plan_sql("idx", "SELECT title FROM idx WHERE _score > 0.5").unwrap();
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn underscore_score_not_pushed_in_or() {
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE _score > 0.5 OR author = 'pg'",
        )
        .unwrap();
        // _score branch can't be pushed → whole OR stays residual
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn underscore_score_between_not_pushed() {
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE _score BETWEEN 0.1 AND 0.9",
        )
        .unwrap();
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn underscore_score_in_not_pushed() {
        let plan = plan_sql("idx", "SELECT title FROM idx WHERE _score IN (0.5, 0.6)").unwrap();
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
    }

    #[test]
    fn underscore_id_not_pushed_but_marked_needed() {
        let plan = plan_sql("idx", "SELECT title FROM idx WHERE _id = 'doc-1'").unwrap();
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
        assert!(plan.needs_id);
    }

    // ── Bug fix: GROUP BY expression falls to DataFusion ────────────

    #[test]
    fn group_by_expression_falls_to_fast_fields() {
        // GROUP BY LOWER(author) is an expression, not a plain column.
        // Must NOT use grouped partials (would group by raw author instead).
        let plan = plan_sql(
            "idx",
            "SELECT LOWER(author), count(*) AS cnt FROM idx GROUP BY LOWER(author)",
        )
        .unwrap();
        // group_by_columns should be empty since LOWER(author) is not a plain column
        assert!(plan.group_by_columns.is_empty());
        // Should NOT use grouped partials
        assert!(!plan.uses_grouped_partials());
        // Must flag as GROUP BY fallback so the scan limit is raised
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn group_by_arithmetic_falls_to_fast_fields() {
        let plan = plan_sql(
            "idx",
            "SELECT upvotes / 100, count(*) AS cnt FROM idx GROUP BY upvotes / 100",
        )
        .unwrap();
        assert!(plan.group_by_columns.is_empty());
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn group_by_plain_column_still_works() {
        // Verify plain GROUP BY still routes to grouped partials
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS cnt FROM idx GROUP BY author",
        )
        .unwrap();
        assert_eq!(plan.group_by_columns, vec!["author"]);
        assert!(plan.uses_grouped_partials());
        assert!(!plan.has_group_by_fallback);
    }

    #[test]
    fn unsupported_aggregate_flags_group_by_fallback() {
        // STDDEV_POP is not a supported grouped partial metric —
        // the query has GROUP BY but falls to fast-fields.
        let plan = plan_sql(
            "idx",
            "SELECT author, STDDEV_POP(upvotes) FROM idx GROUP BY author",
        )
        .unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn flat_query_has_no_group_by_fallback() {
        let plan = plan_sql(
            "idx",
            "SELECT title, upvotes FROM idx WHERE text_match(title, 'rust') ORDER BY upvotes DESC",
        )
        .unwrap();
        assert!(!plan.has_group_by_fallback);
    }

    // ── Bug fix: text_match in OR gives clear error ─────────────────

    #[test]
    fn text_match_in_or_gives_error() {
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') OR author = 'pg'",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("text_match()"),
            "Error should mention text_match: {}",
            err
        );
    }

    #[test]
    fn text_match_in_and_still_works() {
        // text_match at top-level AND is fine
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') AND author = 'pg'",
        )
        .unwrap();
        assert_eq!(plan.text_matches.len(), 1);
        assert!(plan.primary_text_match().is_some());
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn multiple_top_level_text_match_predicates_work() {
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') AND text_match(title, 'performance') AND author = 'pg'",
        )
        .unwrap();

        assert_eq!(plan.text_matches.len(), 2);
        assert_eq!(plan.primary_text_match().unwrap().field, "title");
        assert_eq!(plan.primary_text_match().unwrap().query, "rust");
        assert!(
            plan.text_matches.iter().any(|predicate| {
                predicate.field == "title" && predicate.query == "performance"
            })
        );
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(plan.pushed_filters.iter().any(|clause| matches!(
            clause,
            crate::search::QueryClause::Term(fields)
            if fields.get("author") == Some(&serde_json::Value::String("pg".to_string()))
        )));
        assert!(!plan.has_residual_predicates);

        let request = plan.to_search_request(false);
        match request.query {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.must.len(), 2);
                assert_eq!(bool_query.filter.len(), 1);
            }
            other => panic!("expected Bool query, got {other:?}"),
        }
    }

    #[test]
    fn multiple_text_match_across_different_fields() {
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') AND text_match(body, 'performance')",
        )
        .unwrap();

        assert_eq!(plan.text_matches.len(), 2);
        assert!(
            plan.text_matches
                .iter()
                .any(|p| p.field == "title" && p.query == "rust")
        );
        assert!(
            plan.text_matches
                .iter()
                .any(|p| p.field == "body" && p.query == "performance")
        );
        assert!(plan.pushed_filters.is_empty());

        let request = plan.to_search_request(false);
        match request.query {
            crate::search::QueryClause::Bool(bool_query) => {
                assert_eq!(bool_query.must.len(), 2);
                assert!(bool_query.filter.is_empty());
            }
            other => panic!("expected Bool query, got {other:?}"),
        }
    }

    // ── Bug fix: collect_expr_columns handles Cast/Case/Like ────────

    #[test]
    fn cast_column_collected() {
        let plan = plan_sql("idx", "SELECT CAST(price AS BIGINT) FROM idx").unwrap();
        assert!(plan.required_columns.contains(&"price".to_string()));
    }

    #[test]
    fn case_column_collected() {
        let plan = plan_sql(
            "idx",
            "SELECT CASE WHEN status = 'active' THEN upvotes ELSE 0 END FROM idx",
        )
        .unwrap();
        assert!(plan.required_columns.contains(&"status".to_string()));
        assert!(plan.required_columns.contains(&"upvotes".to_string()));
    }

    // ── Bug fix: mixed GROUP BY (plain + expression) bails out ──────

    #[test]
    fn group_by_mixed_plain_and_expression_bails() {
        // GROUP BY author, LOWER(category) — mixed plain + expression.
        // Must NOT use grouped partials (would silently drop LOWER(category)).
        let plan = plan_sql(
            "idx",
            "SELECT author, LOWER(category), count(*) AS cnt FROM idx GROUP BY author, LOWER(category)",
        )
        .unwrap();
        assert!(!plan.uses_grouped_partials());
    }

    #[test]
    fn group_by_expression_only_count_star_not_fast() {
        // GROUP BY LOWER(author) with count(*) must NOT use count_star_fast
        // path because there's a GROUP BY (even though it's expression-based).
        let plan = plan_sql("idx", "SELECT count(*) FROM idx GROUP BY LOWER(author)").unwrap();
        assert!(!plan.is_count_star_only());
        assert!(!plan.uses_grouped_partials());
    }

    // ── Bug fix: text_match guard covers complex expressions ────────

    #[test]
    fn text_match_in_is_not_null_gives_error() {
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE text_match(title, 'rust') IS NOT NULL",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("text_match()"), "Error: {}", err);
    }

    #[test]
    fn text_match_in_case_where_gives_error() {
        // text_match inside a CASE in the WHERE clause stays as residual
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE CASE WHEN text_match(title, 'rust') THEN true ELSE false END",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("text_match()"), "Error: {}", err);
    }

    // ── Unsupported query shape validation tests ───────────────────────

    #[test]
    fn supports_same_index_semijoin() {
        let plan = plan_sql(
            "idx",
            "SELECT title FROM idx WHERE id IN (SELECT id FROM idx)",
        )
        .unwrap();
        let semijoin = plan.semijoin.as_ref().expect("semijoin plan");

        assert_eq!(semijoin.outer_key, "id");
        assert_eq!(semijoin.inner_key, "id");
        assert!(!plan.has_residual_predicates);
        assert!(!plan.rewritten_sql.contains("IN (SELECT"));
    }

    #[test]
    fn rejects_exists_subquery() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE EXISTS (SELECT 1 FROM idx WHERE idx.id = idx.id)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("EXISTS") && err.contains("not supported"),
            "Expected EXISTS error, got: {err}"
        );
    }

    #[test]
    fn rejects_scalar_subquery_in_where() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE price > (SELECT MAX(price) FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("subqueries") || err.contains("Subqueries"),
            "Expected subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_cte() {
        let result = plan_sql(
            "idx",
            "WITH top_authors AS (SELECT author FROM idx) SELECT * FROM idx",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("CTEs") || err.contains("WITH"),
            "Expected CTE error, got: {err}"
        );
    }

    #[test]
    fn rejects_union() {
        let result = plan_sql("idx", "SELECT title FROM idx UNION SELECT title FROM idx");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UNION"), "Expected UNION error, got: {err}");
    }

    #[test]
    fn rejects_except() {
        let result = plan_sql("idx", "SELECT title FROM idx EXCEPT SELECT title FROM idx");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("EXCEPT"), "Expected EXCEPT error, got: {err}");
    }

    #[test]
    fn rejects_intersect() {
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx INTERSECT SELECT title FROM idx",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("INTERSECT"),
            "Expected INTERSECT error, got: {err}"
        );
    }

    #[test]
    fn rejects_join() {
        let result = plan_sql("idx", "SELECT * FROM idx JOIN idx AS b ON idx.id = b.id");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("JOIN") || err.contains("join"),
            "Expected JOIN error, got: {err}"
        );
    }

    #[test]
    fn rejects_derived_table_in_from() {
        let result = plan_sql("idx", "SELECT * FROM (SELECT * FROM idx) AS sub");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("derived table") || err.contains("Subqueries in FROM"),
            "Expected derived table error, got: {err}"
        );
    }

    #[test]
    fn rejects_multi_table_from() {
        let result = plan_sql("idx", "SELECT * FROM idx, other");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Multi-table") || err.contains("implicit join"),
            "Expected multi-table error, got: {err}"
        );
    }

    #[test]
    fn supports_semijoin_nested_in_and_conjunction() {
        let plan = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE price > 10 AND id IN (SELECT id FROM idx)",
        )
        .unwrap();

        assert!(plan.semijoin.is_some());
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn rejects_semijoin_inside_or() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE id IN (SELECT id FROM idx) OR price > 10",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("top-level AND predicates"),
            "Expected top-level semijoin error, got: {err}"
        );
    }

    #[test]
    fn rejects_correlated_semijoin_subquery() {
        let result = plan_sql(
            "idx",
            "SELECT outer_idx.id FROM idx AS outer_idx WHERE outer_idx.id IN (SELECT inner_idx.id FROM idx AS inner_idx WHERE inner_idx.price > outer_idx.price)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Correlated semijoin") || err.contains("outer alias"),
            "Expected correlated semijoin error, got: {err}"
        );
    }

    #[test]
    fn rejects_multi_index_semijoin_subquery() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE id IN (SELECT id FROM other)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must match request index"),
            "Expected same-index error, got: {err}"
        );
    }

    #[test]
    fn rejects_multiple_semijoin_predicates() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE id IN (SELECT id FROM idx) AND author IN (SELECT author FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Only one IN (SELECT ...) semijoin predicate"),
            "Expected multiple-semijoin error, got: {err}"
        );
    }

    #[test]
    fn rejects_semijoin_outer_expression_key() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx WHERE LOWER(author) IN (SELECT author FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("plain source column"),
            "Expected outer-key shape error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_having() {
        let result = plan_sql(
            "idx",
            "SELECT author, count(*) AS cnt FROM idx GROUP BY author HAVING count(*) > (SELECT 1 FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("HAVING") || err.contains("subquer"),
            "Expected HAVING subquery error, got: {err}"
        );
    }

    #[test]
    fn simple_select_still_works_after_validation() {
        // Sanity: normal queries must still pass through validation
        let plan = plan_sql("idx", "SELECT title, author FROM idx WHERE price > 10").unwrap();
        assert_eq!(plan.index_name, "idx");
        assert!(!plan.pushed_filters.is_empty());
    }

    #[test]
    fn grouped_query_still_works_after_validation() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS cnt FROM idx GROUP BY author",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
    }

    // ── Binder unit tests ──────────────────────────────────────────────

    #[test]
    fn binder_detects_source_columns() {
        let plan = plan_sql("idx", "SELECT author, price FROM idx").unwrap();
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(plan.required_columns.contains(&"price".to_string()));
        assert!(!plan.needs_id);
        assert!(!plan.needs_score);
    }

    #[test]
    fn binder_detects_synthetic_id() {
        let plan = plan_sql("idx", "SELECT _id, author FROM idx").unwrap();
        assert!(plan.needs_id);
        assert!(!plan.needs_score);
        assert!(plan.required_columns.contains(&"author".to_string()));
        // _id should NOT be in required_columns (it's synthetic)
        assert!(!plan.required_columns.contains(&"_id".to_string()));
    }

    #[test]
    fn binder_detects_synthetic_score() {
        let plan = plan_sql("idx", "SELECT _score, author FROM idx ORDER BY _score DESC").unwrap();
        assert!(plan.needs_score);
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(!plan.required_columns.contains(&"_score".to_string()));
    }

    #[test]
    fn binder_extracts_aggregate_field() {
        let plan = plan_sql(
            "idx",
            "SELECT author, avg(price) AS avg_price FROM idx GROUP BY author",
        )
        .unwrap();
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(plan.required_columns.contains(&"price".to_string()));
        assert!(!plan.required_columns.contains(&"avg_price".to_string()));
    }

    #[test]
    fn binder_alias_not_in_required_columns() {
        let plan = plan_sql("idx", "SELECT count(*) AS total FROM idx").unwrap();
        assert!(!plan.required_columns.contains(&"total".to_string()));
    }

    #[test]
    fn binder_expression_group_by_detected() {
        let plan = plan_sql(
            "idx",
            "SELECT LOWER(author) AS a, count(*) AS cnt FROM idx GROUP BY LOWER(author)",
        )
        .unwrap();
        // Expression GROUP BY should not be eligible for grouped partials
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn binder_synthetic_in_group_by_flags_expression() {
        let plan = plan_sql("idx", "SELECT _id, count(*) AS cnt FROM idx GROUP BY _id").unwrap();
        // GROUP BY _id is treated as expression GROUP BY (synthetic)
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn binder_plain_group_by_eligible() {
        let plan = plan_sql(
            "idx",
            "SELECT category, count(*) AS cnt FROM idx GROUP BY category",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.has_group_by_fallback);
    }

    #[test]
    fn binder_where_column_from_residual() {
        // A column used only in WHERE (that can't be pushed down) should
        // still be in required_columns
        let plan = plan_sql("idx", "SELECT title FROM idx WHERE LOWER(author) = 'alice'").unwrap();
        assert!(plan.required_columns.contains(&"title".to_string()));
        assert!(plan.required_columns.contains(&"author".to_string()));
    }

    #[test]
    fn binder_having_column_extraction() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author HAVING count(*) > 5",
        )
        .unwrap();
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(!plan.required_columns.contains(&"posts".to_string()));
    }

    #[test]
    fn binder_order_by_alias_not_in_required() {
        let plan = plan_sql(
            "idx",
            "SELECT author, count(*) AS posts FROM idx GROUP BY author ORDER BY posts DESC",
        )
        .unwrap();
        assert!(plan.required_columns.contains(&"author".to_string()));
        // 'posts' is an alias — should not be in required_columns
        assert!(!plan.required_columns.contains(&"posts".to_string()));
    }

    #[test]
    fn binder_score_in_order_by_sets_needs_score() {
        let plan = plan_sql("idx", "SELECT title FROM idx ORDER BY _score DESC").unwrap();
        assert!(plan.needs_score);
        assert!(!plan.required_columns.contains(&"_score".to_string()));
    }

    #[test]
    fn binder_select_star_sets_flag() {
        let plan = plan_sql("idx", "SELECT * FROM idx").unwrap();
        assert!(plan.selects_all_columns);
    }

    #[test]
    fn binder_unresolved_expression_extracts_source_columns() {
        let plan = plan_sql("idx", "SELECT price * 2 AS double_price, author FROM idx").unwrap();
        assert!(plan.required_columns.contains(&"price".to_string()));
        assert!(plan.required_columns.contains(&"author".to_string()));
        assert!(!plan.required_columns.contains(&"double_price".to_string()));
    }

    // ── Subquery validation: SELECT projection ─────────────────────────

    #[test]
    fn rejects_scalar_subquery_in_select() {
        let result = plan_sql("idx", "SELECT (SELECT 1 FROM idx) AS x FROM idx");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in SELECT"),
            "Expected SELECT subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_select_expression() {
        let result = plan_sql(
            "idx",
            "SELECT price + (SELECT MAX(price) FROM idx) AS x FROM idx",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in SELECT"),
            "Expected SELECT subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_in_subquery_in_select() {
        let result = plan_sql(
            "idx",
            "SELECT price IN (SELECT price FROM idx) AS flag FROM idx",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in SELECT") || err.contains("subquer"),
            "Expected SELECT subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_select_case_expression() {
        let result = plan_sql(
            "idx",
            "SELECT CASE WHEN true THEN (SELECT 1 FROM idx) ELSE 0 END AS x FROM idx",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in SELECT"),
            "Expected SELECT CASE subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_where_like_pattern() {
        let result = plan_sql("idx", "SELECT * FROM idx WHERE title LIKE (SELECT 'r%')");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries") || err.contains("subqueries"),
            "Expected WHERE LIKE subquery error, got: {err}"
        );
    }

    // ── Subquery validation: ORDER BY ──────────────────────────────────

    #[test]
    fn rejects_scalar_subquery_in_order_by() {
        let result = plan_sql(
            "idx",
            "SELECT * FROM idx ORDER BY (SELECT MAX(price) FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in ORDER BY"),
            "Expected ORDER BY subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_order_by_expression() {
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx ORDER BY price + (SELECT 1 FROM idx)",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in ORDER BY"),
            "Expected ORDER BY subquery error, got: {err}"
        );
    }

    #[test]
    fn rejects_subquery_in_order_by_like_expression() {
        let result = plan_sql(
            "idx",
            "SELECT title FROM idx ORDER BY title LIKE (SELECT 'r%') DESC",
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Subqueries in ORDER BY"),
            "Expected ORDER BY LIKE subquery error, got: {err}"
        );
    }

    // ── GROUP BY alias classification ──────────────────────────────────

    #[test]
    fn group_by_alias_falls_back_to_expression() {
        // SELECT LOWER(author) AS author, count(*) FROM idx GROUP BY author
        // "author" in GROUP BY is an alias for LOWER(author), not a source column.
        // The binder must classify it as Expression, triggering fallback.
        let plan = plan_sql(
            "idx",
            "SELECT LOWER(author) AS author, count(*) AS cnt FROM idx GROUP BY author",
        )
        .unwrap();
        assert!(
            !plan.uses_grouped_partials(),
            "GROUP BY alias should not use grouped partials"
        );
        assert!(
            plan.has_group_by_fallback,
            "GROUP BY alias should trigger fallback"
        );
    }

    #[test]
    fn group_by_same_name_source_alias_still_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT author AS author, count(*) AS cnt FROM idx GROUP BY author",
        )
        .unwrap();
        assert_eq!(plan.group_by_columns, vec!["author"]);
        assert!(plan.uses_grouped_partials());
        assert!(!plan.has_group_by_fallback);
    }

    #[test]
    fn where_on_same_name_source_alias_still_pushes_down() {
        let plan = plan_sql(
            "idx",
            "SELECT author AS author FROM idx WHERE author = 'alice'",
        )
        .unwrap();
        assert_eq!(plan.pushed_filters.len(), 1);
        assert!(!plan.has_residual_predicates);
    }

    #[test]
    fn group_by_non_alias_column_still_eligible() {
        // "category" is NOT an alias — it's a real source column. Grouped partials OK.
        let plan = plan_sql(
            "idx",
            "SELECT category, count(*) AS cnt FROM idx GROUP BY category",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.has_group_by_fallback);
    }

    #[test]
    fn group_by_alias_with_different_name_falls_back() {
        // SELECT category, count(*) AS total FROM idx GROUP BY total
        // "total" is an alias for count(*), not a source column.
        let plan = plan_sql(
            "idx",
            "SELECT category, count(*) AS total FROM idx GROUP BY total",
        )
        .unwrap();
        assert!(
            !plan.uses_grouped_partials(),
            "GROUP BY on aggregate alias should not use grouped partials"
        );
        assert!(
            plan.has_group_by_fallback,
            "GROUP BY aggregate alias should trigger fallback"
        );
    }

    #[test]
    fn duplicate_output_alias_in_grouped_partials_returns_error() {
        // SELECT brand AS x, count(*) AS x FROM t GROUP BY brand
        // → "ambiguous column reference 'x'" error, matching PostgreSQL semantics
        let err = plan_sql(
            "products",
            "SELECT brand AS x, count(*) AS x FROM products GROUP BY brand",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("ambiguous column reference"),
            "expected ambiguous column error, got: {}",
            err
        );
    }

    #[test]
    fn non_duplicate_aliases_in_grouped_partials_are_accepted() {
        let plan = plan_sql(
            "products",
            "SELECT brand, count(*) AS cnt FROM products GROUP BY brand HAVING cnt > 1 ORDER BY cnt DESC",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        assert_eq!(grouped.group_columns[0].output_name, "brand");
        assert_eq!(grouped.metrics[0].output_name, "cnt");
        assert_eq!(grouped.having.len(), 1);
        assert_eq!(grouped.order_by.len(), 1);
    }
}
