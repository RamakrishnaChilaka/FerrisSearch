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
                    if let Some(ref expr) = metric.field_expr {
                        let mut fields = Vec::new();
                        expr.collect_fields(&mut fields);
                        columns.extend(fields.into_iter().map(str::to_string));
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
            if let Some(name) =
                extract_supported_group_key(expr).or_else(|| expr_to_field_name(expr))
            {
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

fn extract_supported_group_key(expr: &Expr) -> Option<String> {
    let Expr::Case {
        operand: None,
        conditions,
        else_result,
        ..
    } = expr
    else {
        return None;
    };

    if conditions.is_empty() {
        return None;
    }

    let mut source_field = None;
    let mut buckets = Vec::with_capacity(conditions.len());

    for case_when in conditions {
        let (field_name, bucket) = parse_case_bucket(case_when)?;
        match &source_field {
            Some(existing) if existing != &field_name => return None,
            None => source_field = Some(field_name),
            _ => {}
        }
        buckets.push(bucket);
    }

    let else_label = match else_result.as_ref() {
        Some(expr) => Some(expr_to_string_literal(expr.as_ref())?),
        None => None,
    };
    Some(crate::search::encode_derived_group_key(
        &crate::search::DerivedGroupKey {
            source_field: source_field?,
            buckets,
            else_label,
        },
    ))
}

fn parse_case_bucket(
    case_when: &sqlparser::ast::CaseWhen,
) -> Option<(String, crate::search::DerivedGroupBucket)> {
    let mut field_name = None;
    let mut lower = None;
    let mut upper = None;

    collect_case_bucket_bounds(
        &case_when.condition,
        &mut field_name,
        &mut lower,
        &mut upper,
    )?;

    Some((
        field_name?,
        crate::search::DerivedGroupBucket {
            lower: lower.as_ref()?.0.clone(),
            lower_inclusive: lower?.1,
            upper: upper.as_ref()?.0.clone(),
            upper_inclusive: upper?.1,
            label: expr_to_string_literal(&case_when.result)?,
        },
    ))
}

fn collect_case_bucket_bounds(
    expr: &Expr,
    field_name: &mut Option<String>,
    lower: &mut Option<(String, bool)>,
    upper: &mut Option<(String, bool)>,
) -> Option<()> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_case_bucket_bounds(left, field_name, lower, upper)?;
            collect_case_bucket_bounds(right, field_name, lower, upper)?;
            Some(())
        }
        Expr::BinaryOp { left, op, right } => {
            let left_field = expr_to_field_name(left);
            let right_field = expr_to_field_name(right);
            let left_literal = expr_to_string_literal(left);
            let right_literal = expr_to_string_literal(right);

            match (left_field, right_literal) {
                (Some(name), Some(bound)) => {
                    assign_case_bucket_bound(field_name, lower, upper, name, op, bound, true)
                }
                _ => match (left_literal, right_field) {
                    (Some(bound), Some(name)) => {
                        assign_case_bucket_bound(field_name, lower, upper, name, op, bound, false)
                    }
                    _ => None,
                },
            }
        }
        _ => None,
    }
}

fn assign_case_bucket_bound(
    field_name: &mut Option<String>,
    lower: &mut Option<(String, bool)>,
    upper: &mut Option<(String, bool)>,
    name: String,
    op: &BinaryOperator,
    bound: String,
    field_on_left: bool,
) -> Option<()> {
    match field_name {
        Some(existing) if existing != &name => return None,
        None => *field_name = Some(name),
        _ => {}
    }

    match (field_on_left, op) {
        (true, BinaryOperator::Gt) => set_bound(lower, bound, false),
        (true, BinaryOperator::GtEq) => set_bound(lower, bound, true),
        (true, BinaryOperator::Lt) => set_bound(upper, bound, false),
        (true, BinaryOperator::LtEq) => set_bound(upper, bound, true),
        (false, BinaryOperator::Lt) => set_bound(lower, bound, false),
        (false, BinaryOperator::LtEq) => set_bound(lower, bound, true),
        (false, BinaryOperator::Gt) => set_bound(upper, bound, false),
        (false, BinaryOperator::GtEq) => set_bound(upper, bound, true),
        _ => None,
    }
}

fn set_bound(slot: &mut Option<(String, bool)>, value: String, inclusive: bool) -> Option<()> {
    match slot {
        Some((existing, existing_inclusive))
            if existing != &value || *existing_inclusive != inclusive =>
        {
            None
        }
        Some(_) => Some(()),
        None => {
            *slot = Some((value, inclusive));
            Some(())
        }
    }
}

fn expr_to_string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text) => {
                Some(text.clone())
            }
            _ => None,
        },
        _ => None,
    }
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
                if let Some(field_expr) = &metric.field_expr {
                    let mut fields = Vec::new();
                    field_expr.collect_fields(&mut fields);
                    columns.extend(fields.into_iter().map(str::to_string));
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
        if let Some(field_expr) = metric.field_expr {
            let mut fields = Vec::new();
            field_expr.collect_fields(&mut fields);
            columns.extend(fields.into_iter().map(str::to_string));
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

#[derive(Debug, Clone, PartialEq)]
/// A small expression tree for post-aggregate scalar computation.
/// Built by the planner when SELECT items wrap or combine aggregates
/// (e.g. `ROUND(AVG(x), 2)`, `SUM(a)/COUNT(*)`).  Evaluated after
/// cross-shard merge using resolved metric values.
pub enum ResidualExpr {
    /// Reference a merged metric by its output_name.
    MetricRef(String),
    /// A numeric literal.
    Literal(f64),
    /// ROUND(expr, precision)
    Round(Box<ResidualExpr>, i32),
    /// CAST(expr AS FLOAT) — identity for f64 values.
    CastFloat(Box<ResidualExpr>),
    /// CAST(expr AS INTEGER) — truncate to i64.
    CastInt(Box<ResidualExpr>),
    /// Arithmetic: +, -, *, /
    BinaryOp(Box<ResidualExpr>, ResidualBinOp, Box<ResidualExpr>),
    /// Unary negation.
    Negate(Box<ResidualExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidualBinOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlGroupedMetric {
    pub output_name: String,
    pub function: SqlGroupedMetricFunction,
    pub field: Option<String>,
    /// Per-doc binary expression on two fields (e.g. `SUM(a + b)`).
    /// When set, the collector reads both columns and computes inline.
    pub field_expr: Option<crate::search::MetricFieldExpr>,
    pub projected: bool,
    /// When the SELECT item is not a bare aggregate, this holds the scalar
    /// expression tree to evaluate after merge.  `None` means the metric
    /// value is used directly (the common case).
    pub residual_expr: Option<ResidualExpr>,
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
    /// True when the SQL SELECT contains aggregate functions (AVG, SUM, etc.)
    /// but there is no GROUP BY AND `grouped_sql` is None (e.g. the aggregate
    /// is wrapped in ROUND/CAST/other expressions the grouped-partials parser
    /// does not handle).  Like `has_group_by_fallback`, these queries need ALL
    /// matching docs for correct results.
    pub has_ungrouped_aggregate_fallback: bool,
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
                // Metrics with a residual_expr are computed after merge from
                // other metrics — they have no real field to aggregate on.
                .filter(|metric| metric.residual_expr.is_none())
                .map(|metric| crate::search::GroupedMetricAgg {
                    output_name: metric.output_name.clone(),
                    function: metric.function.to_search_function(),
                    field: metric.field.clone(),
                    field_expr: metric.field_expr.clone(),
                })
                .collect();

            // Shard-level top-K pruning is controlled by `sql_approximate_top_k`
            // in ferrissearch.yml and defaults to enabled. Standard SQL engines
            // compute exact GROUP BY results; this approximation trades some
            // correctness for latency on high-cardinality full-scan GROUP BY
            // with ORDER BY + LIMIT.
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
                let matching_metric = grouped_sql.metrics.iter().find(|m| {
                    m.output_name == first_order.output_name && m.residual_expr.is_none()
                });
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
        } else if self.has_ungrouped_aggregate_fallback {
            // Ungrouped aggregates (e.g. ROUND(AVG(...))) MUST scan all matching
            // docs.  Use usize::MAX here; the API layer will clamp to the
            // configured sql_group_by_scan_limit just like GROUP BY fallback.
            (usize::MAX, HashMap::new())
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

    // Ungrouped aggregate fallback: SELECT AVG(...), ROUND(AVG(...)), etc.
    // without GROUP BY but grouped_sql failed to parse the expressions.
    // These queries MUST scan all matching docs for correct results.
    let has_ungrouped_aggregate_fallback = group_by_columns.is_empty()
        && !has_expression_group_by
        && grouped_sql.is_none()
        && select_contains_aggregate(select);

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
        has_ungrouped_aggregate_fallback,
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
                if let Some(source_name) = extract_supported_group_key(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name,
                        output_name: expr.to_string(),
                    });
                    continue;
                }

                if let Some(source_name) = expr_to_field_name(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name: source_name.clone(),
                        output_name: source_name,
                    });
                    continue;
                }

                if let Some(metric) = extract_projected_metric(expr, None, &mut metrics)? {
                    ensure_grouped_metric(&mut metrics, metric, true);
                    continue;
                }
                return Ok(None);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(source_name) = extract_supported_group_key(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name,
                        output_name: alias.value.clone(),
                    });
                    continue;
                }

                if let Some(source_name) = expr_to_field_name(expr)
                    && group_by_columns.iter().any(|column| column == &source_name)
                {
                    group_columns.push(SqlGroupColumn {
                        source_name,
                        output_name: alias.value.clone(),
                    });
                    continue;
                }

                if let Some(metric) =
                    extract_projected_metric(expr, Some(alias.value.clone()), &mut metrics)?
                {
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
                        resolve_or_register_residual(&expr.expr, &valid_names, &mut metrics)
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
        && !parse_having_filters(having_expr, &valid_names, &mut metrics, &mut having_filters)
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
    metrics: &mut Vec<SqlGroupedMetric>,
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
                resolve_or_register_residual(left, valid_names, metrics),
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
                resolve_or_register_residual(right, valid_names, metrics),
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

/// Try to resolve an expression to an existing metric output name.
/// If resolution fails but the expression can be built as a residual tree,
/// register it as a hidden metric and return that name.
/// Used by both ORDER BY and HAVING resolution.
fn resolve_or_register_residual(
    expr: &Expr,
    valid_names: &HashMap<String, String>,
    metrics: &mut Vec<SqlGroupedMetric>,
) -> Option<String> {
    // Fast path: identifier, bare aggregate, or existing residual match.
    if let Some(name) = resolve_grouped_output_name(expr, valid_names, metrics) {
        return Some(name);
    }
    // Build a residual expression and register it as a hidden metric.
    if let Ok(Some(residual)) = try_build_residual_expr(expr, metrics) {
        if !residual_contains_metric(&residual) {
            return None;
        }
        let hidden_name = format!("__hidden_having_residual_{}", metrics.len());
        metrics.push(SqlGroupedMetric {
            output_name: hidden_name.clone(),
            function: SqlGroupedMetricFunction::Avg, // placeholder
            field: None,
            field_expr: None,
            projected: false,
            residual_expr: Some(residual),
        });
        return Some(hidden_name);
    }
    None
}

/// Resolve a grouped-query reference to its output name.
/// Tries identifier lookup first, then bare aggregate function matching,
/// then residual expression matching against existing metrics.
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
    // 1b. Try the supported searched-CASE group key encoding so unnamed
    // CASE GROUP BY columns can still be referenced by the same expression
    // in ORDER BY / HAVING without leaking the internal encoded key.
    if let Some(name) = extract_supported_group_key(expr)
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
    // 3. Try matching the expression as a residual tree against existing metrics.
    //    This handles ORDER BY ROUND(AVG(price), 1) when the SELECT has the same
    //    expression — we build a temporary residual tree and compare structurally.
    {
        let mut scratch = metrics.to_vec();
        if let Ok(Some(residual)) = try_build_residual_expr(expr, &mut scratch) {
            for metric in metrics {
                if let Some(ref existing) = metric.residual_expr
                    && *existing == residual
                {
                    return Some(metric.output_name.clone());
                }
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
    // Simple aggregate detection — used by HAVING/ORDER BY resolution.
    try_parse_direct_aggregate(expr, alias)
}

/// Try to extract a projected metric from a SELECT expression.
/// Handles bare aggregates, scalar wrappers (ROUND, CAST), and arbitrary
/// arithmetic combining multiple aggregates (AVG(x) + AVG(y)).
///
/// Any inner aggregates are registered as hidden metrics in `metrics`.
/// Returns a single projected metric with an optional `residual_expr`
/// tree that references those hidden metrics.
fn extract_projected_metric(
    expr: &Expr,
    alias: Option<String>,
    metrics: &mut Vec<SqlGroupedMetric>,
) -> Result<Option<SqlGroupedMetric>> {
    // Fast path: bare aggregate like AVG(x)
    if let Some(metric) = try_parse_direct_aggregate(expr, alias.clone())? {
        return Ok(Some(metric));
    }

    // Build a ResidualExpr tree, extracting aggregates as hidden metrics.
    if let Some(residual) = try_build_residual_expr(expr, metrics)? {
        let output_name = alias.unwrap_or_else(|| format!("__expr_{}", metrics.len()));
        Ok(Some(SqlGroupedMetric {
            output_name,
            function: SqlGroupedMetricFunction::Avg, // placeholder, not used when residual_expr is Some
            field: None,
            field_expr: None,
            projected: true,
            residual_expr: Some(residual),
        }))
    } else {
        Ok(None)
    }
}

/// Recursively build a `ResidualExpr` from a SQL expression.
/// Aggregates (COUNT/SUM/AVG/MIN/MAX) are extracted into `metrics` and
/// referenced via `ResidualExpr::MetricRef`.
fn try_build_residual_expr(
    expr: &Expr,
    metrics: &mut Vec<SqlGroupedMetric>,
) -> Result<Option<ResidualExpr>> {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_lowercase();
            let args = match &f.args {
                FunctionArguments::List(list) => &list.args,
                _ => return Ok(None),
            };
            match name.as_str() {
                "round" => {
                    if args.len() != 2 {
                        return Ok(None);
                    }
                    let inner = match &args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                        _ => return Ok(None),
                    };
                    let precision = match &args[1] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            expr_to_f64(e).map(|v| v as i32)
                        }
                        _ => None,
                    };
                    let Some(precision) = precision else {
                        return Ok(None);
                    };
                    let inner_expr = try_build_residual_expr(inner, metrics)?;
                    Ok(inner_expr.map(|e| ResidualExpr::Round(Box::new(e), precision)))
                }
                "count" | "sum" | "avg" | "min" | "max" => {
                    if let Some(metric) = try_parse_direct_aggregate(expr, None)? {
                        let ref_name = ensure_grouped_metric(metrics, metric, false);
                        Ok(Some(ResidualExpr::MetricRef(ref_name)))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        }
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            use sqlparser::ast::DataType;
            let inner_r = try_build_residual_expr(inner, metrics)?;
            let Some(inner_r) = inner_r else {
                return Ok(None);
            };
            match data_type {
                DataType::Float(_)
                | DataType::Float4
                | DataType::Float8
                | DataType::Float32
                | DataType::Float64
                | DataType::Double(..)
                | DataType::DoublePrecision
                | DataType::Real
                | DataType::Numeric(_)
                | DataType::Decimal(_)
                | DataType::Dec(_) => Ok(Some(ResidualExpr::CastFloat(Box::new(inner_r)))),
                DataType::Integer(_)
                | DataType::Int(_)
                | DataType::Int2(_)
                | DataType::Int4(_)
                | DataType::Int8(_)
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int128
                | DataType::Int256
                | DataType::BigInt(_)
                | DataType::SmallInt(_)
                | DataType::TinyInt(_) => Ok(Some(ResidualExpr::CastInt(Box::new(inner_r)))),
                _ => Ok(None),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let bin_op = match op {
                BinaryOperator::Plus => ResidualBinOp::Add,
                BinaryOperator::Minus => ResidualBinOp::Sub,
                BinaryOperator::Multiply => ResidualBinOp::Mul,
                BinaryOperator::Divide => ResidualBinOp::Div,
                _ => return Ok(None),
            };
            let left_r = try_build_residual_expr(left, metrics)?;
            let right_r = try_build_residual_expr(right, metrics)?;
            match (left_r, right_r) {
                (Some(l), Some(r)) => Ok(Some(ResidualExpr::BinaryOp(
                    Box::new(l),
                    bin_op,
                    Box::new(r),
                ))),
                _ => Ok(None),
            }
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => {
            let inner_r = try_build_residual_expr(inner, metrics)?;
            Ok(inner_r.map(|e| ResidualExpr::Negate(Box::new(e))))
        }
        Expr::Value(v) => {
            if let Some(val) = match &v.value {
                SqlValue::Number(n, _) => n.parse::<f64>().ok(),
                _ => None,
            } {
                Ok(Some(ResidualExpr::Literal(val)))
            } else {
                Ok(None)
            }
        }
        Expr::Nested(inner) => try_build_residual_expr(inner, metrics),
        _ => Ok(None),
    }
}

/// Parse a bare aggregate function: COUNT/SUM/AVG/MIN/MAX.
fn try_parse_direct_aggregate(
    expr: &Expr,
    alias: Option<String>,
) -> Result<Option<SqlGroupedMetric>> {
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
            let inner_expr = match &args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
                FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                    if let FunctionArgExpr::Expr(expr) = arg {
                        expr
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            };
            // Try bare field first
            if let Some(field) = aggregate_arg_to_field(inner_expr) {
                let function = match normalized.as_str() {
                    "sum" => SqlGroupedMetricFunction::Sum,
                    "avg" => SqlGroupedMetricFunction::Avg,
                    "min" => SqlGroupedMetricFunction::Min,
                    "max" => SqlGroupedMetricFunction::Max,
                    _ => unreachable!(),
                };
                return Ok(Some(SqlGroupedMetric {
                    output_name: alias.unwrap_or_else(|| format!("{}_{}", normalized, field)),
                    function,
                    field: Some(field),
                    field_expr: None,
                    projected: true,
                    residual_expr: None,
                }));
            }
            // Try binary expression: AGG(a + b), AGG(a / b), etc.
            if let Some(field_expr) = aggregate_arg_to_expr(inner_expr) {
                let function = match normalized.as_str() {
                    "sum" => SqlGroupedMetricFunction::Sum,
                    "avg" => SqlGroupedMetricFunction::Avg,
                    "min" => SqlGroupedMetricFunction::Min,
                    "max" => SqlGroupedMetricFunction::Max,
                    _ => unreachable!(),
                };
                let default_name = format!("{}_{}", normalized, field_expr.display_fragment());
                return Ok(Some(SqlGroupedMetric {
                    output_name: alias.unwrap_or(default_name),
                    function,
                    field: None,
                    field_expr: Some(field_expr),
                    projected: true,
                    residual_expr: None,
                }));
            }
            return Ok(None);
        }
        _ => return Ok(None),
    };

    Ok(Some(SqlGroupedMetric {
        output_name: alias.unwrap_or(default_name),
        function,
        field,
        field_expr: None,
        projected: true,
        residual_expr: None,
    }))
}

/// Resolve the field name for an aggregate argument, seeing through CAST.
/// `AVG(CAST("tips" AS FLOAT))` → `"tips"`, `AVG(price)` → `"price"`.
fn aggregate_arg_to_field(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => expr_to_field_name(expr),
        Expr::Cast { expr: inner, .. } => aggregate_arg_to_field(inner),
        _ => None,
    }
}

/// Try to parse an arithmetic expression over field references: `a + b`,
/// `(a - b) / a`, etc. Returns a `MetricFieldExpr` when all leaves are field
/// references (optionally through CAST / nesting) and every operator is one of
/// `+`, `-`, `*`, `/`.
fn aggregate_arg_to_expr(expr: &Expr) -> Option<crate::search::MetricFieldExpr> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            aggregate_arg_to_field(expr).map(crate::search::MetricFieldExpr::field)
        }
        Expr::Cast { expr: inner, .. } | Expr::Nested(inner) => aggregate_arg_to_expr(inner),
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                BinaryOperator::Plus => crate::search::MetricFieldOp::Add,
                BinaryOperator::Minus => crate::search::MetricFieldOp::Sub,
                BinaryOperator::Multiply => crate::search::MetricFieldOp::Mul,
                BinaryOperator::Divide => crate::search::MetricFieldOp::Div,
                _ => return None,
            };
            let left_expr = aggregate_arg_to_expr(left)?;
            let right_expr = aggregate_arg_to_expr(right)?;
            Some(crate::search::MetricFieldExpr::binary(
                left_expr, op, right_expr,
            ))
        }
        _ => None,
    }
}

fn ensure_grouped_metric(
    metrics: &mut Vec<SqlGroupedMetric>,
    parsed: SqlGroupedMetric,
    projected: bool,
) -> String {
    // Residual projected metrics are synthetic containers with placeholder
    // (function, field) values.  Skip dedup so different residual expressions
    // don't collide on the same placeholder key.
    if parsed.residual_expr.is_none()
        && let Some(idx) = metrics.iter().position(|metric| {
            metric.function == parsed.function
                && metric.field == parsed.field
                && metric.field_expr == parsed.field_expr
        })
    {
        if projected {
            if !metrics[idx].projected {
                let old_name = metrics[idx].output_name.clone();
                metrics[idx].projected = true;
                metrics[idx].output_name = parsed.output_name.clone();
                metrics[idx].residual_expr = parsed.residual_expr;

                // Cascade the rename to all MetricRef references in existing
                // residual expression trees so they don't point at a stale name.
                let new_name = metrics[idx].output_name.clone();
                if old_name != new_name {
                    for m in metrics.iter_mut() {
                        if let Some(ref mut expr) = m.residual_expr {
                            rename_metric_refs(expr, &old_name, &new_name);
                        }
                    }
                }
            } else if metrics[idx].output_name != parsed.output_name {
                let canonical_name = metrics[idx].output_name.clone();
                metrics.push(SqlGroupedMetric {
                    output_name: parsed.output_name.clone(),
                    function: parsed.function,
                    field: None,
                    field_expr: None,
                    projected: true,
                    residual_expr: Some(ResidualExpr::MetricRef(canonical_name)),
                });
                return parsed.output_name;
            }
        }
        return metrics[idx].output_name.clone();
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
        field_expr: parsed.field_expr,
        projected,
        residual_expr: parsed.residual_expr,
    });
    output_name
}

/// Recursively update MetricRef names in a ResidualExpr tree after a metric
/// is renamed (e.g. when a hidden metric is promoted to a user-visible alias).
fn rename_metric_refs(expr: &mut ResidualExpr, old_name: &str, new_name: &str) {
    match expr {
        ResidualExpr::MetricRef(name) => {
            if name.as_str() == old_name {
                *name = new_name.to_string();
            }
        }
        ResidualExpr::Round(inner, _)
        | ResidualExpr::CastFloat(inner)
        | ResidualExpr::CastInt(inner)
        | ResidualExpr::Negate(inner) => rename_metric_refs(inner, old_name, new_name),
        ResidualExpr::BinaryOp(left, _, right) => {
            rename_metric_refs(left, old_name, new_name);
            rename_metric_refs(right, old_name, new_name);
        }
        ResidualExpr::Literal(_) => {}
    }
}

fn residual_contains_metric(expr: &ResidualExpr) -> bool {
    match expr {
        ResidualExpr::MetricRef(_) => true,
        ResidualExpr::Round(inner, _)
        | ResidualExpr::CastFloat(inner)
        | ResidualExpr::CastInt(inner)
        | ResidualExpr::Negate(inner) => residual_contains_metric(inner),
        ResidualExpr::BinaryOp(left, _, right) => {
            residual_contains_metric(left) || residual_contains_metric(right)
        }
        ResidualExpr::Literal(_) => false,
    }
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
    // First try a bare aggregate: COUNT(*), AVG(x), etc.
    if let Some(metric) = parse_grouped_metric(expr, None)? {
        ensure_grouped_metric(metrics, metric, false);
        return Ok(());
    }
    // Then try building a residual tree — any inner aggregates get registered
    // as hidden metrics via try_build_residual_expr → ensure_grouped_metric.
    let _ = try_build_residual_expr(expr, metrics)?;
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

fn collect_and_branches<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_branches(left, out);
            collect_and_branches(right, out);
        }
        Expr::Nested(inner) => collect_and_branches(inner, out),
        other => out.push(other),
    }
}

fn parse_pushdown_predicate(expr: &Expr) -> Result<Option<crate::search::QueryClause>> {
    // Unwrap parenthesized expressions
    if let Expr::Nested(inner) = expr {
        return parse_pushdown_predicate(inner);
    }

    // Nested AND conjunction inside an OR branch: push if every branch is individually pushable.
    if matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::And,
            ..
        }
    ) {
        let mut branches = Vec::new();
        collect_and_branches(expr, &mut branches);
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
                filter: clauses,
                ..Default::default()
            },
        )));
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

/// Returns true if the SELECT clause contains any SQL aggregate function
/// (COUNT, SUM, AVG, MIN, MAX) at any nesting depth.  Used to detect
/// ungrouped-aggregate fallback where the 100K TopDocs cap would silently
/// produce wrong results.  Does NOT need to parse the full expression —
/// just finding one aggregate anywhere is sufficient.
fn select_contains_aggregate(select: &Select) -> bool {
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => continue,
        };
        if expr_contains_aggregate(expr) {
            return true;
        }
    }
    false
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_lowercase();
            if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
                return true;
            }
            // Check inside function arguments for nested aggregates
            if let FunctionArguments::List(list) = &f.args {
                for arg in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = arg
                        && expr_contains_aggregate(inner)
                    {
                        return true;
                    }
                }
            }
            false
        }
        Expr::Nested(inner) => expr_contains_aggregate(inner),
        Expr::UnaryOp { expr, .. } => expr_contains_aggregate(expr),
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand
                && expr_contains_aggregate(op)
            {
                return true;
            }
            for cond in conditions {
                let sqlparser::ast::CaseWhen { condition, result } = cond;
                if expr_contains_aggregate(condition) || expr_contains_aggregate(result) {
                    return true;
                }
            }
            if let Some(e) = else_result
                && expr_contains_aggregate(e)
            {
                return true;
            }
            false
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_aggregate(inner),
        Expr::Between {
            expr: e, low, high, ..
        } => {
            expr_contains_aggregate(e)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr: e, list, .. } => {
            expr_contains_aggregate(e) || list.iter().any(expr_contains_aggregate)
        }
        _ => false,
    }
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
    fn grouped_expression_aggregate_preserves_field_expr_into_search_request() {
        let plan = plan_sql(
            "idx",
            "SELECT author, SUM(price + cost) AS gross FROM idx GROUP BY author ORDER BY gross DESC",
        )
        .unwrap();

        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.metrics.len(), 1);
        assert!(grouped.metrics[0].field.is_none());

        let expr = grouped.metrics[0]
            .field_expr
            .as_ref()
            .expect("expression aggregate must preserve field_expr");
        assert_eq!(
            expr,
            &crate::search::MetricFieldExpr::binary(
                crate::search::MetricFieldExpr::field("price"),
                crate::search::MetricFieldOp::Add,
                crate::search::MetricFieldExpr::field("cost"),
            )
        );

        let req = plan.to_search_request(false);
        let crate::search::AggregationRequest::GroupedMetrics(params) = req
            .aggs
            .get(INTERNAL_SQL_GROUPED_AGG)
            .expect("grouped agg missing")
        else {
            panic!("expected grouped metrics request");
        };
        let search_expr = params.metrics[0]
            .field_expr
            .as_ref()
            .expect("search request must retain field_expr");
        assert_eq!(search_expr, expr);
    }

    #[test]
    fn grouped_nested_expression_aggregate_stays_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT hvfhs_license_num, AVG((base_passenger_fare - driver_pay) / base_passenger_fare) AS platform_margin FROM idx GROUP BY hvfhs_license_num",
        )
        .unwrap();

        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped.metrics.len(), 1);
        assert!(grouped.metrics[0].field.is_none());
        assert_eq!(
            grouped.metrics[0].field_expr,
            Some(crate::search::MetricFieldExpr::binary(
                crate::search::MetricFieldExpr::binary(
                    crate::search::MetricFieldExpr::field("base_passenger_fare"),
                    crate::search::MetricFieldOp::Sub,
                    crate::search::MetricFieldExpr::field("driver_pay"),
                ),
                crate::search::MetricFieldOp::Div,
                crate::search::MetricFieldExpr::field("base_passenger_fare"),
            ))
        );
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
    fn computed_expression_uses_grouped_partials() {
        // SUM(upvotes) * 1.0 / COUNT(*) is extracted into hidden metrics
        // with a residual expression tree evaluated after merge.
        let plan = plan_sql(
            "hackernews",
            "SELECT author, COUNT(*) AS posts, SUM(upvotes) * 1.0 / COUNT(*) AS upvotes_per_post FROM hackernews GROUP BY author HAVING posts >= 100 ORDER BY upvotes_per_post DESC LIMIT 20",
        ).unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.limit_pushed_down);
    }

    #[test]
    fn computed_expression_with_group_by_no_limit_pushdown() {
        // GROUP BY + computed aggregate + LIMIT must NOT push LIMIT to Tantivy.
        // Grouped partials handle ORDER BY + LIMIT natively.
        let plan = plan_sql(
            "idx",
            "SELECT author, SUM(price) * 1.0 / COUNT(*) AS avg_p FROM idx GROUP BY author ORDER BY avg_p DESC LIMIT 5",
        ).unwrap();
        assert!(plan.uses_grouped_partials());
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
    fn max_minus_min_group_by_uses_grouped_partials() {
        // MAX(x) - MIN(x) is extracted into hidden metrics with a residual.
        let plan = plan_sql(
            "idx",
            "SELECT author, MAX(price) - MIN(price) AS spread FROM idx GROUP BY author ORDER BY spread DESC LIMIT 3",
        ).unwrap();
        assert!(plan.uses_grouped_partials());
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
    fn searched_case_hour_bucket_uses_grouped_partials() {
        let sql = "SELECT CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
                WHEN pickup_datetime >= '2025-01-05T17:00:00' AND pickup_datetime < '2025-01-05T18:00:00' THEN '17'
            END AS hour_bucket,
            count(*) AS rides,
            avg(driver_pay) AS avg_driver_pay
        FROM idx
        WHERE airport_fee = 0
            AND (
                (pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00')
                OR (pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00')
                OR (pickup_datetime >= '2025-01-05T17:00:00' AND pickup_datetime < '2025-01-05T18:00:00')
            )
        GROUP BY CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
                WHEN pickup_datetime >= '2025-01-05T17:00:00' AND pickup_datetime < '2025-01-05T18:00:00' THEN '17'
            END
        ORDER BY hour_bucket";

        let plan = plan_sql("idx", sql).unwrap();
        assert!(plan.uses_grouped_partials());
        assert!(!plan.has_group_by_fallback);
        assert!(!plan.has_residual_predicates);
        assert_eq!(plan.pushed_filters.len(), 2);
        let grouped = plan.grouped_sql.expect("grouped sql plan");
        assert_eq!(grouped.group_columns.len(), 1);
        assert_eq!(grouped.group_columns[0].output_name, "hour_bucket");
        assert!(crate::search::is_derived_group_key(
            &grouped.group_columns[0].source_name
        ));
    }

    #[test]
    fn searched_case_with_non_literal_else_falls_back() {
        let sql = "SELECT CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                ELSE pickup_datetime
            END AS hour_bucket,
            count(*) AS rides
        FROM idx
        GROUP BY CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                ELSE pickup_datetime
            END
        ORDER BY hour_bucket";

        let plan = plan_sql("idx", sql).unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn unaliased_searched_case_group_column_uses_sql_name() {
        let sql = "SELECT CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
            END,
            count(*) AS rides
        FROM idx
        GROUP BY CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
            END
        ORDER BY CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
            END";

        let plan = plan_sql("idx", sql).unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        assert_eq!(grouped.group_columns.len(), 1);
        assert_eq!(grouped.order_by.len(), 1);
        assert!(!crate::search::is_derived_group_key(
            &grouped.group_columns[0].output_name
        ));
        let output_name = grouped.group_columns[0].output_name.to_ascii_lowercase();
        assert!(output_name.contains("case"));
        assert!(output_name.contains("pickup_datetime"));
        assert_eq!(
            grouped.order_by[0].output_name,
            grouped.group_columns[0].output_name
        );
    }

    #[test]
    fn searched_case_with_literal_else_stays_on_grouped_partials() {
        let sql = "SELECT CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
                ELSE 'other'
            END AS hour_bucket,
            count(*) AS rides
        FROM idx
        GROUP BY CASE
                WHEN pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00' THEN '08'
                WHEN pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00' THEN '12'
                ELSE 'other'
            END
        ORDER BY hour_bucket";

        let plan = plan_sql("idx", sql).unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "literal ELSE should keep the query on grouped partials"
        );
        assert!(!plan.has_group_by_fallback);
        let grouped = plan.grouped_sql.unwrap();
        assert_eq!(grouped.group_columns[0].output_name, "hour_bucket");
        let spec = crate::search::decode_derived_group_key(&grouped.group_columns[0].source_name)
            .expect("should decode derived key");
        assert_eq!(spec.else_label.as_deref(), Some("other"));
        assert_eq!(spec.buckets.len(), 2);
    }

    #[test]
    fn searched_case_with_parenthesized_condition_falls_back() {
        // Parser may wrap the AND in Nested parens depending on SQL formatting.
        // collect_case_bucket_bounds does not handle Expr::Nested, so this
        // should cleanly fall back instead of panicking.
        let sql = "SELECT CASE
                WHEN (pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00') THEN '08'
            END AS bucket,
            count(*) AS rides
        FROM idx
        GROUP BY CASE
                WHEN (pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00') THEN '08'
            END";

        let plan = plan_sql("idx", sql).unwrap();
        // The parser may or may not strip the parens depending on sqlparser version.
        // If it keeps the Nested wrapper, this falls back cleanly.
        // If it strips them, it stays on grouped partials — both are correct.
        // The key assertion: no panic, and if it uses grouped partials the output name is sane.
        if plan.uses_grouped_partials() {
            let grouped = plan.grouped_sql.unwrap();
            assert!(!crate::search::is_derived_group_key(
                &grouped.group_columns[0].output_name
            ));
        } else {
            assert!(plan.has_group_by_fallback);
        }
    }

    #[test]
    fn and_inside_or_branches_pushed_as_nested_bool_filter() {
        // WHERE (x >= '08' AND x < '09') OR (x >= '12' AND x < '13')
        // Each OR branch is an AND conjunction that should push down as
        // Bool { filter: [Range, Range] }, wrapped in Bool { should: [...] }.
        let plan = plan_sql(
            "idx",
            "SELECT pickup_datetime FROM idx WHERE \
             (pickup_datetime >= '2025-01-05T08:00:00' AND pickup_datetime < '2025-01-05T09:00:00') \
             OR (pickup_datetime >= '2025-01-05T12:00:00' AND pickup_datetime < '2025-01-05T13:00:00')",
        )
        .unwrap();

        assert_eq!(
            plan.pushed_filters.len(),
            1,
            "should produce one Bool{{should}} filter"
        );
        assert!(!plan.has_residual_predicates);
        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Bool(bq) => {
                assert_eq!(bq.should.len(), 2, "two OR branches");
                // Each branch should be a Bool { filter: [Range, Range] }
                for branch in &bq.should {
                    match branch {
                        crate::search::QueryClause::Bool(inner) => {
                            assert_eq!(
                                inner.filter.len(),
                                2,
                                "each AND branch has two range filters"
                            );
                            for f in &inner.filter {
                                assert!(
                                    matches!(f, crate::search::QueryClause::Range(_)),
                                    "expected Range filter, got: {f:?}"
                                );
                            }
                        }
                        other => panic!("expected inner Bool filter, got: {other:?}"),
                    }
                }
            }
            other => panic!("expected outer Bool should, got: {other:?}"),
        }
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

    #[test]
    fn wrapped_aggregate_uses_grouped_partials_with_residual() {
        // ROUND(AVG(CAST(price AS FLOAT)), 2) — the inner AVG(CAST(price...))
        // is extracted as a hidden metric; the ROUND is a residual expression.
        let plan = plan_sql(
            "idx",
            "SELECT ROUND(AVG(CAST(price AS FLOAT)), 2) AS avg_price FROM idx",
        )
        .unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "wrapped aggregate should use grouped partials"
        );
        assert!(!plan.has_group_by_fallback);
        assert!(!plan.has_ungrouped_aggregate_fallback);
        let grouped = plan.grouped_sql.unwrap();
        // The projected metric should have a residual_expr
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].output_name, "avg_price");
        assert!(projected[0].residual_expr.is_some());
    }

    #[test]
    fn direct_ungrouped_aggregate_uses_grouped_partials_not_fallback() {
        let plan = plan_sql("idx", "SELECT AVG(price) AS avg_price FROM idx").unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "direct AVG should use grouped partials"
        );
        assert!(!plan.has_ungrouped_aggregate_fallback);
        let grouped = plan.grouped_sql.unwrap();
        assert!(grouped.metrics[0].residual_expr.is_none());
    }

    #[test]
    fn non_aggregate_query_does_not_trigger_fallback() {
        let plan = plan_sql("idx", "SELECT price, title FROM idx LIMIT 10").unwrap();
        assert!(!plan.has_ungrouped_aggregate_fallback);
        assert!(!plan.has_group_by_fallback);
    }

    #[test]
    fn cast_avg_as_int_uses_grouped_partials_with_residual_expr() {
        let plan = plan_sql(
            "idx",
            "SELECT CAST(AVG(price) AS INTEGER) AS avg_int FROM idx",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected[0].output_name, "avg_int");
        assert!(projected[0].residual_expr.is_some());
    }

    #[test]
    fn cast_to_float_around_avg_is_residual_expr() {
        let plan = plan_sql("idx", "SELECT CAST(AVG(price) AS FLOAT) AS avg_f FROM idx").unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert!(projected[0].residual_expr.is_some());
    }

    #[test]
    fn grouped_partials_with_round_on_group_by() {
        let plan = plan_sql(
            "idx",
            "SELECT category, ROUND(AVG(price), 1) AS avg_p FROM idx GROUP BY category",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        assert_eq!(grouped.group_columns.len(), 1);
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected[0].output_name, "avg_p");
        assert!(projected[0].residual_expr.is_some());
    }

    #[test]
    fn avg_plus_avg_uses_grouped_partials_with_binary_residual() {
        let plan = plan_sql("idx", "SELECT AVG(price) + AVG(cost) AS spread FROM idx").unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "AVG(x) + AVG(y) should use grouped partials"
        );
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].output_name, "spread");
        assert!(projected[0].residual_expr.is_some());
        // There should be hidden metrics for AVG(price) and AVG(cost)
        let hidden: Vec<_> = grouped.metrics.iter().filter(|m| !m.projected).collect();
        assert_eq!(hidden.len(), 2);
    }

    #[test]
    fn sum_divided_by_count_uses_grouped_partials() {
        let plan = plan_sql("idx", "SELECT SUM(total) / COUNT(*) AS manual_avg FROM idx").unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected[0].output_name, "manual_avg");
        assert!(projected[0].residual_expr.is_some());
    }

    #[test]
    fn round_avg_plus_avg_uses_grouped_partials() {
        let plan = plan_sql(
            "idx",
            "SELECT ROUND(AVG(price) + AVG(cost), 2) AS spread FROM idx",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
    }

    // ── Residual expression correctness regressions ────────────────────

    #[test]
    fn residual_metric_ref_survives_promotion_to_projected() {
        // SELECT ROUND(AVG(price), 2) AS rounded, AVG(price) AS avg_price
        // Processing order: ROUND creates a hidden metric for AVG(price), then
        // the bare AVG(price) AS avg_price promotes it with a new name.
        // The MetricRef inside ROUND must track the rename.
        let plan = plan_sql(
            "idx",
            "SELECT ROUND(AVG(price), 2) AS rounded, AVG(price) AS avg_price FROM idx",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();

        let rounded = grouped
            .metrics
            .iter()
            .find(|m| m.output_name == "rounded")
            .expect("rounded metric must exist");
        assert!(rounded.residual_expr.is_some());

        // Collect all MetricRef names from the residual expression tree.
        fn collect_refs(expr: &ResidualExpr, out: &mut Vec<String>) {
            match expr {
                ResidualExpr::MetricRef(name) => out.push(name.clone()),
                ResidualExpr::Round(inner, _)
                | ResidualExpr::CastFloat(inner)
                | ResidualExpr::CastInt(inner)
                | ResidualExpr::Negate(inner) => collect_refs(inner, out),
                ResidualExpr::BinaryOp(l, _, r) => {
                    collect_refs(l, out);
                    collect_refs(r, out);
                }
                ResidualExpr::Literal(_) => {}
            }
        }
        let mut refs = Vec::new();
        collect_refs(rounded.residual_expr.as_ref().unwrap(), &mut refs);
        for r in &refs {
            assert!(
                grouped.metrics.iter().any(|m| &m.output_name == r),
                "MetricRef('{}') must reference an existing metric, found: {:?}",
                r,
                grouped
                    .metrics
                    .iter()
                    .map(|m| &m.output_name)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn multiple_residual_projections_do_not_collide() {
        // Two different residual expressions must produce distinct projected metrics.
        let plan = plan_sql(
            "idx",
            "SELECT AVG(price) + 1 AS a, AVG(cost) + 1 AS b FROM idx",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(
            projected.len(),
            2,
            "both residual projections must be distinct"
        );
        assert!(projected.iter().any(|m| m.output_name == "a"));
        assert!(projected.iter().any(|m| m.output_name == "b"));
    }

    #[test]
    fn same_aggregate_in_two_residual_expressions() {
        // SELECT AVG(price) + 1 AS a, AVG(price) * 2 AS b
        // Both reference the same AVG(price) hidden metric — only one hidden entry.
        let plan = plan_sql(
            "idx",
            "SELECT AVG(price) + 1 AS a, AVG(price) * 2 AS b FROM idx",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let grouped = plan.grouped_sql.unwrap();
        let projected: Vec<_> = grouped.metrics.iter().filter(|m| m.projected).collect();
        assert_eq!(projected.len(), 2);
        assert!(projected.iter().any(|m| m.output_name == "a"));
        assert!(projected.iter().any(|m| m.output_name == "b"));
        // Only one hidden metric for AVG(price) — deduped correctly.
        let hidden: Vec<_> = grouped.metrics.iter().filter(|m| !m.projected).collect();
        assert_eq!(hidden.len(), 1);
        assert_eq!(hidden[0].function, SqlGroupedMetricFunction::Avg);
        assert_eq!(hidden[0].field.as_deref(), Some("price"));
    }

    #[test]
    fn grouped_order_by_ordinal_falls_back() {
        let plan = plan_sql(
            "idx",
            "SELECT author, COUNT(*) AS posts FROM idx GROUP BY author ORDER BY 1 DESC",
        )
        .unwrap();
        assert!(
            !plan.uses_grouped_partials(),
            "ORDER BY ordinal should not be misparsed as a constant grouped residual"
        );
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn shard_top_k_disabled_for_residual_order_by() {
        // ORDER BY on a residual metric alias must not enable shard-level top-K
        // pruning, which can't evaluate post-merge expressions per shard.
        let plan = plan_sql(
            "idx",
            "SELECT author, SUM(price) / COUNT(*) AS avg_p FROM idx \
             GROUP BY author ORDER BY avg_p DESC LIMIT 10",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());

        let req = plan.to_search_request(true);
        let agg_entry = req.aggs.values().next().expect("Expected grouped agg");
        if let crate::search::AggregationRequest::GroupedMetrics(params) = agg_entry {
            assert!(
                params.shard_top_k.is_none(),
                "shard_top_k must be None when ORDER BY targets a residual metric"
            );
        } else {
            panic!("Expected GroupedMetrics aggregation");
        }
    }

    #[test]
    fn shard_top_k_still_works_for_bare_metric_order_by() {
        // ORDER BY on a bare metric should still build ShardTopK when enabled.
        let plan = plan_sql(
            "idx",
            "SELECT author, COUNT(*) AS posts FROM idx \
             GROUP BY author ORDER BY posts DESC LIMIT 10",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
        let req = plan.to_search_request(true);
        let agg_entry = req.aggs.values().next().expect("Expected grouped agg");
        if let crate::search::AggregationRequest::GroupedMetrics(params) = agg_entry {
            assert!(
                params.shard_top_k.is_some(),
                "shard_top_k should be Some for bare metric ORDER BY"
            );
        } else {
            panic!("Expected GroupedMetrics aggregation");
        }
    }

    #[test]
    fn fallback_detector_catches_aggregate_in_is_null() {
        // AVG(price) IS NULL wraps an aggregate in a shape that grouped partials
        // can't handle. The safety net must still detect the aggregate.
        let plan = plan_sql("idx", "SELECT AVG(price) IS NULL AS is_null FROM idx").unwrap();
        assert!(
            !plan.uses_grouped_partials(),
            "IS NULL wrapper should not pass grouped partials"
        );
        assert!(
            plan.has_ungrouped_aggregate_fallback,
            "aggregate in IS NULL must be detected by the safety net"
        );
    }

    #[test]
    fn fallback_detector_catches_aggregate_in_between() {
        let plan = plan_sql(
            "idx",
            "SELECT AVG(price) BETWEEN 0 AND 100 AS in_range FROM idx",
        )
        .unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(
            plan.has_ungrouped_aggregate_fallback,
            "aggregate in BETWEEN must be detected by the safety net"
        );
    }

    // ── Aliasless residual ORDER BY / HAVING resolution ────────────────

    #[test]
    fn aliasless_residual_order_by_stays_on_grouped_partials() {
        // ORDER BY ROUND(AVG(price), 1) without an alias — the planner must
        // match the ORDER BY expression against the projected residual metric.
        let plan = plan_sql(
            "idx",
            "SELECT category, ROUND(AVG(price), 1) FROM idx \
             GROUP BY category ORDER BY ROUND(AVG(price), 1) DESC",
        )
        .unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "aliasless residual ORDER BY must stay on grouped partials"
        );
        let grouped = plan.grouped_sql.unwrap();
        assert!(!grouped.order_by.is_empty(), "ORDER BY must be resolved");
    }

    #[test]
    fn non_projected_residual_order_by_stays_on_grouped_partials() {
        // ORDER BY ROUND(AVG(price), 1) when the SELECT does NOT project it.
        // The planner must register a hidden residual metric for the ORDER BY.
        let plan = plan_sql(
            "idx",
            "SELECT category FROM idx \
             GROUP BY category ORDER BY ROUND(AVG(price), 1) DESC",
        )
        .unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "non-projected residual ORDER BY must stay on grouped partials"
        );
        let grouped = plan.grouped_sql.unwrap();
        assert!(!grouped.order_by.is_empty(), "ORDER BY must be resolved");
        // The hidden residual metric must exist but not be projected
        let hidden_residuals: Vec<_> = grouped
            .metrics
            .iter()
            .filter(|m| !m.projected && m.residual_expr.is_some())
            .collect();
        assert!(
            !hidden_residuals.is_empty(),
            "must have a hidden residual metric for ORDER BY"
        );
    }

    #[test]
    fn aliasless_residual_order_by_with_alias_also_works() {
        // Same but with an alias — must still work (regression guard).
        let plan = plan_sql(
            "idx",
            "SELECT category, ROUND(AVG(price), 1) AS avg_p FROM idx \
             GROUP BY category ORDER BY avg_p DESC",
        )
        .unwrap();
        assert!(plan.uses_grouped_partials());
    }

    #[test]
    fn non_projected_residual_having_stays_on_grouped_partials() {
        // HAVING ROUND(AVG(price), 1) > 10 when the SELECT does NOT project it.
        // The planner must register a hidden residual metric for the HAVING filter.
        let plan = plan_sql(
            "idx",
            "SELECT category FROM idx \
             GROUP BY category HAVING ROUND(AVG(price), 1) > 10",
        )
        .unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "non-projected residual HAVING must stay on grouped partials"
        );
        let grouped = plan.grouped_sql.unwrap();
        assert!(!grouped.having.is_empty(), "HAVING must be resolved");
        let hidden_residuals: Vec<_> = grouped
            .metrics
            .iter()
            .filter(|m| !m.projected && m.residual_expr.is_some())
            .collect();
        assert!(
            !hidden_residuals.is_empty(),
            "must have a hidden residual metric for HAVING"
        );
    }

    #[test]
    fn having_with_arithmetic_residual_stays_on_grouped_partials() {
        // HAVING AVG(price) + 1 > 10 — arithmetic on an aggregate in HAVING.
        let plan = plan_sql(
            "idx",
            "SELECT category, COUNT(*) AS cnt FROM idx \
             GROUP BY category HAVING AVG(price) + 1 > 10",
        )
        .unwrap();
        assert!(
            plan.uses_grouped_partials(),
            "arithmetic residual HAVING must stay on grouped partials"
        );
        let grouped = plan.grouped_sql.unwrap();
        assert!(!grouped.having.is_empty());
    }
}
