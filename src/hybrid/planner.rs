use anyhow::{Context, Result, bail};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, OrderByKind, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeSet, HashMap};

const INTERNAL_TABLE_NAME: &str = "matched_rows";
const SQL_MATCH_LIMIT: usize = 100_000;

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
    /// Whether the SQL query references `score` in any projection, filter, or ordering.
    pub needs_score: bool,
    /// When ORDER BY is a single non-score column, capture (field, descending) for
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

    pub fn to_search_request(&self) -> crate::search::SearchRequest {
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
        let search_request = self.to_search_request();

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
        } else {
            "All projected columns can be read from Tantivy fast fields"
        };

        // Build pipeline stages
        let mut stages = Vec::new();

        // Stage 1: Tantivy search
        let mut tantivy_stage = serde_json::json!({
            "stage": 1,
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
                "stage": 2,
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
                    }))
                    .collect::<Vec<_>>(),
            }));
            stages.push(serde_json::json!({
                "stage": 3,
                "name": "grouped_partial_merge",
                "description": "Merge compact grouped partial states at the coordinator",
            }));
            let mut final_stage = serde_json::json!({
                "stage": 4,
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
                "stage": 2,
                "name": "fast_field_read",
                "description": "Read required columns directly from Tantivy fast fields (columnar storage)",
                "columns": self.required_columns,
            }));
        } else {
            stages.push(serde_json::json!({
                "stage": 2,
                "name": "source_materialization",
                "description": "Materialize full _source documents for SELECT * projection",
            }));
        }

        if self.grouped_sql.is_none() {
            // Stage 3: Arrow batch building
            stages.push(serde_json::json!({
                "stage": 3,
                "name": "arrow_batch",
                "description": "Build Arrow RecordBatch from extracted columns with score column",
            }));

            // Stage 4: DataFusion SQL execution
            let mut datafusion_stage = serde_json::json!({
                "stage": 4,
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

        serde_json::json!({
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
        })
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
    let order_by = query.order_by.clone();
    let (limit, offset) = extract_limit_offset(&query.limit_clause);

    let select = match query.body.as_mut() {
        SetExpr::Select(select) => select,
        _ => bail!("Only simple SELECT queries are supported"),
    };

    let source_table = extract_single_table(select)?;
    if source_table != index_name {
        bail!(
            "SQL FROM [{}] must match request index [{}]",
            source_table,
            index_name
        );
    }

    let selects_all_columns = projection_has_wildcard(select);
    let select_aliases = collect_select_aliases(select);
    let (text_matches, pushed_filters) = extract_pushdowns(&mut select.selection, &select_aliases)?;
    let has_residual_predicates = select.selection.is_some();
    rewrite_table_name(select);
    let (required_columns, needs_id, needs_score) =
        collect_required_columns(select, order_by.as_ref());
    let (group_by_columns, has_expression_group_by) = collect_group_by_columns(select);
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

    // Safety: if no data columns, _id, or score are needed and we're not on the
    // grouped partial or SELECT * paths, force needs_score so the Arrow batch has
    // the correct row count.  Edge case: `SELECT 1 FROM ... WHERE text_match(...)`.
    let needs_score = needs_score
        || (required_columns.is_empty()
            && !needs_id
            && !selects_all_columns
            && grouped_sql.is_none());

    let has_group_by_fallback =
        (!group_by_columns.is_empty() || has_expression_group_by) && grouped_sql.is_none();

    Ok(QueryPlan {
        index_name: index_name.to_string(),
        original_sql: sql.to_string(),
        rewritten_sql: statement.to_string(),
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
                    metrics.push(metric);
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
                    metrics.push(metric);
                    continue;
                }
                return Ok(None);
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
        }
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
                    let Some(name) = expr_to_field_name(&expr.expr) else {
                        return Ok(None);
                    };
                    let Some(output_name) = valid_names.get(&name).cloned() else {
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
                resolve_having_name(left, valid_names, metrics),
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
                resolve_having_name(right, valid_names, metrics),
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

/// Resolve a HAVING expression to its output name.
/// Tries identifier lookup first (e.g. `HAVING posts > 10`),
/// then aggregate function matching (e.g. `HAVING COUNT(*) > 10`).
fn resolve_having_name(
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
    }))
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
                .map(|name| name == "score" || name == "_score")
                .unwrap_or(false)
        }),
        OrderByKind::All(_) => false,
    }
}

/// Extract a single-column ORDER BY on a non-score field for fast-field sort pushdown.
/// Returns `Some((field_name, is_desc))` when the ORDER BY is a single data column
/// (not `score` or `_score`), enabling Tantivy's `TopDocs::order_by_fast_field()`.
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
    if name == "score" || name == "_score" {
        return None;
    }
    let desc = expr.options.asc == Some(false);
    Some((name, desc))
}

fn extract_single_table(select: &Select) -> Result<String> {
    if select.from.len() != 1 {
        bail!("Exactly one FROM table is supported");
    }

    let table = &select.from[0];
    if !table.joins.is_empty() {
        bail!("JOIN is not supported in the SQL search MVP");
    }

    match &table.relation {
        TableFactor::Table { name, .. } => {
            if name.0.len() != 1 {
                bail!("Qualified table names are not supported in the SQL search MVP");
            }
            match &name.0[0] {
                ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
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
        if predicate_references_alias(&predicate, select_aliases) {
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
fn predicate_references_alias(expr: &Expr, aliases: &BTreeSet<String>) -> bool {
    match expr {
        Expr::Identifier(ident) => aliases.contains(&ident.value),
        Expr::BinaryOp { left, right, .. } => {
            predicate_references_alias(left, aliases) || predicate_references_alias(right, aliases)
        }
        Expr::Between {
            expr: between_expr, ..
        } => predicate_references_alias(between_expr, aliases),
        Expr::InList { expr: in_expr, .. } => predicate_references_alias(in_expr, aliases),
        Expr::UnaryOp { expr, .. } => predicate_references_alias(expr, aliases),
        Expr::Nested(inner) => predicate_references_alias(inner, aliases),
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
        if field == "score" || field == "_score" || field == "_id" {
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
        if field == "score" || field == "_score" || field == "_id" {
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

    if field == "score" || field == "_score" || field == "_id" {
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

fn projection_has_wildcard(select: &Select) -> bool {
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    })
}

fn collect_required_columns(
    select: &Select,
    order_by: Option<&sqlparser::ast::OrderBy>,
) -> (Vec<String>, bool, bool) {
    let mut columns = BTreeSet::new();

    for item in &select.projection {
        collect_select_item_columns(item, &mut columns);
    }
    if let Some(selection) = &select.selection {
        collect_expr_columns(selection, &mut columns);
    }
    collect_group_by_exprs(select, &mut columns);
    if let Some(having) = &select.having {
        collect_expr_columns(having, &mut columns);
    }

    if let Some(order_by) = order_by {
        match &order_by.kind {
            OrderByKind::Expressions(exprs) => {
                for expr in exprs {
                    collect_expr_columns(&expr.expr, &mut columns);
                }
            }
            OrderByKind::All(_) => {}
        }
    }

    // Remove computed aliases (e.g., `count(*) AS total`) — these are not
    // data columns that can be read from storage. ORDER BY or HAVING may
    // reference them as identifiers, but they are resolved by DataFusion,
    // not by the fast-field reader.
    let aliases = collect_select_aliases(select);
    for alias in &aliases {
        columns.remove(alias);
    }

    columns.remove(INTERNAL_TABLE_NAME);

    // Detect whether _id and score are actually referenced before removing them
    let needs_id = columns.contains("_id");
    let needs_score = columns.contains("score") || columns.contains("_score");

    columns.remove("score");
    columns.remove("_score");
    columns.remove("_id");
    (columns.into_iter().collect(), needs_id, needs_score)
}

/// Returns (plain_column_names, has_expression_group_by).
/// `has_expression_group_by` is true when any GROUP BY item is NOT a plain column
/// reference (e.g., `GROUP BY LOWER(author)` or `GROUP BY year / 10`).
fn collect_group_by_columns(select: &Select) -> (Vec<String>, bool) {
    let mut columns = BTreeSet::new();
    let mut has_expression = false;
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                // Only accept plain column references for the grouped partials path.
                // Expression-based GROUP BY (e.g., LOWER(author), year / 10) must
                // fall through to DataFusion, which can evaluate them correctly.
                if let Some(name) = expr_to_field_name(expr) {
                    if name != "score" && name != "_id" {
                        columns.insert(name);
                    } else {
                        has_expression = true;
                    }
                } else {
                    has_expression = true;
                }
            }
        }
        sqlparser::ast::GroupByExpr::All(_) => {}
    }
    (columns.into_iter().collect(), has_expression)
}

fn collect_group_by_exprs(select: &Select, columns: &mut BTreeSet<String>) {
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                collect_expr_columns(expr, columns);
            }
        }
        sqlparser::ast::GroupByExpr::All(_) => {}
    }
}

fn collect_select_item_columns(item: &SelectItem, columns: &mut BTreeSet<String>) {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            collect_expr_columns(expr, columns)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
    }
}

/// Collect all SELECT aliases (e.g., `count(*) AS total` yields `"total"`).
/// These are computed columns that should not be sent to the fast-field reader.
fn collect_select_aliases(select: &Select) -> BTreeSet<String> {
    let mut aliases = BTreeSet::new();
    for item in &select.projection {
        if let SelectItem::ExprWithAlias { alias, .. } = item {
            aliases.insert(alias.value.clone());
        }
    }
    aliases
}

fn collect_expr_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.insert(ident.value.clone());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(ident) = idents.last() {
                columns.insert(ident.value.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, columns);
            collect_expr_columns(right, columns);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_expr_columns(expr, columns),
        Expr::Between {
            expr: between_expr,
            low,
            high,
            ..
        } => {
            collect_expr_columns(between_expr, columns);
            collect_expr_columns(low, columns);
            collect_expr_columns(high, columns);
        }
        Expr::InList {
            expr: in_expr,
            list,
            ..
        } => {
            collect_expr_columns(in_expr, columns);
            for item in list {
                collect_expr_columns(item, columns);
            }
        }
        Expr::Function(function) => {
            if let FunctionArguments::List(list) = &function.args {
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            collect_expr_columns(expr, columns)
                        }
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            if let FunctionArgExpr::Expr(expr) = arg {
                                collect_expr_columns(expr, columns)
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Cast { expr, .. } => {
            collect_expr_columns(expr, columns);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_expr_columns(op, columns);
            }
            for case_when in conditions {
                collect_expr_columns(&case_when.condition, columns);
                collect_expr_columns(&case_when.result, columns);
            }
            if let Some(el) = else_result {
                collect_expr_columns(el, columns);
            }
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_expr_columns(expr, columns);
            collect_expr_columns(pattern, columns);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let req = plan.to_search_request();
        assert_eq!(req.size, 10);
    }

    #[test]
    fn select_star_limit_with_offset_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" LIMIT 5 OFFSET 20").unwrap();
        assert!(plan.selects_all_columns);
        assert!(plan.limit_pushed_down);
        let req = plan.to_search_request();
        assert_eq!(req.size, 25); // limit + offset
    }

    #[test]
    fn select_star_limit_order_by_score_pushes_down() {
        let plan = plan_sql("idx", "SELECT * FROM \"idx\" ORDER BY score DESC LIMIT 4").unwrap();
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
    fn or_on_score_not_pushed() {
        let plan = plan_sql(
            "idx",
            "SELECT author FROM idx WHERE score > 0.5 OR author = 'pg'",
        )
        .unwrap();
        // score branch can't be pushed → whole OR stays residual
        assert!(plan.pushed_filters.is_empty());
        assert!(plan.has_residual_predicates);
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

        let request = plan.to_search_request();
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

        let request = plan.to_search_request();
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
}
