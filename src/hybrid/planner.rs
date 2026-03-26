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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedSqlPlan {
    pub group_columns: Vec<SqlGroupColumn>,
    pub metrics: Vec<SqlGroupedMetric>,
    pub order_by: Vec<SqlOrderBy>,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub index_name: String,
    pub original_sql: String,
    pub rewritten_sql: String,
    pub text_match: Option<TextMatchPredicate>,
    pub pushed_filters: Vec<crate::search::QueryClause>,
    pub required_columns: Vec<String>,
    pub group_by_columns: Vec<String>,
    pub has_residual_predicates: bool,
    pub selects_all_columns: bool,
    pub grouped_sql: Option<GroupedSqlPlan>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub limit_pushed_down: bool,
}

impl QueryPlan {
    pub fn uses_grouped_partials(&self) -> bool {
        self.grouped_sql.is_some()
    }

    pub fn to_search_request(&self) -> crate::search::SearchRequest {
        let query = if self.text_match.is_none() && self.pushed_filters.is_empty() {
            crate::search::QueryClause::MatchAll(serde_json::Value::Object(Default::default()))
        } else {
            let mut bool_query = crate::search::BoolQuery::default();

            if let Some(text_match) = &self.text_match {
                bool_query
                    .must
                    .push(crate::search::QueryClause::Match(HashMap::from([(
                        text_match.field.clone(),
                        serde_json::Value::String(text_match.query.clone()),
                    )])));
            }

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
            sort: vec![],
            aggs,
        }
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
        if let Some(tm) = &self.text_match {
            tantivy_stage["text_match"] = serde_json::json!({
                "field": tm.field,
                "query": tm.query,
            });
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
            stages.push(serde_json::json!({
                "stage": 4,
                "name": "final_grouped_sql_shape",
                "description": "Apply final projection and ordering to merged grouped results",
            }));
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
                "text_match": self.text_match.as_ref().map(|tm| serde_json::json!({
                    "field": tm.field,
                    "query": tm.query,
                })),
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
    let limit = parse_limit_expr(&query.limit);
    let offset = parse_offset_expr(&query.offset);

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
    let (text_match, pushed_filters) = extract_pushdowns(&mut select.selection)?;
    let has_residual_predicates = select.selection.is_some();
    rewrite_table_name(select);
    let required_columns = collect_required_columns(select, order_by.as_ref());
    let group_by_columns = collect_group_by_columns(select);
    let grouped_sql = extract_grouped_sql_plan(
        select,
        order_by.as_ref(),
        has_residual_predicates,
        selects_all_columns,
        limit,
        offset,
        &group_by_columns,
    )?;

    let limit_pushed_down = limit.is_some()
        && !has_residual_predicates
        && grouped_sql.is_none()
        && !selects_all_columns
        && is_order_by_score_only(order_by.as_ref());

    Ok(QueryPlan {
        index_name: index_name.to_string(),
        original_sql: sql.to_string(),
        rewritten_sql: statement.to_string(),
        text_match,
        pushed_filters,
        required_columns,
        group_by_columns,
        has_residual_predicates,
        selects_all_columns,
        grouped_sql,
        limit,
        offset,
        limit_pushed_down,
    })
}

fn extract_grouped_sql_plan(
    select: &Select,
    order_by: Option<&sqlparser::ast::OrderBy>,
    has_residual_predicates: bool,
    selects_all_columns: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    group_by_columns: &[String],
) -> Result<Option<GroupedSqlPlan>> {
    if group_by_columns.is_empty()
        || has_residual_predicates
        || selects_all_columns
        || select.having.is_some()
        || limit.is_some()
        || offset.is_some()
    {
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

    Ok(Some(GroupedSqlPlan {
        group_columns,
        metrics,
        order_by: order_items,
    }))
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

fn parse_limit_expr(limit_expr: &Option<Expr>) -> Option<usize> {
    let expr = limit_expr.as_ref()?;
    match expr {
        Expr::Value(value) => match &value.value {
            SqlValue::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn parse_offset_expr(offset: &Option<sqlparser::ast::Offset>) -> Option<usize> {
    let offset = offset.as_ref()?;
    match &offset.value {
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
) -> Result<(Option<TextMatchPredicate>, Vec<crate::search::QueryClause>)> {
    let Some(expr) = selection.take() else {
        return Ok((None, Vec::new()));
    };

    let mut predicates = Vec::new();
    split_conjunction(expr, &mut predicates);

    let mut text_match = None;
    let mut pushed_filters = Vec::new();
    let mut residual = Vec::new();
    for predicate in predicates {
        match parse_text_match(&predicate)? {
            Some(found) => {
                if text_match.is_some() {
                    bail!("Only one text_match(field, query) predicate is supported");
                }
                text_match = Some(found);
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
    Ok((text_match, pushed_filters))
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

fn parse_pushdown_predicate(expr: &Expr) -> Result<Option<crate::search::QueryClause>> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };

    let Some((field, value, flipped)) = extract_comparison_parts(left, right)? else {
        return Ok(None);
    };

    if field == "score" || field == "_id" {
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
) -> Vec<String> {
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
    columns.remove("score");
    columns.remove("_id");
    columns.into_iter().collect()
}

fn collect_group_by_columns(select: &Select) -> Vec<String> {
    let mut columns = BTreeSet::new();
    collect_group_by_exprs(select, &mut columns);
    columns.remove("score");
    columns.remove("_id");
    columns.into_iter().collect()
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
        _ => {}
    }
}
