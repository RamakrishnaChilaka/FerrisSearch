//! SQL field name canonicalization and case-insensitive resolution.

use axum::{Json, http::StatusCode};
use serde_json::Value;
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    OrderByKind as SqlOrderByKind, Query as SqlQuery, Select as SqlSelect,
    SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, Statement as SqlStatement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};

pub(super) fn resolve_case_insensitive_mapping_name(
    field_name: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<Option<String>, (StatusCode, Json<Value>)> {
    if matches!(field_name, "_id" | "_score") || mappings.contains_key(field_name) {
        return Ok(Some(field_name.to_string()));
    }

    let matches: Vec<&String> = mappings
        .keys()
        .filter(|candidate| candidate.eq_ignore_ascii_case(field_name))
        .collect();

    match matches.as_slice() {
        [] => Ok(None),
        [only] => Ok(Some((*only).clone())),
        _ => {
            let candidates = matches
                .iter()
                .map(|candidate| candidate.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "ambiguous_column_reference_exception",
                format!(
                    "Unquoted SQL column [{}] is ambiguous. Quote the exact field name. Candidates: {}",
                    field_name, candidates
                ),
            ))
        }
    }
}

pub(super) fn canonicalize_sql_source_field_name(
    field_name: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<String, (StatusCode, Json<Value>)> {
    Ok(resolve_case_insensitive_mapping_name(field_name, mappings)?
        .unwrap_or_else(|| field_name.to_string()))
}

pub(super) fn canonicalize_query_field_map<T>(
    fields: &mut HashMap<String, T>,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if fields.is_empty() {
        return Ok(());
    }

    let old_fields = std::mem::take(fields);
    let mut canonical_fields = HashMap::with_capacity(old_fields.len());
    for (field_name, value) in old_fields {
        canonical_fields.insert(
            canonicalize_sql_source_field_name(&field_name, mappings)?,
            value,
        );
    }
    *fields = canonical_fields;
    Ok(())
}

pub(super) fn canonicalize_query_clause_fields(
    clause: &mut crate::search::QueryClause,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<(), (StatusCode, Json<Value>)> {
    match clause {
        crate::search::QueryClause::Term(fields)
        | crate::search::QueryClause::Match(fields)
        | crate::search::QueryClause::Wildcard(fields)
        | crate::search::QueryClause::Prefix(fields) => {
            canonicalize_query_field_map(fields, mappings)
        }
        crate::search::QueryClause::Range(fields) => canonicalize_query_field_map(fields, mappings),
        crate::search::QueryClause::Fuzzy(fields) => canonicalize_query_field_map(fields, mappings),
        crate::search::QueryClause::Bool(bool_query) => {
            for nested in &mut bool_query.must {
                canonicalize_query_clause_fields(nested, mappings)?;
            }
            for nested in &mut bool_query.should {
                canonicalize_query_clause_fields(nested, mappings)?;
            }
            for nested in &mut bool_query.must_not {
                canonicalize_query_clause_fields(nested, mappings)?;
            }
            for nested in &mut bool_query.filter {
                canonicalize_query_clause_fields(nested, mappings)?;
            }
            Ok(())
        }
        crate::search::QueryClause::MatchAll(_) | crate::search::QueryClause::MatchNone(_) => {
            Ok(())
        }
    }
}

pub(super) fn canonicalize_sql_field_list(
    fields: &mut Vec<String>,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<(), (StatusCode, Json<Value>)> {
    let mut canonical_fields = Vec::with_capacity(fields.len());
    let mut seen = HashSet::with_capacity(fields.len());
    for field_name in fields.drain(..) {
        let canonical = canonicalize_sql_source_field_name(&field_name, mappings)?;
        if seen.insert(canonical.clone()) {
            canonical_fields.push(canonical);
        }
    }
    *fields = canonical_fields;
    Ok(())
}

pub(super) fn canonicalize_group_key_name(
    field_name: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<String, (StatusCode, Json<Value>)> {
    if let Some(mut derived) = crate::search::decode_derived_group_key(field_name) {
        derived.source_field = canonicalize_sql_source_field_name(&derived.source_field, mappings)?;
        Ok(crate::search::encode_derived_group_key(&derived))
    } else {
        canonicalize_sql_source_field_name(field_name, mappings)
    }
}

pub(super) fn grouped_column_mapping_name(source_name: &str) -> String {
    crate::search::decode_derived_group_key(source_name)
        .map(|derived| derived.source_field)
        .unwrap_or_else(|| source_name.to_string())
}

pub(super) fn derived_group_key_requires_group_by_fallback(
    source_name: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> bool {
    let Some(derived) = crate::search::decode_derived_group_key(source_name) else {
        return false;
    };

    match mappings
        .get(&derived.source_field)
        .map(|mapping| &mapping.field_type)
    {
        Some(crate::cluster::state::FieldType::Keyword)
        | Some(crate::cluster::state::FieldType::Boolean)
        | Some(crate::cluster::state::FieldType::Text) => false,
        Some(_) => true,
        None => false,
    }
}

pub(super) fn canonicalize_sql_plan_fields(
    mut plan: crate::hybrid::QueryPlan,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<crate::hybrid::QueryPlan, (StatusCode, Json<Value>)> {
    for text_match in &mut plan.text_matches {
        text_match.field = canonicalize_sql_source_field_name(&text_match.field, mappings)?;
    }

    for filter in &mut plan.pushed_filters {
        canonicalize_query_clause_fields(filter, mappings)?;
    }

    canonicalize_sql_field_list(&mut plan.required_columns, mappings)?;
    let mut canonical_group_by = Vec::with_capacity(plan.group_by_columns.len());
    for field_name in plan.group_by_columns.drain(..) {
        canonical_group_by.push(canonicalize_group_key_name(&field_name, mappings)?);
    }
    plan.group_by_columns = canonical_group_by;

    if let Some((field_name, _)) = &mut plan.sort_pushdown {
        *field_name = canonicalize_sql_source_field_name(field_name, mappings)?;
    }

    if let Some(grouped_sql) = &mut plan.grouped_sql {
        for column in &mut grouped_sql.group_columns {
            column.source_name = canonicalize_group_key_name(&column.source_name, mappings)?;
        }
        for metric in &mut grouped_sql.metrics {
            if let Some(field_name) = &mut metric.field {
                *field_name = canonicalize_sql_source_field_name(field_name, mappings)?;
            }
        }
    }

    if plan.grouped_sql.as_ref().is_some_and(|grouped_sql| {
        grouped_sql.group_columns.iter().any(|column| {
            derived_group_key_requires_group_by_fallback(&column.source_name, mappings)
        })
    }) {
        plan.grouped_sql = None;
        plan.has_group_by_fallback = true;
        plan.limit_pushed_down = false;
    }

    if let Some(semijoin) = &mut plan.semijoin {
        semijoin.outer_key = canonicalize_sql_source_field_name(&semijoin.outer_key, mappings)?;
        semijoin.inner_key = canonicalize_sql_source_field_name(&semijoin.inner_key, mappings)?;
        *semijoin.inner_plan =
            canonicalize_sql_plan_fields(semijoin.inner_plan.as_ref().clone(), mappings)?;
    }

    plan.rewritten_sql = canonicalize_rewritten_sql_source_fields(&plan.rewritten_sql, mappings)?;

    Ok(plan)
}

#[derive(Default)]
pub(super) struct SqlAliasContext {
    aliases: HashSet<String>,
    identity_source_aliases: HashSet<String>,
}

impl SqlAliasContext {
    fn blocks_source_name(&self, name: &str) -> bool {
        let folded = name.to_ascii_lowercase();
        self.aliases.contains(&folded) && !self.identity_source_aliases.contains(&folded)
    }
}

#[derive(Clone, Copy)]
pub(super) enum SqlExprRewriteMode {
    SourceOnly,
    OutputAware,
}

pub(super) fn canonicalize_rewritten_sql_source_fields(
    sql: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<String, (StatusCode, Json<Value>)> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|error| {
        crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "planning_exception",
            format!("Failed to parse rewritten SQL for execution: {}", error),
        )
    })?;

    if statements.len() != 1 {
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "planning_exception",
            format!(
                "Expected one rewritten SQL statement, got {}",
                statements.len()
            ),
        ));
    }

    match statements.first_mut() {
        Some(SqlStatement::Query(query)) => {
            canonicalize_query_source_fields(query, mappings)?;
        }
        _ => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "planning_exception",
                "Rewritten SQL is not a simple SELECT query",
            ));
        }
    }

    Ok(statements.remove(0).to_string())
}

pub(super) fn canonicalize_query_source_fields(
    query: &mut SqlQuery,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<(), (StatusCode, Json<Value>)> {
    let alias_ctx = match query.body.as_ref() {
        SqlSetExpr::Select(select) => collect_sql_alias_context(select),
        _ => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "planning_exception",
                "Rewritten SQL contains an unsupported query shape",
            ));
        }
    };

    match query.body.as_mut() {
        SqlSetExpr::Select(select) => {
            canonicalize_select_source_fields(select, mappings, &alias_ctx)?
        }
        _ => unreachable!("query shape checked above"),
    }

    if let Some(order_by) = &mut query.order_by
        && let SqlOrderByKind::Expressions(order_by_exprs) = &mut order_by.kind
    {
        for order_by_expr in order_by_exprs {
            canonicalize_sql_expr_source_fields(
                &mut order_by_expr.expr,
                mappings,
                &alias_ctx,
                SqlExprRewriteMode::OutputAware,
            )?;
        }
    }

    Ok(())
}

pub(super) fn canonicalize_select_source_fields(
    select: &mut SqlSelect,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    alias_ctx: &SqlAliasContext,
) -> Result<(), (StatusCode, Json<Value>)> {
    for projection_item in &mut select.projection {
        match projection_item {
            SqlSelectItem::UnnamedExpr(expr) | SqlSelectItem::ExprWithAlias { expr, .. } => {
                canonicalize_sql_expr_source_fields(
                    expr,
                    mappings,
                    alias_ctx,
                    SqlExprRewriteMode::SourceOnly,
                )?;
            }
            SqlSelectItem::Wildcard(_) | SqlSelectItem::QualifiedWildcard(_, _) => {}
        }
    }

    if let Some(selection) = &mut select.selection {
        canonicalize_sql_expr_source_fields(
            selection,
            mappings,
            alias_ctx,
            SqlExprRewriteMode::SourceOnly,
        )?;
    }

    if let Some(having) = &mut select.having {
        canonicalize_sql_expr_source_fields(
            having,
            mappings,
            alias_ctx,
            SqlExprRewriteMode::OutputAware,
        )?;
    }

    if let GroupByExpr::Expressions(group_by_exprs, _) = &mut select.group_by {
        for expr in group_by_exprs {
            canonicalize_sql_expr_source_fields(
                expr,
                mappings,
                alias_ctx,
                SqlExprRewriteMode::OutputAware,
            )?;
        }
    }

    Ok(())
}

pub(super) fn collect_sql_alias_context(select: &SqlSelect) -> SqlAliasContext {
    let mut alias_ctx = SqlAliasContext::default();

    for projection_item in &select.projection {
        if let SqlSelectItem::ExprWithAlias { expr, alias } = projection_item {
            let alias_folded = alias.value.to_ascii_lowercase();
            if let Some(source_name) = sql_expr_source_name(expr)
                && source_name.eq_ignore_ascii_case(&alias.value)
            {
                alias_ctx
                    .identity_source_aliases
                    .insert(alias_folded.clone());
            }
            alias_ctx.aliases.insert(alias_folded);
        }
    }

    alias_ctx
}

pub(super) fn sql_expr_source_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.as_str()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
}

pub(super) fn canonicalize_sql_expr_source_fields(
    expr: &mut SqlExpr,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    alias_ctx: &SqlAliasContext,
    mode: SqlExprRewriteMode,
) -> Result<(), (StatusCode, Json<Value>)> {
    match expr {
        SqlExpr::Identifier(ident) => {
            canonicalize_sql_ident(ident, mappings, alias_ctx, mode)?;
        }
        SqlExpr::CompoundIdentifier(parts) => {
            if let Some(last_ident) = parts.last_mut() {
                canonicalize_sql_ident(
                    last_ident,
                    mappings,
                    alias_ctx,
                    SqlExprRewriteMode::SourceOnly,
                )?;
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            canonicalize_sql_expr_source_fields(left, mappings, alias_ctx, mode)?;
            canonicalize_sql_expr_source_fields(right, mappings, alias_ctx, mode)?;
        }
        SqlExpr::UnaryOp { expr: inner, .. }
        | SqlExpr::Nested(inner)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::Cast { expr: inner, .. } => {
            canonicalize_sql_expr_source_fields(inner, mappings, alias_ctx, mode)?;
        }
        SqlExpr::Between {
            expr: between_expr,
            low,
            high,
            ..
        } => {
            canonicalize_sql_expr_source_fields(between_expr, mappings, alias_ctx, mode)?;
            canonicalize_sql_expr_source_fields(low, mappings, alias_ctx, mode)?;
            canonicalize_sql_expr_source_fields(high, mappings, alias_ctx, mode)?;
        }
        SqlExpr::InList {
            expr: list_expr,
            list,
            ..
        } => {
            canonicalize_sql_expr_source_fields(list_expr, mappings, alias_ctx, mode)?;
            for item in list {
                canonicalize_sql_expr_source_fields(item, mappings, alias_ctx, mode)?;
            }
        }
        SqlExpr::Function(function) => {
            let next_mode = if sql_function_is_aggregate(function.name.to_string().as_str()) {
                SqlExprRewriteMode::SourceOnly
            } else {
                mode
            };
            if let FunctionArguments::List(list) = &mut function.args {
                for arg in &mut list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(inner),
                            ..
                        }
                        | FunctionArg::ExprNamed {
                            arg: FunctionArgExpr::Expr(inner),
                            ..
                        } => {
                            canonicalize_sql_expr_source_fields(
                                inner, mappings, alias_ctx, next_mode,
                            )?;
                        }
                        _ => {}
                    }
                }
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                canonicalize_sql_expr_source_fields(operand, mappings, alias_ctx, mode)?;
            }
            for case_when in conditions {
                canonicalize_sql_expr_source_fields(
                    &mut case_when.condition,
                    mappings,
                    alias_ctx,
                    mode,
                )?;
                canonicalize_sql_expr_source_fields(
                    &mut case_when.result,
                    mappings,
                    alias_ctx,
                    mode,
                )?;
            }
            if let Some(else_result) = else_result {
                canonicalize_sql_expr_source_fields(else_result, mappings, alias_ctx, mode)?;
            }
        }
        SqlExpr::Like {
            expr: like_expr,
            pattern,
            ..
        }
        | SqlExpr::ILike {
            expr: like_expr,
            pattern,
            ..
        } => {
            canonicalize_sql_expr_source_fields(like_expr, mappings, alias_ctx, mode)?;
            canonicalize_sql_expr_source_fields(pattern, mappings, alias_ctx, mode)?;
        }
        _ => {}
    }

    Ok(())
}

pub(super) fn canonicalize_sql_ident(
    ident: &mut sqlparser::ast::Ident,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    alias_ctx: &SqlAliasContext,
    mode: SqlExprRewriteMode,
) -> Result<(), (StatusCode, Json<Value>)> {
    if ident.quote_style.is_some() || matches!(ident.value.as_str(), "_id" | "_score") {
        return Ok(());
    }

    if matches!(mode, SqlExprRewriteMode::OutputAware) && alias_ctx.blocks_source_name(&ident.value)
    {
        return Ok(());
    }

    if let Some(canonical) = resolve_case_insensitive_mapping_name(&ident.value, mappings)?
        && canonical != ident.value
    {
        ident.value = canonical;
        ident.quote_style = Some('"');
    } else if mappings.contains_key(&ident.value) {
        ident.quote_style = Some('"');
    }

    Ok(())
}

pub(super) fn sql_function_is_aggregate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "value_count"
            | "any_value"
    )
}
