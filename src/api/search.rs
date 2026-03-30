use crate::api::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use futures::future::join_all;
use serde::Deserialize;
use serde_json::Value;
use std::time::Instant;

/// GET/POST /{index}/_count — Count documents matching a query.
/// GET returns total doc count (match_all). POST accepts an optional query body.
///
/// Fast path: when the query is match_all, sums `doc_count()` from local shards
/// and `GetShardStats` from remote nodes — no search execution at all.
pub async fn count_documents(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let query = match &body {
        Some(Json(req)) => match serde_json::from_value::<CountRequest>(req.clone()) {
            Ok(cr) => cr.query,
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "parsing_exception",
                    format!("Invalid count query: {}", e),
                );
            }
        },
        None => crate::search::QueryClause::MatchAll(serde_json::json!({})),
    };

    // Fast path: match_all counts use doc_count() metadata — no search needed
    if query.is_match_all() {
        return count_match_all_fast(&state, &index_name).await;
    }

    // Slow path: run distributed search with size=0 to count matching docs
    let search_req = crate::search::SearchRequest {
        query,
        size: 0,
        from: 0,
        knn: None,
        sort: vec![],
        aggs: std::collections::HashMap::new(),
    };

    match crate::api::index::execute_distributed_dsl_search(&state, &index_name, &search_req).await
    {
        Ok(result) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "count": result.total_hits,
                "_shards": {
                    "total": result.successful_shards + result.failed_shards,
                    "successful": result.successful_shards,
                    "failed": result.failed_shards
                }
            })),
        ),
        Err(err) => err,
    }
}

/// Fast path for match_all count: sum doc_count() from all shards without running a query.
async fn count_match_all_fast(state: &AppState, index_name: &str) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let (count, successful, failed) = count_docs_from_metadata(state, index_name, &metadata).await;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "count": count,
            "_shards": {
                "total": successful + failed,
                "successful": successful,
                "failed": failed
            }
        })),
    )
}

#[derive(Deserialize)]
struct CountRequest {
    #[serde(default = "default_match_all_query")]
    query: crate::search::QueryClause,
}

fn default_match_all_query() -> crate::search::QueryClause {
    crate::search::QueryClause::MatchAll(serde_json::json!({}))
}

/// Shared helper: count docs from metadata (doc_count + GetShardStats) without executing a query.
/// Returns (total_count, successful_shards, failed_shards).
async fn count_docs_from_metadata(
    state: &AppState,
    index_name: &str,
    metadata: &crate::cluster::state::IndexMetadata,
) -> (u64, u32, u32) {
    let cluster_state = state.cluster_manager.get_state();
    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        metadata,
        "count_fast",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    let mut count: u64 = 0;
    let mut successful: u32 = 0;
    let mut failed: u32 = 0;

    for (_shard_id, engine) in &local_shards {
        count += engine.doc_count();
        successful += 1;
    }

    let mut remote_handles = Vec::new();
    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }
        if let Some(node) = cluster_state.nodes.get(&routing.primary) {
            let client = state.transport_client.clone();
            let node = node.clone();
            let idx = index_name.to_string();
            let sid = *shard_id;
            remote_handles.push(tokio::spawn(async move {
                client
                    .get_shard_stats(&node)
                    .await
                    .map(|stats| stats.get(&(idx, sid)).copied().unwrap_or(0))
            }));
        }
    }

    for handle in remote_handles {
        match handle.await {
            Ok(Ok(remote_count)) => {
                count += remote_count;
                successful += 1;
            }
            _ => {
                failed += 1;
            }
        }
    }

    (count, successful, failed)
}

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(default = "default_query")]
    q: String,
    #[serde(default = "default_size")]
    size: usize,
    #[serde(default)]
    from: usize,
}

fn default_query() -> String {
    "*".to_string()
}

fn default_size() -> usize {
    10
}

#[derive(Deserialize, Default)]
pub struct SqlQueryRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub analyze: bool,
}

/// GET /{index}/_search?q=... — query-string search across all shards (local + remote) for this index.
pub async fn search_documents(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(params): Query<SearchParams>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let mut all_hits = Vec::new();
    let mut successful_shards = 0u32;
    let mut failed_shards = 0u32;

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        &state,
        &index_name,
        &metadata,
        "GET search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    for (shard_id, engine) in &local_shards {
        let engine = engine.clone();
        let query = params.q.clone();
        let shard_id = *shard_id;
        let search_result = state
            .worker_pools
            .spawn_search(move || engine.search(&query))
            .await;
        match search_result {
            Ok(Ok(hits)) => {
                successful_shards += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name,
                        "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Ok(Err(e)) | Err(e) => {
                tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e);
                failed_shards += 1;
            }
        }
    }

    // Scatter to remote shards
    let mut remote_futures = Vec::new();
    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }
        if let Some(node_info) = cluster_state.nodes.get(&routing.primary) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let index = index_name.clone();
            let sid = *shard_id;
            let query = params.q.clone();
            remote_futures.push(tokio::spawn(async move {
                (
                    sid,
                    client
                        .forward_search_to_shard(&node_info, &index, sid, &query)
                        .await,
                )
            }));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for result in remote_results {
        match result {
            Ok((shard_id, Ok(hits))) => {
                successful_shards += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Ok((shard_id, Err(e))) => {
                tracing::error!(
                    "Remote shard {}/{} search failed: {}",
                    index_name,
                    shard_id,
                    e
                );
                failed_shards += 1;
            }
            Err(e) => {
                tracing::error!("Remote shard search task panicked: {}", e);
                failed_shards += 1;
            }
        }
    }

    // Sort by _score descending, then apply from/size pagination
    all_hits.sort_by(|a, b| {
        let sa = a.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let sb = b.get("_score").and_then(|v| v.as_f64()).unwrap_or(0.0);
        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
    });

    let total = all_hits.len();
    let from = params.from;
    let size = params.size;
    let paginated: Vec<_> = all_hits.into_iter().skip(from).take(size).collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": {
                "total": successful_shards + failed_shards,
                "successful": successful_shards,
                "failed": failed_shards
            },
            "hits": {
                "total": { "value": total, "relation": "eq" },
                "hits": paginated
            }
        })),
    )
}

/// POST /{index}/_sql/explain — Explain the SQL query plan, optionally with execution timings.
///
/// With `"analyze": false` (default): returns the plan without executing the query.
/// With `"analyze": true`: executes the query and returns plan + per-stage timings + result rows.
pub async fn explain_sql(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    if !cluster_state.indices.contains_key(&index_name) {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    }

    if !req.analyze {
        // Plan-only mode: parse and return the plan without executing
        let plan = match crate::hybrid::planner::plan_sql(&index_name, &req.query) {
            Ok(plan) => plan,
            Err(e) => {
                return crate::api::error_response(StatusCode::BAD_REQUEST, "parsing_exception", e);
            }
        };
        return (StatusCode::OK, Json(plan.to_explain_json()));
    }

    // EXPLAIN ANALYZE: execute the query and merge plan + timings + rows
    match execute_sql_query(&state, &index_name, &req.query).await {
        Ok(result) => {
            let mut explain = result.plan.to_explain_json();
            explain["timings"] = serde_json::to_value(&result.timings).unwrap();
            explain["execution_mode"] = serde_json::json!(result.execution_mode);
            explain["matched_hits"] = serde_json::json!(result.matched_hits);
            explain["row_count"] = serde_json::json!(result.sql_result.rows.len());
            explain["truncated"] = serde_json::json!(result.truncated);
            explain["_shards"] = serde_json::json!({
                "total": result.successful_shards + result.failed_shards,
                "successful": result.successful_shards,
                "failed": result.failed_shards,
            });
            explain["columns"] = serde_json::json!(result.sql_result.columns);
            explain["rows"] = serde_json::json!(result.sql_result.rows);
            (StatusCode::OK, Json(explain))
        }
        Err(err) => err,
    }
}

/// Internal result of a fully-executed SQL query, with timings.
struct SqlExecutionResult {
    plan: crate::hybrid::QueryPlan,
    sql_result: crate::hybrid::SqlQueryResult,
    matched_hits: usize,
    successful_shards: u32,
    failed_shards: u32,
    execution_mode: &'static str,
    truncated: bool,
    timings: crate::hybrid::SqlTimings,
}

/// Execute a SQL query and return the result for integration testing.
/// Exposes the same execution pipeline as the HTTP handler but returns
/// the raw `SqlQueryResult` without HTTP response formatting.
pub async fn execute_sql_for_testing(
    state: &AppState,
    index_name: &str,
    sql: &str,
) -> crate::common::Result<crate::hybrid::SqlQueryResult> {
    let result = execute_sql_query(state, index_name, sql)
        .await
        .map_err(|(_, json_body)| {
            anyhow::anyhow!(
                "{}",
                json_body
                    .0
                    .get("error")
                    .and_then(|e| e.get("reason"))
                    .and_then(|r| r.as_str())
                    .unwrap_or("SQL execution error")
            )
        })?;
    Ok(result.sql_result)
}

/// Execute a SQL query end-to-end with per-stage timing instrumentation.
/// Used by both `search_sql` (for the normal response) and `explain_sql`
/// (for EXPLAIN ANALYZE with timings + rows).
async fn execute_sql_query(
    state: &AppState,
    index_name: &str,
    query: &str,
) -> Result<SqlExecutionResult, (StatusCode, Json<Value>)> {
    let total_start = Instant::now();

    // Stage 1: Planning
    let plan_start = Instant::now();
    let plan = match crate::hybrid::planner::plan_sql(index_name, query) {
        Ok(plan) => plan,
        Err(e) => {
            return Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                e,
            ));
        }
    };
    let search_req = plan.to_search_request();
    let planning_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            ));
        }
    };

    // count(*) fast path: answer from doc_count() metadata without scanning docs
    if plan.is_count_star_only() {
        let search_start = Instant::now();
        let (count, successful_shards, failed_shards) =
            count_docs_from_metadata(state, index_name, &metadata).await;
        let search_ms = search_start.elapsed().as_secs_f64() * 1000.0;

        let grouped_sql = plan.grouped_sql.as_ref().unwrap();
        let columns: Vec<String> = grouped_sql
            .metrics
            .iter()
            .map(|m| m.output_name.clone())
            .collect();
        let mut row = serde_json::Map::new();
        for m in &grouped_sql.metrics {
            row.insert(m.output_name.clone(), serde_json::json!(count));
        }
        let rows = vec![serde_json::Value::Object(row)];

        return Ok(SqlExecutionResult {
            plan,
            sql_result: crate::hybrid::SqlQueryResult { columns, rows },
            matched_hits: count as usize,
            successful_shards,
            failed_shards,
            execution_mode: "count_star_fast",
            truncated: false,
            timings: crate::hybrid::SqlTimings {
                planning_ms,
                search_ms,
                collect_ms: 0.0,
                merge_ms: 0.0,
                datafusion_ms: 0.0,
                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            },
        });
    }

    // Grouped partials path
    if plan.uses_grouped_partials() {
        let search_start = Instant::now();
        let distributed =
            match crate::api::index::execute_distributed_dsl_search(state, index_name, &search_req)
                .await
            {
                Ok(result) => result,
                Err(err) => return Err(err),
            };
        let search_ms = search_start.elapsed().as_secs_f64() * 1000.0;

        let merge_start = Instant::now();
        let sql_result =
            match crate::hybrid::execute_grouped_partial_sql(&plan, &distributed.partial_aggs) {
                Ok(result) => result,
                Err(error) => {
                    return Err(crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sql_execution_exception",
                        error,
                    ));
                }
            };
        let merge_ms = merge_start.elapsed().as_secs_f64() * 1000.0;

        return Ok(SqlExecutionResult {
            plan,
            sql_result,
            matched_hits: distributed.total_hits,
            successful_shards: distributed.successful_shards,
            failed_shards: distributed.failed_shards,
            execution_mode: "tantivy_grouped_partials",
            truncated: false,
            timings: crate::hybrid::SqlTimings {
                planning_ms,
                search_ms,
                collect_ms: 0.0,
                merge_ms,
                datafusion_ms: 0.0,
                total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            },
        });
    }

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        &metadata,
        "SQL search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    // Fast-field path: collect Arrow batches from local + remote shards
    let search_start = Instant::now();
    let direct_sql = if !plan.selects_all_columns {
        let mut batches = Vec::new();
        let mut total_hits = 0usize;
        let mut successful_shards = 0u32;
        let mut direct_error: Option<anyhow::Error> = None;

        for (_shard_id, engine) in &local_shards {
            let engine = engine.clone();
            let req = search_req.clone();
            let cols = plan.required_columns.clone();
            let nid = plan.needs_id;
            let nsc = plan.needs_score;
            let batch_result = state
                .worker_pools
                .spawn_search(move || engine.sql_record_batch(&req, &cols, nid, nsc))
                .await;
            match batch_result {
                Ok(Ok(Some(batch_result))) => {
                    successful_shards += 1;
                    total_hits += batch_result.total_hits;
                    batches.push(batch_result.batch);
                }
                Ok(Ok(None)) => {
                    direct_error = Some(anyhow::anyhow!(
                        "local shard does not support direct SQL batches"
                    ));
                    break;
                }
                Ok(Err(error)) | Err(error) => {
                    direct_error = Some(error);
                    break;
                }
            }
        }

        if direct_error.is_none() {
            let cs = state.cluster_manager.get_state();
            let mut remote_futures = Vec::new();

            for (shard_id, routing) in &metadata.shard_routing {
                if local_shard_ids.contains(shard_id) {
                    continue;
                }
                let primary_node_id = &routing.primary;
                if let Some(node) = cs.nodes.get(primary_node_id) {
                    let client = state.transport_client.clone();
                    let node = node.clone();
                    let idx = index_name.to_string();
                    let sid = *shard_id;
                    let req = search_req.clone();
                    let cols = plan.required_columns.clone();
                    let nid = plan.needs_id;
                    let nsc = plan.needs_score;
                    remote_futures.push(tokio::spawn(async move {
                        client
                            .forward_sql_batch_to_shard(&node, &idx, sid, &req, &cols, nid, nsc)
                            .await
                    }));
                }
            }

            let remote_results = futures::future::join_all(remote_futures).await;
            for result in remote_results {
                match result {
                    Ok(Ok((batch, hits))) => {
                        successful_shards += 1;
                        total_hits += hits;
                        batches.push(batch);
                    }
                    Ok(Err(e)) => {
                        direct_error = Some(e);
                        break;
                    }
                    Err(e) => {
                        direct_error = Some(anyhow::anyhow!("remote shard task failed: {}", e));
                        break;
                    }
                }
            }
        }

        match direct_error {
            None => Some((batches, total_hits, successful_shards, 0u32)),
            Some(error) => {
                tracing::warn!(
                    "Falling back to materialized SQL execution for [{}]: {}",
                    index_name,
                    error
                );
                None
            }
        }
    } else {
        None
    };
    let search_ms = search_start.elapsed().as_secs_f64() * 1000.0;

    // Stage 3: DataFusion SQL execution (or materialized fallback)
    let datafusion_start = Instant::now();
    let (sql_result, matched_hits, successful_shards, failed_shards, execution_mode) =
        if let Some((batches, total_hits, successful_shards, failed_shards)) = direct_sql {
            let sql_result = match crate::hybrid::execute_planned_sql_batches(&plan, batches).await
            {
                Ok(result) => result,
                Err(e) => {
                    return Err(crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sql_execution_exception",
                        e,
                    ));
                }
            };
            (
                sql_result,
                total_hits,
                successful_shards,
                failed_shards,
                "tantivy_fast_fields",
            )
        } else {
            let distributed = match crate::api::index::execute_distributed_dsl_search(
                state,
                index_name,
                &search_req,
            )
            .await
            {
                Ok(result) => result,
                Err(err) => return Err(err),
            };

            let sql_result = match crate::hybrid::execute_planned_sql_with_mappings(
                &plan,
                &distributed.all_hits,
                &metadata.mappings,
            )
            .await
            {
                Ok(result) => result,
                Err(e) => {
                    return Err(crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sql_execution_exception",
                        e,
                    ));
                }
            };
            (
                sql_result,
                distributed.total_hits,
                distributed.successful_shards,
                distributed.failed_shards,
                "materialized_hits_fallback",
            )
        };
    let datafusion_ms = datafusion_start.elapsed().as_secs_f64() * 1000.0;

    let truncated = !plan.limit_pushed_down
        && plan.limit.is_none()
        && matched_hits > search_req.size
        && execution_mode != "tantivy_grouped_partials";

    Ok(SqlExecutionResult {
        plan,
        sql_result,
        matched_hits,
        successful_shards,
        failed_shards,
        execution_mode,
        truncated,
        timings: crate::hybrid::SqlTimings {
            planning_ms,
            search_ms,
            collect_ms: 0.0,
            merge_ms: 0.0,
            datafusion_ms,
            total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
        },
    })
}

/// POST /{index}/_sql — SQL projection/aggregation over distributed search hits.
pub async fn search_sql(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let result = match execute_sql_query(&state, &index_name, &req.query).await {
        Ok(result) => result,
        Err(err) => return err,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "execution_mode": result.execution_mode,
            "truncated": result.truncated,
            "planner": {
                "text_match": result.plan.text_match.as_ref().map(|text_match| serde_json::json!({
                    "field": text_match.field,
                    "query": text_match.query,
                })),
                "pushed_down_filters": result.plan.pushed_filters,
                "group_by_columns": result.plan.group_by_columns,
                "required_columns": result.plan.required_columns,
                "has_residual_predicates": result.plan.has_residual_predicates,
            },
            "_shards": {
                "total": result.successful_shards + result.failed_shards,
                "successful": result.successful_shards,
                "failed": result.failed_shards
            },
            "matched_hits": result.matched_hits,
            "columns": result.sql_result.columns,
            "rows": result.sql_result.rows
        })),
    )
}

/// POST /_sql — Global SQL endpoint (no index in path).
/// Handles SHOW TABLES, DESCRIBE {table}, SHOW CREATE TABLE {table},
/// and regular SELECT queries (index extracted from FROM clause).
pub async fn global_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    let trimmed = req.query.trim();

    // SHOW TABLES / SHOW INDICES
    if matches_command(trimmed, "SHOW TABLES") || matches_command(trimmed, "SHOW INDICES") {
        return handle_show_tables(&state).await;
    }

    // DESCRIBE {table} / DESC {table}
    if let Some(table) =
        strip_command(trimmed, "DESCRIBE").or_else(|| strip_command(trimmed, "DESC"))
    {
        let table = unquote_identifier(table);
        return handle_describe(&state, &table).await;
    }

    // SHOW CREATE TABLE {table}
    if let Some(table) = strip_command(trimmed, "SHOW CREATE TABLE") {
        let table = unquote_identifier(table);
        return handle_show_create_table(&state, &table).await;
    }

    // Regular SELECT — extract index from FROM clause
    let index_name = match extract_index_from_sql(trimmed) {
        Some(name) => name,
        None => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                "Could not determine index from SQL. Use: SELECT ... FROM \"index_name\" ..., or use SHOW TABLES / DESCRIBE",
            );
        }
    };

    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let result = match execute_sql_query(&state, &index_name, &req.query).await {
        Ok(result) => result,
        Err(err) => return err,
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "execution_mode": result.execution_mode,
            "truncated": result.truncated,
            "planner": {
                "text_match": result.plan.text_match.as_ref().map(|text_match| serde_json::json!({
                    "field": text_match.field,
                    "query": text_match.query,
                })),
                "pushed_down_filters": result.plan.pushed_filters,
                "group_by_columns": result.plan.group_by_columns,
                "required_columns": result.plan.required_columns,
                "has_residual_predicates": result.plan.has_residual_predicates,
            },
            "_shards": {
                "total": result.successful_shards + result.failed_shards,
                "successful": result.successful_shards,
                "failed": result.failed_shards
            },
            "matched_hits": result.matched_hits,
            "columns": result.sql_result.columns,
            "rows": result.sql_result.rows
        })),
    )
}

/// Check if the input matches a command (case-insensitive), optionally followed by a semicolon.
fn matches_command(input: &str, command: &str) -> bool {
    let stripped = input.trim().trim_end_matches(';').trim();
    stripped.eq_ignore_ascii_case(command)
}

/// Strip a command prefix (case-insensitive) and return the remaining argument, trimmed.
fn strip_command<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let stripped = input.trim_end_matches(';').trim();
    if stripped.len() >= prefix.len() && stripped[..prefix.len()].eq_ignore_ascii_case(prefix) {
        let rest = stripped[prefix.len()..].trim();
        if rest.is_empty() { None } else { Some(rest) }
    } else {
        None
    }
}

/// Remove surrounding quotes from an identifier: "foo" -> foo, `foo` -> foo
fn unquote_identifier(s: &str) -> String {
    let s = s.trim().trim_end_matches(';').trim();
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('`') && s.ends_with('`')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Extract the index/table name from a SELECT ... FROM "index" ... query.
fn extract_index_from_sql(sql: &str) -> Option<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match &statements[0] {
        sqlparser::ast::Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
                && select.from.len() == 1
                && let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation
            {
                return Some(
                    name.0
                        .iter()
                        .filter_map(|p| match p {
                            sqlparser::ast::ObjectNamePart::Identifier(id) => {
                                Some(id.value.clone())
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("."),
                );
            }
            None
        }
        _ => None,
    }
}

/// SHOW TABLES — list all indices with doc counts and shard info.
async fn handle_show_tables(state: &AppState) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let mut rows = Vec::new();

    for (name, metadata) in &cluster_state.indices {
        let num_shards = metadata.number_of_shards;
        let num_replicas = metadata.number_of_replicas;
        let field_count = metadata.mappings.len();

        // Count docs across all shards
        let (doc_count, _, _) = count_docs_from_metadata(state, name, metadata).await;

        rows.push(serde_json::json!({
            "index": name,
            "docs": doc_count,
            "shards": num_shards,
            "replicas": num_replicas,
            "fields": field_count,
        }));
    }

    // Sort by index name
    rows.sort_by(|a, b| {
        let na = a.get("index").and_then(|v| v.as_str()).unwrap_or("");
        let nb = b.get("index").and_then(|v| v.as_str()).unwrap_or("");
        na.cmp(nb)
    });

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "columns": ["index", "docs", "shards", "replicas", "fields"],
            "rows": rows,
        })),
    )
}

/// DESCRIBE {table} — show field names and types for an index.
async fn handle_describe(state: &AppState, index_name: &str) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m,
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let mut rows: Vec<Value> = metadata
        .mappings
        .iter()
        .map(|(field_name, mapping)| {
            let type_str = match mapping.field_type {
                crate::cluster::state::FieldType::Text => "text",
                crate::cluster::state::FieldType::Keyword => "keyword",
                crate::cluster::state::FieldType::Integer => "integer",
                crate::cluster::state::FieldType::Float => "float",
                crate::cluster::state::FieldType::Boolean => "boolean",
                crate::cluster::state::FieldType::KnnVector => "knn_vector",
            };
            let mut row = serde_json::json!({
                "field": field_name,
                "type": type_str,
            });
            if let Some(dim) = mapping.dimension {
                row["dimension"] = serde_json::json!(dim);
            }
            row
        })
        .collect();

    // Sort by field name
    rows.sort_by(|a, b| {
        let fa = a.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let fb = b.get("field").and_then(|v| v.as_str()).unwrap_or("");
        fa.cmp(fb)
    });

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "index": index_name,
            "columns": ["field", "type"],
            "rows": rows,
        })),
    )
}

/// SHOW CREATE TABLE {table} — show the CREATE INDEX JSON for an index.
async fn handle_show_create_table(state: &AppState, index_name: &str) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name) {
        Some(m) => m,
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let mut properties = serde_json::Map::new();
    for (field_name, mapping) in &metadata.mappings {
        let type_str = match mapping.field_type {
            crate::cluster::state::FieldType::Text => "text",
            crate::cluster::state::FieldType::Keyword => "keyword",
            crate::cluster::state::FieldType::Integer => "integer",
            crate::cluster::state::FieldType::Float => "float",
            crate::cluster::state::FieldType::Boolean => "boolean",
            crate::cluster::state::FieldType::KnnVector => "knn_vector",
        };
        let mut field_def = serde_json::json!({"type": type_str});
        if let Some(dim) = mapping.dimension {
            field_def["dimension"] = serde_json::json!(dim);
        }
        properties.insert(field_name.clone(), field_def);
    }

    let create_body = serde_json::json!({
        "settings": {
            "number_of_shards": metadata.number_of_shards,
            "number_of_replicas": metadata.number_of_replicas,
            "refresh_interval_ms": metadata.settings.refresh_interval_ms,
        },
        "mappings": {
            "properties": properties,
        }
    });

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "index": index_name,
            "create_statement": create_body,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        ClusterState, FieldMapping, FieldType, IndexMetadata, IndexSettings, NodeInfo, NodeRole,
        ShardRoutingEntry,
    };
    use axum::extract::{Path, State};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_test_node(id: &str) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 1,
        }
    }

    fn make_sql_metadata(index: &str) -> IndexMetadata {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".to_string(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );

        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "description".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "brand".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        IndexMetadata {
            name: index.to_string(),
            uuid: String::new(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        }
    }

    fn make_test_app_state(index_name: &str) -> (tempfile::TempDir, AppState) {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));
        cluster_state.add_index(make_sql_metadata(index_name));

        let temp_dir = tempfile::tempdir().unwrap();
        let manager = crate::cluster::ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state.clone());
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft: None,
            worker_pools: crate::worker::WorkerPools::new(2, 2),
        };

        let metadata = cluster_state.indices.get(index_name).unwrap().clone();
        assert!(
            crate::api::index::ensure_local_index_shards_open(
                &state, index_name, &metadata, "SQL test"
            )
            .len()
                == 1
        );

        let shard = state.shard_manager.get_shard(index_name, 0).unwrap();
        shard
            .add_document(
                "1",
                json!({"title": "iPhone Pro", "description": "iphone flagship", "brand": "Apple", "price": 999.0}),
            )
            .unwrap();
        shard
            .add_document(
                "2",
                json!({"title": "iPhone", "description": "iphone standard", "brand": "Apple", "price": 899.0}),
            )
            .unwrap();
        shard
            .add_document(
                "3",
                json!({"title": "Galaxy", "description": "iphone competitor", "brand": "Samsung", "price": 799.0}),
            )
            .unwrap();
        shard.refresh().unwrap();

        (temp_dir, state)
    }

    #[tokio::test]
    async fn search_sql_uses_tantivy_fast_fields_for_local_non_wildcard_query() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC, brand ASC".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
        assert_eq!(body["planner"]["group_by_columns"], json!(["brand"]));
        assert_eq!(body["planner"]["has_residual_predicates"], json!(false));
        assert_eq!(body["matched_hits"], 3);
    }

    #[tokio::test]
    async fn search_sql_uses_materialized_hits_fallback_for_select_star() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "materialized_hits_fallback");
        assert!(body["rows"].as_array().is_some());
        assert_eq!(body["matched_hits"], 3);
    }

    #[tokio::test]
    async fn explain_sql_returns_plan_without_executing() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') AND price > 500 GROUP BY brand ORDER BY total DESC".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["index"], "products");
        assert_eq!(body["execution_strategy"], "tantivy_grouped_partials");
        assert_eq!(
            body["pushdown_summary"]["text_match"]["field"],
            "description"
        );
        assert_eq!(body["pushdown_summary"]["pushed_filter_count"], 1);
        assert_eq!(body["columns"]["group_by"], json!(["brand"]));

        let pipeline = body["pipeline"].as_array().unwrap();
        assert_eq!(pipeline.len(), 4);
        assert_eq!(pipeline[0]["name"], "tantivy_search");
        assert_eq!(pipeline[1]["name"], "grouped_partial_collect");
        assert_eq!(pipeline[2]["name"], "grouped_partial_merge");
        assert_eq!(pipeline[3]["name"], "final_grouped_sql_shape");

        // Should NOT have rows/matched_hits — explain doesn't execute
        assert!(body.get("rows").is_none());
        assert!(body.get("matched_hits").is_none());
    }

    #[tokio::test]
    async fn explain_sql_returns_error_for_invalid_sql() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "NOT A VALID SQL".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
    }

    #[tokio::test]
    async fn explain_sql_returns_error_for_nonexistent_index() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("nonexistent".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM nonexistent".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
    }

    #[tokio::test]
    async fn explain_sql_shows_materialized_fallback_for_select_star() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT * FROM products WHERE text_match(description, 'iphone')".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_strategy"], "materialized_hits_fallback");
        assert!(body["columns"]["selects_all"].as_bool().unwrap());

        let pipeline = body["pipeline"].as_array().unwrap();
        assert_eq!(pipeline[1]["name"], "source_materialization");
    }

    #[tokio::test]
    async fn explain_analyze_returns_timings_and_rows() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY brand".to_string(),
                analyze: true,
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);

        // Plan fields should still be present
        assert_eq!(body["execution_strategy"], "tantivy_grouped_partials");
        assert!(body["pipeline"].as_array().is_some());

        // ANALYZE-specific fields: timings, rows, matched_hits
        let timings = &body["timings"];
        assert!(timings["planning_ms"].as_f64().unwrap() >= 0.0);
        assert!(timings["search_ms"].as_f64().unwrap() >= 0.0);
        assert!(timings["total_ms"].as_f64().unwrap() > 0.0);

        assert_eq!(body["matched_hits"], 3);
        assert!(body["row_count"].as_u64().unwrap() > 0);
        let rows = body["rows"].as_array().unwrap();
        assert!(!rows.is_empty());
    }

    #[tokio::test]
    async fn explain_without_analyze_has_no_timings_or_rows() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand FROM products WHERE text_match(description, 'iphone')"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        // Plan-only mode: no timings, no rows, no matched_hits
        assert!(body.get("timings").is_none());
        assert!(body.get("rows").is_none());
        assert!(body.get("matched_hits").is_none());
    }

    #[tokio::test]
    async fn explain_analyze_fast_fields_returns_timings() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = explain_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, price FROM products WHERE text_match(description, 'iphone')"
                    .to_string(),
                analyze: true,
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_fast_fields");
        assert!(body["timings"]["total_ms"].as_f64().unwrap() > 0.0);
        assert_eq!(body["matched_hits"], 3);
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
    }

    // -------- Tests for global SQL helper functions --------

    #[test]
    fn matches_command_case_insensitive() {
        assert!(super::matches_command("SHOW TABLES", "SHOW TABLES"));
        assert!(super::matches_command("show tables", "SHOW TABLES"));
        assert!(super::matches_command("Show Tables", "SHOW TABLES"));
        assert!(super::matches_command("SHOW TABLES;", "SHOW TABLES"));
        assert!(super::matches_command("  SHOW TABLES  ;  ", "SHOW TABLES"));
        assert!(!super::matches_command("SHOW", "SHOW TABLES"));
        assert!(!super::matches_command("SELECT * FROM foo", "SHOW TABLES"));
    }

    #[test]
    fn strip_command_extracts_argument() {
        assert_eq!(
            super::strip_command("DESCRIBE hackernews", "DESCRIBE"),
            Some("hackernews")
        );
        assert_eq!(
            super::strip_command("describe hackernews;", "DESCRIBE"),
            Some("hackernews")
        );
        assert_eq!(
            super::strip_command("DESC \"hackernews\"", "DESC"),
            Some("\"hackernews\"")
        );
        assert_eq!(super::strip_command("DESCRIBE", "DESCRIBE"), None);
        assert_eq!(
            super::strip_command("SHOW CREATE TABLE hackernews", "SHOW CREATE TABLE"),
            Some("hackernews")
        );
    }

    #[test]
    fn unquote_identifier_handles_all_formats() {
        assert_eq!(super::unquote_identifier("hackernews"), "hackernews");
        assert_eq!(super::unquote_identifier("\"hackernews\""), "hackernews");
        assert_eq!(super::unquote_identifier("`hackernews`"), "hackernews");
        assert_eq!(super::unquote_identifier("  hackernews  "), "hackernews");
        assert_eq!(super::unquote_identifier("\"hackernews\";"), "hackernews");
    }

    #[test]
    fn extract_index_from_sql_finds_table_name() {
        assert_eq!(
            super::extract_index_from_sql(r#"SELECT * FROM "hackernews" WHERE x > 1"#),
            Some("hackernews".to_string())
        );
        assert_eq!(
            super::extract_index_from_sql("SELECT count(*) FROM hackernews"),
            Some("hackernews".to_string())
        );
        assert_eq!(super::extract_index_from_sql("SHOW TABLES"), None);
        assert_eq!(super::extract_index_from_sql("not valid sql at all"), None);
    }

    #[tokio::test]
    async fn global_sql_show_tables_returns_index_list() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "SHOW TABLES".to_string(),
            analyze: false,
        };
        let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
        let columns = body["columns"].as_array().unwrap();
        assert!(columns.contains(&json!("index")));
        assert!(columns.contains(&json!("docs")));
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["index"], "products");
    }

    #[tokio::test]
    async fn global_sql_show_tables_case_insensitive() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "show tables;".to_string(),
            analyze: false,
        };
        let (status, _) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn global_sql_describe_returns_field_mappings() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "DESCRIBE products".to_string(),
            analyze: false,
        };
        let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["index"], "products");
        let rows = body["rows"].as_array().unwrap();
        assert!(!rows.is_empty());
        // Check that known fields are present
        let field_names: Vec<&str> = rows
            .iter()
            .filter_map(|r| r.get("field").and_then(|v| v.as_str()))
            .collect();
        assert!(field_names.contains(&"title"));
        assert!(field_names.contains(&"price"));
    }

    #[tokio::test]
    async fn global_sql_describe_nonexistent_index_returns_404() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "DESCRIBE nonexistent".to_string(),
            analyze: false,
        };
        let (status, _) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn global_sql_show_create_table_returns_index_json() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "SHOW CREATE TABLE products".to_string(),
            analyze: false,
        };
        let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["index"], "products");
        let create = &body["create_statement"];
        assert!(create.get("settings").is_some());
        assert!(create.get("mappings").is_some());
        let props = &create["mappings"]["properties"];
        assert!(props.get("title").is_some());
    }

    #[tokio::test]
    async fn global_sql_select_routes_to_correct_index() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: r#"SELECT count(*) AS total FROM "products""#.to_string(),
            analyze: false,
        };
        let (status, Json(body)) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("columns").is_some());
        assert!(body.get("rows").is_some());
    }

    #[tokio::test]
    async fn global_sql_invalid_query_returns_error() {
        let (_tmp, state) = make_test_app_state("products");
        let req = SqlQueryRequest {
            query: "THIS IS NOT SQL".to_string(),
            analyze: false,
        };
        let (status, _) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
