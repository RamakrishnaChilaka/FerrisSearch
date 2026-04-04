use crate::api::AppState;
use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::future::join_all;
use futures::{FutureExt, StreamExt, stream};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
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
    let local_shard_ids: HashSet<u32> = local_shards.iter().map(|(id, _)| *id).collect();

    let mut count: u64 = 0;
    let mut successful: u32 = 0;
    let mut failed: u32 = 0;

    for (_shard_id, engine) in &local_shards {
        count += engine.doc_count();
        successful += 1;
    }

    let mut remote_handles = Vec::new();
    for (node, shard_ids) in remote_count_targets(&cluster_state, metadata, &local_shard_ids) {
        let client = state.transport_client.clone();
        let idx = index_name.to_string();
        let num_shards = shard_ids.len() as u32;
        remote_handles.push(tokio::spawn(async move {
            let result = client.get_shard_stats(&node).await.map(|stats| {
                shard_ids
                    .into_iter()
                    .map(|shard_id| stats.get(&(idx.clone(), shard_id)).copied().unwrap_or(0))
                    .sum::<u64>()
            });
            (result, num_shards)
        }));
    }

    for handle in remote_handles {
        match handle.await {
            Ok((Ok(remote_count), num_shards)) => {
                count += remote_count;
                successful += num_shards;
            }
            Ok((Err(_), num_shards)) => {
                failed += num_shards;
            }
            Err(_) => {
                failed += 1;
            }
        }
    }

    (count, successful, failed)
}

fn remote_count_targets(
    cluster_state: &crate::cluster::state::ClusterState,
    metadata: &crate::cluster::state::IndexMetadata,
    local_shard_ids: &HashSet<u32>,
) -> Vec<(crate::cluster::state::NodeInfo, Vec<u32>)> {
    let mut per_node: HashMap<String, (crate::cluster::state::NodeInfo, Vec<u32>)> = HashMap::new();

    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }
        if let Some(node) = cluster_state.nodes.get(&routing.primary) {
            let entry = per_node
                .entry(node.id.clone())
                .or_insert_with(|| (node.clone(), Vec::new()));
            entry.1.push(*shard_id);
        }
    }

    per_node.into_values().collect()
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
    let _timer = crate::metrics::SEARCH_LATENCY_SECONDS.start_timer();

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

    // Dispatch all local shard searches in parallel
    let search_futures: Vec<_> = local_shards
        .iter()
        .map(|(shard_id, engine)| {
            let engine = engine.clone();
            let query = params.q.clone();
            let shard_id = *shard_id;
            let pools = state.worker_pools.clone();
            async move {
                let result = pools.spawn_search(move || engine.search(&query)).await;
                (shard_id, result)
            }
        })
        .collect();
    let search_results = futures::future::join_all(search_futures).await;

    for (shard_id, search_result) in search_results {
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

    crate::metrics::SEARCH_QUERIES_TOTAL.inc();

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
            let timings = match serde_json::to_value(&result.timings) {
                Ok(timings) => timings,
                Err(err) => {
                    return crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "serialization_exception",
                        format!("Failed to serialize timings: {}", err),
                    );
                }
            };
            explain["timings"] = timings;
            explain["execution_mode"] = serde_json::json!(result.execution_mode);
            explain["streaming_used"] = serde_json::json!(result.streaming_used);
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
            if let Some(key_count) = result.semijoin_key_count {
                explain["semijoin"]["resolved_key_count"] = serde_json::json!(key_count);
            }
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
    streaming_used: bool,
    truncated: bool,
    semijoin_key_count: Option<usize>,
    timings: crate::hybrid::SqlTimings,
}

struct DirectSqlBatches {
    batches: Vec<RecordBatch>,
    total_hits: usize,
    collected_hits: usize,
    streaming_used: bool,
    successful_shards: u32,
    failed_shards: u32,
}

struct DirectSqlPartitions {
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    partitions: Vec<Arc<dyn datafusion::physical_plan::streaming::PartitionStream>>,
    total_hits: usize,
    collected_hits: usize,
    streaming_used: bool,
    successful_shards: u32,
    failed_shards: u32,
}

struct SqlStreamMeta {
    execution_mode: &'static str,
    streaming_used: bool,
    truncated: bool,
    matched_hits: usize,
    successful_shards: u32,
    failed_shards: u32,
    columns: Vec<String>,
}

async fn collect_direct_sql_batches(
    state: &AppState,
    index_name: &str,
    metadata: &crate::cluster::state::IndexMetadata,
    plan: &crate::hybrid::QueryPlan,
    search_req: &crate::search::SearchRequest,
    local_shards: &[(u32, Arc<dyn crate::engine::SearchEngine>)],
    local_shard_ids: &HashSet<u32>,
) -> anyhow::Result<DirectSqlBatches> {
    let mut batches = Vec::new();
    let mut total_hits = 0usize;
    let mut collected_hits = 0usize;
    let mut successful_shards = 0u32;
    let use_streaming = plan.has_group_by_fallback;
    let mut used_streaming = false;

    let sql_futures: Vec<_> = local_shards
        .iter()
        .map(|(_shard_id, engine)| {
            let engine = engine.clone();
            let req = search_req.clone();
            let columns = plan.required_columns.clone();
            let needs_id = plan.needs_id;
            let needs_score = plan.needs_score;
            let pools = state.worker_pools.clone();
            if use_streaming {
                async move {
                    pools
                        .spawn_search(move || {
                            match engine.sql_streaming_batches(
                                &req,
                                &columns,
                                needs_id,
                                needs_score,
                                0,
                            )? {
                                Some(result) => Ok(Some((result.batches, result.total_hits, true))),
                                None => engine
                                    .sql_record_batch(&req, &columns, needs_id, needs_score)
                                    .map(|opt| opt.map(|r| (vec![r.batch], r.total_hits, false))),
                            }
                        })
                        .await
                }
                .boxed()
            } else {
                async move {
                    pools
                        .spawn_search(move || {
                            engine
                                .sql_record_batch(&req, &columns, needs_id, needs_score)
                                .map(|opt| opt.map(|r| (vec![r.batch], r.total_hits, false)))
                        })
                        .await
                }
                .boxed()
            }
        })
        .collect();
    let sql_results = join_all(sql_futures).await;

    for batch_result in sql_results {
        match batch_result {
            Ok(Ok(Some((shard_batches, hits, streamed)))) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += shard_batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>();
                if streamed {
                    used_streaming = true;
                }
                batches.extend(shard_batches);
            }
            Ok(Ok(None)) => {
                return Err(anyhow::anyhow!(
                    "local shard does not support direct SQL batches"
                ));
            }
            Ok(Err(error)) | Err(error) => return Err(error),
        }
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut remote_futures = Vec::new();

    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }

        let primary_node_id = &routing.primary;
        if let Some(node) = cluster_state.nodes.get(primary_node_id) {
            let client = state.transport_client.clone();
            let node = node.clone();
            let index_name = index_name.to_string();
            let shard_id = *shard_id;
            let req = search_req.clone();
            let columns = plan.required_columns.clone();
            let needs_id = plan.needs_id;
            let needs_score = plan.needs_score;
            remote_futures.push(tokio::spawn(async move {
                if use_streaming {
                    client
                        .forward_sql_batch_stream_to_shard(
                            &node,
                            &index_name,
                            shard_id,
                            &req,
                            &columns,
                            needs_id,
                            needs_score,
                            0,
                        )
                        .await
                        .map(|(batches, hits)| (batches, hits, true))
                } else {
                    client
                        .forward_sql_batch_to_shard(
                            &node,
                            &index_name,
                            shard_id,
                            &req,
                            &columns,
                            needs_id,
                            needs_score,
                        )
                        .await
                        .map(|(batch, hits)| (vec![batch], hits, false))
                }
            }));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for result in remote_results {
        match result {
            Ok(Ok((shard_batches, hits, streamed))) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += shard_batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>();
                if streamed {
                    used_streaming = true;
                }
                batches.extend(shard_batches);
            }
            Ok(Err(error)) => return Err(error),
            Err(error) => {
                return Err(anyhow::anyhow!("remote shard task failed: {}", error));
            }
        }
    }

    Ok(DirectSqlBatches {
        batches,
        total_hits,
        collected_hits,
        streaming_used: used_streaming,
        successful_shards,
        failed_shards: 0,
    })
}

async fn collect_direct_sql_partitions(
    state: &AppState,
    index_name: &str,
    metadata: &crate::cluster::state::IndexMetadata,
    plan: &crate::hybrid::QueryPlan,
    search_req: &crate::search::SearchRequest,
    local_shards: &[(u32, Arc<dyn crate::engine::SearchEngine>)],
    local_shard_ids: &HashSet<u32>,
) -> anyhow::Result<DirectSqlPartitions> {
    let target_schema = crate::hybrid::direct_sql_input_schema(plan, &metadata.mappings)?;
    let plan = Arc::new(plan.clone());
    let mut partitions = Vec::new();
    let mut total_hits = 0usize;
    let mut collected_hits = 0usize;
    let mut successful_shards = 0u32;
    let mut used_streaming = false;

    // Gate streaming on has_group_by_fallback to match collect_direct_sql_batches.
    // Non-GROUP-BY queries go straight to TopDocs — avoids a wasted
    // sql_streaming_batches() call that will return None for scored/LIMIT queries.
    let use_streaming = plan.has_group_by_fallback;

    let local_futures: Vec<_> = local_shards
        .iter()
        .map(|(_shard_id, engine)| {
            let engine = engine.clone();
            let req = search_req.clone();
            let columns = plan.required_columns.clone();
            let needs_id = plan.needs_id;
            let needs_score = plan.needs_score;
            let pools = state.worker_pools.clone();
            if use_streaming {
                async move {
                    pools
                        .spawn_search(move || {
                            match engine.sql_streaming_batches(
                                &req,
                                &columns,
                                needs_id,
                                needs_score,
                                0,
                            )? {
                                Some(result) => Ok(Some((result.batches, result.total_hits, true))),
                                None => engine
                                    .sql_record_batch(&req, &columns, needs_id, needs_score)
                                    .map(|opt| opt.map(|r| (vec![r.batch], r.total_hits, false))),
                            }
                        })
                        .await
                }
                .boxed()
            } else {
                async move {
                    pools
                        .spawn_search(move || {
                            engine
                                .sql_record_batch(&req, &columns, needs_id, needs_score)
                                .map(|opt| opt.map(|r| (vec![r.batch], r.total_hits, false)))
                        })
                        .await
                }
                .boxed()
            }
        })
        .collect();

    for batch_result in join_all(local_futures).await {
        match batch_result {
            Ok(Ok(Some((shard_batches, hits, streamed)))) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += shard_batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>();
                used_streaming |= streamed;

                let normalized_batches = shard_batches
                    .into_iter()
                    .map(|batch| {
                        crate::hybrid::normalize_sql_batch_for_schema(
                            batch,
                            plan.as_ref(),
                            &target_schema,
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let partition_schema = target_schema.clone();
                let stream = RecordBatchStreamAdapter::new(
                    partition_schema.clone(),
                    stream::iter(
                        normalized_batches
                            .into_iter()
                            .map(Ok::<RecordBatch, DataFusionError>),
                    ),
                );
                partitions.push(crate::hybrid::one_shot_partition_stream(
                    partition_schema,
                    Box::pin(stream),
                ));
            }
            Ok(Ok(None)) => {
                return Err(anyhow::anyhow!(
                    "local shard does not support direct SQL batches"
                ));
            }
            Ok(Err(error)) | Err(error) => return Err(error),
        }
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut remote_futures = Vec::new();

    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue;
        }

        let primary_node_id = &routing.primary;
        if let Some(node) = cluster_state.nodes.get(primary_node_id) {
            let client = state.transport_client.clone();
            let node = node.clone();
            let index_name = index_name.to_string();
            let shard_id = *shard_id;
            let req = search_req.clone();
            let columns = plan.required_columns.clone();
            let needs_id = plan.needs_id;
            let needs_score = plan.needs_score;
            remote_futures.push(tokio::spawn(async move {
                if use_streaming {
                    client
                        .forward_sql_batch_stream_to_shard(
                            &node,
                            &index_name,
                            shard_id,
                            &req,
                            &columns,
                            needs_id,
                            needs_score,
                            0,
                        )
                        .await
                        .map(|(batches, hits)| (batches, hits, true))
                } else {
                    client
                        .forward_sql_batch_to_shard(
                            &node,
                            &index_name,
                            shard_id,
                            &req,
                            &columns,
                            needs_id,
                            needs_score,
                        )
                        .await
                        .map(|(batch, hits)| (vec![batch], hits, false))
                }
            }));
        }
    }

    for result in join_all(remote_futures).await {
        match result {
            Ok(Ok((shard_batches, hits, streamed))) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += shard_batches
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>();
                if streamed {
                    used_streaming = true;
                }

                let normalized_batches = shard_batches
                    .into_iter()
                    .map(|batch| {
                        crate::hybrid::normalize_sql_batch_for_schema(
                            batch,
                            plan.as_ref(),
                            &target_schema,
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let partition_schema = target_schema.clone();
                let stream = RecordBatchStreamAdapter::new(
                    partition_schema.clone(),
                    stream::iter(
                        normalized_batches
                            .into_iter()
                            .map(Ok::<RecordBatch, DataFusionError>),
                    ),
                );
                partitions.push(crate::hybrid::one_shot_partition_stream(
                    partition_schema,
                    Box::pin(stream),
                ));
            }
            Ok(Err(error)) => return Err(error),
            Err(error) => {
                return Err(anyhow::anyhow!("remote shard task failed: {}", error));
            }
        }
    }

    Ok(DirectSqlPartitions {
        schema: target_schema,
        partitions,
        total_hits,
        collected_hits,
        streaming_used: used_streaming,
        successful_shards,
        failed_shards: 0,
    })
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

fn semijoin_filter_clause(outer_key: &str, key_values: &[Value]) -> crate::search::QueryClause {
    if key_values.len() == 1 {
        return crate::search::QueryClause::Term(HashMap::from([(
            outer_key.to_string(),
            key_values[0].clone(),
        )]));
    }

    crate::search::QueryClause::Bool(crate::search::BoolQuery {
        should: key_values
            .iter()
            .cloned()
            .map(|value| {
                crate::search::QueryClause::Term(HashMap::from([(outer_key.to_string(), value)]))
            })
            .collect(),
        ..Default::default()
    })
}

fn apply_semijoin_filter(
    search_req: &mut crate::search::SearchRequest,
    semijoin: &crate::hybrid::planner::SemijoinPlan,
    key_values: &[Value],
) {
    if key_values.is_empty() {
        search_req.query = crate::search::QueryClause::MatchNone(serde_json::json!({}));
        return;
    }

    let filter_clause = semijoin_filter_clause(&semijoin.outer_key, key_values);
    match &mut search_req.query {
        crate::search::QueryClause::MatchAll(_) => {
            search_req.query = crate::search::QueryClause::Bool(crate::search::BoolQuery {
                filter: vec![filter_clause],
                ..Default::default()
            });
        }
        crate::search::QueryClause::Bool(bool_query) => {
            bool_query.filter.push(filter_clause);
        }
        existing => {
            let current = existing.clone();
            search_req.query = crate::search::QueryClause::Bool(crate::search::BoolQuery {
                must: vec![current],
                filter: vec![filter_clause],
                ..Default::default()
            });
        }
    }
}

fn collect_semijoin_key(
    keys: &mut Vec<Value>,
    seen: &mut HashSet<String>,
    value: Value,
) -> Result<(), (StatusCode, Json<Value>)> {
    if value.is_null() {
        return Ok(());
    }

    let serialized = serde_json::to_string(&value).unwrap_or_else(|_| value.to_string());
    if !seen.insert(serialized) {
        return Ok(());
    }

    if keys.len() >= crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "semijoin_key_limit_exceeded",
            format!(
                "Semijoin inner query produced more than {} distinct keys. Narrow the inner query or add stronger filters before lowering it into the outer search.",
                crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS
            ),
        ));
    }

    keys.push(value);
    Ok(())
}

async fn resolve_semijoin_keys(
    state: &AppState,
    semijoin: &crate::hybrid::planner::SemijoinPlan,
) -> Result<Vec<Value>, (StatusCode, Json<Value>)> {
    let inner_result = execute_sql_query_with_plan(
        state,
        semijoin.inner_plan.as_ref().clone(),
        0.0,
        Instant::now(),
        false,
    );
    let inner_result = Box::pin(inner_result).await?;

    let column = inner_result
        .sql_result
        .columns
        .first()
        .cloned()
        .ok_or_else(|| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sql_execution_exception",
                "semijoin inner query did not return a key column",
            )
        })?;

    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for row in &inner_result.sql_result.rows {
        let Some(value) = row.get(&column).cloned() else {
            continue;
        };
        collect_semijoin_key(&mut keys, &mut seen, value)?;
    }

    Ok(keys)
}

async fn execute_sql_query_with_plan(
    state: &AppState,
    plan: crate::hybrid::QueryPlan,
    planning_ms: f64,
    total_start: Instant,
    record_metrics: bool,
) -> Result<SqlExecutionResult, (StatusCode, Json<Value>)> {
    let mut semijoin_key_count = None;
    let mut semijoin_ms = 0.0;
    let mut search_req = plan.to_search_request(state.sql_approximate_top_k);

    if let Some(semijoin) = &plan.semijoin {
        let semijoin_start = Instant::now();
        let key_values = resolve_semijoin_keys(state, semijoin).await?;
        semijoin_ms = semijoin_start.elapsed().as_secs_f64() * 1000.0;
        semijoin_key_count = Some(key_values.len());
        apply_semijoin_filter(&mut search_req, semijoin, &key_values);
    }

    // GROUP BY fallback: override the default 100K cap so DataFusion sees all
    // matching docs.  Uses the configurable sql_group_by_scan_limit (0 = unlimited).
    if plan.has_group_by_fallback {
        search_req.size = if state.sql_group_by_scan_limit == 0 {
            usize::MAX
        } else {
            state.sql_group_by_scan_limit
        };
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&plan.index_name) {
        Some(m) => m.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", plan.index_name),
            ));
        }
    };

    // count(*) fast path: answer from doc_count() metadata without scanning docs
    if plan.is_count_star_only() {
        let search_start = Instant::now();
        let (count, successful_shards, failed_shards) =
            count_docs_from_metadata(state, &plan.index_name, &metadata).await;
        let search_ms = search_start.elapsed().as_secs_f64() * 1000.0;

        let Some(grouped_sql) = plan.grouped_sql.as_ref() else {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "planning_exception",
                "count_star_fast selected without grouped SQL metadata",
            ));
        };
        let columns: Vec<String> = grouped_sql
            .metrics
            .iter()
            .filter(|metric| metric.projected)
            .map(|metric| metric.output_name.clone())
            .collect();
        let mut row = serde_json::Map::new();
        for m in grouped_sql.metrics.iter().filter(|metric| metric.projected) {
            row.insert(m.output_name.clone(), serde_json::json!(count));
        }
        let rows = vec![serde_json::Value::Object(row)];

        if record_metrics {
            crate::metrics::SQL_QUERIES_TOTAL
                .with_label_values(&["count_star_fast"])
                .inc();
        }

        return Ok(SqlExecutionResult {
            plan,
            sql_result: crate::hybrid::SqlQueryResult { columns, rows },
            matched_hits: count as usize,
            successful_shards,
            failed_shards,
            execution_mode: "count_star_fast",
            streaming_used: false,
            truncated: false,
            semijoin_key_count,
            timings: crate::hybrid::SqlTimings {
                planning_ms,
                semijoin_ms,
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
        // Validate that GROUP BY columns are fast-field eligible (keyword, integer, float).
        // Text fields are analyzed/tokenized and don't have fast-field columnar storage.
        if let Some(grouped) = &plan.grouped_sql {
            for col in &grouped.group_columns {
                if let Some(mapping) = metadata.mappings.get(&col.source_name)
                    && matches!(mapping.field_type, crate::cluster::state::FieldType::Text)
                {
                    return Err(crate::api::error_response(
                        StatusCode::BAD_REQUEST,
                        "group_by_text_field_exception",
                        format!(
                            "Cannot GROUP BY field '{}': text fields are tokenized and don't support grouping. Use a 'keyword' type field instead.",
                            col.source_name
                        ),
                    ));
                }
            }
        }

        let search_start = Instant::now();
        let distributed = match crate::api::index::execute_distributed_dsl_search(
            state,
            &plan.index_name,
            &search_req,
        )
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

        if record_metrics {
            crate::metrics::SQL_QUERIES_TOTAL
                .with_label_values(&["tantivy_grouped_partials"])
                .inc();
        }

        return Ok(SqlExecutionResult {
            plan,
            sql_result,
            matched_hits: distributed.total_hits,
            successful_shards: distributed.successful_shards,
            failed_shards: distributed.failed_shards,
            execution_mode: "tantivy_grouped_partials",
            streaming_used: false,
            truncated: false,
            semijoin_key_count,
            timings: crate::hybrid::SqlTimings {
                planning_ms,
                semijoin_ms,
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
        &plan.index_name,
        &metadata,
        "SQL search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    // Fast-field path: collect Arrow batches from local + remote shards
    let search_start = Instant::now();
    let direct_sql = if !plan.selects_all_columns {
        match collect_direct_sql_batches(
            state,
            &plan.index_name,
            &metadata,
            &plan,
            &search_req,
            &local_shards,
            &local_shard_ids,
        )
        .await
        {
            Ok(result) => Some(result),
            Err(error) => {
                tracing::warn!(
                    "Falling back to materialized SQL execution for [{}]: {}",
                    plan.index_name,
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
    let (
        sql_result,
        matched_hits,
        collected_hits,
        streaming_used,
        successful_shards,
        failed_shards,
        execution_mode,
    ) = if let Some(direct_sql) = direct_sql {
        let sql_result =
            match crate::hybrid::execute_planned_sql_batches(&plan, direct_sql.batches).await {
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
            direct_sql.total_hits,
            direct_sql.collected_hits,
            direct_sql.streaming_used,
            direct_sql.successful_shards,
            direct_sql.failed_shards,
            "tantivy_fast_fields",
        )
    } else {
        let distributed = match crate::api::index::execute_distributed_dsl_search(
            state,
            &plan.index_name,
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
            distributed.all_hits.len(),
            false,
            distributed.successful_shards,
            distributed.failed_shards,
            "materialized_hits_fallback",
        )
    };
    let datafusion_ms = datafusion_start.elapsed().as_secs_f64() * 1000.0;

    let truncated = !plan.limit_pushed_down
        && plan.limit.is_none()
        && collected_hits < matched_hits
        && execution_mode != "tantivy_grouped_partials";

    // GROUP BY over incomplete data produces wrong aggregates — error instead
    // of returning silently incorrect results.
    // Note: streaming removes the local and remote unary-batch bottleneck only
    // for score-free queries whose requested columns stay fully fast-field-backed.
    // Any shard that still needs score or stored-doc fallback remains capped, so
    // the guard keys off whether we actually collected fewer rows than matched.
    if truncated && plan.has_group_by_fallback {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "group_by_scan_limit_exceeded",
            format!(
                "GROUP BY query matched {} docs but only {} were collected. \
                 Results would be incorrect. Narrow the query with filters, \
                 rewrite to use simple GROUP BY columns with count/sum/avg/min/max \
                 for the grouped_partials path, or increase sql_group_by_scan_limit \
                 (currently {}) in ferrissearch.yml.",
                matched_hits, collected_hits, state.sql_group_by_scan_limit
            ),
        ));
    }

    if record_metrics {
        crate::metrics::SQL_QUERIES_TOTAL
            .with_label_values(&[execution_mode])
            .inc();
    }

    Ok(SqlExecutionResult {
        plan,
        sql_result,
        matched_hits,
        successful_shards,
        failed_shards,
        execution_mode,
        streaming_used,
        truncated,
        semijoin_key_count,
        timings: crate::hybrid::SqlTimings {
            planning_ms,
            semijoin_ms,
            search_ms,
            collect_ms: 0.0,
            merge_ms: 0.0,
            datafusion_ms,
            total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
        },
    })
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
    let _sql_timer = crate::metrics::SQL_LATENCY_SECONDS.start_timer();

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
    let planning_ms = plan_start.elapsed().as_secs_f64() * 1000.0;

    execute_sql_query_with_plan(state, plan, planning_ms, total_start, true).await
}

fn planner_metadata_json(plan: &crate::hybrid::planner::QueryPlan) -> Value {
    serde_json::json!({
        "text_match": plan.primary_text_match().map(|tm| serde_json::json!({
            "field": tm.field,
            "query": tm.query,
        })),
        "text_matches": plan.text_matches_json(),
        "pushed_down_filters": &plan.pushed_filters,
        "group_by_columns": &plan.group_by_columns,
        "required_columns": &plan.required_columns,
        "has_residual_predicates": plan.has_residual_predicates,
        "semijoin": plan.semijoin.as_ref().map(|semijoin| serde_json::json!({
            "outer_key": semijoin.outer_key,
            "inner_key": semijoin.inner_key,
            "key_limit": crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS,
        })),
    })
}

fn sql_response_body(result: &SqlExecutionResult) -> Value {
    serde_json::json!({
        "execution_mode": result.execution_mode,
        "streaming_used": result.streaming_used,
        "truncated": result.truncated,
        "planner": planner_metadata_json(&result.plan),
        "_shards": {
            "total": result.successful_shards + result.failed_shards,
            "successful": result.successful_shards,
            "failed": result.failed_shards,
        },
        "matched_hits": result.matched_hits,
        "columns": result.sql_result.columns,
        "rows": result.sql_result.rows,
    })
}

fn sql_stream_meta_json(plan: &crate::hybrid::QueryPlan, meta: SqlStreamMeta) -> Value {
    serde_json::json!({
        "execution_mode": meta.execution_mode,
        "streaming_used": meta.streaming_used,
        "truncated": meta.truncated,
        "planner": planner_metadata_json(plan),
        "_shards": {
            "total": meta.successful_shards + meta.failed_shards,
            "successful": meta.successful_shards,
            "failed": meta.failed_shards,
        },
        "matched_hits": meta.matched_hits,
        "columns": meta.columns,
    })
}

fn ndjson_bytes(frame: &Value) -> Bytes {
    let mut bytes = match serde_json::to_vec(frame) {
        Ok(bytes) => bytes,
        Err(error) => serde_json::to_vec(&serde_json::json!({
            "type": "error",
            "reason": format!("Failed to serialize SQL stream frame: {}", error),
        }))
        .unwrap_or_else(|_| {
            b"{\"type\":\"error\",\"reason\":\"Failed to serialize SQL stream frame\"}".to_vec()
        }),
    };
    bytes.push(b'\n');
    Bytes::from(bytes)
}

fn stream_json_response(mut body: Value) -> Response {
    let rows = body
        .get("rows")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();

    if let Some(object) = body.as_object_mut() {
        object.remove("rows");
        object.insert("type".to_string(), serde_json::json!("meta"));
    }

    let mut frames = vec![body];
    if !rows.is_empty() {
        frames.push(serde_json::json!({
            "type": "rows",
            "rows": rows,
        }));
    }

    let body_stream = stream::iter(
        frames
            .into_iter()
            .map(|frame| Ok::<Bytes, Infallible>(ndjson_bytes(&frame))),
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(body_stream))
        .unwrap()
}

fn stream_sql_batches_response(
    mut meta: Value,
    batch_stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Response {
    if let Some(object) = meta.as_object_mut() {
        object.insert("type".to_string(), serde_json::json!("meta"));
    }

    let body_stream = stream::once(async move { Ok::<Bytes, Infallible>(ndjson_bytes(&meta)) })
        .chain(batch_stream.filter_map(|batch_result| async {
            match batch_result {
                Ok(batch) if batch.num_rows() == 0 => None,
                Ok(batch) => {
                    let frame = match crate::hybrid::merge::record_batches_to_json_rows(&[batch]) {
                        Ok((_columns, rows)) => serde_json::json!({
                            "type": "rows",
                            "rows": rows,
                        }),
                        Err(error) => serde_json::json!({
                            "type": "error",
                            "reason": format!("Failed to serialize streamed SQL batch: {}", error),
                        }),
                    };
                    Some(Ok::<Bytes, Infallible>(ndjson_bytes(&frame)))
                }
                Err(error) => {
                    let frame = serde_json::json!({
                        "type": "error",
                        "reason": error.to_string(),
                    });
                    Some(Ok::<Bytes, Infallible>(ndjson_bytes(&frame)))
                }
            }
        }));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(body_stream))
        .unwrap()
}

async fn execute_sql_stream_query(
    state: &AppState,
    index_name: &str,
    query: &str,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let _sql_timer = crate::metrics::SQL_LATENCY_SECONDS.start_timer();

    let plan = match crate::hybrid::planner::plan_sql(index_name, query) {
        Ok(plan) => plan,
        Err(error) => {
            return Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                error,
            ));
        }
    };

    if plan.is_count_star_only() || plan.uses_grouped_partials() || plan.selects_all_columns {
        let result = execute_sql_query(state, index_name, query).await?;
        return Ok(stream_json_response(sql_response_body(&result)));
    }

    let mut search_req = plan.to_search_request(state.sql_approximate_top_k);
    if plan.has_group_by_fallback {
        search_req.size = if state.sql_group_by_scan_limit == 0 {
            usize::MAX
        } else {
            state.sql_group_by_scan_limit
        };
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name) {
        Some(metadata) => metadata.clone(),
        None => {
            return Err(crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            ));
        }
    };

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        &metadata,
        "SQL search",
    );
    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

    let direct_sql = match collect_direct_sql_partitions(
        state,
        index_name,
        &metadata,
        &plan,
        &search_req,
        &local_shards,
        &local_shard_ids,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(
                "Falling back to buffered SQL execution for streamed endpoint [{}]: {}",
                index_name,
                error
            );
            let result = execute_sql_query(state, index_name, query).await?;
            return Ok(stream_json_response(sql_response_body(&result)));
        }
    };

    let truncated = !plan.limit_pushed_down
        && plan.limit.is_none()
        && direct_sql.collected_hits < direct_sql.total_hits;

    if truncated && plan.has_group_by_fallback {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "group_by_scan_limit_exceeded",
            format!(
                "GROUP BY query matched {} docs but only {} were collected. \
                 Results would be incorrect. Narrow the query with filters, \
                 rewrite to use simple GROUP BY columns with count/sum/avg/min/max \
                 for the grouped_partials path, or increase sql_group_by_scan_limit \
                 (currently {}) in ferrissearch.yml.",
                direct_sql.total_hits, direct_sql.collected_hits, state.sql_group_by_scan_limit
            ),
        ));
    }

    let (columns, batch_stream) = match crate::hybrid::execute_planned_sql_partition_streams(
        &plan,
        direct_sql.schema.clone(),
        direct_sql.partitions,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sql_execution_exception",
                error,
            ));
        }
    };

    crate::metrics::SQL_QUERIES_TOTAL
        .with_label_values(&["tantivy_fast_fields"])
        .inc();

    Ok(stream_sql_batches_response(
        sql_stream_meta_json(
            &plan,
            SqlStreamMeta {
                execution_mode: "tantivy_fast_fields",
                streaming_used: direct_sql.streaming_used,
                truncated,
                matched_hits: direct_sql.total_hits,
                successful_shards: direct_sql.successful_shards,
                failed_shards: direct_sql.failed_shards,
                columns,
            },
        ),
        batch_stream,
    ))
}

/// POST /{index}/_sql/stream — NDJSON stream of SQL results.
pub async fn search_sql_stream(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<SqlQueryRequest>,
) -> Response {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        )
        .into_response();
    }

    match execute_sql_stream_query(&state, &index_name, &req.query).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

/// POST /_sql/stream — Global NDJSON SQL stream endpoint.
pub async fn global_sql_stream(
    State(state): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> Response {
    let trimmed = req.query.trim();

    if matches_command(trimmed, "SHOW TABLES") || matches_command(trimmed, "SHOW INDICES") {
        let (status, body) = handle_show_tables(&state).await;
        return if status.is_success() {
            stream_json_response(body.0)
        } else {
            (status, body).into_response()
        };
    }

    if let Some(table) =
        strip_command(trimmed, "DESCRIBE").or_else(|| strip_command(trimmed, "DESC"))
    {
        let table = unquote_identifier(table);
        let (status, body) = handle_describe(&state, &table).await;
        return if status.is_success() {
            stream_json_response(body.0)
        } else {
            (status, body).into_response()
        };
    }

    if let Some(table) = strip_command(trimmed, "SHOW CREATE TABLE") {
        let table = unquote_identifier(table);
        let (status, body) = handle_show_create_table(&state, &table).await;
        return if status.is_success() {
            stream_json_response(body.0)
        } else {
            (status, body).into_response()
        };
    }

    let index_name = match extract_index_from_sql(trimmed) {
        Some(name) => name,
        None => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                "Could not determine index from SQL. Use: SELECT ... FROM \"index_name\" ..., or use SHOW TABLES / DESCRIBE",
            )
            .into_response();
        }
    };

    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        )
        .into_response();
    }

    match execute_sql_stream_query(&state, &index_name, &req.query).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
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

    (StatusCode::OK, Json(sql_response_body(&result)))
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

    (StatusCode::OK, Json(sql_response_body(&result)))
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
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    fn sorted_remote_targets(
        cluster_state: &ClusterState,
        metadata: &IndexMetadata,
        local_shard_ids: &HashSet<u32>,
    ) -> Vec<(String, Vec<u32>)> {
        let mut targets: Vec<_> =
            super::remote_count_targets(cluster_state, metadata, local_shard_ids)
                .into_iter()
                .map(|(node, mut shard_ids)| {
                    shard_ids.sort_unstable();
                    (node.id, shard_ids)
                })
                .collect();
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        targets
    }

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
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
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
    async fn search_sql_marks_expression_group_by_fast_field_streaming() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT LOWER(brand) AS brand_bucket, COUNT(*) AS total FROM products WHERE text_match(description, 'iphone') GROUP BY LOWER(brand) ORDER BY brand_bucket ASC".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_fast_fields");
        assert_eq!(body["streaming_used"], true);
        assert_eq!(body["matched_hits"], 3);

        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["brand_bucket"], json!("apple"));
        assert_eq!(rows[0]["total"], json!(2));
        assert_eq!(rows[1]["brand_bucket"], json!("samsung"));
        assert_eq!(rows[1]["total"], json!(1));
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
    async fn search_sql_semijoin_inner_hidden_order_by_metric_executes() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT title, brand FROM products WHERE brand IN (SELECT brand FROM products GROUP BY brand ORDER BY SUM(price) DESC LIMIT 1) ORDER BY title ASC".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], json!("tantivy_fast_fields"));
        assert_eq!(body["matched_hits"], json!(2));
        assert_eq!(body["planner"]["semijoin"]["outer_key"], json!("brand"));

        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["title"], json!("iPhone"));
        assert_eq!(rows[0]["brand"], json!("Apple"));
        assert_eq!(rows[1]["title"], json!("iPhone Pro"));
        assert_eq!(rows[1]["brand"], json!("Apple"));
    }

    #[test]
    fn collect_semijoin_key_allows_exact_limit_and_rejects_next_distinct_key() {
        let mut keys = Vec::new();
        let mut seen = HashSet::new();

        for key in 0..crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS {
            collect_semijoin_key(&mut keys, &mut seen, json!(key)).unwrap();
        }

        let err = collect_semijoin_key(
            &mut keys,
            &mut seen,
            json!(crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS),
        )
        .unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(keys.len(), crate::hybrid::planner::SQL_SEMIJOIN_MAX_KEYS);
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
        assert_eq!(body["streaming_used"], false);
        assert!(body["timings"]["total_ms"].as_f64().unwrap() > 0.0);
        assert_eq!(body["matched_hits"], 3);
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[tokio::test]
    async fn search_sql_order_by_aggregate_expr_without_alias_uses_grouped_partials() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, SUM(price) FROM products WHERE text_match(description, 'iphone') GROUP BY brand ORDER BY SUM(price) DESC LIMIT 2".to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
        assert_eq!(body["matched_hits"], 3);

        let columns = body["columns"].as_array().unwrap();
        assert_eq!(columns.len(), 2);
        let metric_column = columns[1].as_str().unwrap().to_string();
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["brand"], json!("Apple"));
        assert_eq!(rows[0].get(&metric_column), Some(&json!(1898.0)));
        assert_eq!(rows[1]["brand"], json!("Samsung"));
        assert_eq!(rows[1].get(&metric_column), Some(&json!(799.0)));
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

    #[tokio::test(flavor = "multi_thread")]
    async fn group_by_text_field_returns_error() {
        // Create metadata with a Text field (not Keyword)
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));

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
            "description".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        cluster_state.add_index(IndexMetadata {
            name: "texttest".to_string(),
            uuid: String::new(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        });

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
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
        };

        // GROUP BY on text field should return error
        let (status, Json(body)) = search_sql(
            State(state.clone()),
            Path("texttest".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT description, count(*) AS cnt FROM texttest GROUP BY description"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"]["type"], "group_by_text_field_exception",
            "should reject GROUP BY on text fields"
        );

        // GROUP BY on keyword field should succeed (not error)
        let (status2, _) = search_sql(
            State(state),
            Path("texttest".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT category, count(*) AS cnt FROM texttest GROUP BY category"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status2, StatusCode::OK, "keyword fields should work");
    }

    #[tokio::test]
    async fn non_streamable_expression_group_by_with_tiny_scan_limit_returns_error() {
        // Set sql_group_by_scan_limit to 1 so even 3 docs triggers the error.
        let (_tmp, mut state) = make_test_app_state("products");
        state.sql_group_by_scan_limit = 1;

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT LOWER(description) AS d, count(*) AS cnt FROM products GROUP BY LOWER(description)"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"]["type"], "group_by_scan_limit_exceeded",
            "should error when GROUP BY fallback hits scan limit"
        );
        let reason = body["error"]["reason"].as_str().unwrap();
        assert!(
            reason.contains("only") && reason.contains("were collected"),
            "error should mention collected vs matched: {reason}"
        );
    }

    #[tokio::test]
    async fn streamable_expression_group_by_ignores_tiny_scan_limit() {
        let (_tmp, mut state) = make_test_app_state("products");
        state.sql_group_by_scan_limit = 1;

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT LOWER(brand) AS b, count(*) AS cnt FROM products GROUP BY LOWER(brand) ORDER BY b"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_fast_fields");
        assert_eq!(body["streaming_used"], true);
        assert_eq!(body["truncated"], false);

        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["b"], "apple");
        assert_eq!(rows[0]["cnt"], 2);
        assert_eq!(rows[1]["b"], "samsung");
        assert_eq!(rows[1]["cnt"], 1);
    }

    #[tokio::test]
    async fn having_aggregate_source_field_survives_alias_shadowing() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS price FROM products GROUP BY brand HAVING AVG(price) > 850 ORDER BY brand"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_grouped_partials");
        assert_eq!(body["streaming_used"], false);

        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["brand"], "Apple");
        assert_eq!(rows[0]["price"], 2);
    }

    #[tokio::test]
    async fn wrapped_having_aggregate_source_field_survives_alias_shadowing() {
        let (_tmp, state) = make_test_app_state("products");

        let (status, Json(body)) = search_sql(
            State(state),
            Path("products".to_string()),
            Json(SqlQueryRequest {
                query: "SELECT brand, count(*) AS price FROM products GROUP BY brand HAVING COALESCE(AVG(price), 0) > 850 ORDER BY brand"
                    .to_string(),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["execution_mode"], "tantivy_fast_fields");
        assert_eq!(body["streaming_used"], true);

        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["brand"], "Apple");
        assert_eq!(rows[0]["price"], 2);
    }

    #[test]
    fn remote_count_targets_groups_shards_per_node() {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));
        cluster_state.add_node(make_test_node("node-2"));
        cluster_state.add_node(make_test_node("node-3"));

        let mut metadata = make_sql_metadata("products");
        metadata.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-2".to_string(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        metadata.shard_routing.insert(
            2,
            ShardRoutingEntry {
                primary: "node-2".to_string(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        metadata.shard_routing.insert(
            3,
            ShardRoutingEntry {
                primary: "node-3".to_string(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );

        let local_shard_ids = HashSet::from([0u32]);
        let targets = sorted_remote_targets(&cluster_state, &metadata, &local_shard_ids);

        assert_eq!(
            targets,
            vec![
                ("node-2".to_string(), vec![1, 2]),
                ("node-3".to_string(), vec![3]),
            ]
        );
    }
}
