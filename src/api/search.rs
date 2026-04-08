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
use futures::{FutureExt, StreamExt, channel::mpsc, stream};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    OrderByKind as SqlOrderByKind, Query as SqlQuery, Select as SqlSelect,
    SelectItem as SqlSelectItem, SetExpr as SqlSetExpr, Statement as SqlStatement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
    )
    .await;
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
    )
    .await;
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
        // Plan-only mode: parse, canonicalize field names, and return the plan
        let plan = match crate::hybrid::planner::plan_sql(&index_name, &req.query) {
            Ok(plan) => plan,
            Err(e) => {
                return crate::api::error_response(StatusCode::BAD_REQUEST, "parsing_exception", e);
            }
        };
        let mappings = &cluster_state.indices.get(&index_name).unwrap().mappings;
        let plan = match canonicalize_sql_plan_fields(plan, mappings) {
            Ok(plan) => plan,
            Err(err) => return err,
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

enum RemoteSqlPartitionResult {
    Stream(Box<crate::transport::client::SqlBatchStream>),
    Batch {
        batch: RecordBatch,
        total_hits: usize,
    },
}

enum LocalSqlPartitionResult {
    Stream(crate::engine::SqlStreamingBatchHandle),
    Batch {
        batch: RecordBatch,
        total_hits: usize,
    },
}

fn direct_sql_partition_from_stream<S>(
    target_schema: Arc<datafusion::arrow::datatypes::Schema>,
    plan: Arc<crate::hybrid::QueryPlan>,
    batch_stream: S,
) -> Arc<dyn datafusion::physical_plan::streaming::PartitionStream>
where
    S: futures::Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    let partition_schema = target_schema.clone();
    let normalize_schema = target_schema.clone();
    let normalize_plan = plan.clone();
    let stream = batch_stream.map(move |batch_result| match batch_result {
        Ok(batch) => crate::hybrid::normalize_sql_batch_for_schema(
            batch,
            normalize_plan.as_ref(),
            &normalize_schema,
        )
        .map_err(|error| DataFusionError::Execution(error.to_string())),
        Err(error) => Err(DataFusionError::Execution(error.to_string())),
    });
    let stream = RecordBatchStreamAdapter::new(partition_schema.clone(), stream);
    crate::hybrid::one_shot_partition_stream(partition_schema, Box::pin(stream))
}

fn direct_sql_partition_from_batches(
    target_schema: Arc<datafusion::arrow::datatypes::Schema>,
    plan: Arc<crate::hybrid::QueryPlan>,
    batches: Vec<RecordBatch>,
) -> Arc<dyn datafusion::physical_plan::streaming::PartitionStream> {
    direct_sql_partition_from_stream(
        target_schema,
        plan,
        stream::iter(batches.into_iter().map(Ok::<RecordBatch, anyhow::Error>)),
    )
}

fn direct_sql_partition_from_local_handle(
    target_schema: Arc<datafusion::arrow::datatypes::Schema>,
    plan: Arc<crate::hybrid::QueryPlan>,
    worker_pools: crate::worker::WorkerPools,
    mut handle: crate::engine::SqlStreamingBatchHandle,
) -> Arc<dyn datafusion::physical_plan::streaming::PartitionStream> {
    let (tx, rx) = mpsc::unbounded::<Result<RecordBatch, anyhow::Error>>();

    tokio::spawn(async move {
        let batch_tx = tx.clone();
        let producer = worker_pools
            .spawn_search(move || -> anyhow::Result<()> {
                while let Some(batch) = handle.next_batch()? {
                    if batch_tx.unbounded_send(Ok(batch)).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })
            .await;

        match producer {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = tx.unbounded_send(Err(error));
            }
            Err(error) => {
                let _ = tx.unbounded_send(Err(error));
            }
        }
    });

    direct_sql_partition_from_stream(target_schema, plan, rx)
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
    let use_streaming = plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback;
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
        // This direct fast-field path is all-or-nothing today: any local or remote
        // shard error aborts the query before we return a partial result.
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
    let use_streaming = plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback;

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
                            match engine.sql_streaming_batch_handle(
                                &req,
                                &columns,
                                needs_id,
                                needs_score,
                                0,
                            )? {
                                Some(result) => Ok(Some(LocalSqlPartitionResult::Stream(result))),
                                None => engine
                                    .sql_record_batch(&req, &columns, needs_id, needs_score)
                                    .map(|opt| {
                                        opt.map(|r| LocalSqlPartitionResult::Batch {
                                            batch: r.batch,
                                            total_hits: r.total_hits,
                                        })
                                    }),
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
                                .map(|opt| {
                                    opt.map(|r| LocalSqlPartitionResult::Batch {
                                        batch: r.batch,
                                        total_hits: r.total_hits,
                                    })
                                })
                        })
                        .await
                }
                .boxed()
            }
        })
        .collect();

    for batch_result in join_all(local_futures).await {
        match batch_result {
            Ok(Ok(Some(LocalSqlPartitionResult::Stream(handle)))) => {
                successful_shards += 1;
                total_hits += handle.total_hits;
                collected_hits += handle.collected_rows;
                used_streaming = true;
                partitions.push(direct_sql_partition_from_local_handle(
                    target_schema.clone(),
                    plan.clone(),
                    state.worker_pools.clone(),
                    handle,
                ));
            }
            Ok(Ok(Some(LocalSqlPartitionResult::Batch {
                batch,
                total_hits: hits,
            }))) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += batch.num_rows();
                partitions.push(direct_sql_partition_from_batches(
                    target_schema.clone(),
                    plan.clone(),
                    vec![batch],
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
                        .open_sql_batch_stream_to_shard(
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
                        .map(|stream| RemoteSqlPartitionResult::Stream(Box::new(stream)))
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
                        .map(|(batch, hits)| RemoteSqlPartitionResult::Batch {
                            batch,
                            total_hits: hits,
                        })
                }
            }));
        }
    }

    for result in join_all(remote_futures).await {
        match result {
            Ok(Ok(RemoteSqlPartitionResult::Stream(shard_stream))) => {
                successful_shards += 1;
                total_hits += shard_stream.total_hits();
                collected_hits += shard_stream.collected_rows();
                used_streaming |= shard_stream.streaming_used();
                partitions.push(direct_sql_partition_from_stream(
                    target_schema.clone(),
                    plan.clone(),
                    shard_stream.into_stream(),
                ));
            }
            Ok(Ok(RemoteSqlPartitionResult::Batch {
                batch,
                total_hits: hits,
            })) => {
                successful_shards += 1;
                total_hits += hits;
                collected_hits += batch.num_rows();
                partitions.push(direct_sql_partition_from_batches(
                    target_schema.clone(),
                    plan.clone(),
                    vec![batch],
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
        // This direct fast-field path is all-or-nothing today: any local or remote
        // shard error aborts the query before we return a partial result.
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

fn resolve_case_insensitive_mapping_name(
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

fn canonicalize_sql_source_field_name(
    field_name: &str,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<String, (StatusCode, Json<Value>)> {
    Ok(resolve_case_insensitive_mapping_name(field_name, mappings)?
        .unwrap_or_else(|| field_name.to_string()))
}

fn canonicalize_query_field_map<T>(
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

fn canonicalize_query_clause_fields(
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

fn canonicalize_sql_field_list(
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

fn canonicalize_group_key_name(
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

fn grouped_column_mapping_name(source_name: &str) -> String {
    crate::search::decode_derived_group_key(source_name)
        .map(|derived| derived.source_field)
        .unwrap_or_else(|| source_name.to_string())
}

fn derived_group_key_requires_group_by_fallback(
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

fn canonicalize_sql_plan_fields(
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
struct SqlAliasContext {
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
enum SqlExprRewriteMode {
    SourceOnly,
    OutputAware,
}

fn canonicalize_rewritten_sql_source_fields(
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

fn canonicalize_query_source_fields(
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

fn canonicalize_select_source_fields(
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

fn collect_sql_alias_context(select: &SqlSelect) -> SqlAliasContext {
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

fn sql_expr_source_name(expr: &SqlExpr) -> Option<&str> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.as_str()),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
}

fn canonicalize_sql_expr_source_fields(
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

fn canonicalize_sql_ident(
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

fn sql_function_is_aggregate(name: &str) -> bool {
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
    let plan = canonicalize_sql_plan_fields(plan, &metadata.mappings)?;

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

    if plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback {
        search_req.size = if state.sql_group_by_scan_limit == 0 {
            usize::MAX
        } else {
            state.sql_group_by_scan_limit
        };
    }

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
                let mapping_name = grouped_column_mapping_name(&col.source_name);
                if let Some(mapping) = metadata.mappings.get(&mapping_name)
                    && matches!(mapping.field_type, crate::cluster::state::FieldType::Text)
                {
                    return Err(crate::api::error_response(
                        StatusCode::BAD_REQUEST,
                        "group_by_text_field_exception",
                        format!(
                            "Cannot GROUP BY field '{}': text fields are tokenized and don't support grouping. Use a 'keyword' type field instead.",
                            mapping_name
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
    )
    .await;
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
    if truncated && (plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback) {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "group_by_scan_limit_exceeded",
            format!(
                "Aggregate query matched {} docs but only {} were collected. \
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
    let plan = canonicalize_sql_plan_fields(plan, &metadata.mappings)?;

    let mut search_req = plan.to_search_request(state.sql_approximate_top_k);
    if plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback {
        search_req.size = if state.sql_group_by_scan_limit == 0 {
            usize::MAX
        } else {
            state.sql_group_by_scan_limit
        };
    }

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        &metadata,
        "SQL search",
    )
    .await;
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

    if truncated && (plan.has_group_by_fallback || plan.has_ungrouped_aggregate_fallback) {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "group_by_scan_limit_exceeded",
            format!(
                "Aggregate query matched {} docs but only {} were collected. \
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
    crate::common::sql_parse::unquote_identifier(s)
}

fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> Option<String> {
    crate::common::sql_parse::object_name_to_string(name)
}

fn extract_index_from_sql_fallback(sql: &str) -> Option<String> {
    crate::common::sql_parse::extract_index_from_sql_fallback(sql)
}

/// Extract the index/table name from a SELECT ... FROM "index" ... query.
fn extract_index_from_sql(sql: &str) -> Option<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements,
        Err(_) => return extract_index_from_sql_fallback(sql),
    };
    if statements.len() != 1 {
        return None;
    }
    match &statements[0] {
        sqlparser::ast::Statement::Query(query) => {
            if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref()
                && select.from.len() == 1
                && select.from[0].joins.is_empty()
                && let sqlparser::ast::TableFactor::Table { name, .. } = &select.from[0].relation
            {
                return object_name_to_string(name)
                    .or_else(|| extract_index_from_sql_fallback(sql));
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
                crate::cluster::state::FieldType::Date => "date",
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
            crate::cluster::state::FieldType::Date => "date",
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
    use datafusion::arrow::array::{Float32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct DropSignal(Option<std::sync::mpsc::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

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
            uuid: crate::cluster::state::IndexUuid::new(format!("{}-uuid", index)),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        }
    }

    async fn make_test_app_state(index_name: &str) -> (tempfile::TempDir, AppState) {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));
        cluster_state.add_index(make_sql_metadata(index_name));

        let temp_dir = tempfile::tempdir().unwrap();
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
                .await
                .unwrap();
        crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
            .await
            .unwrap();
        for _ in 0..50 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state.clone());
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft,
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
        };

        let metadata = cluster_state.indices.get(index_name).unwrap().clone();
        assert!(
            state
                .shard_manager
                .open_shard_with_settings(
                    index_name,
                    0,
                    &metadata.mappings,
                    &metadata.settings,
                    &metadata.uuid,
                )
                .is_ok()
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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
        assert_eq!(
            super::extract_index_from_sql(r#"SELECT count(*) FROM "benchmark-1gb""#),
            Some("benchmark-1gb".to_string())
        );
        assert_eq!(
            super::extract_index_from_sql(r#"SELECT count(*) from "benchmark-1gb""#),
            Some("benchmark-1gb".to_string())
        );
        assert_eq!(super::extract_index_from_sql("SHOW TABLES"), None);
        assert_eq!(super::extract_index_from_sql("not valid sql at all"), None);
    }

    #[tokio::test]
    async fn canonicalize_sql_plan_fields_rewrites_case_insensitive_columns() {
        let (_tmp, state) = make_test_app_state("products").await;
        let cluster_state = state.cluster_manager.get_state();
        let mappings = &cluster_state.indices.get("products").unwrap().mappings;

        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT BRAND AS brand, AVG(PRICE) AS avg_price FROM products WHERE text_match(DESCRIPTION, 'iphone') AND PRICE > 800 GROUP BY BRAND ORDER BY avg_price DESC",
        )
        .unwrap();

        let plan = super::canonicalize_sql_plan_fields(plan, mappings).unwrap();

        assert_eq!(plan.text_matches[0].field, "description");
        assert_eq!(plan.required_columns, vec!["brand", "price"]);
        assert_eq!(plan.group_by_columns, vec!["brand"]);

        match &plan.pushed_filters[0] {
            crate::search::QueryClause::Range(fields) => {
                assert!(fields.contains_key("price"));
            }
            other => panic!("expected range filter, got {other:?}"),
        }

        let grouped_sql = plan.grouped_sql.as_ref().unwrap();
        assert_eq!(grouped_sql.group_columns[0].source_name, "brand");
        assert_eq!(grouped_sql.group_columns[0].output_name, "brand");
        assert_eq!(grouped_sql.metrics[0].field.as_deref(), Some("price"));
        assert!(plan.rewritten_sql.contains("AVG(\"price\")"));
        assert!(plan.rewritten_sql.contains("GROUP BY \"brand\""));
        assert!(plan.rewritten_sql.contains("ORDER BY avg_price DESC"));
    }

    #[tokio::test]
    async fn canonicalize_sql_plan_fields_quotes_mixed_case_source_identifiers_for_datafusion() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mappings = HashMap::from([
            (
                "PULocationID".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "DOLocationID".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
        ]);

        let plan = crate::hybrid::planner::plan_sql(
            "rides",
            "SELECT CAST(PULocationID / 100 AS INT) AS bucket, COUNT(*) AS rides FROM rides GROUP BY CAST(PULocationID / 100 AS INT) HAVING COUNT(*) > 0 ORDER BY bucket",
        )
        .unwrap();

        let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();

        assert!(
            plan.rewritten_sql
                .contains("CAST(\"PULocationID\" / 100 AS INT)")
        );
        assert!(plan.rewritten_sql.contains("HAVING COUNT(*) > 0"));
        assert!(plan.rewritten_sql.contains("ORDER BY bucket"));
    }

    #[tokio::test]
    async fn canonicalize_sql_plan_fields_preserves_quoted_mixed_case_identifiers() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mappings = HashMap::from([(
            "PULocationID".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        )]);

        let plan = crate::hybrid::planner::plan_sql(
            "rides",
            "SELECT \"PULocationID\" FROM rides ORDER BY \"PULocationID\"",
        )
        .unwrap();

        let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();

        assert!(plan.rewritten_sql.contains("\"PULocationID\""));
        assert!(!plan.rewritten_sql.contains("\"pulocationid\""));
    }

    #[tokio::test]
    async fn direct_sql_partition_from_local_handle_stops_after_consumer_drop() {
        let plan =
            Arc::new(crate::hybrid::planner::plan_sql("test", "SELECT brand FROM test").unwrap());
        let target_schema = Arc::new(Schema::new(vec![Field::new("brand", DataType::Utf8, true)]));
        let worker_pools = crate::worker::WorkerPools::new(1, 1);
        let batch_calls = Arc::new(AtomicUsize::new(0));
        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let handle = crate::engine::SqlStreamingBatchHandle::new(100, 100, {
            let batch_calls = batch_calls.clone();
            let drop_signal = DropSignal(Some(done_tx));
            move || {
                let _keep_drop_signal_alive = &drop_signal;
                let batch_no = batch_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if batch_no > 100 {
                    return Ok(None);
                }

                if batch_no > 1 {
                    std::thread::sleep(Duration::from_millis(50));
                }

                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("_id", DataType::Utf8, false),
                        Field::new("_score", DataType::Float32, false),
                        Field::new("brand", DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec![format!("id-{batch_no}")])),
                        Arc::new(Float32Array::from(vec![0.0f32])),
                        Arc::new(StringArray::from(vec![format!("brand-{batch_no}")])),
                    ],
                )?;
                Ok(Some(batch))
            }
        });

        let partition = super::direct_sql_partition_from_local_handle(
            target_schema,
            plan,
            worker_pools,
            handle,
        );

        let mut stream = partition.execute(SessionContext::new().task_ctx());
        let first_batch = stream
            .next()
            .await
            .expect("stream should yield first batch")
            .expect("first batch should be valid");
        assert_eq!(first_batch.num_rows(), 1);
        drop(stream);

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer should stop after consumer drop");
        assert!(
            batch_calls.load(Ordering::SeqCst) <= 2,
            "producer should stop shortly after the receiver drops"
        );
    }

    #[tokio::test]
    async fn canonicalize_sql_plan_fields_rewrites_semijoin_keys_case_insensitively() {
        let (_tmp, state) = make_test_app_state("products").await;
        let cluster_state = state.cluster_manager.get_state();
        let mappings = &cluster_state.indices.get("products").unwrap().mappings;

        let plan = crate::hybrid::planner::plan_sql(
            "products",
            "SELECT TITLE AS title FROM products WHERE BRAND IN (SELECT BRAND FROM products WHERE PRICE > 800)",
        )
        .unwrap();

        let plan = super::canonicalize_sql_plan_fields(plan, mappings).unwrap();
        let semijoin = plan.semijoin.as_ref().unwrap();

        assert_eq!(semijoin.outer_key, "brand");
        assert_eq!(semijoin.inner_key, "brand");
        assert_eq!(semijoin.inner_plan.required_columns, vec!["brand"]);

        match &semijoin.inner_plan.pushed_filters[0] {
            crate::search::QueryClause::Range(fields) => {
                assert!(fields.contains_key("price"));
            }
            other => panic!("expected range filter, got {other:?}"),
        }
    }

    #[test]
    fn canonicalize_sql_plan_fields_disables_derived_grouped_partials_for_numeric_source() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mappings = HashMap::from([(
            "PULocationID".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        )]);

        let sql = "SELECT CASE
                WHEN PULocationID >= '100' AND PULocationID < '200' THEN '100s'
                WHEN PULocationID >= '200' AND PULocationID < '300' THEN '200s'
            END AS bucket,
            count(*) AS rides
        FROM rides
        GROUP BY CASE
                WHEN PULocationID >= '100' AND PULocationID < '200' THEN '100s'
                WHEN PULocationID >= '200' AND PULocationID < '300' THEN '200s'
            END
        ORDER BY bucket";

        let plan = crate::hybrid::planner::plan_sql("rides", sql).unwrap();
        assert!(plan.uses_grouped_partials());

        let plan = super::canonicalize_sql_plan_fields(plan, &mappings).unwrap();
        assert!(!plan.uses_grouped_partials());
        assert!(plan.has_group_by_fallback);
    }

    #[test]
    fn canonicalize_rejects_ambiguous_case_only_mappings() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mut mappings = HashMap::new();
        mappings.insert(
            "Price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );
        mappings.insert(
            "PRICE".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let result = super::resolve_case_insensitive_mapping_name("price", &mappings);
        let err = result.unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        let body = err.1.0;
        assert!(
            body["error"]["reason"]
                .as_str()
                .unwrap()
                .contains("ambiguous"),
            "Expected ambiguous error, got: {body}"
        );
    }

    #[tokio::test]
    async fn global_sql_show_tables_returns_index_list() {
        let (_tmp, state) = make_test_app_state("products").await;
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
        let (_tmp, state) = make_test_app_state("products").await;
        let req = SqlQueryRequest {
            query: "show tables;".to_string(),
            analyze: false,
        };
        let (status, _) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn global_sql_describe_returns_field_mappings() {
        let (_tmp, state) = make_test_app_state("products").await;
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
        let (_tmp, state) = make_test_app_state("products").await;
        let req = SqlQueryRequest {
            query: "DESCRIBE nonexistent".to_string(),
            analyze: false,
        };
        let (status, _) = super::global_sql(State(state), Json(req)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn global_sql_show_create_table_returns_index_json() {
        let (_tmp, state) = make_test_app_state("products").await;
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
        let (_tmp, state) = make_test_app_state("products").await;
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
        let (_tmp, state) = make_test_app_state("products").await;
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
            uuid: crate::cluster::state::IndexUuid::new("texttest-uuid"),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings,
            settings: IndexSettings::default(),
        });

        let temp_dir = tempfile::tempdir().unwrap();
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
                .await
                .unwrap();
        crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
            .await
            .unwrap();
        for _ in 0..50 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state.clone());
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft,
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
        let (_tmp, mut state) = make_test_app_state("products").await;
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
        let (_tmp, mut state) = make_test_app_state("products").await;
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
        let (_tmp, state) = make_test_app_state("products").await;

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
        let (_tmp, state) = make_test_app_state("products").await;

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
