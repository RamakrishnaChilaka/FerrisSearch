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

mod canonicalization;

use canonicalization::*;

/// GET/POST /{index}/_count — Count documents matching a query.
/// GET returns total doc count (match_all). POST accepts an optional query body.
///
/// Fast path: when the query is match_all, sums `doc_count()` from local shards
/// and `GetShardStats` from remote nodes — no search execution at all.
pub async fn count_documents(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

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
    Path(index_name): Path<crate::common::IndexName>,
    Query(params): Query<SearchParams>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::SEARCH_LATENCY_SECONDS.start_timer();

    // IndexName is validated at extraction time

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
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
            let index = index_name.to_string();
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
    Path(index_name): Path<crate::common::IndexName>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let cluster_state = state.cluster_manager.get_state();
    if !cluster_state.indices.contains_key(index_name.as_str()) {
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
        let mappings = &cluster_state
            .indices
            .get(index_name.as_str())
            .unwrap()
            .mappings;
        let plan = match canonicalize_sql_plan_fields(plan, mappings) {
            Ok(plan) => plan,
            Err(err) => return err,
        };
        let approximate_top_k = search_request_uses_approximate_top_k(
            &plan.to_search_request(state.sql_approximate_top_k),
        );
        let mut explain = plan.to_explain_json();
        explain["approximate_top_k"] = serde_json::json!(approximate_top_k);
        return (StatusCode::OK, Json(explain));
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
            explain["approximate_top_k"] = serde_json::json!(result.approximate_top_k);
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
    approximate_top_k: bool,
    streaming_used: bool,
    truncated: bool,
    semijoin_key_count: Option<usize>,
    timings: crate::hybrid::SqlTimings,
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
    approximate_top_k: bool,
    streaming_used: bool,
    truncated: bool,
    matched_hits: usize,
    successful_shards: u32,
    failed_shards: u32,
    columns: Vec<String>,
    timings: Option<crate::hybrid::SqlTimings>,
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

fn search_request_uses_approximate_top_k(search_req: &crate::search::SearchRequest) -> bool {
    search_req.aggs.values().any(|agg| {
        matches!(
            agg,
            crate::search::AggregationRequest::GroupedMetrics(params)
                if params.shard_top_k.is_some()
        )
    })
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

    // Only fallback aggregate queries are allowed to scan beyond the default
    // TopDocs ceiling, so keep lazy shard streaming reserved for those paths.
    // Ordinary tantivy_fast_fields queries still use sql_record_batch() here,
    // but the coordinator now feeds the per-shard results through partition
    // streams instead of staging every batch into one MemTable first.
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
    let approximate_top_k = search_request_uses_approximate_top_k(&search_req);

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
            approximate_top_k,
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
                grouped_merge: None,
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
        let grouped_sql_result = match crate::hybrid::execute_grouped_partial_sql_with_timings(
            &plan,
            &distributed.partial_aggs,
            &metadata.mappings,
        ) {
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
            sql_result: grouped_sql_result.sql_result,
            matched_hits: distributed.total_hits,
            successful_shards: distributed.successful_shards,
            failed_shards: distributed.failed_shards,
            execution_mode: "tantivy_grouped_partials",
            approximate_top_k,
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
                grouped_merge: Some(grouped_sql_result.timings),
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

    // Fast-field path: collect direct SQL partitions from local + remote shards
    let search_start = Instant::now();
    let direct_sql = if !plan.selects_all_columns {
        match collect_direct_sql_partitions(
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
        let sql_result = match crate::hybrid::execute_planned_sql_partitions(
            &plan,
            direct_sql.schema,
            direct_sql.partitions,
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
        approximate_top_k,
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
            grouped_merge: None,
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
        "approximate_top_k": result.approximate_top_k,
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

fn sql_stream_response_body(result: &SqlExecutionResult) -> Value {
    let mut body = sql_stream_meta_json(
        &result.plan,
        SqlStreamMeta {
            execution_mode: result.execution_mode,
            approximate_top_k: result.approximate_top_k,
            streaming_used: result.streaming_used,
            truncated: result.truncated,
            matched_hits: result.matched_hits,
            successful_shards: result.successful_shards,
            failed_shards: result.failed_shards,
            columns: result.sql_result.columns.clone(),
            timings: Some(result.timings.clone()),
        },
    );

    if let Some(object) = body.as_object_mut() {
        object.insert(
            "rows".to_string(),
            Value::Array(result.sql_result.rows.clone()),
        );
    }

    body
}

fn sql_stream_meta_json(plan: &crate::hybrid::QueryPlan, meta: SqlStreamMeta) -> Value {
    let mut body = serde_json::json!({
        "execution_mode": meta.execution_mode,
        "approximate_top_k": meta.approximate_top_k,
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
    });

    if let Some(timings) = meta.timings {
        match serde_json::to_value(timings) {
            Ok(value) => {
                if let Some(object) = body.as_object_mut() {
                    object.insert("timings".to_string(), value);
                }
            }
            Err(error) => {
                tracing::error!("Failed to serialize streamed SQL timings: {}", error);
            }
        }
    }

    body
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
        return Ok(stream_json_response(sql_stream_response_body(&result)));
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
    let approximate_top_k = search_request_uses_approximate_top_k(&search_req);

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
            return Ok(stream_json_response(sql_stream_response_body(&result)));
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
                approximate_top_k,
                streaming_used: direct_sql.streaming_used,
                truncated,
                matched_hits: direct_sql.total_hits,
                successful_shards: direct_sql.successful_shards,
                failed_shards: direct_sql.failed_shards,
                columns,
                timings: None,
            },
        ),
        batch_stream,
    ))
}

/// POST /{index}/_sql/stream — NDJSON stream of SQL results.
pub async fn search_sql_stream(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(req): Json<SqlQueryRequest>,
) -> Response {
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

    let index_name = match crate::common::IndexName::new(&index_name) {
        Ok(name) => name,
        Err(msg) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "invalid_index_name_exception",
                msg,
            )
            .into_response();
        }
    };

    match execute_sql_stream_query(&state, &index_name, &req.query).await {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

/// POST /{index}/_sql — SQL projection/aggregation over distributed search hits.
pub async fn search_sql(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(req): Json<SqlQueryRequest>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

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

    let index_name = match crate::common::IndexName::new(&index_name) {
        Ok(name) => name,
        Err(msg) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "invalid_index_name_exception",
                msg,
            );
        }
    };

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
mod tests;
