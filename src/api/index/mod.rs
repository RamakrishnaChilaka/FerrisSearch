use crate::api::AppState;
use crate::cluster::state::IndexMetadata;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use futures::future::join_all;
use futures::stream::{self, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::api::{raft_write, resolve_leader_or_master};

mod bulk;
mod maintenance;

pub use bulk::{bulk_index, bulk_index_global};
pub use maintenance::{flush_index, force_merge_index, refresh_index};

#[cfg(test)]
use crate::transport::server::MaintenanceDispatchOp;
#[cfg(test)]
use bulk::{RoutedBulkDoc, finalize_bulk_items, parse_bulk_ndjson, route_bulk_doc};
#[cfg(test)]
use maintenance::{
    enqueue_force_merge_tasks, fan_out_maintenance, maintenance_fanout_concurrency,
    spawn_maintenance_job,
};

pub(crate) struct DistributedDslSearchResult {
    pub all_hits: Vec<Value>,
    pub total_hits: usize,
    pub successful_shards: u32,
    pub failed_shards: u32,
    pub aggregations: HashMap<String, Value>,
    pub partial_aggs: Vec<HashMap<String, crate::search::PartialAggResult>>,
}

pub(crate) async fn ensure_local_index_shards_open(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    context: &str,
) -> Vec<(u32, Arc<dyn crate::engine::SearchEngine>)> {
    for (shard_id, routing) in &metadata.shard_routing {
        let assigned_here = routing.primary == state.local_node_id
            || routing
                .replicas
                .iter()
                .any(|node_id| node_id == &state.local_node_id);
        if !assigned_here
            || state
                .shard_manager
                .get_shard(index_name, *shard_id)
                .is_some()
        {
            continue;
        }

        let expected_dir = state
            .shard_manager
            .data_dir()
            .join(&metadata.uuid)
            .join(format!("shard_{}", shard_id));
        if !expected_dir.exists() {
            tracing::error!(
                "{}: refusing to create fresh shard data for {}/{} on a read/maintenance path because {:?} is missing",
                context,
                index_name,
                shard_id,
                expected_dir
            );
            continue;
        }

        if let Err(e) = state
            .shard_manager
            .open_shard_with_settings_blocking(
                index_name.to_string(),
                *shard_id,
                metadata.mappings.clone(),
                metadata.settings.clone(),
                metadata.uuid.clone(),
            )
            .await
        {
            tracing::error!(
                "{}: failed to open shard {}/{}: {}",
                context,
                index_name,
                shard_id,
                e
            );
        }
    }

    state.shard_manager.get_index_shards(index_name)
}

async fn wait_for_index_metadata(state: &AppState, index_name: &str) -> Option<IndexMetadata> {
    const MAX_ATTEMPTS: usize = 20;
    const RETRY_DELAY_MS: u64 = 50;

    for attempt in 0..MAX_ATTEMPTS {
        let cluster_state = state.cluster_manager.get_state();
        if let Some(metadata) = cluster_state.indices.get(index_name) {
            return Some(metadata.clone());
        }

        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
        }
    }

    None
}

/// Auto-create an index with 1 shard, respecting the coordinator pattern.
/// If this node is NOT the Raft leader, forwards to the master.
async fn auto_create_index(
    state: &AppState,
    index_name: &str,
    _cluster_state: &crate::cluster::state::ClusterState,
) -> Result<IndexMetadata, (StatusCode, Json<Value>)> {
    tracing::warn!(
        "Index '{}' not found, auto-creating with 1 shard",
        index_name
    );
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0u32,
        crate::cluster::state::ShardRoutingEntry {
            primary: state.local_node_id.clone(),
            replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    let m = IndexMetadata {
        name: index_name.to_string(),
        uuid: crate::cluster::state::IndexUuid::new_random(),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    let created_metadata = if let Some(master) =
        resolve_leader_or_master(state, "auto-create index")?
    {
        // Forward auto-create to the leader via gRPC
        let body = serde_json::json!({
            "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
        });
        let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
        match state
            .transport_client
            .forward_create_index(&master, index_name, &body_bytes)
            .await
        {
            Ok(_) => match wait_for_index_metadata(state, index_name).await {
                Some(metadata) => metadata,
                None => {
                    return Err(crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        format!(
                            "Index [{}] was created by the leader but the local cluster state has not caught up yet",
                            index_name
                        ),
                    ));
                }
            },
            Err(e) => {
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Auto-create index forward to master failed: {}", e),
                ));
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::CreateIndex {
            metadata: m.clone(),
        };
        raft_write(state, cmd).await?;
        wait_for_index_metadata(state, index_name)
            .await
            .unwrap_or_else(|| m.clone())
    };

    if let Some(routing) = created_metadata.shard_routing.get(&0)
        && (routing.primary == state.local_node_id
            || routing
                .replicas
                .iter()
                .any(|node_id| node_id == &state.local_node_id))
        && let Err(e) = state
            .shard_manager
            .open_shard_with_settings_blocking(
                created_metadata.name.clone(),
                0,
                created_metadata.mappings.clone(),
                created_metadata.settings.clone(),
                created_metadata.uuid.clone(),
            )
            .await
    {
        tracing::error!(
            "Failed to open auto-created shard {}/0 with authoritative UUID {}: {}",
            created_metadata.name,
            created_metadata.uuid,
            e
        );
    }

    Ok(created_metadata)
}

/// Query parameter for `?refresh=true|false|wait_for`.
/// Matches the OpenSearch refresh parameter on indexing endpoints.
#[derive(serde::Deserialize, Default)]
pub struct RefreshParam {
    pub refresh: Option<String>,
}

impl RefreshParam {
    /// Returns true when the caller explicitly requested an immediate refresh.
    /// OpenSearch treats `?refresh`, `?refresh=true`, and `?refresh=""` as "refresh now".
    fn should_refresh(&self) -> bool {
        matches!(self.as_deref(), Some("true") | Some(""))
    }

    fn as_deref(&self) -> Option<&str> {
        self.refresh.as_deref()
    }
}

/// HEAD /{index} — Check if an index exists.
pub async fn index_exists(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> StatusCode {
    let cluster_state = state.cluster_manager.get_state();
    if cluster_state.indices.contains_key(index_name.as_str()) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// PUT /{index} — Create an index with shard settings.
/// Body: `{ "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }`
pub async fn create_index(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let settings: Value = serde_json::from_slice(&body).unwrap_or(serde_json::json!({}));
    let num_shards = settings
        .pointer("/settings/number_of_shards")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let num_replicas = settings
        .pointer("/settings/number_of_replicas")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let refresh_interval_ms = settings
        .pointer("/settings/refresh_interval_ms")
        .and_then(|v| v.as_u64());
    let flush_threshold_bytes = settings
        .pointer("/settings/flush_threshold_bytes")
        .and_then(|v| v.as_u64());

    let cluster_state = state.cluster_manager.get_state();

    if cluster_state.indices.contains_key(index_name.as_str()) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "resource_already_exists_exception",
            format!("index [{}] already exists", index_name),
        );
    }

    // Build shard assignment: distribute shards round-robin across Data nodes
    let data_nodes: Vec<String> = cluster_state
        .nodes
        .values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| n.id.clone())
        .collect();

    if data_nodes.is_empty() {
        return crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "no_data_nodes_exception",
            "No data nodes available to assign shards",
        );
    }

    let mut metadata =
        IndexMetadata::build_shard_routing(&index_name, num_shards, num_replicas, &data_nodes);

    // Apply per-index settings
    if let Some(ms) = refresh_interval_ms {
        metadata.settings.refresh_interval_ms = Some(ms);
    }
    if let Some(bytes) = flush_threshold_bytes {
        metadata.settings.flush_threshold_bytes = Some(bytes);
    }

    // Parse field mappings: { "mappings": { "properties": { "title": { "type": "text" }, ... } } }
    if let Some(properties) = settings
        .pointer("/mappings/properties")
        .and_then(|v| v.as_object())
    {
        for (field_name, field_def) in properties {
            if let Some(type_str) = field_def.get("type").and_then(|v| v.as_str()) {
                let field_mapping = match type_str {
                    "text" => Some(crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Text,
                        dimension: None,
                    }),
                    "keyword" => Some(crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Keyword,
                        dimension: None,
                    }),
                    "integer" | "long" => Some(crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Integer,
                        dimension: None,
                    }),
                    "float" | "double" => Some(crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Float,
                        dimension: None,
                    }),
                    "boolean" => Some(crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Boolean,
                        dimension: None,
                    }),
                    "date" | "datetime" | "timestamp" => {
                        Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Date,
                            dimension: None,
                        })
                    }
                    "knn_vector" => {
                        let dim = field_def
                            .get("dimension")
                            .and_then(|v| v.as_u64())
                            .map(|d| d as usize);
                        Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::KnnVector,
                            dimension: dim,
                        })
                    }
                    _ => {
                        tracing::warn!(
                            "Unknown field type '{}' for field '{}', skipping",
                            type_str,
                            field_name
                        );
                        None
                    }
                };
                if let Some(fm) = field_mapping {
                    metadata.mappings.insert(field_name.clone(), fm);
                }
            }
        }
    }

    let shard_assignment = metadata.shard_routing.clone();
    let index_mappings = metadata.mappings.clone();
    let index_settings = metadata.settings.clone();
    let index_uuid = metadata.uuid.clone();

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "index creation") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_create_index(&master, &index_name, &body)
            .await
        {
            Ok(resp) => return (StatusCode::OK, Json(resp)),
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward index creation to master: {}", e),
                );
            }
        }
    }
    let cmd = crate::consensus::types::ClusterCommand::CreateIndex { metadata };
    if let Err(e) = raft_write(&state, cmd).await {
        return e;
    }

    // Open local shard engines for shards assigned to this node (primary or replica)
    for (shard_id, routing) in &shard_assignment {
        if (routing.primary == state.local_node_id
            || routing.replicas.contains(&state.local_node_id))
            && let Err(e) = state
                .shard_manager
                .open_shard_with_settings_blocking(
                    index_name.to_string(),
                    *shard_id,
                    index_mappings.clone(),
                    index_settings.clone(),
                    index_uuid.clone(),
                )
                .await
        {
            tracing::error!(
                "Failed to open shard {} for {}: {}",
                shard_id,
                index_name,
                e
            );
        }
    }

    tracing::info!(
        "Created index '{}' with {} shards, {} replicas",
        index_name,
        num_shards,
        num_replicas
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index_name
        })),
    )
}

/// POST /{index}/_doc — Index a single document with shard routing.
pub async fn index_document(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Query(refresh_param): Query<RefreshParam>,
    Json(mut payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();

    // IndexName is validated at extraction time

    // Extract or generate _id, then strip it from the document body
    let doc_id = if let Some(id) = payload.get("_id").and_then(|v| v.as_str()) {
        id.to_string()
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    // Remove _id from the stored payload — it's metadata, not part of the document source
    if let Some(obj) = payload.as_object_mut() {
        obj.remove("_id");
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
        m.clone()
    } else {
        match auto_create_index(&state, &index_name, &cluster_state).await {
            Ok(m) => m,
            Err(err_resp) => return err_resp,
        }
    };

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_index_to_shard(&target_node, &index_name, shard_id, &doc_id, &payload)
        .await
    {
        Ok(res) => {
            if refresh_param.should_refresh()
                && let Some(engine) = state.shard_manager.get_shard(&index_name, shard_id)
            {
                let _ = state
                    .worker_pools
                    .spawn_write(move || engine.refresh())
                    .await;
            }
            (StatusCode::CREATED, Json(res))
        }
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forward_exception",
            format!("Forward failed: {}", e),
        ),
    }
}

/// PUT /{index}/_doc/{id} — Index a single document with an explicit ID.
pub async fn index_document_with_id(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Query(refresh_param): Query<RefreshParam>,
    Json(mut payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();

    // IndexName is validated at extraction time

    // Remove _id from stored payload if present — it's metadata, not document source
    if let Some(obj) = payload.as_object_mut() {
        obj.remove("_id");
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
        m.clone()
    } else {
        match auto_create_index(&state, &index_name, &cluster_state).await {
            Ok(m) => m,
            Err(err_resp) => return err_resp,
        }
    };

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_index_to_shard(&target_node, &index_name, shard_id, &doc_id, &payload)
        .await
    {
        Ok(res) => {
            if refresh_param.should_refresh()
                && let Some(engine) = state.shard_manager.get_shard(&index_name, shard_id)
            {
                let _ = state
                    .worker_pools
                    .spawn_write(move || engine.refresh())
                    .await;
            }
            (StatusCode::CREATED, Json(res))
        }
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forward_exception",
            format!("Forward failed: {}", e),
        ),
    }
}

pub(crate) async fn execute_distributed_dsl_search(
    state: &AppState,
    index_name: &str,
    search_req: &crate::search::SearchRequest,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    // IndexName is validated at extraction time

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

    let mut shard_hit_lists: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut knn_hits = Vec::new();
    let mut successful = 0u32;
    let mut failed = 0u32;
    let mut total_hits: usize = 0;
    let is_hybrid = search_req.knn.is_some();

    let mut all_partial_aggs: Vec<HashMap<String, crate::search::PartialAggResult>> = Vec::new();

    let local_shards =
        ensure_local_index_shards_open(state, index_name, &metadata, "DSL search").await;

    // Dispatch all local shard searches in parallel
    let search_futures: Vec<_> = local_shards
        .iter()
        .map(|(shard_id, engine)| {
            let engine = engine.clone();
            let search_req = search_req.clone();
            let shard_id = *shard_id;
            let pools = state.worker_pools.clone();
            async move {
                let result = pools
                    .spawn_search(move || engine.search_query(&search_req))
                    .await;
                (shard_id, result)
            }
        })
        .collect();
    let search_results = futures::future::join_all(search_futures).await;

    for (shard_id, search_result) in search_results {
        match search_result {
            Ok(Ok((hits, shard_total, partial_aggs))) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                let mut shard_list = Vec::with_capacity(hits.len());
                for hit in hits {
                    shard_list.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
                shard_hit_lists.push(shard_list);
            }
            Ok(Err(e)) | Err(e) => {
                tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e);
                failed += 1;
            }
        }
    }

    if let Some(ref knn) = search_req.knn
        && let Some((field_name, params)) = knn.fields.iter().next()
    {
        for (shard_id, engine) in &local_shards {
            let engine = engine.clone();
            let field_name = field_name.clone();
            let vector = params.vector.clone();
            let k = params.k;
            let filter = params.filter.clone();
            let shard_id = *shard_id;
            let knn_result = state
                .worker_pools
                .spawn_search(move || {
                    engine.search_knn_filtered(&field_name, &vector, k, filter.as_ref())
                })
                .await;
            match knn_result {
                Ok(Ok(hits)) => {
                    for hit in hits {
                        knn_hits.push(serde_json::json!({
                            "_index": index_name,
                            "_shard": shard_id,
                            "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                            "_score": hit.get("_score"),
                            "_source": hit.get("_source"),
                            "_knn_field": hit.get("_knn_field"),
                            "_knn_distance": hit.get("_knn_distance"),
                        }));
                    }
                }
                Ok(Err(e)) | Err(e) => {
                    tracing::error!(
                        "Vector search on {}/shard_{} failed: {}",
                        index_name,
                        shard_id,
                        e
                    );
                    failed += 1;
                }
            }
        }
    }

    let local_shard_ids: std::collections::HashSet<u32> =
        local_shards.iter().map(|(id, _)| *id).collect();

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
            let req_clone = search_req.clone();
            remote_futures.push(tokio::spawn(async move {
                (
                    sid,
                    client
                        .forward_search_dsl_to_shard(&node_info, &index, sid, &req_clone)
                        .await,
                )
            }));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for result in remote_results {
        match result {
            Ok((shard_id, Ok((hits, shard_total, partial_aggs)))) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                let mut shard_text = Vec::new();
                for hit in hits {
                    let enriched = serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit),
                        "_knn_field": hit.get("_knn_field"),
                        "_knn_distance": hit.get("_knn_distance"),
                    });
                    if hit.get("_knn_field").is_some() {
                        knn_hits.push(enriched);
                    } else {
                        shard_text.push(enriched);
                    }
                }
                if !shard_text.is_empty() {
                    shard_hit_lists.push(shard_text);
                }
            }
            Ok((shard_id, Err(e))) => {
                tracing::error!(
                    "Remote shard {}/{} search failed: {}",
                    index_name,
                    shard_id,
                    e
                );
                failed += 1;
            }
            Err(e) => {
                tracing::error!("Remote shard search task panicked: {}", e);
                failed += 1;
            }
        }
    }

    let mut all_hits = if is_hybrid {
        // Hybrid search: flatten shard lists, merge with kNN via RRF
        let text_hits: Vec<serde_json::Value> = shard_hit_lists.into_iter().flatten().collect();
        crate::search::merge_hybrid_hits(text_hits, knn_hits)
    } else {
        // K-way merge of pre-sorted shard hit lists (O(N log K) vs O(N log N))
        crate::search::merge_sorted_hit_lists(shard_hit_lists, &search_req.sort)
    };
    if is_hybrid {
        crate::search::sort_hits(&mut all_hits, &search_req.sort);
    }

    let merged_aggs = if !all_partial_aggs.is_empty() {
        crate::search::merge_aggregations(all_partial_aggs.clone(), &search_req.aggs)
    } else {
        HashMap::new()
    };

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits,
        successful_shards: successful,
        failed_shards: failed,
        aggregations: merged_aggs,
        partial_aggs: all_partial_aggs,
    })
}

pub async fn search_documents_dsl(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::SEARCH_LATENCY_SECONDS.start_timer();

    let search_req: crate::search::SearchRequest = match serde_json::from_value(req) {
        Ok(r) => r,
        Err(e) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parsing_exception",
                format!("Invalid query DSL: {}", e),
            );
        }
    };

    let result = match execute_distributed_dsl_search(&state, &index_name, &search_req).await {
        Ok(result) => result,
        Err(err) => return err,
    };

    crate::metrics::SEARCH_QUERIES_TOTAL.inc();

    let paginated: Vec<_> = result
        .all_hits
        .into_iter()
        .skip(search_req.from)
        .take(search_req.size)
        .collect();

    let mut response = serde_json::json!({
        "_shards": {
            "total": result.successful_shards + result.failed_shards,
            "successful": result.successful_shards,
            "failed": result.failed_shards
        },
        "hits": {
            "total": { "value": result.total_hits, "relation": "eq" },
            "hits": paginated
        }
    });
    if !result.aggregations.is_empty() {
        response["aggregations"] = serde_json::json!(result.aggregations);
    }

    (StatusCode::OK, Json(response))
}

/// GET /{index}/_doc/{id} — Retrieve a document by its ID.
pub async fn get_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
) -> (StatusCode, Json<Value>) {
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

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_get_to_shard(&target_node, &index_name, shard_id, &doc_id)
        .await
    {
        Ok(Some(source)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "_index": index_name, "_id": doc_id, "_shard": shard_id, "found": true, "_source": source
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "_index": index_name, "_id": doc_id, "found": false
            })),
        ),
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "search_exception",
            format!("{}", e),
        ),
    }
}

/// POST /{index}/_update/{id} — Partial update a document by merging fields.
/// Body: `{ "doc": { "field": "new_value" } }`
/// Fetches the existing document, merges the provided fields, and re-indexes.
pub async fn update_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let partial = match body.get("doc") {
        Some(d) if d.is_object() => d.clone(),
        _ => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "action_request_validation_exception",
                "update requires a 'doc' object",
            );
        }
    };

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

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    // 1. Fetch the existing document
    let existing = match state
        .transport_client
        .forward_get_to_shard(&target_node, &index_name, shard_id, &doc_id)
        .await
    {
        Ok(Some(source)) => source,
        Ok(None) => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "document_missing_exception",
                format!("[{}]: document missing", doc_id),
            );
        }
        Err(e) => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "get_exception",
                format!("{}", e),
            );
        }
    };

    // 2. Merge: overlay partial fields onto existing _source
    let merged = if let (Some(existing_obj), Some(partial_obj)) =
        (existing.as_object(), partial.as_object())
    {
        let mut merged_obj = existing_obj.clone();
        for (key, value) in partial_obj {
            merged_obj.insert(key.clone(), value.clone());
        }
        serde_json::Value::Object(merged_obj)
    } else {
        partial
    };

    // 3. Re-index the merged document
    match state
        .transport_client
        .forward_index_to_shard(&target_node, &index_name, shard_id, &doc_id, &merged)
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "_index": index_name, "_id": doc_id, "_shard": shard_id, "result": "updated"
            })),
        ),
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forward_exception",
            format!("Update failed: {}", e),
        ),
    }
}

/// DELETE /{index}/_doc/{id} — Delete a document by its ID.
pub async fn delete_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(crate::common::IndexName, String)>,
) -> (StatusCode, Json<Value>) {
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

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned node",
            );
        }
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "node_not_found_exception",
                "Target node not in cluster state",
            );
        }
    };

    match state
        .transport_client
        .forward_delete_to_shard(&target_node, &index_name, shard_id, &doc_id)
        .await
    {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(e) => crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "search_exception",
            format!("{}", e),
        ),
    }
}

/// DELETE /{index} — Delete an entire index (remove from cluster state, close shards, delete data).
/// GET /{index}/_settings — Get the current index settings.
pub async fn get_index_settings(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m,
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            index_name.to_string(): {
                "settings": {
                    "index": {
                        "number_of_shards": metadata.number_of_shards,
                        "number_of_replicas": metadata.number_of_replicas,
                        "refresh_interval_ms": metadata.settings.refresh_interval_ms,
                        "flush_threshold_bytes": metadata.settings.flush_threshold_bytes,
                    }
                }
            }
        })),
    )
}

/// PUT /{index}/_settings — Update dynamic index settings.
///
/// Supported dynamic settings:
/// - `index.number_of_replicas` (u32) — adjusts replica count
/// - `index.refresh_interval_ms` (u64 | null) — per-index refresh interval
/// - `index.flush_threshold_bytes` (u64 | null) — auto-flush threshold for the WAL
///
/// Immutable settings (rejected with 400):
/// - `index.number_of_shards`
///
/// Body format (OpenSearch-compatible):
/// ```json
/// {
///   "index": {
///     "number_of_replicas": 2,
///     "refresh_interval_ms": 10000,
///     "flush_threshold_bytes": 536870912
///   }
/// }
/// ```
pub async fn update_index_settings(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    // IndexName is validated at extraction time

    // Reject static settings
    if body.pointer("/index/number_of_shards").is_some() {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "index.number_of_shards is immutable and cannot be changed after index creation",
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut metadata = match cluster_state.indices.get(index_name.as_str()) {
        Some(m) => m.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "index_not_found_exception",
                format!("no such index [{}]", index_name),
            );
        }
    };

    let mut changed = false;

    // Update number_of_replicas
    if let Some(new_replicas) = body
        .pointer("/index/number_of_replicas")
        .and_then(|v| v.as_u64())
    {
        let new_replicas = new_replicas as u32;
        if new_replicas != metadata.number_of_replicas {
            metadata.update_number_of_replicas(new_replicas);
            changed = true;
        }
    }

    // Update refresh_interval_ms
    if let Some(val) = body.pointer("/index/refresh_interval_ms") {
        if val.is_null() {
            if metadata.settings.refresh_interval_ms.is_some() {
                metadata.settings.refresh_interval_ms = None;
                changed = true;
            }
        } else if let Some(ms) = val.as_u64()
            && metadata.settings.refresh_interval_ms != Some(ms)
        {
            metadata.settings.refresh_interval_ms = Some(ms);
            changed = true;
        }
    }

    // Update flush_threshold_bytes
    if let Some(val) = body.pointer("/index/flush_threshold_bytes") {
        if val.is_null() {
            if metadata.settings.flush_threshold_bytes.is_some() {
                metadata.settings.flush_threshold_bytes = None;
                changed = true;
            }
        } else if let Some(bytes) = val.as_u64()
            && metadata.settings.flush_threshold_bytes != Some(bytes)
        {
            metadata.settings.flush_threshold_bytes = Some(bytes);
            changed = true;
        }
    }

    if !changed {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "acknowledged": true })),
        );
    }

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "settings update") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_update_settings(&master, &index_name, &body)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward settings update to master: {}", e),
                );
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::UpdateIndex {
            metadata: metadata.clone(),
        };
        if let Err(e) = raft_write(&state, cmd).await {
            return e;
        }
    }

    // Apply settings to live engines on this node via watch channels
    state
        .shard_manager
        .apply_settings(&index_name, &metadata.settings);

    tracing::info!("Updated settings for index '{}'", index_name);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "acknowledged": true })),
    )
}

pub async fn delete_index(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
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

    // Coordinator: forward to leader or write locally via Raft
    if let Some(master) = match resolve_leader_or_master(&state, "index deletion") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_delete_index(&master, &index_name)
            .await
        {
            Ok(()) => {}
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward index deletion to master: {}", e),
                );
            }
        }
    } else {
        let cmd = crate::consensus::types::ClusterCommand::DeleteIndex {
            index_name: index_name.to_string(),
        };
        if let Err(e) = raft_write(&state, cmd).await {
            return e;
        }
    }

    // Close local shard engines and delete data
    if let Err(e) = state
        .shard_manager
        .close_index_shards_blocking_with_reason(
            index_name.to_string(),
            crate::shard::SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX,
        )
        .await
    {
        tracing::error!("Failed to close shards for index '{}': {}", index_name, e);
    }

    tracing::info!("Deleted index '{}'", index_name);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "acknowledged": true })),
    )
}

#[cfg(test)]
mod tests;
