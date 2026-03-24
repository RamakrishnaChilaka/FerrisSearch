use crate::api::AppState;
use crate::cluster::state::IndexMetadata;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct DistributedDslSearchResult {
    pub all_hits: Vec<Value>,
    pub total_hits: usize,
    pub successful_shards: u32,
    pub failed_shards: u32,
    pub aggregations: HashMap<String, Value>,
}

pub(crate) fn ensure_local_index_shards_open(
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

        if let Err(e) = state.shard_manager.open_shard_with_settings(
            index_name,
            *shard_id,
            &metadata.mappings,
            &metadata.settings,
        ) {
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

/// Auto-create an index with 1 shard, respecting the coordinator pattern.
/// If Raft is active and this node is NOT the leader, forwards to the master.
async fn auto_create_index(
    state: &AppState,
    index_name: &str,
    cluster_state: &crate::cluster::state::ClusterState,
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
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    if let Some(ref raft) = state.raft {
        if !raft.is_leader() {
            // Forward auto-create to the leader via gRPC
            let master_id = match cluster_state.master_node.as_ref() {
                Some(id) => id,
                None => {
                    return Err(crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "No master node available to forward auto-create index",
                    ));
                }
            };
            let master_node = match cluster_state.nodes.get(master_id) {
                Some(n) => n.clone(),
                None => {
                    return Err(crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "Master node info not found in cluster state",
                    ));
                }
            };
            let body = serde_json::json!({
                "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
            });
            let body_bytes = serde_json::to_vec(&body).unwrap_or_default();
            match state
                .transport_client
                .forward_create_index(&master_node, index_name, &body_bytes)
                .await
            {
                Ok(_) => {
                    // Re-read cluster state after leader has created the index
                    let updated_cs = state.cluster_manager.get_state();
                    if let Some(created_meta) = updated_cs.indices.get(index_name) {
                        return Ok(created_meta.clone());
                    }
                    // Raft replication may not have arrived yet; use our local metadata
                    return Ok(m);
                }
                Err(e) => {
                    return Err(crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "forward_exception",
                        format!("Auto-create index forward to master failed: {}", e),
                    ));
                }
            }
        }
        let cmd = crate::consensus::types::ClusterCommand::CreateIndex {
            metadata: m.clone(),
        };
        if let Err(e) = raft.client_write(cmd).await {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "raft_write_exception",
                format!("Auto-create index via Raft failed: {}", e),
            ));
        }
    } else {
        let mut new_state = cluster_state.clone();
        new_state.add_index(m.clone());
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
    }
    let _ = state.shard_manager.open_shard(index_name, 0);
    Ok(m)
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
    Path(index_name): Path<String>,
) -> StatusCode {
    let cluster_state = state.cluster_manager.get_state();
    if cluster_state.indices.contains_key(&index_name) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// PUT /{index} — Create an index with shard settings.
/// Body: `{ "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }`
pub async fn create_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

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

    let cluster_state = state.cluster_manager.get_state();

    if cluster_state.indices.contains_key(&index_name) {
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

    // Write through Raft if available, otherwise fallback
    if let Some(ref raft) = state.raft {
        if !raft.is_leader() {
            // Forward to the master via gRPC — this node acts as coordinator
            let cluster_state_for_fwd = state.cluster_manager.get_state();
            let master_id = match cluster_state_for_fwd.master_node.as_ref() {
                Some(id) => id,
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "No master node available to forward index creation",
                    );
                }
            };
            let master_node = match cluster_state_for_fwd.nodes.get(master_id) {
                Some(n) => n.clone(),
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "Master node info not found in cluster state",
                    );
                }
            };
            match state
                .transport_client
                .forward_create_index(&master_node, &index_name, &body)
                .await
            {
                Ok(resp) => {
                    return (StatusCode::OK, Json(resp));
                }
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
        if let Err(e) = raft.client_write(cmd).await {
            return crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "raft_write_exception",
                format!("Raft write failed: {}", e),
            );
        }
    } else {
        let mut new_state = cluster_state.clone();
        new_state.add_index(metadata);
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
    }

    // Open local shard engines for shards assigned to this node (primary or replica)
    for (shard_id, routing) in &shard_assignment {
        if (routing.primary == state.local_node_id
            || routing.replicas.contains(&state.local_node_id))
            && let Err(e) = state.shard_manager.open_shard_with_settings(
                &index_name,
                *shard_id,
                &index_mappings,
                &index_settings,
            )
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
    Path(index_name): Path<String>,
    Query(refresh_param): Query<RefreshParam>,
    Json(mut payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

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
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
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
                let _ = engine.refresh();
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
    Path((index_name, doc_id)): Path<(String, String)>,
    Query(refresh_param): Query<RefreshParam>,
    Json(mut payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    // Remove _id from stored payload if present — it's metadata, not document source
    if let Some(obj) = payload.as_object_mut() {
        obj.remove("_id");
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
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
                let _ = engine.refresh();
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

/// POST|GET /{index}/_refresh — Scatter refresh to all local shard engines for this index.
/// If no local shards are open but the index exists in cluster state, opens them first.
pub async fn refresh_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = cluster_state.indices.get(&index_name);

    let shards = if let Some(meta) = metadata {
        ensure_local_index_shards_open(&state, &index_name, meta, "Refresh")
    } else {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    };

    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.refresh() {
            Ok(_) => successful += 1,
            Err(e) => {
                tracing::error!("Refresh failed: {}", e);
                failed += 1;
            }
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
        })),
    )
}

/// POST|GET /{index}/_flush — Scatter flush to all local shard engines for this index.
/// If no local shards are open but the index exists in cluster state, opens them first.
pub async fn flush_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = cluster_state.indices.get(&index_name);

    let shards = if let Some(meta) = metadata {
        ensure_local_index_shards_open(&state, &index_name, meta, "Flush")
    } else {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    };

    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.flush_with_global_checkpoint() {
            Ok(_) => successful += 1,
            Err(e) => {
                tracing::error!("Flush failed: {}", e);
                failed += 1;
            }
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
        })),
    )
}

/// Parsed bulk document with optional index from action metadata.
struct BulkDoc {
    doc_id: String,
    index: Option<String>,
    payload: Value,
}

#[derive(Debug)]
struct RoutedBulkDoc {
    position: usize,
    index_name: String,
    doc_id: String,
    payload: Value,
    shard_id: u32,
    node_id: String,
}

type BulkTargetKey = (String, String, u32);

fn bulk_success_item(index_name: &str, doc_id: &str) -> Value {
    serde_json::json!({
        "index": {
            "_index": index_name,
            "_id": doc_id,
            "_version": 1,
            "result": "created",
            "status": 201,
            "_shards": { "total": 1, "successful": 1, "failed": 0 },
            "_seq_no": 0,
            "_primary_term": 1
        }
    })
}

fn bulk_error_item(
    index_name: Option<&str>,
    doc_id: &str,
    status: StatusCode,
    error_type: &str,
    reason: impl std::fmt::Display,
) -> Value {
    let mut item = serde_json::json!({
        "index": {
            "_id": doc_id,
            "status": status.as_u16(),
            "error": {
                "type": error_type,
                "reason": reason.to_string(),
            }
        }
    });

    if let Some(index_name) = index_name {
        item["index"]["_index"] = serde_json::json!(index_name);
    }

    item
}

fn route_bulk_doc(
    position: usize,
    index_name: String,
    doc_id: String,
    payload: Value,
    metadata: &IndexMetadata,
    cluster_state: &crate::cluster::state::ClusterState,
) -> Result<RoutedBulkDoc, Value> {
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let node_id = match metadata.primary_node(shard_id) {
        Some(node_id) => node_id.clone(),
        None => {
            return Err(bulk_error_item(
                Some(&index_name),
                &doc_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_not_available_exception",
                "Shard has no assigned primary",
            ));
        }
    };

    if !cluster_state.nodes.contains_key(&node_id) {
        return Err(bulk_error_item(
            Some(&index_name),
            &doc_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "node_not_found_exception",
            format!("Primary node [{}] not found in cluster state", node_id),
        ));
    }

    Ok(RoutedBulkDoc {
        position,
        index_name,
        doc_id,
        payload,
        shard_id,
        node_id,
    })
}

async fn forward_bulk_batches(
    state: &AppState,
    cluster_state: &crate::cluster::state::ClusterState,
    routed_docs: &[RoutedBulkDoc],
) -> std::collections::HashSet<BulkTargetKey> {
    let mut shard_batches: HashMap<BulkTargetKey, Vec<(String, Value)>> = HashMap::new();
    for doc in routed_docs {
        shard_batches
            .entry((doc.index_name.clone(), doc.node_id.clone(), doc.shard_id))
            .or_default()
            .push((doc.doc_id.clone(), doc.payload.clone()));
    }

    let mut futures = Vec::new();
    let mut shard_keys = Vec::new();
    let mut failed_targets = std::collections::HashSet::new();

    for ((index_name, node_id, shard_id), batch) in shard_batches {
        if let Some(node_info) = cluster_state.nodes.get(&node_id) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let batch_index = index_name.clone();
            futures.push(tokio::spawn(async move {
                client
                    .forward_bulk_to_shard(&node_info, &batch_index, shard_id, &batch)
                    .await
            }));
            shard_keys.push((index_name, node_id, shard_id));
        } else {
            failed_targets.insert((index_name, node_id, shard_id));
        }
    }

    let results = join_all(futures).await;
    for (key, result) in shard_keys.into_iter().zip(results.into_iter()) {
        if !matches!(result, Ok(Ok(_))) {
            failed_targets.insert(key);
        }
    }

    failed_targets
}

fn finalize_bulk_items(
    mut item_results: Vec<Option<Value>>,
    routed_docs: Vec<RoutedBulkDoc>,
    failed_targets: &std::collections::HashSet<BulkTargetKey>,
) -> Vec<Value> {
    for doc in routed_docs {
        let target = (doc.index_name.clone(), doc.node_id.clone(), doc.shard_id);
        let item = if failed_targets.contains(&target) {
            bulk_error_item(
                Some(&doc.index_name),
                &doc.doc_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_failure",
                "Failed to index to shard",
            )
        } else {
            bulk_success_item(&doc.index_name, &doc.doc_id)
        };
        item_results[doc.position] = Some(item);
    }

    item_results
        .into_iter()
        .enumerate()
        .map(|(position, item)| {
            item.unwrap_or_else(|| {
                bulk_error_item(
                    None,
                    "",
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "bulk_item_exception",
                    format!("bulk item {} was dropped unexpectedly", position),
                )
            })
        })
        .collect()
}

/// Parse NDJSON bulk body into a list of documents.
/// Supports standard OpenSearch format:
///   {"index": {"_index": "idx", "_id": "1"}}
///   {"field": "value"}
/// Also supports legacy FerrisSearch format where _id is in the doc itself.
fn parse_bulk_ndjson(text: &str) -> Vec<BulkDoc> {
    let mut docs = Vec::new();
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    while let Some(action_line) = lines.next() {
        if let Some(doc_line) = lines.next()
            && let Ok(mut doc) = serde_json::from_str::<Value>(doc_line)
        {
            // Parse action metadata
            let action_meta = serde_json::from_str::<Value>(action_line)
                .ok()
                .and_then(|action| {
                    action
                        .as_object()
                        .and_then(|obj| obj.values().next().cloned())
                });

            let action_id = action_meta
                .as_ref()
                .and_then(|m| m.get("_id").and_then(|v| v.as_str().map(String::from)));

            let action_index = action_meta
                .as_ref()
                .and_then(|m| m.get("_index").and_then(|v| v.as_str().map(String::from)));

            let doc_id = if let Some(id) = action_id {
                id
            } else if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                id.to_string()
            } else if let Some(id) = doc.get("_doc_id").and_then(|v| v.as_str()) {
                id.to_string()
            } else {
                uuid::Uuid::new_v4().to_string()
            };
            // Strip id metadata from stored payload
            if let Some(obj) = doc.as_object_mut() {
                obj.remove("_id");
                obj.remove("_doc_id");
            }
            // If doc has _source wrapper, unwrap it
            let payload = if let Some(source) = doc.get("_source").cloned() {
                source
            } else {
                doc
            };
            docs.push(BulkDoc {
                doc_id,
                index: action_index,
                payload,
            });
        }
    }
    docs
}

/// POST /_bulk — Global bulk endpoint (index name comes from action metadata).
pub async fn bulk_index_global(
    State(state): State<AppState>,
    Query(refresh_param): Query<RefreshParam>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parse_exception",
                "Invalid UTF-8 body",
            );
        }
    };

    let docs = parse_bulk_ndjson(text);

    if docs.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "took": 0, "errors": false, "items": [] })),
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut item_results: Vec<Option<Value>> = vec![None; docs.len()];
    let mut has_errors = false;
    let mut by_index: HashMap<String, Vec<(usize, String, Value)>> = HashMap::new();

    for (position, doc) in docs.into_iter().enumerate() {
        if let Some(index_name) = doc.index {
            by_index
                .entry(index_name)
                .or_default()
                .push((position, doc.doc_id, doc.payload));
        } else {
            has_errors = true;
            item_results[position] = Some(bulk_error_item(
                None,
                &doc.doc_id,
                StatusCode::BAD_REQUEST,
                "action_request_validation_exception",
                "bulk action metadata must include _index",
            ));
        }
    }

    let mut routed_docs = Vec::new();
    for (index_name, batch) in by_index {
        let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
            m.clone()
        } else {
            match auto_create_index(&state, &index_name, &cluster_state).await {
                Ok(m) => m,
                Err(_) => {
                    has_errors = true;
                    for (position, doc_id, _) in batch {
                        item_results[position] = Some(bulk_error_item(
                            Some(&index_name),
                            &doc_id,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "auto_create_exception",
                            "Failed to auto-create index",
                        ));
                    }
                    continue;
                }
            }
        };

        for (position, doc_id, payload) in batch {
            match route_bulk_doc(
                position,
                index_name.clone(),
                doc_id,
                payload,
                &metadata,
                &cluster_state,
            ) {
                Ok(doc) => routed_docs.push(doc),
                Err(item) => {
                    has_errors = true;
                    item_results[position] = Some(item);
                }
            }
        }
    }

    let failed_targets = forward_bulk_batches(&state, &cluster_state, &routed_docs).await;
    if !failed_targets.is_empty() {
        has_errors = true;
    }

    // ?refresh=true: commit + reload all affected shards so docs are immediately searchable
    if refresh_param.should_refresh() {
        let affected_indices: std::collections::HashSet<String> = routed_docs
            .iter()
            .map(|doc| doc.index_name.clone())
            .collect();
        for index_name in affected_indices {
            for (_, engine) in state.shard_manager.get_index_shards(&index_name) {
                let _ = engine.refresh();
            }
        }
    }

    let all_items = finalize_bulk_items(item_results, routed_docs, &failed_targets);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "took": 0,
            "errors": has_errors,
            "items": all_items
        })),
    )
}

/// POST /{index}/_bulk — Parse NDJSON, route each doc to the correct shard node.
pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(refresh_param): Query<RefreshParam>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => {
            return crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "parse_exception",
                "Invalid UTF-8 body",
            );
        }
    };

    // Parse NDJSON body
    let docs: Vec<(String, Value)> = parse_bulk_ndjson(text)
        .into_iter()
        .map(|d| (d.doc_id, d.payload))
        .collect();

    if docs.is_empty() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "took": 0, "errors": false, "items": [] })),
        );
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index if it doesn't exist
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
        m.clone()
    } else {
        match auto_create_index(&state, &index_name, &cluster_state).await {
            Ok(m) => m,
            Err(err_resp) => return err_resp,
        }
    };

    let mut item_results: Vec<Option<Value>> = vec![None; docs.len()];
    let mut has_errors = false;
    let mut routed_docs = Vec::new();

    for (position, (doc_id, payload)) in docs.into_iter().enumerate() {
        match route_bulk_doc(
            position,
            index_name.clone(),
            doc_id,
            payload,
            &metadata,
            &cluster_state,
        ) {
            Ok(doc) => routed_docs.push(doc),
            Err(item) => {
                has_errors = true;
                item_results[position] = Some(item);
            }
        }
    }

    let failed_targets = forward_bulk_batches(&state, &cluster_state, &routed_docs).await;
    if !failed_targets.is_empty() {
        has_errors = true;
    }

    // ?refresh=true: commit + reload all affected shards so docs are immediately searchable
    if refresh_param.should_refresh() {
        for (_, engine) in state.shard_manager.get_index_shards(&index_name) {
            let _ = engine.refresh();
        }
    }

    let items = finalize_bulk_items(item_results, routed_docs, &failed_targets);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "took": 0,
            "errors": has_errors,
            "items": items
        })),
    )
}

pub(crate) async fn execute_distributed_dsl_search(
    state: &AppState,
    index_name: &str,
    search_req: &crate::search::SearchRequest,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    if let Err(msg) = crate::common::validate_index_name(index_name) {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        ));
    }

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

    let mut text_hits = Vec::new();
    let mut knn_hits = Vec::new();
    let mut successful = 0u32;
    let mut failed = 0u32;
    let mut total_hits: usize = 0;
    let is_hybrid = search_req.knn.is_some();

    let mut all_partial_aggs: Vec<HashMap<String, crate::search::PartialAggResult>> = Vec::new();

    let local_shards = ensure_local_index_shards_open(state, index_name, &metadata, "DSL search");

    for (shard_id, engine) in &local_shards {
        match engine.search_query(search_req) {
            Ok((hits, shard_total, partial_aggs)) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                for hit in hits {
                    text_hits.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Err(e) => {
                tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e);
                failed += 1;
            }
        }
    }

    if let Some(ref knn) = search_req.knn
        && let Some((field_name, params)) = knn.fields.iter().next()
    {
        for (shard_id, engine) in &local_shards {
            match engine.search_knn_filtered(
                field_name,
                &params.vector,
                params.k,
                params.filter.as_ref(),
            ) {
                Ok(hits) => {
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
                Err(e) => {
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
                        text_hits.push(enriched);
                    }
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
        crate::search::merge_hybrid_hits(text_hits, knn_hits)
    } else {
        text_hits
    };
    crate::search::sort_hits(&mut all_hits, &search_req.sort);

    let merged_aggs = if !all_partial_aggs.is_empty() {
        crate::search::merge_aggregations(all_partial_aggs, &search_req.aggs)
    } else {
        HashMap::new()
    };

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits,
        successful_shards: successful,
        failed_shards: failed,
        aggregations: merged_aggs,
    })
}

/// POST /{index}/_search — DSL search across all shards (local + remote) for this index.
pub async fn search_documents_dsl(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
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
    Path((index_name, doc_id)): Path<(String, String)>,
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
    Path((index_name, doc_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

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
    Path((index_name, doc_id)): Path<(String, String)>,
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
    Path(index_name): Path<String>,
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
            index_name.clone(): {
                "settings": {
                    "index": {
                        "number_of_shards": metadata.number_of_shards,
                        "number_of_replicas": metadata.number_of_replicas,
                        "refresh_interval_ms": metadata.settings.refresh_interval_ms,
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
///
/// Immutable settings (rejected with 400):
/// - `index.number_of_shards`
///
/// Body format (OpenSearch-compatible):
/// ```json
/// {
///   "index": {
///     "number_of_replicas": 2,
///     "refresh_interval_ms": 10000
///   }
/// }
/// ```
pub async fn update_index_settings(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "invalid_index_name_exception",
            msg,
        );
    }

    // Reject static settings
    if body.pointer("/index/number_of_shards").is_some() {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "index.number_of_shards is immutable and cannot be changed after index creation",
        );
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut metadata = match cluster_state.indices.get(&index_name) {
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

    if !changed {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "acknowledged": true })),
        );
    }

    // Persist via Raft
    if let Some(ref raft) = state.raft {
        if !raft.is_leader() {
            // Forward to the master via gRPC
            let cluster_state_for_fwd = state.cluster_manager.get_state();
            let master_id = match cluster_state_for_fwd.master_node.as_ref() {
                Some(id) => id,
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "No master node available to forward settings update",
                    );
                }
            };
            let master_node = match cluster_state_for_fwd.nodes.get(master_id) {
                Some(n) => n.clone(),
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "Master node info not found in cluster state",
                    );
                }
            };
            match state
                .transport_client
                .forward_update_settings(&master_node, &index_name, &body)
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
            if let Err(e) = raft.client_write(cmd).await {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "raft_write_exception",
                    format!("Raft write failed: {}", e),
                );
            }
        }
    } else {
        let mut new_state = cluster_state.clone();
        new_state
            .indices
            .insert(index_name.clone(), metadata.clone());
        new_state.version += 1;
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
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
    Path(index_name): Path<String>,
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

    // Remove from cluster state via Raft if available, otherwise fallback
    if let Some(ref raft) = state.raft {
        if !raft.is_leader() {
            // Forward to the master via gRPC — this node acts as coordinator
            let cluster_state_for_fwd = state.cluster_manager.get_state();
            let master_id = match cluster_state_for_fwd.master_node.as_ref() {
                Some(id) => id,
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "No master node available to forward index deletion",
                    );
                }
            };
            let master_node = match cluster_state_for_fwd.nodes.get(master_id) {
                Some(n) => n.clone(),
                None => {
                    return crate::api::error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "master_not_discovered_exception",
                        "Master node info not found in cluster state",
                    );
                }
            };
            match state
                .transport_client
                .forward_delete_index(&master_node, &index_name)
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
                index_name: index_name.clone(),
            };
            if let Err(e) = raft.client_write(cmd).await {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "raft_write_exception",
                    format!("Raft write failed: {}", e),
                );
            }
        }
    } else {
        let mut new_state = cluster_state.clone();
        new_state.indices.remove(&index_name);
        new_state.version += 1;
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
    }

    // Close local shard engines and delete data
    if let Err(e) = state.shard_manager.close_index_shards(&index_name) {
        tracing::error!("Failed to close shards for index '{}': {}", index_name, e);
    }

    tracing::info!("Deleted index '{}'", index_name);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "acknowledged": true })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        ClusterState, IndexSettings, NodeInfo, NodeRole, ShardRoutingEntry,
    };
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn parse_opensearch_ndjson_format() {
        let input = r#"{"index":{"_index":"my-index","_id":"1"}}
{"title":"Hello","year":2024}
{"index":{"_index":"my-index","_id":"2"}}
{"title":"World","year":2025}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].doc_id, "1");
        assert_eq!(docs[0].index.as_deref(), Some("my-index"));
        assert_eq!(docs[0].payload["title"], "Hello");
        assert_eq!(docs[1].doc_id, "2");
        assert_eq!(docs[1].payload["year"], 2025);
    }

    #[test]
    fn parse_opensearch_create_action() {
        let input = r#"{"create":{"_index":"logs","_id":"abc"}}
{"msg":"test log"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, "abc");
        assert_eq!(docs[0].index.as_deref(), Some("logs"));
        assert_eq!(docs[0].payload["msg"], "test log");
    }

    #[test]
    fn parse_legacy_ferrissearch_format() {
        let input = r#"{}
{"_doc_id":"d1","_source":{"name":"Alice"}}
{}
{"_doc_id":"d2","_source":{"name":"Bob"}}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].doc_id, "d1");
        assert_eq!(docs[0].payload["name"], "Alice");
        assert_eq!(docs[1].doc_id, "d2");
        assert_eq!(docs[1].payload["name"], "Bob");
    }

    #[test]
    fn parse_id_in_doc_body_fallback() {
        let input = r#"{"index":{}}
{"_id":"from-body","title":"test"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, "from-body");
        assert!(docs[0].payload.get("_id").is_none());
        assert_eq!(docs[0].payload["title"], "test");
    }

    #[test]
    fn parse_action_id_takes_precedence_over_body_id() {
        let input = r#"{"index":{"_id":"action-id"}}
{"_id":"body-id","title":"test"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, "action-id");
    }

    #[test]
    fn parse_auto_generates_id_when_missing() {
        let input = r#"{"index":{}}
{"title":"no id"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert!(!docs[0].doc_id.is_empty());
        assert_eq!(docs[0].payload["title"], "no id");
    }

    #[test]
    fn parse_empty_body() {
        let docs = parse_bulk_ndjson("");
        assert!(docs.is_empty());
    }

    #[test]
    fn parse_blank_lines_are_skipped() {
        let input = r#"{"index":{"_id":"1"}}

{"title":"Hello"}

{"index":{"_id":"2"}}

{"title":"World"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 2);
    }

    #[test]
    fn parse_source_wrapper_unwrapped() {
        let input = r#"{"index":{"_id":"1"}}
{"_source":{"name":"Alice"},"_doc_id":"ignored"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, "1");
        assert_eq!(docs[0].payload["name"], "Alice");
    }

    #[test]
    fn parse_index_extracted_from_action() {
        let input = r#"{"index":{"_index":"idx-a","_id":"1"}}
{"f":"v1"}
{"index":{"_index":"idx-b","_id":"2"}}
{"f":"v2"}
{"index":{"_id":"3"}}
{"f":"v3"}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].index.as_deref(), Some("idx-a"));
        assert_eq!(docs[1].index.as_deref(), Some("idx-b"));
        assert!(docs[2].index.is_none());
    }

    #[test]
    fn parse_odd_number_of_lines_ignores_trailing() {
        let input = r#"{"index":{"_id":"1"}}
{"title":"complete"}
{"index":{"_id":"2"}}
"#;
        let docs = parse_bulk_ndjson(input);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].doc_id, "1");
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

    fn make_test_metadata(primary: Option<&str>) -> IndexMetadata {
        let mut shard_routing = HashMap::new();
        if let Some(primary) = primary {
            shard_routing.insert(
                0,
                ShardRoutingEntry {
                    primary: primary.to_string(),
                    replicas: vec![],
                    unassigned_replicas: 0,
                },
            );
        }
        IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: IndexSettings::default(),
        }
    }

    fn make_test_app_state(cluster_state: ClusterState) -> (tempfile::TempDir, AppState) {
        let temp_dir = tempfile::tempdir().unwrap();
        let manager = crate::cluster::ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);
        let state = AppState {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft: None,
        };
        (temp_dir, state)
    }

    #[test]
    fn route_bulk_doc_reports_missing_primary() {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));

        let err = route_bulk_doc(
            0,
            "idx".into(),
            "doc-1".into(),
            serde_json::json!({"title": "hello"}),
            &make_test_metadata(None),
            &cluster_state,
        )
        .unwrap_err();

        assert_eq!(err["index"]["status"], 500);
        assert_eq!(
            err["index"]["error"]["type"],
            "shard_not_available_exception"
        );
    }

    #[tokio::test]
    async fn bulk_index_reports_missing_primary_node_as_item_error() {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));
        cluster_state.add_index(make_test_metadata(Some("missing-node")));

        let (_tmp, state) = make_test_app_state(cluster_state);
        let input = axum::body::Bytes::from("{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n");

        let (status, Json(body)) = bulk_index(
            State(state),
            Path("idx".to_string()),
            Query(RefreshParam { refresh: None }),
            input,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["errors"], true);
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
        assert_eq!(body["items"][0]["index"]["status"], 500);
        assert_eq!(
            body["items"][0]["index"]["error"]["type"],
            "node_not_found_exception"
        );
    }

    #[tokio::test]
    async fn bulk_index_global_reports_missing_action_index() {
        let mut cluster_state = ClusterState::new("test-cluster".into());
        cluster_state.add_node(make_test_node("node-1"));

        let (_tmp, state) = make_test_app_state(cluster_state);
        let input = axum::body::Bytes::from("{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"hello\"}\n");

        let (status, Json(body)) =
            bulk_index_global(State(state), Query(RefreshParam { refresh: None }), input).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["errors"], true);
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
        assert_eq!(body["items"][0]["index"]["status"], 400);
        assert_eq!(
            body["items"][0]["index"]["error"]["type"],
            "action_request_validation_exception"
        );
    }
}
