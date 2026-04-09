//! Bulk document indexing: NDJSON parsing, routing, forwarding, and response assembly.

use super::*;

pub(super) struct BulkDoc {
    pub doc_id: String,
    pub index: Option<String>,
    pub payload: Value,
}

#[derive(Debug)]
pub(super) struct RoutedBulkDoc {
    pub position: usize,
    pub index_name: String,
    pub doc_id: String,
    pub payload: Value,
    pub shard_id: u32,
    pub node_id: String,
}

pub(super) type BulkTargetKey = (String, String, u32);

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

pub(super) fn route_bulk_doc(
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
) -> HashMap<BulkTargetKey, String> {
    let mut shard_batches: HashMap<BulkTargetKey, Vec<(String, Value)>> = HashMap::new();
    for doc in routed_docs {
        shard_batches
            .entry((
                doc.index_name.to_string(),
                doc.node_id.clone(),
                doc.shard_id,
            ))
            .or_default()
            .push((doc.doc_id.clone(), doc.payload.clone()));
    }

    let mut futures = Vec::new();
    let mut shard_keys = Vec::new();
    let mut failed_targets = HashMap::new();

    for ((index_name, node_id, shard_id), batch) in shard_batches {
        if let Some(node_info) = cluster_state.nodes.get(&node_id) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let batch_index = index_name.to_string();
            futures.push(tokio::spawn(async move {
                client
                    .forward_bulk_to_shard(&node_info, &batch_index, shard_id, &batch)
                    .await
            }));
            shard_keys.push((index_name, node_id, shard_id));
        } else {
            failed_targets.insert(
                (index_name, node_id, shard_id),
                "Primary node missing from cluster state".to_string(),
            );
        }
    }

    let results = join_all(futures).await;
    for (key, result) in shard_keys.into_iter().zip(results.into_iter()) {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                failed_targets.insert(key, e.to_string());
            }
            Err(join_err) => {
                failed_targets.insert(key, format!("bulk forwarding task failed: {}", join_err));
            }
        }
    }

    failed_targets
}

pub(super) fn finalize_bulk_items(
    mut item_results: Vec<Option<Value>>,
    routed_docs: Vec<RoutedBulkDoc>,
    failed_targets: &HashMap<BulkTargetKey, String>,
) -> Vec<Value> {
    for doc in routed_docs {
        let target = (
            doc.index_name.to_string(),
            doc.node_id.clone(),
            doc.shard_id,
        );
        let item = if let Some(reason) = failed_targets.get(&target) {
            bulk_error_item(
                Some(&doc.index_name),
                &doc.doc_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_failure",
                reason,
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
pub(super) fn parse_bulk_ndjson(text: &str) -> Vec<BulkDoc> {
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
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();

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
        let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
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
                index_name.to_string(),
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
            .map(|doc| doc.index_name.to_string())
            .collect();
        for index_name in affected_indices {
            for (_, engine) in state.shard_manager.get_index_shards(&index_name) {
                let _ = state
                    .worker_pools
                    .spawn_write(move || engine.refresh())
                    .await;
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
    Path(index_name): Path<crate::common::IndexName>,
    Query(refresh_param): Query<RefreshParam>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    let _timer = crate::metrics::INDEX_LATENCY_SECONDS.start_timer();
    crate::metrics::BULK_REQUESTS_TOTAL.inc();

    // IndexName is validated at extraction time

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
    let metadata = if let Some(m) = cluster_state.indices.get(index_name.as_str()) {
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
            index_name.to_string(),
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
            let _ = state
                .worker_pools
                .spawn_write(move || engine.refresh())
                .await;
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
