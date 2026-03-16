use crate::api::AppState;
use crate::cluster::state::IndexMetadata;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;

/// PUT /{index} — Create an index with shard settings.
/// Body: `{ "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }`
pub async fn create_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": { "type": "invalid_index_name_exception", "reason": msg } })));
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

    let cluster_state = state.cluster_manager.get_state();

    if cluster_state.indices.contains_key(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": { "type": "resource_already_exists_exception", "index": index_name }
        })));
    }

    // Build shard assignment: distribute shards round-robin across Data nodes
    let data_nodes: Vec<String> = cluster_state.nodes.values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| n.id.clone())
        .collect();

    if data_nodes.is_empty() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "No data nodes available to assign shards" })));
    }

    let mut shard_assignment: HashMap<u32, crate::cluster::state::ShardRoutingEntry> = HashMap::new();
    for shard_id in 0..num_shards {
        let primary_node = data_nodes[(shard_id as usize) % data_nodes.len()].clone();
        // Assign replicas round-robin from different nodes than the primary
        let mut replicas = Vec::new();
        for r in 0..num_replicas {
            let replica_idx = ((shard_id as usize) + 1 + (r as usize)) % data_nodes.len();
            let replica_node = &data_nodes[replica_idx];
            if *replica_node != primary_node {
                replicas.push(replica_node.clone());
            }
        }
        shard_assignment.insert(shard_id, crate::cluster::state::ShardRoutingEntry {
            primary: primary_node,
            replicas,
        });
    }

    let metadata = IndexMetadata {
        name: index_name.clone(),
        number_of_shards: num_shards,
        number_of_replicas: num_replicas,
        shard_routing: shard_assignment.clone(),
    };

    // Register in cluster state (this node is master for now)
    let mut new_state = cluster_state.clone();
    new_state.add_index(metadata);
    state.cluster_manager.update_state(new_state.clone());

    // Broadcast updated state to all nodes so they learn about the new index
    state.transport_client.publish_state(&new_state).await;

    // Open local shard engines for shards assigned to this node (primary or replica)
    for (shard_id, routing) in &shard_assignment {
        if routing.primary == state.local_node_id || routing.replicas.contains(&state.local_node_id) {
            if let Err(e) = state.shard_manager.open_shard(&index_name, *shard_id) {
                tracing::error!("Failed to open shard {} for {}: {}", shard_id, index_name, e);
            }
        }
    }

    tracing::info!("Created index '{}' with {} shards, {} replicas", index_name, num_shards, num_replicas);

    (StatusCode::OK, Json(serde_json::json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index_name
    })))
}

/// POST /{index}/_doc — Index a single document with shard routing.
pub async fn index_document(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(mut payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": { "type": "invalid_index_name_exception", "reason": msg } })));
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
        tracing::warn!("Index '{}' not found, auto-creating with 1 shard", index_name);
        let mut shard_routing = HashMap::new();
        shard_routing.insert(0u32, crate::cluster::state::ShardRoutingEntry {
            primary: state.local_node_id.clone(),
            replicas: vec![],
        });
        let m = IndexMetadata {
            name: index_name.clone(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
        };
        let mut new_state = cluster_state.clone();
        new_state.add_index(m.clone());
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
        let _ = state.shard_manager.open_shard(&index_name, 0);
        m
    };

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Shard has no assigned node" }))),
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Target node not in cluster state" }))),
    };

    match state.transport_client.forward_index_to_shard(&target_node, &index_name, shard_id, &doc_id, &payload).await {
        Ok(res) => (StatusCode::CREATED, Json(res)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": format!("Forward failed: {}", e) }))),
    }
}

/// POST /{index}/_refresh — Scatter refresh to all local shard engines for this index.
pub async fn refresh_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let shards = state.shard_manager.get_index_shards(&index_name);
    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.refresh() {
            Ok(_) => successful += 1,
            Err(e) => { tracing::error!("Refresh failed: {}", e); failed += 1; }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
    })))
}

/// POST /{index}/_flush — Scatter flush to all local shard engines for this index.
pub async fn flush_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let shards = state.shard_manager.get_index_shards(&index_name);
    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.flush() {
            Ok(_) => successful += 1,
            Err(e) => { tracing::error!("Flush failed: {}", e); failed += 1; }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
    })))
}

/// POST /{index}/_bulk — Parse NDJSON, route each doc to the correct shard node.
pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "Invalid UTF-8 body" }))),
    };

    // Parse NDJSON: alternating action/doc lines
    let mut docs: Vec<(String, Value)> = Vec::new(); // (doc_id, payload)
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    while let Some(_action_line) = lines.next() {
        if let Some(doc_line) = lines.next() {
            if let Ok(mut doc) = serde_json::from_str::<Value>(doc_line) {
                let doc_id = if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                    id.to_string()
                } else {
                    uuid::Uuid::new_v4().to_string()
                };
                // Strip _id from stored payload — it's metadata, not document source
                if let Some(obj) = doc.as_object_mut() {
                    obj.remove("_id");
                }
                docs.push((doc_id, doc));
            }
        }
    }

    if docs.is_empty() {
        return (StatusCode::OK, Json(serde_json::json!({ "took": 0, "errors": false, "items": [] })));
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index if it doesn't exist
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
        m.clone()
    } else {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(0u32, crate::cluster::state::ShardRoutingEntry {
            primary: state.local_node_id.clone(),
            replicas: vec![],
        });
        let m = IndexMetadata {
            name: index_name.clone(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
        };
        let mut new_state = cluster_state.clone();
        new_state.add_index(m.clone());
        state.cluster_manager.update_state(new_state.clone());
        state.transport_client.publish_state(&new_state).await;
        let _ = state.shard_manager.open_shard(&index_name, 0);
        m
    };

    // Group docs by (node_id, shard_id) — each shard on each node gets its own batch
    let mut shard_batches: HashMap<(String, u32), Vec<(String, Value)>> = HashMap::new();

    for (doc_id, payload) in &docs {
        let shard_id = crate::engine::routing::calculate_shard(doc_id, metadata.number_of_shards);
        if let Some(node_id) = metadata.primary_node(shard_id) {
            shard_batches.entry((node_id.clone(), shard_id)).or_default().push((doc_id.clone(), payload.clone()));
        }
    }

    // Forward each batch to the owning node's specific shard
    let mut all_futures = Vec::new();

    for ((node_id, shard_id), batch) in shard_batches {
        if let Some(node_info) = cluster_state.nodes.get(&node_id) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let index = index_name.clone();
            all_futures.push(tokio::spawn(async move {
                client.forward_bulk_to_shard(&node_info, &index, shard_id, &batch).await
            }));
        }
    }

    let results = join_all(all_futures).await;
    let successful = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();
    let has_errors = successful < results.len();

    (StatusCode::OK, Json(serde_json::json!({
        "took": 0,
        "errors": has_errors,
        "items": docs.iter().map(|(id, _)| serde_json::json!({ "index": { "_id": id, "result": "created" } })).collect::<Vec<_>>()
    })))
}

/// POST /{index}/_search — DSL search across all shards (local + remote) for this index.
pub async fn search_documents_dsl(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let search_req: crate::search::SearchRequest = match serde_json::from_value(req) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": format!("Invalid query DSL: {}", e) }))),
    };

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": { "type": "index_not_found_exception", "index": index_name } }))),
    };

    let mut all_hits = Vec::new();
    let mut successful = 0u32;
    let mut failed = 0u32;

    // Query local shards directly
    for (shard_id, engine) in state.shard_manager.get_index_shards(&index_name) {
        match engine.search_query(&search_req) {
            Ok(hits) => {
                successful += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Err(e) => { tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e); failed += 1; }
        }
    }

    // Scatter to remote shards (shards on other nodes)
    let local_shard_ids: std::collections::HashSet<u32> = state.shard_manager
        .get_index_shards(&index_name)
        .iter()
        .map(|(id, _)| *id)
        .collect();

    let mut remote_futures = Vec::new();
    for (shard_id, routing) in &metadata.shard_routing {
        if local_shard_ids.contains(shard_id) {
            continue; // already queried locally
        }
        if let Some(node_info) = cluster_state.nodes.get(&routing.primary) {
            let client = state.transport_client.clone();
            let node_info = node_info.clone();
            let index = index_name.clone();
            let sid = *shard_id;
            let req_clone = search_req.clone();
            remote_futures.push(tokio::spawn(async move {
                (sid, client.forward_search_dsl_to_shard(&node_info, &index, sid, &req_clone).await)
            }));
        }
    }

    let remote_results = join_all(remote_futures).await;
    for result in remote_results {
        match result {
            Ok((shard_id, Ok(hits))) => {
                successful += 1;
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name, "_shard": shard_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_source": hit.get("_source").unwrap_or(&hit)
                    }));
                }
            }
            Ok((shard_id, Err(e))) => {
                tracing::error!("Remote shard {}/{} search failed: {}", index_name, shard_id, e);
                failed += 1;
            }
            Err(e) => {
                tracing::error!("Remote shard search task panicked: {}", e);
                failed += 1;
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed },
        "hits": { "total": { "value": all_hits.len(), "relation": "eq" }, "hits": all_hits }
    })))
}

/// GET /{index}/_doc/{id} — Retrieve a document by its ID.
pub async fn get_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": { "type": "index_not_found_exception", "index": index_name } }))),
    };

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Shard has no assigned node" }))),
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Target node not in cluster state" }))),
    };

    match state.transport_client.forward_get_to_shard(&target_node, &index_name, shard_id, &doc_id).await {
        Ok(Some(source)) => (StatusCode::OK, Json(serde_json::json!({
            "_index": index_name, "_id": doc_id, "_shard": shard_id, "found": true, "_source": source
        }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "_index": index_name, "_id": doc_id, "found": false
        }))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": format!("{}", e) }))),
    }
}

/// DELETE /{index}/_doc/{id} — Delete a document by its ID.
pub async fn delete_document(
    State(state): State<AppState>,
    Path((index_name, doc_id)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": { "type": "index_not_found_exception", "index": index_name } }))),
    };

    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.primary_node(shard_id) {
        Some(id) => id.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Shard has no assigned node" }))),
    };

    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "Target node not in cluster state" }))),
    };

    match state.transport_client.forward_delete_to_shard(&target_node, &index_name, shard_id, &doc_id).await {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": format!("{}", e) }))),
    }
}

/// DELETE /{index} — Delete an entire index (remove from cluster state, close shards, delete data).
pub async fn delete_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let cluster_state = state.cluster_manager.get_state();

    if !cluster_state.indices.contains_key(&index_name) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": { "type": "index_not_found_exception", "index": index_name }
        })));
    }

    // Remove from cluster state
    let mut new_state = cluster_state.clone();
    new_state.indices.remove(&index_name);
    new_state.version += 1;
    state.cluster_manager.update_state(new_state.clone());

    // Broadcast updated state so all nodes learn the index is gone
    state.transport_client.publish_state(&new_state).await;

    // Close local shard engines and delete data
    if let Err(e) = state.shard_manager.close_index_shards(&index_name) {
        tracing::error!("Failed to close shards for index '{}': {}", index_name, e);
    }

    tracing::info!("Deleted index '{}'", index_name);

    (StatusCode::OK, Json(serde_json::json!({ "acknowledged": true })))
}
