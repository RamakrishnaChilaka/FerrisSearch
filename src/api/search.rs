use crate::api::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::Value;
use futures::future::join_all;

#[derive(Deserialize)]
pub struct SearchParams {
    #[serde(default = "default_query")]
    q: String,
}

fn default_query() -> String {
    "*".to_string()
}

/// GET /{index}/_search?q=... — query-string search across all shards (local + remote) for this index.
pub async fn search_documents(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(params): Query<SearchParams>,
) -> (StatusCode, Json<Value>) {
    if let Err(msg) = crate::common::validate_index_name(&index_name) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": msg })));
    }

    let cluster_state = state.cluster_manager.get_state();
    let metadata = match cluster_state.indices.get(&index_name) {
        Some(m) => m.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": { "type": "index_not_found_exception", "index": index_name } }))),
    };

    let mut all_hits = Vec::new();
    let mut successful_shards = 0u32;
    let mut failed_shards = 0u32;

    // Query local shard engines directly
    let local_shards = state.shard_manager.get_index_shards(&index_name);
    let local_shard_ids: std::collections::HashSet<u32> = local_shards.iter().map(|(id, _)| *id).collect();

    for (shard_id, engine) in &local_shards {
        match engine.search(&params.q) {
            Ok(hits) => {
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
            Err(e) => {
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
                (sid, client.forward_search_to_shard(&node_info, &index, sid, &query).await)
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
                tracing::error!("Remote shard {}/{} search failed: {}", index_name, shard_id, e);
                failed_shards += 1;
            }
            Err(e) => {
                tracing::error!("Remote shard search task panicked: {}", e);
                failed_shards += 1;
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({
        "_shards": {
            "total": successful_shards + failed_shards,
            "successful": successful_shards,
            "failed": failed_shards
        },
        "hits": {
            "total": { "value": all_hits.len(), "relation": "eq" },
            "hits": all_hits
        }
    })))
}
