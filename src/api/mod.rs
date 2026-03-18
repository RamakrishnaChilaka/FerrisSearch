//! HTTP REST API Layer.
//! Long-term goal: Implement OpenSearch-compatible REST APIs for searching, indexing, and cluster management.

pub mod cat;
pub mod cluster;
pub mod index;
pub mod search;

use crate::cluster::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::TransportClient;
use axum::{
    Json, Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct NodeInfoResponse {
    name: String,
    version: String,
    engine: String,
}

async fn handle_root() -> Json<NodeInfoResponse> {
    Json(NodeInfoResponse {
        name: "ferrissearch-node".into(),
        version: "0.1.0".into(),
        engine: "tantivy".into(),
    })
}

#[derive(Clone)]
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    /// The local node ID — needed for routing decisions
    pub local_node_id: String,
    /// Raft consensus instance (if enabled)
    pub raft: Option<Arc<RaftInstance>>,
}

/// Build a consistent OpenSearch-compatible error response.
pub fn error_response(
    status: StatusCode,
    error_type: &str,
    reason: impl std::fmt::Display,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        status,
        Json(serde_json::json!({
            "error": { "type": error_type, "reason": reason.to_string() },
            "status": status.as_u16()
        })),
    )
}

/// Middleware that pretty-prints JSON responses when `?pretty` is in the query string.
async fn pretty_json_middleware(req: Request<Body>, next: Next) -> Response {
    let wants_pretty = req.uri().query().map_or(false, |q| {
        q.split('&').any(|param| {
            let key = param.split('=').next().unwrap_or("");
            key == "pretty"
        })
    });

    let response = next.run(req).await;

    if !wants_pretty {
        return response;
    }

    // Only reformat if the response content-type is JSON
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |ct| ct.contains("application/json"));

    if !is_json {
        return response;
    }

    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        if let Ok(pretty) = serde_json::to_string_pretty(&value) {
            let mut response = Response::from_parts(parts, Body::from(pretty));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json; charset=utf-8"),
            );
            return response;
        }
    }

    // Fallback: return original bytes if parsing failed
    Response::from_parts(parts, Body::from(bytes))
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(handle_root))
        .route("/_cluster/health", get(cluster::get_health))
        .route("/_cluster/state", get(cluster::get_state))
        .route("/_cluster/transfer_master", post(cluster::transfer_master))
        // _cat APIs
        .route("/_cat/nodes", get(cat::cat_nodes))
        .route("/_cat/shards", get(cat::cat_shards))
        .route("/_cat/indices", get(cat::cat_indices))
        .route("/_cat/master", get(cat::cat_master))
        // Index management
        .route("/{index}", head(index::index_exists))
        .route("/{index}", put(index::create_index))
        .route("/{index}", delete(index::delete_index))
        // Document operations
        .route("/{index}/_doc", post(index::index_document))
        .route("/{index}/_doc/{id}", put(index::index_document_with_id))
        .route("/{index}/_doc/{id}", get(index::get_document))
        .route("/{index}/_doc/{id}", delete(index::delete_document))
        .route("/{index}/_update/{id}", post(index::update_document))
        .route("/_bulk", post(index::bulk_index_global))
        .route("/{index}/_bulk", post(index::bulk_index))
        // Search
        .route("/{index}/_search", get(search::search_documents))
        .route("/{index}/_search", post(index::search_documents_dsl))
        // Maintenance
        .route("/{index}/_refresh", post(index::refresh_index))
        .route("/{index}/_refresh", get(index::refresh_index))
        .route("/{index}/_flush", post(index::flush_index))
        .route("/{index}/_flush", get(index::flush_index))
        .layer(middleware::from_fn(pretty_json_middleware))
        .with_state(state)
}
