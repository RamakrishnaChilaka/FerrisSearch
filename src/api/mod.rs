//! HTTP REST API Layer.
//! Long-term goal: Implement OpenSearch-compatible REST APIs for searching, indexing, and cluster management.

pub mod cat;
pub mod cluster;
pub mod index;
pub mod search;
pub mod tasks;

use crate::cluster::ClusterManager;
use crate::cluster::state::NodeId;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::TransportClient;
use crate::worker::WorkerPools;
use axum::{
    Json, Router,
    body::Body,
    extract::DefaultBodyLimit,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, head, post, put},
};
use serde::Serialize;
use std::sync::Arc;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize)]
struct NodeInfoResponse {
    name: String,
    version: String,
    engine: String,
}

async fn handle_root() -> Json<NodeInfoResponse> {
    Json(NodeInfoResponse {
        name: "ferrissearch-node".into(),
        version: VERSION.into(),
        engine: "tantivy".into(),
    })
}

#[derive(Clone)]
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    /// The local node ID — needed for routing decisions
    pub local_node_id: NodeId,
    /// Raft consensus instance — always present, cluster state is Raft-managed
    pub raft: Arc<RaftInstance>,
    /// Dedicated thread pools for search and write workloads
    pub worker_pools: WorkerPools,
    /// Tracks background maintenance and other asynchronous node-local tasks.
    pub task_manager: Arc<crate::tasks::TaskManager>,
    /// Max docs to scan for GROUP BY queries on the fast-fields fallback path.
    /// 0 = unlimited.
    pub sql_group_by_scan_limit: usize,
    /// Enable approximate shard-level top-K pruning for grouped-partials GROUP BY.
    /// When true, each shard keeps only (offset + limit) * 3 + 10 buckets before
    /// shipping to the coordinator.  Reduces latency on high-cardinality GROUP BY
    /// with LIMIT, but results are approximate (not standard SQL semantics).
    /// Default: true. Set to false to force exact coordinator-side merge.
    pub sql_approximate_top_k: bool,
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

/// If this node is the Raft leader, returns `None`.
/// If not, resolves the master node for forwarding and returns `Some(master)`.
/// Returns an error response if no master is available.
pub(crate) fn resolve_leader_or_master(
    state: &AppState,
    operation: &str,
) -> Result<Option<crate::cluster::state::NodeInfo>, (StatusCode, Json<serde_json::Value>)> {
    if state.raft.is_leader() {
        return Ok(None);
    }
    let cs = state.cluster_manager.get_state();
    let master_id = cs.master_node.as_ref().ok_or_else(|| {
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "master_not_discovered_exception",
            format!("No master node available to forward {}", operation),
        )
    })?;
    let master_node = cs.nodes.get(master_id).cloned().ok_or_else(|| {
        error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "master_not_discovered_exception",
            "Master node info not found in cluster state",
        )
    })?;
    Ok(Some(master_node))
}

/// Write a command through Raft and return a standard error response on failure.
pub(crate) async fn raft_write(
    state: &AppState,
    cmd: crate::consensus::types::ClusterCommand,
) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    state.raft.client_write(cmd).await.map_err(|e| {
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "raft_write_exception",
            format!("Raft write failed: {}", e),
        )
    })?;
    Ok(())
}

/// Middleware that pretty-prints JSON responses when `?pretty` is in the query string.
async fn pretty_json_middleware(req: Request<Body>, next: Next) -> Response {
    let wants_pretty = req.uri().query().is_some_and(|q| {
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
        .is_some_and(|ct| ct.contains("application/json"));

    if !is_json {
        return response;
    }

    let (parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Ok(pretty) = serde_json::to_string_pretty(&value)
    {
        let mut response = Response::from_parts(parts, Body::from(pretty));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json; charset=utf-8"),
        );
        return response;
    }

    // Fallback: return original bytes if parsing failed
    Response::from_parts(parts, Body::from(bytes))
}

/// Middleware that records HTTP request count and latency as Prometheus metrics.
async fn metrics_middleware(req: Request<Body>, next: Next) -> Response {
    let method = req.method().to_string();
    let path = normalize_metrics_path(req.uri().path());
    let start = std::time::Instant::now();

    let response = next.run(req).await;

    let status = response.status().as_u16().to_string();
    let elapsed = start.elapsed().as_secs_f64();

    crate::metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&[&method, &path, &status])
        .inc();
    crate::metrics::HTTP_REQUEST_DURATION_SECONDS
        .with_label_values(&[&method, &path])
        .observe(elapsed);

    response
}

/// Normalize request paths for metric labels to avoid high cardinality.
/// Replaces index names and doc IDs with placeholders.
fn normalize_metrics_path(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    match segments.as_slice() {
        // Root and system endpoints — keep as-is
        ["", ""] => "/".to_string(),
        ["", "_tasks", _] => "/_tasks/{task_id}".to_string(),
        ["", p] if p.starts_with('_') => format!("/{p}"),
        ["", p, s] if p.starts_with('_') => format!("/{p}/{s}"),
        // /{index}/_doc/{id}
        ["", _, "_doc", _] => "/{index}/_doc/{id}".to_string(),
        // /{index}/_update/{id}
        ["", _, "_update", _] => "/{index}/_update/{id}".to_string(),
        // /{index}/{action}/{sub} — _sql/explain
        ["", _, action, sub] if action.starts_with('_') => {
            format!("/{{index}}/{action}/{sub}")
        }
        // /{index}/{action} — _search, _bulk, _sql, _refresh, etc.
        ["", _, action] if action.starts_with('_') => {
            format!("/{{index}}/{action}")
        }
        // /{index} — PUT/DELETE/HEAD
        ["", _] => "/{index}".to_string(),
        _ => "/{unknown}".to_string(),
    }
}

/// Prometheus metrics endpoint. Returns metrics in text exposition format.
async fn handle_metrics(State(state): State<AppState>) -> Response {
    let metrics_state = state.clone();
    let body = match tokio::task::spawn_blocking(move || {
        crate::metrics::update_cluster_metrics(&metrics_state);
        crate::metrics::gather()
    })
    .await
    {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("Metrics rendering task failed: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(Body::from(body))
        .unwrap()
}

pub fn create_router(state: AppState) -> Router {
    let bulk_router = Router::new()
        .route("/_bulk", post(index::bulk_index_global))
        .route("/{index}/_bulk", post(index::bulk_index))
        // Bulk ingestion intentionally accepts large NDJSON bodies.
        // The default Axum buffered-body limit rejects benchmark-sized requests
        // before they ever reach the handler.
        .layer(DefaultBodyLimit::disable());

    Router::new()
        .route("/", get(handle_root))
        .route("/_cluster/health", get(cluster::get_health))
        .route("/_cluster/state", get(cluster::get_state))
        .route("/_cluster/transfer_master", post(cluster::transfer_master))
        // Metrics
        .route("/_metrics", get(handle_metrics))
        // _cat APIs
        .route("/_cat/nodes", get(cat::cat_nodes))
        .route("/_cat/shards", get(cat::cat_shards))
        .route("/_cat/indices", get(cat::cat_indices))
        .route("/_cat/master", get(cat::cat_master))
        .route("/_cat/segments", get(cat::cat_segments))
        .route("/_tasks/{task_id}", get(tasks::get_task))
        // Index management
        .route("/{index}", head(index::index_exists))
        .route("/{index}", put(index::create_index))
        .route("/{index}", delete(index::delete_index))
        .route("/{index}/_settings", get(index::get_index_settings))
        .route("/{index}/_settings", put(index::update_index_settings))
        // Document operations
        .route("/{index}/_doc", post(index::index_document))
        .route("/{index}/_doc/{id}", put(index::index_document_with_id))
        .route("/{index}/_doc/{id}", get(index::get_document))
        .route("/{index}/_doc/{id}", delete(index::delete_document))
        .route("/{index}/_update/{id}", post(index::update_document))
        .route("/_sql", post(search::global_sql))
        .route("/_sql/stream", post(search::global_sql_stream))
        // Search
        .route("/{index}/_search", get(search::search_documents))
        .route("/{index}/_search", post(index::search_documents_dsl))
        .route("/{index}/_count", get(search::count_documents))
        .route("/{index}/_count", post(search::count_documents))
        .route("/{index}/_sql", post(search::search_sql))
        .route("/{index}/_sql/stream", post(search::search_sql_stream))
        .route("/{index}/_sql/explain", post(search::explain_sql))
        // Maintenance
        .route("/{index}/_refresh", post(index::refresh_index))
        .route("/{index}/_refresh", get(index::refresh_index))
        .route("/{index}/_flush", post(index::flush_index))
        .route("/{index}/_flush", get(index::flush_index))
        .route("/{index}/_forcemerge", post(index::force_merge_index))
        .merge(bulk_router)
        .layer(middleware::from_fn(pretty_json_middleware))
        .layer(middleware::from_fn(metrics_middleware))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::bootstrap_single_node;
    use crate::tasks::TaskManager;
    use std::time::Duration;
    use tower::ServiceExt;

    #[tokio::test]
    async fn handle_root_uses_package_version() {
        let Json(response) = handle_root().await;

        assert_eq!(response.version, VERSION);
    }

    #[test]
    fn normalize_root() {
        assert_eq!(normalize_metrics_path("/"), "/");
    }

    #[test]
    fn normalize_system_endpoints() {
        assert_eq!(
            normalize_metrics_path("/_cluster/health"),
            "/_cluster/health"
        );
        assert_eq!(normalize_metrics_path("/_cat/nodes"), "/_cat/nodes");
        assert_eq!(normalize_metrics_path("/_metrics"), "/_metrics");
        assert_eq!(normalize_metrics_path("/_bulk"), "/_bulk");
        assert_eq!(normalize_metrics_path("/_sql"), "/_sql");
        assert_eq!(
            normalize_metrics_path("/_tasks/abc123"),
            "/_tasks/{task_id}"
        );
    }

    #[test]
    fn normalize_index_operations() {
        assert_eq!(normalize_metrics_path("/movies"), "/{index}");
        assert_eq!(normalize_metrics_path("/benchmark-1gb"), "/{index}");
    }

    #[test]
    fn normalize_doc_operations() {
        assert_eq!(
            normalize_metrics_path("/movies/_doc/abc123"),
            "/{index}/_doc/{id}"
        );
        assert_eq!(
            normalize_metrics_path("/movies/_update/abc123"),
            "/{index}/_update/{id}"
        );
    }

    #[test]
    fn normalize_search_operations() {
        assert_eq!(
            normalize_metrics_path("/movies/_search"),
            "/{index}/_search"
        );
        assert_eq!(normalize_metrics_path("/movies/_bulk"), "/{index}/_bulk");
        assert_eq!(normalize_metrics_path("/movies/_sql"), "/{index}/_sql");
        assert_eq!(
            normalize_metrics_path("/movies/_refresh"),
            "/{index}/_refresh"
        );
        assert_eq!(normalize_metrics_path("/movies/_count"), "/{index}/_count");
    }

    #[test]
    fn normalize_sql_explain() {
        assert_eq!(
            normalize_metrics_path("/movies/_sql/explain"),
            "/{index}/_sql/explain"
        );
    }

    #[test]
    fn normalize_unknown_paths() {
        assert_eq!(
            normalize_metrics_path("/totally-unknown/path"),
            "/{unknown}"
        );
    }

    async fn make_test_state() -> (tempfile::TempDir, AppState) {
        let cluster_state = crate::cluster::state::ClusterState::new("api-test-cluster".into());
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, "api-test-cluster".into())
                .await
                .unwrap();
        bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
            .await
            .unwrap();
        let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state);
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        (
            temp_dir,
            AppState {
                cluster_manager: std::sync::Arc::new(manager),
                shard_manager: std::sync::Arc::new(crate::shard::ShardManager::new(
                    &data_dir,
                    Duration::from_secs(60),
                )),
                transport_client: crate::transport::TransportClient::new(),
                local_node_id: "node-1".into(),
                raft,
                worker_pools: crate::worker::WorkerPools::new(2, 2),
                task_manager: std::sync::Arc::new(TaskManager::new()),
                sql_group_by_scan_limit: 1_000_000,
                sql_approximate_top_k: false,
            },
        )
    }

    #[tokio::test]
    async fn bulk_routes_accept_large_request_bodies() {
        let (_temp_dir, state) = make_test_state().await;
        let app = create_router(state);
        let oversized_invalid_body = vec![0xFF; 3 * 1024 * 1024];

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_bulk")
                    .header(header::CONTENT_TYPE, "application/x-ndjson")
                    .body(Body::from(oversized_invalid_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The route should reach the handler and fail on UTF-8 parsing, not on Axum's
        // default buffered-body limit.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
