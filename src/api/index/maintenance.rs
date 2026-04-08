//! Cluster-wide refresh and flush fan-out.

use super::*;
use crate::transport::server::MaintenanceDispatchOp;
use crate::transport::server::run_maintenance_on_assigned_shards_async;

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
    if !cluster_state.indices.contains_key(&index_name) {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    }

    let (successful, failed) =
        fan_out_maintenance(&state, &index_name, MaintenanceDispatchOp::Refresh).await;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
        })),
    )
}

/// POST|GET /{index}/_flush — Flush all shards across the cluster for this index.
/// Fans out to remote nodes via gRPC concurrently.
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
    if !cluster_state.indices.contains_key(&index_name) {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    }

    let (successful, failed) =
        fan_out_maintenance(&state, &index_name, MaintenanceDispatchOp::Flush).await;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
        })),
    )
}

/// Fan out a refresh or flush operation to all nodes in the cluster.
/// Local and remote nodes participate in the same per-node dispatch set;
/// the local node is dispatched via the same async maintenance helper rather
/// than being processed inline before remote nodes.
type MaintenanceJob = Pin<Box<dyn Future<Output = Result<(u32, u32), anyhow::Error>> + Send>>;

pub(super) fn spawn_maintenance_job<F>(job: F) -> MaintenanceJob
where
    F: Future<Output = Result<(u32, u32), anyhow::Error>> + Send + 'static,
{
    Box::pin(async move {
        tokio::spawn(job)
            .await
            .map_err(|e| anyhow::anyhow!("maintenance task panicked: {}", e))?
    })
}

pub(super) fn maintenance_fanout_concurrency(targets: usize) -> usize {
    targets.max(1)
}

pub(super) async fn fan_out_maintenance(
    state: &AppState,
    index_name: &str,
    op: MaintenanceDispatchOp,
) -> (u32, u32) {
    let cs = state.cluster_manager.get_state();
    let mut jobs: Vec<MaintenanceJob> = Vec::new();
    let mut dispatched_local = false;

    let spawn_local = |idx: String, dispatch_op: MaintenanceDispatchOp| {
        let cm = state.cluster_manager.clone();
        let sm = state.shard_manager.clone();
        let wp = state.worker_pools.clone();
        let nid = state.local_node_id.clone();
        spawn_maintenance_job(async move {
            Ok::<(u32, u32), anyhow::Error>(
                run_maintenance_on_assigned_shards_async(cm, sm, wp, nid, idx, dispatch_op).await,
            )
        })
    };

    for node in cs.nodes.values() {
        if node.id == state.local_node_id {
            dispatched_local = true;
            jobs.push(spawn_local(index_name.to_string(), op));
            continue;
        }

        let client = state.transport_client.clone();
        let node = node.clone();
        let idx = index_name.to_string();
        let is_flush = matches!(op, MaintenanceDispatchOp::Flush);
        jobs.push(spawn_maintenance_job(async move {
            if is_flush {
                client.forward_flush(&node, &idx).await
            } else {
                client.forward_refresh(&node, &idx).await
            }
        }));
    }

    // If this node isn't in the cluster's node list yet (e.g. still joining),
    // still dispatch locally so locally-assigned shards get maintained.
    if !dispatched_local {
        jobs.push(spawn_local(index_name.to_string(), op));
    }

    let mut successful = 0u32;
    let mut failed = 0u32;
    let concurrency = maintenance_fanout_concurrency(jobs.len());

    let mut results = stream::iter(jobs).buffer_unordered(concurrency);

    while let Some(result) = results.next().await {
        match result {
            Ok((s, f)) => {
                successful += s;
                failed += f;
            }
            Err(e) => {
                tracing::error!("Maintenance fan-out job failed: {}", e);
                failed += 1;
            }
        }
    }

    (successful, failed)
}
