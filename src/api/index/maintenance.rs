//! Cluster-wide refresh and flush fan-out.

use super::*;
use crate::transport::server::MaintenanceDispatchOp;
use crate::transport::server::enqueue_force_merge_task_on_assigned_shards;
use crate::transport::server::run_maintenance_on_assigned_shards_async;

pub(super) struct ForceMergeDispatchResult {
    pub total_nodes: u32,
    pub node_tasks: HashMap<String, String>,
    pub dispatch_failures: HashMap<String, String>,
}

pub async fn refresh_index(
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

    let (successful, failed) =
        fan_out_maintenance(&state, &index_name, MaintenanceDispatchOp::Flush).await;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
        })),
    )
}

/// POST /{index}/_forcemerge — Force-merge all shards across the cluster for this index.
/// Accepts `?max_num_segments=N` (default 1) and enqueues the work asynchronously.
pub async fn force_merge_index(
    State(state): State<AppState>,
    Path(index_name): Path<crate::common::IndexName>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let cluster_state = state.cluster_manager.get_state();
    if !cluster_state.indices.contains_key(index_name.as_str()) {
        return crate::api::error_response(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{}]", index_name),
        );
    }

    let max_num_segments: usize = params
        .get("max_num_segments")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);

    let dispatch = enqueue_force_merge_tasks(&state, &index_name, max_num_segments).await;
    let task_id = state.task_manager.create_cluster_force_merge(
        index_name.as_str(),
        max_num_segments,
        dispatch.node_tasks.clone(),
        dispatch.dispatch_failures.clone(),
    );
    let started = dispatch.node_tasks.len() as u32;
    let failed = dispatch.dispatch_failures.len() as u32;
    let task_status = if failed > 0 {
        crate::tasks::TaskStatus::Failed
    } else {
        crate::tasks::TaskStatus::Queued
    };

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "acknowledged": true,
            "index": index_name.as_str(),
            "max_num_segments": max_num_segments,
            "task": {
                "id": task_id,
                "action": "indices:admin/forcemerge",
                "status": task_status.as_str(),
                "coordinator_node": state.local_node_id,
            },
            "_nodes": { "total": dispatch.total_nodes, "started": started, "failed": failed }
        })),
    )
}

/// Fan out a maintenance operation to all nodes in the cluster.
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

pub(super) async fn enqueue_force_merge_tasks(
    state: &AppState,
    index_name: &str,
    max_num_segments: usize,
) -> ForceMergeDispatchResult {
    let cs = state.cluster_manager.get_state();
    let mut total_nodes = 0u32;
    let mut dispatched_local = false;
    let mut node_tasks = HashMap::new();
    let mut dispatch_failures = HashMap::new();
    let mut remote_jobs = Vec::new();

    let enqueue_local = |idx: String| {
        enqueue_force_merge_task_on_assigned_shards(
            state.cluster_manager.clone(),
            state.shard_manager.clone(),
            state.task_manager.clone(),
            state.worker_pools.clone(),
            state.local_node_id.clone(),
            idx,
            max_num_segments,
        )
    };

    for node in cs.nodes.values() {
        total_nodes += 1;
        if node.id == state.local_node_id {
            dispatched_local = true;
            let task_id = enqueue_local(index_name.to_string());
            node_tasks.insert(node.id.clone(), task_id);
            continue;
        }

        let client = state.transport_client.clone();
        let node = node.clone();
        let idx = index_name.to_string();
        remote_jobs.push(async move {
            let result = client
                .forward_force_merge(&node, &idx, max_num_segments as u32)
                .await;
            (node.id.clone(), idx, result)
        });
    }

    if !dispatched_local {
        total_nodes += 1;
        let task_id = enqueue_local(index_name.to_string());
        node_tasks.insert(state.local_node_id.clone(), task_id);
    }

    for (node_id, idx, result) in join_all(remote_jobs).await {
        match result {
            Ok(task_id) => {
                tracing::info!(
                    "Enqueued maintenance forcemerge on node {} for {} as task {}",
                    node_id,
                    idx,
                    task_id
                );
                node_tasks.insert(node_id, task_id);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to enqueue maintenance forcemerge on node {} for {}: {}",
                    node_id,
                    idx,
                    e
                );
                dispatch_failures.insert(node_id, e.to_string());
            }
        }
    }

    ForceMergeDispatchResult {
        total_nodes,
        node_tasks,
        dispatch_failures,
    }
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
        jobs.push(spawn_maintenance_job(async move {
            match op {
                MaintenanceDispatchOp::Flush => client.forward_flush(&node, &idx).await,
                MaintenanceDispatchOp::ForceMerge(max_segments) => client
                    .forward_force_merge(&node, &idx, max_segments as u32)
                    .await
                    .map(|_| (0, 0)),
                MaintenanceDispatchOp::Refresh => client.forward_refresh(&node, &idx).await,
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
