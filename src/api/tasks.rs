use crate::api::{AppState, error_response};
use crate::common::date::epoch_millis_to_iso8601;
use crate::tasks::{ClusterForceMergeTaskSnapshot, LocalForceMergeTaskSnapshot, TaskStatus};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use futures::future::join_all;
use serde_json::{Value, json};

struct NodeTaskView {
    node_id: String,
    task_id: Option<String>,
    status: TaskStatus,
    successful_shards: u32,
    failed_shards: u32,
    error: Option<String>,
    created_at_epoch_ms: u64,
    started_at_epoch_ms: Option<u64>,
    completed_at_epoch_ms: Option<u64>,
}

fn iso8601_value(epoch_ms: Option<u64>) -> Value {
    epoch_ms
        .map(|value| Value::String(epoch_millis_to_iso8601(value as i64)))
        .unwrap_or(Value::Null)
}

fn node_task_json(task: &NodeTaskView) -> Value {
    json!({
        "node_id": &task.node_id,
        "task_id": &task.task_id,
        "status": task.status.as_str(),
        "successful_shards": task.successful_shards,
        "failed_shards": task.failed_shards,
        "error": &task.error,
        "created_at": iso8601_value(Some(task.created_at_epoch_ms)),
        "started_at": iso8601_value(task.started_at_epoch_ms),
        "completed_at": iso8601_value(task.completed_at_epoch_ms),
    })
}

fn local_task_response(task: &LocalForceMergeTaskSnapshot) -> Value {
    json!({
        "task": {
            "id": &task.task_id,
            "action": &task.action,
            "status": task.status.as_str(),
            "coordinator_node": &task.node_id,
            "node_id": &task.node_id,
            "index": &task.index_name,
            "max_num_segments": task.max_num_segments,
            "successful_shards": task.successful_shards,
            "failed_shards": task.failed_shards,
            "error": &task.error,
            "created_at": iso8601_value(Some(task.created_at_epoch_ms)),
            "started_at": iso8601_value(task.started_at_epoch_ms),
            "completed_at": iso8601_value(task.completed_at_epoch_ms),
        }
    })
}

fn cluster_status(children: &[NodeTaskView]) -> TaskStatus {
    if children
        .iter()
        .any(|child| child.status == TaskStatus::Failed)
    {
        return TaskStatus::Failed;
    }
    if children
        .iter()
        .all(|child| child.status == TaskStatus::Completed)
    {
        return TaskStatus::Completed;
    }
    if children
        .iter()
        .any(|child| child.status == TaskStatus::Unknown)
    {
        return TaskStatus::Unknown;
    }
    if children
        .iter()
        .all(|child| child.status == TaskStatus::Queued)
    {
        return TaskStatus::Queued;
    }
    if children
        .iter()
        .any(|child| matches!(child.status, TaskStatus::Running | TaskStatus::Queued))
    {
        return TaskStatus::Running;
    }
    TaskStatus::Unknown
}

async fn resolve_cluster_children(
    state: &AppState,
    task: &ClusterForceMergeTaskSnapshot,
) -> Vec<NodeTaskView> {
    let cluster_state = state.cluster_manager.get_state();
    let mut children = Vec::new();
    let mut remote_jobs = Vec::new();

    for (node_id, error) in &task.dispatch_failures {
        children.push(NodeTaskView {
            node_id: node_id.clone(),
            task_id: None,
            status: TaskStatus::Failed,
            successful_shards: 0,
            failed_shards: 0,
            error: Some(error.clone()),
            created_at_epoch_ms: task.created_at_epoch_ms,
            started_at_epoch_ms: None,
            completed_at_epoch_ms: Some(task.created_at_epoch_ms),
        });
    }

    for (node_id, node_task_id) in &task.node_tasks {
        if node_id == &state.local_node_id {
            match state.task_manager.get_local_force_merge(node_task_id) {
                Some(local_task) => children.push(NodeTaskView {
                    node_id: node_id.clone(),
                    task_id: Some(node_task_id.clone()),
                    status: local_task.status,
                    successful_shards: local_task.successful_shards,
                    failed_shards: local_task.failed_shards,
                    error: local_task.error,
                    created_at_epoch_ms: local_task.created_at_epoch_ms,
                    started_at_epoch_ms: local_task.started_at_epoch_ms,
                    completed_at_epoch_ms: local_task.completed_at_epoch_ms,
                }),
                None => children.push(NodeTaskView {
                    node_id: node_id.clone(),
                    task_id: Some(node_task_id.clone()),
                    status: TaskStatus::Unknown,
                    successful_shards: 0,
                    failed_shards: 0,
                    error: Some("task not found locally".to_string()),
                    created_at_epoch_ms: task.created_at_epoch_ms,
                    started_at_epoch_ms: None,
                    completed_at_epoch_ms: None,
                }),
            }
            continue;
        }

        let Some(node) = cluster_state.nodes.get(node_id).cloned() else {
            children.push(NodeTaskView {
                node_id: node_id.clone(),
                task_id: Some(node_task_id.clone()),
                status: TaskStatus::Unknown,
                successful_shards: 0,
                failed_shards: 0,
                error: Some("node not present in cluster state".to_string()),
                created_at_epoch_ms: task.created_at_epoch_ms,
                started_at_epoch_ms: None,
                completed_at_epoch_ms: None,
            });
            continue;
        };

        let client = state.transport_client.clone();
        let node_id = node_id.clone();
        let node_task_id = node_task_id.clone();
        remote_jobs.push(async move {
            let response = client.get_task_status(&node, &node_task_id).await;
            (node_id, node_task_id, response)
        });
    }

    for (node_id, node_task_id, response) in join_all(remote_jobs).await {
        match response {
            Ok(Some(remote_task)) => children.push(NodeTaskView {
                node_id,
                task_id: Some(node_task_id),
                status: remote_task.status,
                successful_shards: remote_task.successful_shards,
                failed_shards: remote_task.failed_shards,
                error: remote_task.error,
                created_at_epoch_ms: remote_task.created_at_epoch_ms,
                started_at_epoch_ms: remote_task.started_at_epoch_ms,
                completed_at_epoch_ms: remote_task.completed_at_epoch_ms,
            }),
            Ok(None) => children.push(NodeTaskView {
                node_id,
                task_id: Some(node_task_id),
                status: TaskStatus::Unknown,
                successful_shards: 0,
                failed_shards: 0,
                error: Some("task not found on node".to_string()),
                created_at_epoch_ms: task.created_at_epoch_ms,
                started_at_epoch_ms: None,
                completed_at_epoch_ms: None,
            }),
            Err(error) => children.push(NodeTaskView {
                node_id,
                task_id: Some(node_task_id),
                status: TaskStatus::Unknown,
                successful_shards: 0,
                failed_shards: 0,
                error: Some(error.to_string()),
                created_at_epoch_ms: task.created_at_epoch_ms,
                started_at_epoch_ms: None,
                completed_at_epoch_ms: None,
            }),
        }
    }

    children.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    children
}

pub async fn get_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> (StatusCode, Json<Value>) {
    if let Some(task) = state.task_manager.get_local_force_merge(&task_id) {
        return (StatusCode::OK, Json(local_task_response(&task)));
    }

    let Some(cluster_task) = state.task_manager.get_cluster_force_merge(&task_id) else {
        return error_response(
            StatusCode::NOT_FOUND,
            "resource_not_found_exception",
            format!("task [{}] not found", task_id),
        );
    };

    let children = resolve_cluster_children(&state, &cluster_task).await;
    let status = cluster_status(&children);
    let created_at = Some(cluster_task.created_at_epoch_ms);
    let started_at = children
        .iter()
        .filter_map(|child| child.started_at_epoch_ms)
        .min();
    let completed_at = if matches!(status, TaskStatus::Completed | TaskStatus::Failed) {
        children
            .iter()
            .filter_map(|child| child.completed_at_epoch_ms)
            .max()
    } else {
        None
    };
    let error = if status == TaskStatus::Failed {
        Some("one or more node-level force-merge tasks failed".to_string())
    } else {
        None
    };

    let queued = children
        .iter()
        .filter(|child| child.status == TaskStatus::Queued)
        .count() as u32;
    let running = children
        .iter()
        .filter(|child| child.status == TaskStatus::Running)
        .count() as u32;
    let completed = children
        .iter()
        .filter(|child| child.status == TaskStatus::Completed)
        .count() as u32;
    let failed = children
        .iter()
        .filter(|child| child.status == TaskStatus::Failed)
        .count() as u32;
    let unknown = children
        .iter()
        .filter(|child| child.status == TaskStatus::Unknown)
        .count() as u32;

    (
        StatusCode::OK,
        Json(json!({
            "task": {
                "id": &cluster_task.task_id,
                "action": &cluster_task.action,
                "status": status.as_str(),
                "coordinator_node": state.local_node_id,
                "index": &cluster_task.index_name,
                "max_num_segments": cluster_task.max_num_segments,
                "error": error,
                "created_at": iso8601_value(created_at),
                "started_at": iso8601_value(started_at),
                "completed_at": iso8601_value(completed_at),
            },
            "_nodes": {
                "total": children.len() as u32,
                "queued": queued,
                "running": running,
                "completed": completed,
                "failed": failed,
                "unknown": unknown,
            },
            "nodes": children.iter().map(node_task_json).collect::<Vec<_>>(),
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::AppState;
    use crate::consensus::bootstrap_single_node;
    use crate::tasks::TaskManager;
    use std::collections::HashMap;
    use std::time::Duration;

    struct TestState {
        app_state: AppState,
        _temp_dir: tempfile::TempDir,
    }

    async fn make_test_state() -> TestState {
        let cluster_state = crate::cluster::state::ClusterState::new("task-cluster".into());
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, "task-cluster".into())
                .await
                .unwrap();
        bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
            .await
            .unwrap();
        let manager = crate::cluster::ClusterManager::with_shared_state(shared_state);
        manager.update_state(cluster_state);
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();

        TestState {
            app_state: AppState {
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
            _temp_dir: temp_dir,
        }
    }

    #[tokio::test]
    async fn get_task_returns_local_force_merge_task() {
        let TestState {
            app_state: state,
            _temp_dir,
        } = make_test_state().await;
        let task_id = state
            .task_manager
            .create_local_force_merge("node-1", "idx", 1);
        state.task_manager.mark_running(&task_id);
        state
            .task_manager
            .finish_local_force_merge(&task_id, 2, 0, None);

        let (status, Json(body)) = get_task(State(state), Path(task_id.clone())).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["task"]["id"], task_id);
        assert_eq!(body["task"]["status"], "completed");
        assert_eq!(body["task"]["coordinator_node"], "node-1");
        assert_eq!(body["task"]["successful_shards"], 2);
    }

    #[tokio::test]
    async fn get_task_returns_cluster_force_merge_status() {
        let TestState {
            app_state: state,
            _temp_dir,
        } = make_test_state().await;
        let local_task_id = state
            .task_manager
            .create_local_force_merge("node-1", "idx", 1);
        state.task_manager.mark_running(&local_task_id);
        state
            .task_manager
            .finish_local_force_merge(&local_task_id, 1, 0, None);
        let cluster_task_id = state.task_manager.create_cluster_force_merge(
            "idx",
            1,
            HashMap::from([("node-1".to_string(), local_task_id)]),
            HashMap::new(),
        );

        let (status, Json(body)) = get_task(State(state), Path(cluster_task_id.clone())).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["task"]["id"], cluster_task_id);
        assert_eq!(body["task"]["status"], "completed");
        assert_eq!(body["task"]["coordinator_node"], "node-1");
        assert_eq!(body["_nodes"]["total"], 1);
        assert_eq!(body["_nodes"]["completed"], 1);
        assert_eq!(body["nodes"][0]["status"], "completed");
    }

    #[tokio::test]
    async fn get_task_returns_not_found_for_unknown_id() {
        let TestState {
            app_state: state,
            _temp_dir,
        } = make_test_state().await;

        let (status, Json(body)) = get_task(State(state), Path("missing".to_string())).await;

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "resource_not_found_exception");
    }
}
