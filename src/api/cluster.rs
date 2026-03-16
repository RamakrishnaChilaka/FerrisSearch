use crate::api::AppState;
use crate::cluster::state::ClusterState;
use axum::{extract::State, http::StatusCode, Json};
use openraft::type_config::async_runtime::WatchReceiver;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct ClusterHealth {
    pub cluster_name: String,
    pub status: String,
    pub timed_out: bool,
    pub number_of_nodes: usize,
    pub number_of_data_nodes: usize,
}

/// Compute cluster health status based on shard allocation.
/// - "green": all shards assigned to known data nodes
/// - "yellow": some shards unassigned (no replicas yet counts as yellow)
/// - "red": no data nodes or no indices at all with data nodes missing
fn compute_health_status(cs: &ClusterState) -> &'static str {
    let data_node_ids: std::collections::HashSet<&String> = cs.nodes.values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| &n.id)
        .collect();

    if data_node_ids.is_empty() {
        return "red";
    }

    if cs.indices.is_empty() {
        return "green";
    }

    let mut all_assigned = true;
    for index_meta in cs.indices.values() {
        for routing in index_meta.shard_routing.values() {
            if !data_node_ids.contains(&routing.primary) {
                all_assigned = false;
            }
            for replica in &routing.replicas {
                if !data_node_ids.contains(replica) {
                    all_assigned = false;
                }
            }
        }
    }

    if all_assigned { "green" } else { "yellow" }
}

/// Handler for `GET /_cluster/health`
pub async fn get_health(State(state): State<AppState>) -> Json<ClusterHealth> {
    let cs = state.cluster_manager.get_state();
    let data_nodes = cs.nodes.values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .count();
    let status = compute_health_status(&cs).to_string();
    Json(ClusterHealth {
        cluster_name: cs.cluster_name,
        status,
        timed_out: false,
        number_of_nodes: cs.nodes.len(),
        number_of_data_nodes: data_nodes,
    })
}

/// Handler for `GET /_cluster/state`
pub async fn get_state(State(state): State<AppState>) -> Json<ClusterState> {
    Json(state.cluster_manager.get_state())
}

// ─── Transfer Master (ROpenSearch-only) ─────────────────────────────────────

#[derive(Deserialize)]
pub struct TransferMasterRequest {
    /// The node name (e.g. "node-2") to transfer leadership to.
    pub node_id: String,
}

/// Handler for `POST /_cluster/transfer_master`
///
/// Gracefully transfers Raft leadership to the specified node.
/// This is a ROpenSearch-specific API — not present in OpenSearch.
pub async fn transfer_master(
    State(state): State<AppState>,
    Json(req): Json<TransferMasterRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let raft = match state.raft {
        Some(ref r) => r,
        None => return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": "Raft consensus is not enabled on this node"
        }))),
    };

    if !raft.is_leader() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
            "error": { "type": "master_not_discovered_exception", "reason": "This node is not the current master. Send transfer requests to the master node." }
        })));
    }

    // Look up the target node's raft_node_id
    let cs = state.cluster_manager.get_state();
    let target_info = match cs.nodes.get(&req.node_id) {
        Some(n) => n.clone(),
        None => return (StatusCode::NOT_FOUND, Json(serde_json::json!({
            "error": format!("Node '{}' not found in cluster state", req.node_id)
        }))),
    };

    if target_info.raft_node_id == 0 {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "error": format!("Node '{}' has no Raft ID assigned", req.node_id)
        })));
    }

    // Get current vote from metrics
    let vote = {
        let m = raft.metrics();
        m.borrow_watched().vote.clone()
    };

    let last_log_id = {
        let m = raft.metrics();
        m.borrow_watched().last_applied
    };

    let transfer_req = openraft::raft::TransferLeaderRequest::new(
        vote,
        target_info.raft_node_id,
        last_log_id,
    );

    if let Err(e) = raft.handle_transfer_leader(transfer_req).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "error": format!("Transfer leader failed: {}", e)
        })));
    }

    (StatusCode::OK, Json(serde_json::json!({
        "acknowledged": true,
        "message": format!("Leadership transfer initiated to node '{}'", req.node_id)
    })))
}
