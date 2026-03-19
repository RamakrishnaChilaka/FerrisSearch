use crate::api::AppState;
use crate::cluster::state::ClusterState;
use axum::{Json, extract::State, http::StatusCode};
use openraft::type_config::async_runtime::WatchReceiver;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct ClusterHealth {
    pub cluster_name: String,
    pub status: String,
    pub timed_out: bool,
    pub number_of_nodes: usize,
    pub number_of_data_nodes: usize,
    pub unassigned_shards: u32,
}

/// Compute cluster health status based on shard allocation.
/// - "green": all primary and replica shards are assigned
/// - "yellow": all primaries assigned, but some replicas are unassigned
/// - "red": no data nodes, or a primary shard is assigned to a missing node
fn compute_health_status(cs: &ClusterState) -> (&'static str, u32) {
    let data_node_ids: std::collections::HashSet<&String> = cs
        .nodes
        .values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| &n.id)
        .collect();

    if data_node_ids.is_empty() {
        return ("red", 0);
    }

    if cs.indices.is_empty() {
        return ("green", 0);
    }

    let mut total_unassigned = 0u32;
    let mut primary_missing = false;

    for index_meta in cs.indices.values() {
        // Count explicitly tracked unassigned replicas
        total_unassigned += index_meta.unassigned_replica_count();

        for routing in index_meta.shard_routing.values() {
            // Primary assigned to a node that no longer exists → red
            if !data_node_ids.contains(&routing.primary) {
                primary_missing = true;
            }
            // Replica assigned to a node that no longer exists → unassigned
            for replica in &routing.replicas {
                if !data_node_ids.contains(replica) {
                    total_unassigned += 1;
                }
            }
        }
    }

    if primary_missing {
        ("red", total_unassigned)
    } else if total_unassigned > 0 {
        ("yellow", total_unassigned)
    } else {
        ("green", total_unassigned)
    }
}

/// Handler for `GET /_cluster/health`
pub async fn get_health(State(state): State<AppState>) -> Json<ClusterHealth> {
    let cs = state.cluster_manager.get_state();
    let data_nodes = cs
        .nodes
        .values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .count();
    let (status, unassigned) = compute_health_status(&cs);
    Json(ClusterHealth {
        cluster_name: cs.cluster_name,
        status: status.to_string(),
        timed_out: false,
        number_of_nodes: cs.nodes.len(),
        number_of_data_nodes: data_nodes,
        unassigned_shards: unassigned,
    })
}

/// Handler for `GET /_cluster/state`
pub async fn get_state(State(state): State<AppState>) -> Json<ClusterState> {
    Json(state.cluster_manager.get_state())
}

// ─── Transfer Master (FerrisSearch-only) ─────────────────────────────────────

#[derive(Deserialize)]
pub struct TransferMasterRequest {
    /// The node name (e.g. "node-2") to transfer leadership to.
    pub node_id: String,
}

/// Handler for `POST /_cluster/transfer_master`
///
/// Gracefully transfers Raft leadership to the specified node.
/// This is a FerrisSearch-specific API — not present in OpenSearch.
pub async fn transfer_master(
    State(state): State<AppState>,
    Json(req): Json<TransferMasterRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let raft = match state.raft {
        Some(ref r) => r,
        None => {
            return crate::api::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "raft_not_enabled_exception",
                "Raft consensus is not enabled on this node",
            );
        }
    };

    if !raft.is_leader() {
        // Forward to the master via gRPC — this node acts as coordinator
        let cs = state.cluster_manager.get_state();
        let master_id = match cs.master_node.as_ref() {
            Some(id) => id,
            None => {
                return crate::api::error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "master_not_discovered_exception",
                    "No master node available to forward transfer request",
                );
            }
        };
        let master_node = match cs.nodes.get(master_id) {
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
            .forward_transfer_master(&master_node, &req.node_id)
            .await
        {
            Ok(()) => {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "acknowledged": true,
                        "message": format!("Leadership transfer initiated to node '{}'", req.node_id)
                    })),
                );
            }
            Err(e) => {
                return crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "forward_exception",
                    format!("Failed to forward transfer request to master: {}", e),
                );
            }
        }
    }

    // Look up the target node's raft_node_id
    let cs = state.cluster_manager.get_state();
    let target_info = match cs.nodes.get(&req.node_id) {
        Some(n) => n.clone(),
        None => {
            return crate::api::error_response(
                StatusCode::NOT_FOUND,
                "node_not_found_exception",
                format!("Node '{}' not found in cluster state", req.node_id),
            );
        }
    };

    if target_info.raft_node_id == 0 {
        return crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!("Node '{}' has no Raft ID assigned", req.node_id),
        );
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

    let transfer_req =
        openraft::raft::TransferLeaderRequest::new(vote, target_info.raft_node_id, last_log_id);

    if let Err(e) = raft.handle_transfer_leader(transfer_req).await {
        return crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "raft_transfer_exception",
            format!("Transfer leader failed: {}", e),
        );
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "acknowledged": true,
            "message": format!("Leadership transfer initiated to node '{}'", req.node_id)
        })),
    )
}
