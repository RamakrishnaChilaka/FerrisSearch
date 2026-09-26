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
/// - "green": all primaries exist and all desired replicas are assigned and in sync
/// - "yellow": all primaries exist, but some replicas are unassigned or out of sync
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
            // Missing or out-of-sync replicas are unavailable copies.
            for replica in &routing.replicas {
                if !data_node_ids.contains(replica) || !routing.is_replica_in_sync(replica) {
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
    // Coordinator: forward to leader if not master
    if let Some(master) = match crate::api::resolve_leader_or_master(&state, "transfer request") {
        Ok(m) => m,
        Err(e) => return e,
    } {
        match state
            .transport_client
            .forward_transfer_master(&master, &req.node_id)
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
                    format!("Failed to forward transfer request to master: {e}"),
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
        let m = state.raft.metrics();
        m.borrow_watched().vote
    };

    let last_log_id = {
        let m = state.raft.metrics();
        m.borrow_watched().last_applied
    };

    let transfer_req =
        openraft::raft::TransferLeaderRequest::new(vote, target_info.raft_node_id, last_log_id);

    if let Err(e) = state.raft.handle_transfer_leader(transfer_req).await {
        return crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "raft_transfer_exception",
            format!("Transfer leader failed: {e}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        IndexMetadata, IndexSettings, IndexUuid, NodeInfo, NodeRole, ShardRoutingEntry,
    };
    use std::collections::HashMap;

    fn health_state(routing: ShardRoutingEntry) -> ClusterState {
        let mut state = ClusterState::new("health".into());
        for node_id in ["node-1", "node-2"] {
            state.add_node(NodeInfo {
                id: node_id.into(),
                name: node_id.into(),
                host: "127.0.0.1".into(),
                transport_port: 9300,
                http_port: 9200,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            });
        }
        state.add_index(IndexMetadata {
            name: "idx".into(),
            uuid: IndexUuid::new("health-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(0, routing)]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        state
    }

    fn in_sync_routing() -> ShardRoutingEntry {
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec!["node-2".into()],
            in_sync_replicas: vec!["node-2".into()],
            unassigned_replicas: 0,
        }
    }

    #[test]
    fn health_is_green_when_all_assigned_replicas_are_in_sync() {
        assert_eq!(
            compute_health_status(&health_state(in_sync_routing())),
            ("green", 0)
        );
    }

    #[test]
    fn health_is_yellow_and_counts_assigned_out_of_sync_replica() {
        let mut routing = in_sync_routing();
        routing.in_sync_replicas.clear();
        assert_eq!(compute_health_status(&health_state(routing)), ("yellow", 1));
    }

    #[test]
    fn health_is_yellow_and_counts_unassigned_replica_slot() {
        let mut routing = in_sync_routing();
        routing.unassigned_replicas = 1;
        assert_eq!(compute_health_status(&health_state(routing)), ("yellow", 1));
    }

    #[test]
    fn health_is_red_when_primary_node_is_missing() {
        let mut routing = in_sync_routing();
        routing.primary = "missing-node".into();
        assert_eq!(compute_health_status(&health_state(routing)), ("red", 0));
    }
}
