use crate::api::AppState;
use crate::cluster::state::ClusterState;
use axum::{extract::State, Json};
use serde::Serialize;

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
        for (_shard_id, assigned_node) in &index_meta.shards {
            if !data_node_ids.contains(assigned_node) {
                all_assigned = false;
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
