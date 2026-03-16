use crate::cluster::state::{ClusterState, NodeInfo};
use std::sync::{Arc, RwLock};

/// Manages thread-safe access to the Cluster State
pub struct ClusterManager {
    state: Arc<RwLock<ClusterState>>,
}

impl ClusterManager {
    pub fn new(cluster_name: String) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new(cluster_name))),
        }
    }

    /// Create a ClusterManager backed by an externally-owned state (e.g. shared
    /// with the Raft state machine).
    pub fn with_shared_state(state: Arc<RwLock<ClusterState>>) -> Self {
        Self { state }
    }

    /// Returns a cloned snapshot of the current state
    pub fn get_state(&self) -> ClusterState {
        self.state.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Safely add a node to the cluster
    pub fn add_node(&self, node: NodeInfo) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.add_node(node);
    }

    /// Overwrite the local state entirely (used when receiving update from Master)
    pub fn update_state(&self, mut new_state: ClusterState) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        // Preserve last_seen since it's transient and not serialized over the network
        new_state.last_seen = std::mem::take(&mut state.last_seen);
        *state = new_state;
    }

    /// Ping a node to update heartbeat timestamp
    pub fn ping_node(&self, node_id: &str) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        state.ping_node(&node_id.to_string());
    }
}
