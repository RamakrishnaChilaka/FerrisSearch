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

    /// Overwrite the local state entirely.
    ///
    /// # Invariant
    /// In production, only the Raft state machine should call this method.
    /// Test harnesses may call it to set up initial cluster state before
    /// exercising handlers.  API handlers must NEVER call this directly —
    /// all cluster-state mutations go through `raft.client_write(...)`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{ClusterState, NodeRole};
    use std::sync::Arc;

    fn make_node(id: &str, raft_id: u64) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: raft_id,
        }
    }

    #[test]
    fn update_state_preserves_last_seen() {
        let cm = ClusterManager::new("test".into());
        cm.add_node(make_node("n1", 1));
        cm.ping_node("n1");

        // Simulate receiving a new state from proto (no last_seen)
        let mut new_state = ClusterState::new("test".into());
        new_state.add_node(make_node("n1", 1));
        new_state.last_seen.clear(); // proto deserialization produces empty last_seen

        cm.update_state(new_state);

        let state = cm.get_state();
        assert!(
            state.last_seen.contains_key("n1"),
            "update_state must preserve existing last_seen entries"
        );
    }

    #[test]
    fn update_state_does_not_erase_raft_node_id() {
        // Bug: when a joining node called update_state() with proto-deserialized
        // state, raft_node_id was 0 for all nodes, overwriting the authoritative
        // values set by the Raft state machine.
        //
        // The fix was to stop calling update_state on the join path entirely.
        // This test documents that update_state DOES overwrite raft_node_id
        // (it's a full state replace), so callers must not use it to overwrite
        // Raft-managed state.
        let cm = ClusterManager::new("test".into());
        cm.add_node(make_node("n1", 5));

        let mut new_state = ClusterState::new("test".into());
        new_state.add_node(make_node("n1", 0)); // proto has raft_node_id=0

        cm.update_state(new_state);
        let state = cm.get_state();
        // update_state is a full overwrite — raft_node_id IS replaced
        assert_eq!(state.nodes["n1"].raft_node_id, 0);
    }

    #[test]
    fn with_shared_state_reads_same_state() {
        let shared = Arc::new(std::sync::RwLock::new(ClusterState::new("shared".into())));

        // Mutate via the Arc directly (simulating Raft SM apply)
        {
            let mut s = shared.write().unwrap();
            s.add_node(make_node("raft-node", 42));
            s.master_node = Some("raft-node".into());
        }

        // ClusterManager should see the same mutation
        let cm = ClusterManager::with_shared_state(shared.clone());
        let state = cm.get_state();
        assert!(state.nodes.contains_key("raft-node"));
        assert_eq!(state.nodes["raft-node"].raft_node_id, 42);
        assert_eq!(state.master_node, Some("raft-node".into()));
    }
}
