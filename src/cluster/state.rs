use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Represents a unique node in the cluster
pub type NodeId = String;

/// The role a node plays in the OpenSearch cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Master,
    Data,
    Client,
}

/// Information about a specific node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub name: String,
    pub host: String,
    pub transport_port: u16,
    pub http_port: u16,
    pub roles: Vec<NodeRole>,
}

/// Metadata defining how an index is sharded across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub number_of_shards: u32,
    // Maps Shard ID (e.g., 0, 1, 2) to the Node ID that holds it
    pub shards: HashMap<u32, NodeId>,
}

/// The globally agreed-upon state of the entire cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub cluster_name: String,
    pub version: u64,
    pub master_node: Option<NodeId>,
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub indices: HashMap<String, IndexMetadata>,
    #[serde(skip, default)]
    pub last_seen: HashMap<NodeId, Instant>,
}

impl ClusterState {
    pub fn new(cluster_name: String) -> Self {
        Self {
            cluster_name,
            version: 0,
            master_node: None,
            nodes: HashMap::new(),
            indices: HashMap::new(),
            last_seen: HashMap::new(),
        }
    }

    /// Add or update a node in the cluster state
    pub fn add_node(&mut self, node: NodeInfo) {
        self.last_seen.insert(node.id.clone(), Instant::now());
        self.nodes.insert(node.id.clone(), node);
        self.version += 1; // Any change bumps the cluster state version
    }

    /// Remove a node from the cluster state
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        let removed = self.nodes.remove(node_id);
        if removed.is_some() {
            self.last_seen.remove(node_id);
            self.version += 1;

            // If the master was removed, we need a new election (simplified here)
            if self.master_node.as_ref() == Some(node_id) {
                self.master_node = None;
            }
        }
        removed
    }

    /// Update the last seen timestamp for a node
    pub fn ping_node(&mut self, node_id: &NodeId) {
        if self.nodes.contains_key(node_id) {
            self.last_seen.insert(node_id.clone(), Instant::now());
        }
    }

    /// Add an index to the cluster state
    pub fn add_index(&mut self, metadata: IndexMetadata) {
        self.indices.insert(metadata.name.clone(), metadata);
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: &str) -> NodeInfo {
        NodeInfo {
            id: id.into(),
            name: id.into(),
            host: "127.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
        }
    }

    #[test]
    fn new_cluster_state_is_empty() {
        let state = ClusterState::new("test-cluster".into());
        assert_eq!(state.cluster_name, "test-cluster");
        assert_eq!(state.version, 0);
        assert!(state.master_node.is_none());
        assert!(state.nodes.is_empty());
        assert!(state.indices.is_empty());
    }

    #[test]
    fn add_node_increments_version() {
        let mut state = ClusterState::new("c".into());
        state.add_node(make_node("n1"));
        assert_eq!(state.version, 1);
        assert!(state.nodes.contains_key("n1"));

        state.add_node(make_node("n2"));
        assert_eq!(state.version, 2);
        assert_eq!(state.nodes.len(), 2);
    }

    #[test]
    fn remove_node_increments_version() {
        let mut state = ClusterState::new("c".into());
        state.add_node(make_node("n1"));
        let removed = state.remove_node(&"n1".into());
        assert!(removed.is_some());
        assert_eq!(state.version, 2);
        assert!(state.nodes.is_empty());
    }

    #[test]
    fn remove_nonexistent_node_is_noop() {
        let mut state = ClusterState::new("c".into());
        let removed = state.remove_node(&"ghost".into());
        assert!(removed.is_none());
        assert_eq!(state.version, 0);
    }

    #[test]
    fn removing_master_clears_master_node() {
        let mut state = ClusterState::new("c".into());
        state.add_node(make_node("master-1"));
        state.master_node = Some("master-1".into());

        state.remove_node(&"master-1".into());
        assert!(state.master_node.is_none());
    }

    #[test]
    fn ping_node_updates_last_seen() {
        let mut state = ClusterState::new("c".into());
        state.add_node(make_node("n1"));
        let first = *state.last_seen.get("n1").unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        state.ping_node(&"n1".into());

        let second = *state.last_seen.get("n1").unwrap();
        assert!(second >= first);
    }

    #[test]
    fn ping_unknown_node_is_noop() {
        let mut state = ClusterState::new("c".into());
        state.ping_node(&"unknown".into());
        assert!(state.last_seen.is_empty());
    }

    #[test]
    fn add_index_increments_version() {
        let mut state = ClusterState::new("c".into());
        let meta = IndexMetadata {
            name: "my-index".into(),
            number_of_shards: 3,
            shards: HashMap::new(),
        };
        state.add_index(meta);
        assert_eq!(state.version, 1);
        assert!(state.indices.contains_key("my-index"));
        assert_eq!(state.indices["my-index"].number_of_shards, 3);
    }
}
