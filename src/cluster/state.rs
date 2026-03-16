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

/// Routing entry for a single shard: who is the primary, who are the replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRoutingEntry {
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

/// Metadata defining how an index is sharded across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub number_of_shards: u32,
    pub number_of_replicas: u32,
    /// Maps Shard ID → routing entry (primary + replicas)
    pub shard_routing: HashMap<u32, ShardRoutingEntry>,
}

impl IndexMetadata {
    /// Get the primary node for a given shard.
    pub fn primary_node(&self, shard_id: u32) -> Option<&NodeId> {
        self.shard_routing.get(&shard_id).map(|r| &r.primary)
    }

    /// Get replica nodes for a given shard.
    pub fn replica_nodes(&self, shard_id: u32) -> Vec<&NodeId> {
        self.shard_routing
            .get(&shard_id)
            .map(|r| r.replicas.iter().collect())
            .unwrap_or_default()
    }

    /// Promote the first replica to primary for a given shard.
    /// Returns true if promotion occurred.
    pub fn promote_replica(&mut self, shard_id: u32) -> bool {
        if let Some(routing) = self.shard_routing.get_mut(&shard_id) {
            if let Some(new_primary) = routing.replicas.first().cloned() {
                routing.primary = new_primary;
                routing.replicas.remove(0);
                return true;
            }
        }
        false
    }

    /// Remove a node from all shard routing entries (both primary and replica).
    /// Returns shard IDs where this node was the primary (need promotion or reassignment).
    pub fn remove_node(&mut self, node_id: &NodeId) -> Vec<u32> {
        let mut orphaned_primaries = Vec::new();
        for (shard_id, routing) in &mut self.shard_routing {
            routing.replicas.retain(|n| n != node_id);
            if routing.primary == *node_id {
                orphaned_primaries.push(*shard_id);
            }
        }
        orphaned_primaries
    }
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
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        state.add_index(meta);
        assert_eq!(state.version, 1);
        assert!(state.indices.contains_key("my-index"));
        assert_eq!(state.indices["my-index"].number_of_shards, 3);
    }

    #[test]
    fn promote_replica_to_primary() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into(), "node-C".into()],
        });

        assert!(meta.promote_replica(0));
        assert_eq!(meta.shard_routing[&0].primary, "node-B");
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-C".to_string()]);
    }

    #[test]
    fn promote_with_no_replicas_fails() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec![],
        });

        assert!(!meta.promote_replica(0));
    }

    #[test]
    fn remove_node_from_index_metadata() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into()],
        });
        meta.shard_routing.insert(1, ShardRoutingEntry {
            primary: "node-B".into(),
            replicas: vec!["node-A".into()],
        });

        let orphaned = meta.remove_node(&"node-A".into());
        assert_eq!(orphaned, vec![0]); // shard 0 lost its primary
        assert!(meta.shard_routing[&1].replicas.is_empty()); // removed from replicas
    }

    // ── IndexMetadata accessor tests ────────────────────────────────────

    #[test]
    fn primary_node_returns_correct_id() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into()],
        });
        meta.shard_routing.insert(1, ShardRoutingEntry {
            primary: "node-B".into(),
            replicas: vec!["node-A".into()],
        });
        assert_eq!(meta.primary_node(0), Some(&"node-A".to_string()));
        assert_eq!(meta.primary_node(1), Some(&"node-B".to_string()));
        assert_eq!(meta.primary_node(99), None);
    }

    #[test]
    fn replica_nodes_returns_correct_list() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 2,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into(), "node-C".into()],
        });
        let replicas = meta.replica_nodes(0);
        assert_eq!(replicas.len(), 2);
        assert!(replicas.contains(&&"node-B".to_string()));
        assert!(replicas.contains(&&"node-C".to_string()));
    }

    #[test]
    fn replica_nodes_empty_for_unknown_shard() {
        let meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
        };
        assert!(meta.replica_nodes(99).is_empty());
    }

    #[test]
    fn chained_promotions() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 2,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into(), "node-C".into()],
        });

        // First promotion: B becomes primary
        assert!(meta.promote_replica(0));
        assert_eq!(meta.shard_routing[&0].primary, "node-B");
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-C".to_string()]);

        // Second promotion: C becomes primary
        assert!(meta.promote_replica(0));
        assert_eq!(meta.shard_routing[&0].primary, "node-C");
        assert!(meta.shard_routing[&0].replicas.is_empty());

        // Third promotion: fails, no more replicas
        assert!(!meta.promote_replica(0));
    }

    #[test]
    fn promote_nonexistent_shard_returns_false() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
        };
        assert!(!meta.promote_replica(99));
    }

    #[test]
    fn remove_node_from_multiple_roles() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 3,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into()],
        });
        meta.shard_routing.insert(1, ShardRoutingEntry {
            primary: "node-B".into(),
            replicas: vec!["node-A".into()],
        });
        meta.shard_routing.insert(2, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-C".into()],
        });

        let mut orphaned = meta.remove_node(&"node-A".into());
        orphaned.sort();
        assert_eq!(orphaned, vec![0, 2]); // primary of shards 0 and 2
        // Removed from shard 1 replicas
        assert!(meta.shard_routing[&1].replicas.is_empty());
        // Shard 2 replica (node-C) is untouched
        assert_eq!(meta.shard_routing[&2].replicas, vec!["node-C".to_string()]);
    }

    #[test]
    fn remove_node_not_in_any_shard() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
        };
        meta.shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-A".into(),
            replicas: vec!["node-B".into()],
        });

        let orphaned = meta.remove_node(&"node-Z".into());
        assert!(orphaned.is_empty());
        // Original routing unchanged
        assert_eq!(meta.shard_routing[&0].primary, "node-A");
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-B".to_string()]);
    }
}
