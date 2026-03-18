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
    /// Unique Raft consensus node ID. 0 means not assigned.
    #[serde(default)]
    pub raft_node_id: u64,
}

/// The allocation state of a shard copy (primary or replica).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShardState {
    /// Shard is assigned to a node and active.
    Started,
    /// Shard needs a node but none is available (e.g. not enough data nodes for replicas).
    Unassigned,
}

/// A single shard copy — either the primary or a replica — with its assignment state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardCopy {
    pub node_id: Option<NodeId>,
    pub state: ShardState,
}

/// Routing entry for a single shard: who is the primary, who are the replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRoutingEntry {
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
    /// Replica copies that couldn't be assigned (not enough distinct nodes).
    #[serde(default)]
    pub unassigned_replicas: u32,
}

/// The type of a mapped field in an index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// Full-text analyzed field (tokenized, BM25 scored).
    Text,
    /// Exact-match keyword (not tokenized, stored verbatim).
    Keyword,
    /// Signed 64-bit integer.
    Integer,
    /// 64-bit floating point.
    Float,
    /// Boolean.
    Boolean,
    /// k-NN vector field (stored in USearch, not in Tantivy).
    KnnVector,
}

/// A single field mapping definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Dimension for knn_vector fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimension: Option<usize>,
}

/// Metadata defining how an index is sharded across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub number_of_shards: u32,
    pub number_of_replicas: u32,
    /// Maps Shard ID → routing entry (primary + replicas)
    pub shard_routing: HashMap<u32, ShardRoutingEntry>,
    /// Field name → mapping definition. Empty means dynamic (all-to-body).
    #[serde(default)]
    pub mappings: HashMap<String, FieldMapping>,
}

impl IndexMetadata {
    /// Build shard routing for a new index, distributing primaries and replicas
    /// round-robin across the given data nodes. Guarantees that a shard's primary
    /// and replicas are never assigned to the same node.
    pub fn build_shard_routing(
        name: &str,
        num_shards: u32,
        num_replicas: u32,
        data_nodes: &[String],
    ) -> Self {
        let mut shard_routing = HashMap::new();
        for shard_id in 0..num_shards {
            let primary_node = data_nodes[(shard_id as usize) % data_nodes.len()].clone();
            let mut replicas = Vec::new();
            let mut unassigned = 0u32;
            for r in 0..num_replicas {
                let replica_idx = ((shard_id as usize) + 1 + (r as usize)) % data_nodes.len();
                let replica_node = &data_nodes[replica_idx];
                if *replica_node != primary_node {
                    replicas.push(replica_node.clone());
                } else {
                    unassigned += 1;
                }
            }
            shard_routing.insert(
                shard_id,
                ShardRoutingEntry {
                    primary: primary_node,
                    replicas,
                    unassigned_replicas: unassigned,
                },
            );
        }
        Self {
            name: name.to_string(),
            number_of_shards: num_shards,
            number_of_replicas: num_replicas,
            shard_routing,
            mappings: HashMap::new(),
        }
    }

    /// Get the primary node for a given shard.
    pub fn primary_node(&self, shard_id: u32) -> Option<&NodeId> {
        self.shard_routing.get(&shard_id).map(|r| &r.primary)
    }

    /// Returns the total number of unassigned replica copies across all shards.
    pub fn unassigned_replica_count(&self) -> u32 {
        self.shard_routing
            .values()
            .map(|r| r.unassigned_replicas)
            .sum()
    }

    /// Try to assign unassigned replicas to available data nodes.
    /// Returns true if any assignments were made (caller should persist via Raft).
    pub fn allocate_unassigned_replicas(&mut self, data_nodes: &[String]) -> bool {
        let mut changed = false;
        for routing in self.shard_routing.values_mut() {
            while routing.unassigned_replicas > 0 {
                // Find a data node not already used by this shard (primary or existing replicas)
                let used: std::collections::HashSet<&str> =
                    std::iter::once(routing.primary.as_str())
                        .chain(routing.replicas.iter().map(|s| s.as_str()))
                        .collect();
                let candidate = data_nodes.iter().find(|n| !used.contains(n.as_str()));
                if let Some(node) = candidate {
                    routing.replicas.push(node.clone());
                    routing.unassigned_replicas -= 1;
                    changed = true;
                } else {
                    break; // No more distinct nodes available
                }
            }
        }
        changed
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

    /// Promote a specific replica to primary for a given shard.
    /// The chosen replica is removed from the replicas list and becomes primary.
    /// Returns true if promotion occurred.
    pub fn promote_replica_to(&mut self, shard_id: u32, new_primary: &str) -> bool {
        if let Some(routing) = self.shard_routing.get_mut(&shard_id) {
            if let Some(pos) = routing.replicas.iter().position(|n| n == new_primary) {
                routing.primary = routing.replicas.remove(pos);
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
            raft_node_id: 0,
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
            mappings: std::collections::HashMap::new(),
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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into(), "node-C".into()],
                unassigned_replicas: 0,
            },
        );

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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );

        assert!(!meta.promote_replica(0));
    }

    #[test]
    fn remove_node_from_index_metadata() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );
        meta.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-B".into(),
                replicas: vec!["node-A".into()],
                unassigned_replicas: 0,
            },
        );

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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );
        meta.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-B".into(),
                replicas: vec!["node-A".into()],
                unassigned_replicas: 0,
            },
        );
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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into(), "node-C".into()],
                unassigned_replicas: 0,
            },
        );
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
            mappings: std::collections::HashMap::new(),
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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into(), "node-C".into()],
                unassigned_replicas: 0,
            },
        );

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
            mappings: std::collections::HashMap::new(),
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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );
        meta.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-B".into(),
                replicas: vec!["node-A".into()],
                unassigned_replicas: 0,
            },
        );
        meta.shard_routing.insert(
            2,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-C".into()],
                unassigned_replicas: 0,
            },
        );

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
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );

        let orphaned = meta.remove_node(&"node-Z".into());
        assert!(orphaned.is_empty());
        // Original routing unchanged
        assert_eq!(meta.shard_routing[&0].primary, "node-A");
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-B".to_string()]);
    }

    // ── Field mapping tests ────────────────────────────────────────────

    #[test]
    fn field_mapping_serde_roundtrip() {
        let fm = FieldMapping {
            field_type: FieldType::Text,
            dimension: None,
        };
        let json = serde_json::to_string(&fm).unwrap();
        let fm2: FieldMapping = serde_json::from_str(&json).unwrap();
        assert_eq!(fm2.field_type, FieldType::Text);
        assert!(fm2.dimension.is_none());
    }

    #[test]
    fn knn_vector_mapping_with_dimension() {
        let fm = FieldMapping {
            field_type: FieldType::KnnVector,
            dimension: Some(128),
        };
        let json = serde_json::to_string(&fm).unwrap();
        assert!(json.contains("knn_vector"));
        assert!(json.contains("128"));
        let fm2: FieldMapping = serde_json::from_str(&json).unwrap();
        assert_eq!(fm2.field_type, FieldType::KnnVector);
        assert_eq!(fm2.dimension, Some(128));
    }

    #[test]
    fn index_metadata_with_mappings_serde() {
        let mut meta = IndexMetadata {
            name: "test".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings: HashMap::new(),
        };
        meta.mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        meta.mappings.insert(
            "year".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let json = serde_json::to_string(&meta).unwrap();
        let meta2: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta2.mappings.len(), 2);
        assert_eq!(meta2.mappings["title"].field_type, FieldType::Text);
        assert_eq!(meta2.mappings["year"].field_type, FieldType::Integer);
    }

    #[test]
    fn index_metadata_without_mappings_defaults_to_empty() {
        let json =
            r#"{"name":"test","number_of_shards":1,"number_of_replicas":0,"shard_routing":{}}"#;
        let meta: IndexMetadata = serde_json::from_str(json).unwrap();
        assert!(
            meta.mappings.is_empty(),
            "missing mappings field should default to empty"
        );
    }

    #[test]
    fn all_field_types_deserialize() {
        for type_str in &[
            "text",
            "keyword",
            "integer",
            "float",
            "boolean",
            "knn_vector",
        ] {
            let json = format!(r#"{{"type":"{}"}}"#, type_str);
            let fm: FieldMapping = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                fm.field_type,
                FieldType::Text
                    | FieldType::Keyword
                    | FieldType::Integer
                    | FieldType::Float
                    | FieldType::Boolean
                    | FieldType::KnnVector
            ));
        }
    }

    // ── Shard state tracking tests ──────────────────────────────────────

    #[test]
    fn single_node_with_replicas_tracks_unassigned() {
        let meta = IndexMetadata::build_shard_routing("test", 1, 2, &["node-1".into()]);
        let routing = &meta.shard_routing[&0];
        assert_eq!(routing.primary, "node-1");
        assert!(
            routing.replicas.is_empty(),
            "no replicas can be placed on single node"
        );
        assert_eq!(
            routing.unassigned_replicas, 2,
            "both replicas should be unassigned"
        );
        assert_eq!(meta.unassigned_replica_count(), 2);
    }

    #[test]
    fn two_nodes_one_replica_fully_assigned() {
        let meta =
            IndexMetadata::build_shard_routing("test", 1, 1, &["node-1".into(), "node-2".into()]);
        let routing = &meta.shard_routing[&0];
        assert_eq!(routing.primary, "node-1");
        assert_eq!(routing.replicas, vec!["node-2".to_string()]);
        assert_eq!(routing.unassigned_replicas, 0);
        assert_eq!(meta.unassigned_replica_count(), 0);
    }

    #[test]
    fn two_nodes_two_replicas_one_unassigned() {
        let meta =
            IndexMetadata::build_shard_routing("test", 1, 2, &["node-1".into(), "node-2".into()]);
        let routing = &meta.shard_routing[&0];
        assert_eq!(
            routing.replicas.len(),
            1,
            "only 1 replica can be placed on 2 nodes"
        );
        assert_eq!(routing.unassigned_replicas, 1, "1 replica unassigned");
    }

    #[test]
    fn three_nodes_two_replicas_all_assigned() {
        let meta = IndexMetadata::build_shard_routing(
            "test",
            1,
            2,
            &["n1".into(), "n2".into(), "n3".into()],
        );
        let routing = &meta.shard_routing[&0];
        assert_eq!(routing.replicas.len(), 2);
        assert_eq!(routing.unassigned_replicas, 0);
    }

    #[test]
    fn multi_shard_unassigned_tracking() {
        let meta = IndexMetadata::build_shard_routing("test", 3, 1, &["node-1".into()]);
        // Each shard has 1 requested replica, but only 1 node → all unassigned
        assert_eq!(meta.unassigned_replica_count(), 3);
        for routing in meta.shard_routing.values() {
            assert!(routing.replicas.is_empty());
            assert_eq!(routing.unassigned_replicas, 1);
        }
    }

    #[test]
    fn zero_replicas_no_unassigned() {
        let meta = IndexMetadata::build_shard_routing("test", 3, 0, &["node-1".into()]);
        assert_eq!(meta.unassigned_replica_count(), 0);
    }

    #[test]
    fn shard_state_serde_roundtrip() {
        let entry = ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec!["node-2".into()],
            unassigned_replicas: 1,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let entry2: ShardRoutingEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry2.unassigned_replicas, 1);
        assert_eq!(entry2.replicas, vec!["node-2".to_string()]);
    }

    #[test]
    fn shard_routing_without_unassigned_field_defaults_to_zero() {
        let json = r#"{"primary":"node-1","replicas":["node-2"]}"#;
        let entry: ShardRoutingEntry = serde_json::from_str(json).unwrap();
        assert_eq!(
            entry.unassigned_replicas, 0,
            "missing field should default to 0"
        );
    }

    // ── Shard allocator tests ───────────────────────────────────────────

    #[test]
    fn allocate_assigns_replica_when_new_node_available() {
        // Created with 1 node → 2 unassigned replicas
        let mut meta = IndexMetadata::build_shard_routing("test", 1, 2, &["node-1".into()]);
        assert_eq!(meta.unassigned_replica_count(), 2);

        // Add node-2 → should assign 1 replica
        let changed = meta.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]);
        assert!(changed);
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-2".to_string()]);
        assert_eq!(
            meta.shard_routing[&0].unassigned_replicas, 1,
            "1 still unassigned (need 3rd node)"
        );
    }

    #[test]
    fn allocate_assigns_all_replicas_with_enough_nodes() {
        let mut meta = IndexMetadata::build_shard_routing("test", 1, 2, &["node-1".into()]);
        assert_eq!(meta.unassigned_replica_count(), 2);

        // Add node-2 and node-3 → both replicas assigned
        let changed =
            meta.allocate_unassigned_replicas(&["node-1".into(), "node-2".into(), "node-3".into()]);
        assert!(changed);
        assert_eq!(meta.shard_routing[&0].replicas.len(), 2);
        assert_eq!(meta.shard_routing[&0].unassigned_replicas, 0);
        assert_eq!(meta.unassigned_replica_count(), 0);
    }

    #[test]
    fn allocate_no_change_when_already_fully_assigned() {
        let mut meta =
            IndexMetadata::build_shard_routing("test", 1, 1, &["node-1".into(), "node-2".into()]);
        assert_eq!(meta.unassigned_replica_count(), 0);

        let changed = meta.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]);
        assert!(!changed, "nothing to allocate");
    }

    #[test]
    fn allocate_no_change_when_no_new_nodes() {
        let mut meta = IndexMetadata::build_shard_routing("test", 1, 2, &["node-1".into()]);
        // Still only 1 node available
        let changed = meta.allocate_unassigned_replicas(&["node-1".into()]);
        assert!(!changed, "no new nodes to assign to");
        assert_eq!(meta.unassigned_replica_count(), 2);
    }

    #[test]
    fn allocate_multi_shard_assigns_to_different_nodes() {
        let mut meta = IndexMetadata::build_shard_routing("test", 2, 1, &["node-1".into()]);
        assert_eq!(meta.unassigned_replica_count(), 2); // 2 shards × 1 unassigned each

        let changed = meta.allocate_unassigned_replicas(&["node-1".into(), "node-2".into()]);
        assert!(changed);
        assert_eq!(meta.unassigned_replica_count(), 0);
        // Each shard's replica should be on node-2 (the only non-primary node)
        for routing in meta.shard_routing.values() {
            assert!(routing.replicas.contains(&"node-2".to_string()));
        }
    }

    #[test]
    fn allocate_prevents_replica_on_same_node_as_primary() {
        let mut meta = IndexMetadata::build_shard_routing("test", 1, 1, &["node-1".into()]);
        // Only node available is the same as primary
        let changed = meta.allocate_unassigned_replicas(&["node-1".into()]);
        assert!(!changed, "should not assign replica to primary node");
    }

    // ── promote_replica_to tests ────────────────────────────────────────

    #[test]
    fn promote_replica_to_specific_node() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 2,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into(), "node-C".into()],
                unassigned_replicas: 0,
            },
        );

        // Promote node-C (skipping node-B which is first)
        assert!(meta.promote_replica_to(0, "node-C"));
        assert_eq!(meta.shard_routing[&0].primary, "node-C");
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-B".to_string()]);
    }

    #[test]
    fn promote_replica_to_nonexistent_node_fails() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );

        assert!(!meta.promote_replica_to(0, "node-X"));
        assert_eq!(
            meta.shard_routing[&0].primary, "node-A",
            "primary should not change"
        );
    }

    #[test]
    fn promote_replica_to_unknown_shard_fails() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );

        assert!(!meta.promote_replica_to(99, "node-B"));
    }

    // ── remove_node + promote flow tests ────────────────────────────────

    #[test]
    fn remove_primary_node_and_promote() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into()],
                unassigned_replicas: 0,
            },
        );
        meta.shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-B".into(),
                replicas: vec!["node-A".into()],
                unassigned_replicas: 0,
            },
        );

        // Remove node-A: orphans shard 0's primary, removed from shard 1's replicas
        let orphaned = meta.remove_node(&"node-A".to_string());
        assert_eq!(orphaned, vec![0]);

        // shard 1's replicas should no longer include node-A
        assert!(meta.shard_routing[&1].replicas.is_empty());

        // Promote for shard 0
        assert!(meta.promote_replica(0));
        assert_eq!(meta.shard_routing[&0].primary, "node-B");
        assert!(meta.shard_routing[&0].replicas.is_empty());
    }

    #[test]
    fn remove_node_returns_empty_for_replica_only() {
        let mut meta = IndexMetadata {
            name: "idx".into(),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::new(),
            mappings: std::collections::HashMap::new(),
        };
        meta.shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-A".into(),
                replicas: vec!["node-B".into(), "node-C".into()],
                unassigned_replicas: 0,
            },
        );

        // Remove a replica-only node — no orphaned primaries
        let orphaned = meta.remove_node(&"node-B".to_string());
        assert!(orphaned.is_empty());
        assert_eq!(meta.shard_routing[&0].replicas, vec!["node-C".to_string()]);
    }
}
