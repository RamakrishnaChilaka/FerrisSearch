//! Raft type configuration and command/response definitions.

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::cluster::state::{IndexMetadata, NodeInfo};

// ─── Raft Type Config ───────────────────────────────────────────────────────

openraft::declare_raft_types!(
    /// Type configuration for the FerrisSearch Raft consensus layer.
    pub TypeConfig:
        D = ClusterCommand,
        R = ClusterResponse,
        Node = BasicNode
);

/// Convenience type aliases to avoid spelling out all generics.
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;

/// The concrete Raft instance type for this application.
pub type RaftInstance = openraft::Raft<TypeConfig, crate::consensus::state_machine::ClusterStateMachine>;

// ─── Commands (log entries applied to the state machine) ─────────────────────

/// A command proposed to the Raft leader and replicated across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Register a new node in the cluster.
    AddNode {
        node: NodeInfo,
    },
    /// Remove a node from the cluster.
    RemoveNode {
        node_id: String,
    },
    /// Create a new index with shard routing.
    CreateIndex {
        metadata: IndexMetadata,
    },
    /// Delete an index.
    DeleteIndex {
        index_name: String,
    },
    /// Set the current cluster master (Raft leader).
    SetMaster {
        node_id: String,
    },
}

impl std::fmt::Display for ClusterCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterCommand::AddNode { node } => write!(f, "AddNode({})", node.id),
            ClusterCommand::RemoveNode { node_id } => write!(f, "RemoveNode({})", node_id),
            ClusterCommand::CreateIndex { metadata } => write!(f, "CreateIndex({})", metadata.name),
            ClusterCommand::DeleteIndex { index_name } => write!(f, "DeleteIndex({})", index_name),
            ClusterCommand::SetMaster { node_id } => write!(f, "SetMaster({})", node_id),
        }
    }
}

// ─── Responses ──────────────────────────────────────────────────────────────

/// Response returned after a command is applied to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    Ok,
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{NodeInfo, NodeRole, IndexMetadata, ShardRoutingEntry};
    use std::collections::HashMap;

    #[test]
    fn cluster_command_display_add_node() {
        let cmd = ClusterCommand::AddNode {
            node: NodeInfo {
                id: "node-1".into(),
                name: "node-1".into(),
                host: "127.0.0.1".into(),
                transport_port: 9300,
                http_port: 9200,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        };
        assert_eq!(format!("{}", cmd), "AddNode(node-1)");
    }

    #[test]
    fn cluster_command_display_remove_node() {
        let cmd = ClusterCommand::RemoveNode { node_id: "node-2".into() };
        assert_eq!(format!("{}", cmd), "RemoveNode(node-2)");
    }

    #[test]
    fn cluster_command_display_create_index() {
        let cmd = ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "test-idx".into(),
                number_of_shards: 1,
                number_of_replicas: 0,
                shard_routing: HashMap::new(),
            },
        };
        assert_eq!(format!("{}", cmd), "CreateIndex(test-idx)");
    }

    #[test]
    fn cluster_command_display_delete_index() {
        let cmd = ClusterCommand::DeleteIndex { index_name: "old-idx".into() };
        assert_eq!(format!("{}", cmd), "DeleteIndex(old-idx)");
    }

    #[test]
    fn cluster_command_serde_roundtrip() {
        let node = NodeInfo {
            id: "n1".into(),
            name: "n1".into(),
            host: "10.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: 0,
        };
        let cmd = ClusterCommand::AddNode { node };
        let json = serde_json::to_string(&cmd).unwrap();
        let deserialized: ClusterCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{}", deserialized), "AddNode(n1)");
    }

    #[test]
    fn cluster_response_serde_roundtrip() {
        let ok = ClusterResponse::Ok;
        let err = ClusterResponse::Error("something went wrong".into());

        let ok_json = serde_json::to_string(&ok).unwrap();
        let err_json = serde_json::to_string(&err).unwrap();

        let ok_back: ClusterResponse = serde_json::from_str(&ok_json).unwrap();
        let err_back: ClusterResponse = serde_json::from_str(&err_json).unwrap();

        assert!(matches!(ok_back, ClusterResponse::Ok));
        assert!(matches!(err_back, ClusterResponse::Error(msg) if msg == "something went wrong"));
    }

    #[test]
    fn create_index_command_preserves_shard_routing() {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(0, ShardRoutingEntry {
            primary: "n1".into(),
            replicas: vec!["n2".into()],
        });
        let cmd = ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "routed".into(),
                number_of_shards: 3,
                number_of_replicas: 1,
                shard_routing,
            },
        };

        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        if let ClusterCommand::CreateIndex { metadata } = back {
            assert_eq!(metadata.name, "routed");
            assert_eq!(metadata.number_of_shards, 3);
            assert_eq!(metadata.shard_routing[&0].primary, "n1");
            assert_eq!(metadata.shard_routing[&0].replicas, vec!["n2"]);
        } else {
            panic!("Expected CreateIndex");
        }
    }
}

