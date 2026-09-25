//! Raft type configuration and command/response definitions.

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::cluster::state::{
    DynamicMapping, FieldMapping, IndexMetadata, NodeInfo, SecurityApiKeyRecord,
    SecurityRoleDefinition,
};

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
pub type RaftInstance =
    openraft::Raft<TypeConfig, crate::consensus::state_machine::ClusterStateMachine>;

// ─── Commands (log entries applied to the state machine) ─────────────────────

/// A command proposed to the Raft leader and replicated across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Register a new node in the cluster.
    AddNode { node: NodeInfo },
    /// Remove a node from the cluster.
    RemoveNode { node_id: String },
    /// Create a new index with shard routing.
    CreateIndex { metadata: IndexMetadata },
    /// Delete an index.
    DeleteIndex { index_name: String },
    /// Set the current cluster master (Raft leader).
    SetMaster { node_id: String },
    /// Update an existing index's metadata (e.g. shard routing after replica allocation).
    UpdateIndex { metadata: IndexMetadata },
    /// Merge new field mappings into an existing index without replacing the
    /// entire metadata. This avoids TOCTOU races when concurrent documents
    /// discover different new fields at the same time.
    AddMappings {
        index_name: String,
        new_fields: std::collections::HashMap<String, FieldMapping>,
        dynamic: DynamicMapping,
    },
    /// Insert or replace a dynamically-managed API key (stores only the hash).
    PutApiKey { record: SecurityApiKeyRecord },
    /// Remove a dynamically-managed API key by id.
    DeleteApiKey { key_id: String },
    /// Insert or replace a custom role definition.
    PutRole { role: SecurityRoleDefinition },
    /// Remove a custom role definition by name.
    DeleteRole { name: String },
}

impl std::fmt::Display for ClusterCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterCommand::AddNode { node } => write!(f, "AddNode({})", node.id),
            ClusterCommand::RemoveNode { node_id } => {
                write!(f, "RemoveNode({node_id})")
            }
            ClusterCommand::CreateIndex { metadata } => {
                write!(f, "CreateIndex({})", metadata.name)
            }
            ClusterCommand::DeleteIndex { index_name } => {
                write!(f, "DeleteIndex({index_name})")
            }
            ClusterCommand::SetMaster { node_id } => write!(f, "SetMaster({node_id})"),
            ClusterCommand::UpdateIndex { metadata } => {
                write!(f, "UpdateIndex({})", metadata.name)
            }
            ClusterCommand::AddMappings {
                index_name,
                new_fields,
                ..
            } => {
                write!(
                    f,
                    "AddMappings({}, {} fields)",
                    index_name,
                    new_fields.len()
                )
            }
            ClusterCommand::PutApiKey { record } => {
                write!(f, "PutApiKey({})", record.id)
            }
            ClusterCommand::DeleteApiKey { key_id } => {
                write!(f, "DeleteApiKey({key_id})")
            }
            ClusterCommand::PutRole { role } => write!(f, "PutRole({})", role.name),
            ClusterCommand::DeleteRole { name } => write!(f, "DeleteRole({name})"),
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
    use crate::cluster::state::{IndexMetadata, NodeInfo, NodeRole, ShardRoutingEntry};
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
        assert_eq!(format!("{cmd}"), "AddNode(node-1)");
    }

    #[test]
    fn cluster_command_display_remove_node() {
        let cmd = ClusterCommand::RemoveNode {
            node_id: "node-2".into(),
        };
        assert_eq!(format!("{cmd}"), "RemoveNode(node-2)");
    }

    #[test]
    fn cluster_command_display_create_index() {
        let cmd = ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "test-idx".into(),
                uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
                number_of_shards: 1,
                number_of_replicas: 0,
                shard_routing: HashMap::new(),
                mappings: std::collections::HashMap::new(),
                dynamic: Default::default(),
                settings: crate::cluster::state::IndexSettings::default(),
            },
        };
        assert_eq!(format!("{cmd}"), "CreateIndex(test-idx)");
    }

    #[test]
    fn cluster_command_display_delete_index() {
        let cmd = ClusterCommand::DeleteIndex {
            index_name: "old-idx".into(),
        };
        assert_eq!(format!("{cmd}"), "DeleteIndex(old-idx)");
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
        assert_eq!(format!("{deserialized}"), "AddNode(n1)");
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
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "n1".into(),
                replicas: vec!["n2".into()],
                in_sync_replicas: vec!["n2".into()],
                unassigned_replicas: 0,
            },
        );
        let cmd = ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "routed".into(),
                uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
                number_of_shards: 3,
                number_of_replicas: 1,
                shard_routing,
                mappings: std::collections::HashMap::new(),
                dynamic: Default::default(),
                settings: crate::cluster::state::IndexSettings::default(),
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

    fn sample_api_key() -> SecurityApiKeyRecord {
        SecurityApiKeyRecord {
            id: "key-1".into(),
            name: "ci".into(),
            hash_sha256: "c".repeat(64),
            roles: vec!["read".into()],
            indices: vec!["logs-*".into()],
            created_at_millis: 1_700_000_000_000,
        }
    }

    fn sample_role() -> SecurityRoleDefinition {
        SecurityRoleDefinition {
            name: "analyst".into(),
            cluster: vec!["monitor".into()],
            indices: vec!["metrics-*".into()],
            index_privileges: vec!["read".into()],
        }
    }

    #[test]
    fn cluster_command_display_security_variants() {
        assert_eq!(
            format!(
                "{}",
                ClusterCommand::PutApiKey {
                    record: sample_api_key()
                }
            ),
            "PutApiKey(key-1)"
        );
        assert_eq!(
            format!(
                "{}",
                ClusterCommand::DeleteApiKey {
                    key_id: "key-1".into()
                }
            ),
            "DeleteApiKey(key-1)"
        );
        assert_eq!(
            format!(
                "{}",
                ClusterCommand::PutRole {
                    role: sample_role()
                }
            ),
            "PutRole(analyst)"
        );
        assert_eq!(
            format!(
                "{}",
                ClusterCommand::DeleteRole {
                    name: "analyst".into()
                }
            ),
            "DeleteRole(analyst)"
        );
    }

    #[test]
    fn put_api_key_command_serde_roundtrip() {
        let cmd = ClusterCommand::PutApiKey {
            record: sample_api_key(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        if let ClusterCommand::PutApiKey { record } = back {
            assert_eq!(record, sample_api_key());
        } else {
            panic!("Expected PutApiKey");
        }
    }

    #[test]
    fn delete_api_key_command_serde_roundtrip() {
        let cmd = ClusterCommand::DeleteApiKey {
            key_id: "key-1".into(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ClusterCommand::DeleteApiKey { key_id } if key_id == "key-1"));
    }

    #[test]
    fn put_role_command_serde_roundtrip() {
        let cmd = ClusterCommand::PutRole {
            role: sample_role(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        if let ClusterCommand::PutRole { role } = back {
            assert_eq!(role, sample_role());
        } else {
            panic!("Expected PutRole");
        }
    }

    #[test]
    fn delete_role_command_serde_roundtrip() {
        let cmd = ClusterCommand::DeleteRole {
            name: "analyst".into(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: ClusterCommand = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ClusterCommand::DeleteRole { name } if name == "analyst"));
    }
}
