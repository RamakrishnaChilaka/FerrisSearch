//! Raft state machine — applies committed log entries to the ClusterState.

use std::io;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, OptionalSend, RaftSnapshotBuilder};

use crate::cluster::state::ClusterState;
use crate::consensus::types::{
    ClusterCommand, ClusterResponse, Entry, LogId, Snapshot, SnapshotMeta, StoredMembership,
    TypeConfig,
};

/// The Raft state machine that wraps ClusterState.
/// All cluster state mutations go through Raft log → apply().
pub struct ClusterStateMachine {
    /// The authoritative cluster state, updated only via Raft apply().
    state: Arc<RwLock<ClusterState>>,
    /// Last applied log id.
    last_applied: Option<LogId>,
    /// Last applied membership.
    last_membership: StoredMembership,
}

impl ClusterStateMachine {
    pub fn new(cluster_name: String) -> Self {
        Self {
            state: Arc::new(RwLock::new(ClusterState::new(cluster_name))),
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    /// Get a read-only handle to the cluster state (for API queries).
    pub fn state_handle(&self) -> Arc<RwLock<ClusterState>> {
        self.state.clone()
    }

    fn apply_command(&self, cmd: &ClusterCommand) -> ClusterResponse {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        match cmd {
            ClusterCommand::AddNode { node } => {
                state.add_node(node.clone());
                ClusterResponse::Ok
            }
            ClusterCommand::RemoveNode { node_id } => {
                state.remove_node(node_id);
                ClusterResponse::Ok
            }
            ClusterCommand::CreateIndex { metadata } => {
                if metadata.shard_routing.len() != metadata.number_of_shards as usize {
                    return ClusterResponse::Error(format!(
                        "index '{}' has {} shard routing entries but declares {} shards",
                        metadata.name,
                        metadata.shard_routing.len(),
                        metadata.number_of_shards
                    ));
                }
                for shard_id in 0..metadata.number_of_shards {
                    let Some(routing) = metadata.shard_routing.get(&shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{}' is missing routing for shard {}",
                            metadata.name, shard_id
                        ));
                    };
                    if routing.primary_term == 0 {
                        return ClusterResponse::Error(format!(
                            "index '{}' shard {} must start with primary term at least 1",
                            metadata.name, shard_id
                        ));
                    }
                    if let Err(reason) = routing.validate_membership() {
                        return ClusterResponse::Error(format!(
                            "invalid routing for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }
                }
                state.add_index(metadata.clone());
                ClusterResponse::Ok
            }
            ClusterCommand::DeleteIndex { index_name } => {
                state.indices.remove(index_name);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::SetMaster { node_id } => {
                state.master_node = Some(node_id.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::UpdateIndex { metadata } => {
                let Some(current) = state.indices.get(&metadata.name).cloned() else {
                    return ClusterResponse::Error(format!(
                        "index '{}' does not exist",
                        metadata.name
                    ));
                };
                if current.uuid != metadata.uuid {
                    return ClusterResponse::Error(format!(
                        "index '{}' UUID mismatch: expected {}, got {}",
                        metadata.name, current.uuid, metadata.uuid
                    ));
                }
                if current.number_of_shards != metadata.number_of_shards
                    || current.shard_routing.len() != metadata.shard_routing.len()
                {
                    return ClusterResponse::Error(format!(
                        "index '{}' shard topology cannot be replaced by UpdateIndex",
                        metadata.name
                    ));
                }

                let mut updated = metadata.clone();
                for (shard_id, current_routing) in &current.shard_routing {
                    let Some(next_routing) = updated.shard_routing.get_mut(shard_id) else {
                        return ClusterResponse::Error(format!(
                            "index '{}' update is missing shard {}",
                            metadata.name, shard_id
                        ));
                    };

                    next_routing.in_sync_replicas = current_routing
                        .in_sync_replicas
                        .iter()
                        .filter(|replica| next_routing.replicas.contains(replica))
                        .cloned()
                        .collect();

                    if next_routing.primary != current_routing.primary {
                        if !current_routing.is_replica_in_sync(&next_routing.primary) {
                            return ClusterResponse::Error(format!(
                                "cannot promote out-of-sync replica '{}' for index '{}' shard {}",
                                next_routing.primary, metadata.name, shard_id
                            ));
                        }
                        let Some(next_term) = current_routing.primary_term.checked_add(1) else {
                            return ClusterResponse::Error(format!(
                                "primary term exhausted for index '{}' shard {}",
                                metadata.name, shard_id
                            ));
                        };
                        next_routing.primary_term = next_term;
                        next_routing
                            .in_sync_replicas
                            .retain(|replica| replica != &next_routing.primary);
                    } else {
                        next_routing.primary_term = current_routing.primary_term;
                    }

                    if let Err(reason) = next_routing.validate_membership() {
                        return ClusterResponse::Error(format!(
                            "invalid routing update for index '{}' shard {}: {}",
                            metadata.name, shard_id, reason
                        ));
                    }
                }

                state.indices.insert(metadata.name.clone(), updated);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::MarkReplicaInSync {
                index_name,
                index_uuid,
                shard_id,
                replica,
                primary,
                primary_term,
            } => {
                let Some(metadata) = state.indices.get_mut(index_name) else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(routing) = metadata.shard_routing.get_mut(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if &routing.primary != primary {
                    return ClusterResponse::Error(format!(
                        "primary mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.primary_term != *primary_term {
                    return ClusterResponse::Error(format!(
                        "primary term mismatch for index '{index_name}' shard {shard_id}: expected {}, got {}",
                        routing.primary_term, primary_term
                    ));
                }
                if !routing.replicas.contains(replica) {
                    return ClusterResponse::Error(format!(
                        "replica '{replica}' is not assigned to index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.is_replica_in_sync(replica) {
                    return ClusterResponse::Error(format!(
                        "replica '{replica}' is already in sync for index '{index_name}' shard {shard_id}"
                    ));
                }
                routing.in_sync_replicas.push(replica.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::ActivatePrimary {
                index_name,
                index_uuid,
                shard_id,
                primary,
                expected_term,
            } => {
                let Some(metadata) = state.indices.get_mut(index_name) else {
                    return ClusterResponse::Error(format!("index '{index_name}' does not exist"));
                };
                if metadata.uuid.as_str() != index_uuid {
                    return ClusterResponse::Error(format!("index '{index_name}' UUID mismatch"));
                }
                let Some(routing) = metadata.shard_routing.get_mut(shard_id) else {
                    return ClusterResponse::Error(format!(
                        "index '{index_name}' has no shard {shard_id}"
                    ));
                };
                if &routing.primary != primary {
                    return ClusterResponse::Error(format!(
                        "primary mismatch for index '{index_name}' shard {shard_id}"
                    ));
                }
                if routing.primary_term != *expected_term {
                    return ClusterResponse::Error(format!(
                        "primary term mismatch for index '{index_name}' shard {shard_id}: expected {}, got {}",
                        routing.primary_term, expected_term
                    ));
                }
                let Some(next_term) = routing.primary_term.checked_add(1) else {
                    return ClusterResponse::Error(format!(
                        "primary term exhausted for index '{index_name}' shard {shard_id}"
                    ));
                };
                routing.primary_term = next_term;
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::AddMappings {
                index_name,
                new_fields,
                dynamic,
            } => {
                if let Some(existing) = state.indices.get_mut(index_name) {
                    for (name, mapping) in new_fields {
                        existing
                            .mappings
                            .entry(name.clone())
                            .or_insert(mapping.clone());
                    }
                    existing.dynamic = dynamic.clone();
                    state.version += 1;
                }
                ClusterResponse::Ok
            }
            ClusterCommand::PutApiKey { record } => {
                state.api_keys.insert(record.id.clone(), record.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::DeleteApiKey { key_id } => {
                state.api_keys.remove(key_id);
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::PutRole { role } => {
                state.roles.insert(role.name.clone(), role.clone());
                state.version += 1;
                ClusterResponse::Ok
            }
            ClusterCommand::DeleteRole { name } => {
                state.roles.remove(name);
                state.version += 1;
                ClusterResponse::Ok
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for ClusterStateMachine {
    type SnapshotBuilder = ClusterSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<
                    (Entry, Option<openraft::storage::ApplyResponder<TypeConfig>>),
                    io::Error,
                >,
            > + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        futures::pin_mut!(entries);

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;

            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ClusterResponse::Ok,
                EntryPayload::Normal(cmd) => self.apply_command(&cmd),
                EntryPayload::Membership(ref mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    ClusterResponse::Ok
                }
            };

            if let Some(tx) = responder {
                tx.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner()).clone();
        ClusterSnapshotBuilder {
            state,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let new_state: ClusterState = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            *state = new_state;
        }

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner()).clone();
        let data = serde_json::to_vec(&state).map_err(io::Error::other)?;

        let snapshot_id = format!("snap-{}", self.last_applied.map(|l| l.index).unwrap_or(0));

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}

// ─── Snapshot Builder ───────────────────────────────────────────────────────

pub struct ClusterSnapshotBuilder {
    state: ClusterState,
    last_applied: Option<LogId>,
    last_membership: StoredMembership,
}

impl RaftSnapshotBuilder<TypeConfig> for ClusterSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        let data = serde_json::to_vec(&self.state).map_err(io::Error::other)?;

        let snapshot_id = format!("snap-{}", self.last_applied.map(|l| l.index).unwrap_or(0));

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{IndexMetadata, NodeInfo, NodeRole, ShardRoutingEntry};
    use std::collections::HashMap;

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

    fn make_index(name: &str) -> IndexMetadata {
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: vec![],
                in_sync_replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        IndexMetadata {
            name: name.into(),
            uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            dynamic: Default::default(),
            settings: crate::cluster::state::IndexSettings::default(),
        }
    }

    #[test]
    fn new_state_machine_has_empty_state() {
        let sm = ClusterStateMachine::new("test-cluster".into());
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.cluster_name, "test-cluster");
        assert!(state.nodes.is_empty());
        assert!(state.indices.is_empty());
        assert_eq!(state.version, 0);
    }

    #[test]
    fn state_handle_returns_shared_ref() {
        let sm = ClusterStateMachine::new("test".into());
        let h1 = sm.state_handle();
        let h2 = sm.state_handle();
        // Both handles point to the same underlying RwLock
        assert!(std::sync::Arc::ptr_eq(&h1, &h2));
    }

    #[test]
    fn apply_add_node_command() {
        let sm = ClusterStateMachine::new("test".into());
        let node = make_node("n1");
        sm.apply_command(&ClusterCommand::AddNode { node: node.clone() });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.nodes.contains_key("n1"));
        assert_eq!(state.nodes["n1"].name, "n1");
        assert_eq!(state.version, 1);
    }

    #[test]
    fn apply_remove_node_command() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::AddNode {
            node: make_node("n1"),
        });
        sm.apply_command(&ClusterCommand::RemoveNode {
            node_id: "n1".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.nodes.is_empty());
    }

    #[test]
    fn apply_create_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        let idx = make_index("my-index");
        let response = sm.apply_command(&ClusterCommand::CreateIndex { metadata: idx });
        assert_eq!(response, ClusterResponse::Ok);

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.indices.contains_key("my-index"));
        assert_eq!(state.indices["my-index"].number_of_shards, 1);
    }

    #[test]
    fn apply_delete_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx"),
        });
        sm.apply_command(&ClusterCommand::DeleteIndex {
            index_name: "idx".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(!state.indices.contains_key("idx"));
    }

    #[test]
    fn apply_update_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        // Create index with unassigned replicas
        let mut idx = IndexMetadata::build_shard_routing("products", 1, 2, &["node-1".into()]);
        assert_eq!(idx.unassigned_replica_count(), 2);
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: idx.clone(),
        });

        // Simulate allocator: assign replicas
        idx.allocate_unassigned_replicas(&["node-1".into(), "node-2".into(), "node-3".into()]);
        assert_eq!(idx.unassigned_replica_count(), 0);

        // Apply UpdateIndex
        let response = sm.apply_command(&ClusterCommand::UpdateIndex { metadata: idx });
        assert_eq!(response, ClusterResponse::Ok);

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let updated = &state.indices["products"];
        assert_eq!(updated.unassigned_replica_count(), 0);
        assert_eq!(updated.shard_routing[&0].replicas.len(), 2);
    }

    #[test]
    fn create_index_rejects_zero_primary_term() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("legacy");
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 0;

        let response = sm.apply_command(&ClusterCommand::CreateIndex { metadata });

        assert!(matches!(
            response,
            ClusterResponse::Error(error) if error.contains("primary term at least 1")
        ));
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(!state.indices.contains_key("legacy"));
        assert_eq!(state.version, 0);
    }

    #[test]
    fn update_index_cannot_add_in_sync_members() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        metadata.shard_routing.get_mut(&0).unwrap().replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        metadata.shard_routing.get_mut(&0).unwrap().in_sync_replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::UpdateIndex { metadata }),
            ClusterResponse::Ok
        );

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(
            state.indices["idx"].shard_routing[&0]
                .in_sync_replicas
                .is_empty()
        );
    }

    #[test]
    fn update_index_rejects_out_of_sync_primary_without_partial_apply() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        metadata.shard_routing.get_mut(&0).unwrap().replicas = vec!["node-2".into()];
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        let version_before = sm.state_handle().read().unwrap().version;
        let routing = metadata.shard_routing.get_mut(&0).unwrap();
        routing.primary = "node-2".into();
        routing.replicas.clear();
        routing.primary_term = 99;
        metadata.settings.refresh_interval_ms = Some(1234);

        let response = sm.apply_command(&ClusterCommand::UpdateIndex { metadata });

        assert!(matches!(
            response,
            ClusterResponse::Error(error) if error.contains("out-of-sync replica")
        ));
        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let current = &state.indices["idx"];
        assert_eq!(current.shard_routing[&0].primary, "node-1");
        assert_eq!(current.shard_routing[&0].primary_term, 1);
        assert_eq!(current.settings.refresh_interval_ms, None);
        assert_eq!(state.version, version_before);
    }

    #[test]
    fn update_index_promotes_in_sync_primary_and_computes_next_term() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 2;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.primary_term = 7;
            routing.replicas = vec!["node-2".into(), "node-3".into()];
            routing.in_sync_replicas = routing.replicas.clone();
        }
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            }),
            ClusterResponse::Ok
        );

        assert!(metadata.promote_replica_to(0, "node-2"));
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 999;
        assert_eq!(
            sm.apply_command(&ClusterCommand::UpdateIndex { metadata }),
            ClusterResponse::Ok
        );

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let routing = &state.indices["idx"].shard_routing[&0];
        assert_eq!(routing.primary, "node-2");
        assert_eq!(routing.primary_term, 8);
        assert_eq!(routing.replicas, ["node-3"]);
        assert_eq!(routing.in_sync_replicas, ["node-3"]);
    }

    #[test]
    fn mark_replica_in_sync_enforces_all_compare_and_set_fields() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.number_of_replicas = 1;
        {
            let routing = metadata.shard_routing.get_mut(&0).unwrap();
            routing.primary_term = 3;
            routing.replicas = vec!["node-2".into()];
        }
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex { metadata }),
            ClusterResponse::Ok
        );
        let version_before = sm.state_handle().read().unwrap().version;

        for command in [
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: "wrong".into(),
                shard_id: 0,
                replica: "node-2".into(),
                primary: "node-1".into(),
                primary_term: 3,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-2".into(),
                primary: "wrong".into(),
                primary_term: 3,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-2".into(),
                primary: "node-1".into(),
                primary_term: 2,
            },
            ClusterCommand::MarkReplicaInSync {
                index_name: "idx".into(),
                index_uuid: index_uuid.clone(),
                shard_id: 0,
                replica: "node-3".into(),
                primary: "node-1".into(),
                primary_term: 3,
            },
        ] {
            assert!(matches!(
                sm.apply_command(&command),
                ClusterResponse::Error(_)
            ));
        }
        assert_eq!(sm.state_handle().read().unwrap().version, version_before);

        let command = ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid,
            shard_id: 0,
            replica: "node-2".into(),
            primary: "node-1".into(),
            primary_term: 3,
        };
        assert_eq!(sm.apply_command(&command), ClusterResponse::Ok);
        assert_eq!(
            sm.state_handle().read().unwrap().indices["idx"].shard_routing[&0].in_sync_replicas,
            ["node-2"]
        );
        assert!(matches!(
            sm.apply_command(&command),
            ClusterResponse::Error(error) if error.contains("already in sync")
        ));
    }

    #[test]
    fn activate_primary_enforces_compare_and_set_and_advances_once() {
        let sm = ClusterStateMachine::new("test".into());
        let mut metadata = make_index("idx");
        metadata.shard_routing.get_mut(&0).unwrap().primary_term = 4;
        let index_uuid = metadata.uuid.to_string();
        assert_eq!(
            sm.apply_command(&ClusterCommand::CreateIndex { metadata }),
            ClusterResponse::Ok
        );

        let stale = ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: index_uuid.clone(),
            shard_id: 0,
            primary: "node-1".into(),
            expected_term: 3,
        };
        assert!(matches!(
            sm.apply_command(&stale),
            ClusterResponse::Error(error) if error.contains("primary term mismatch")
        ));

        let command = ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid,
            shard_id: 0,
            primary: "node-1".into(),
            expected_term: 4,
        };
        assert_eq!(sm.apply_command(&command), ClusterResponse::Ok);
        assert_eq!(
            sm.state_handle().read().unwrap().indices["idx"].shard_routing[&0].primary_term,
            5
        );
        assert!(matches!(
            sm.apply_command(&command),
            ClusterResponse::Error(error) if error.contains("primary term mismatch")
        ));
    }

    #[test]
    fn update_index_preserves_other_indices() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx-a"),
        });
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: make_index("idx-b"),
        });

        // Update only idx-a
        let mut updated_a = make_index("idx-a");
        updated_a.number_of_replicas = 99;
        sm.apply_command(&ClusterCommand::UpdateIndex {
            metadata: updated_a,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.indices["idx-a"].number_of_replicas, 99);
        assert_eq!(
            state.indices["idx-b"].number_of_replicas, 0,
            "idx-b should be untouched"
        );
    }

    #[test]
    fn applied_state_returns_none_initially() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("test".into());
            let (log_id, membership) = sm.applied_state().await.unwrap();
            assert!(log_id.is_none());
            // Default membership has no log id
            assert!(membership.log_id().is_none());
        });
    }

    #[test]
    fn snapshot_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("snap-test".into());

            // Add some state
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("n1"),
            });
            sm.apply_command(&ClusterCommand::CreateIndex {
                metadata: make_index("idx1"),
            });

            // Get snapshot
            let snap = sm.get_current_snapshot().await.unwrap().unwrap();
            assert_eq!(snap.meta.snapshot_id, "snap-0"); // last_applied is None

            // Deserialize snapshot data to verify state
            let data = snap.snapshot.into_inner();
            let restored: ClusterState = serde_json::from_slice(&data).unwrap();
            assert_eq!(restored.cluster_name, "snap-test");
            assert!(restored.nodes.contains_key("n1"));
            assert!(restored.indices.contains_key("idx1"));
        });
    }

    #[test]
    fn install_snapshot_replaces_state() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("original".into());
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("old"),
            });

            // Build snapshot data from a different state
            let mut new_state = ClusterState::new("replaced".into());
            new_state.add_node(make_node("new-node"));
            let snap_data = serde_json::to_vec(&new_state).unwrap();

            let meta = SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-install".into(),
            };

            sm.install_snapshot(&meta, Cursor::new(snap_data))
                .await
                .unwrap();

            let handle = sm.state_handle();
            let state = handle.read().unwrap();
            assert_eq!(state.cluster_name, "replaced");
            assert!(state.nodes.contains_key("new-node"));
            assert!(!state.nodes.contains_key("old"));
        });
    }

    #[test]
    fn snapshot_builder_builds_correct_snapshot() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut sm = ClusterStateMachine::new("builder-test".into());
            sm.apply_command(&ClusterCommand::AddNode {
                node: make_node("b1"),
            });

            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.unwrap();

            let data = snap.snapshot.into_inner();
            let restored: ClusterState = serde_json::from_slice(&data).unwrap();
            assert!(restored.nodes.contains_key("b1"));
        });
    }

    // ── AddMappings command tests ──────────────────────────────────────

    #[test]
    fn apply_add_mappings_merges_new_fields() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let meta = make_index("idx");
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: meta.clone(),
        });

        let new_fields = std::collections::HashMap::from([
            (
                "count".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "active".to_string(),
                FieldMapping {
                    field_type: FieldType::Boolean,
                    dimension: None,
                },
            ),
        ]);

        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "idx".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let idx = &state.indices["idx"];
        assert_eq!(idx.mappings.len(), 2);
        assert_eq!(idx.mappings["count"].field_type, FieldType::Integer);
        assert_eq!(idx.mappings["active"].field_type, FieldType::Boolean);
        assert_eq!(idx.dynamic, DynamicMapping::True);
    }

    #[test]
    fn apply_add_mappings_preserves_existing_fields() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let mut meta = make_index("idx");
        meta.mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        sm.apply_command(&ClusterCommand::CreateIndex {
            metadata: meta.clone(),
        });

        // Try to add "title" with a different type — existing should win.
        let new_fields = std::collections::HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "idx".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        // Existing mapping wins — still Text, not Keyword.
        assert_eq!(
            state.indices["idx"].mappings["title"].field_type,
            FieldType::Text
        );
    }

    #[test]
    fn apply_add_mappings_noop_for_nonexistent_index() {
        use crate::cluster::state::{DynamicMapping, FieldMapping, FieldType};

        let sm = ClusterStateMachine::new("test".into());
        let new_fields = std::collections::HashMap::from([(
            "f".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        sm.apply_command(&ClusterCommand::AddMappings {
            index_name: "nonexistent".into(),
            new_fields,
            dynamic: DynamicMapping::True,
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        // Version should not have been bumped.
        assert_eq!(state.version, 0);
        assert!(!state.indices.contains_key("nonexistent"));
    }

    fn make_api_key(id: &str) -> crate::cluster::state::SecurityApiKeyRecord {
        crate::cluster::state::SecurityApiKeyRecord {
            id: id.into(),
            name: format!("{id}-name"),
            hash_sha256: "d".repeat(64),
            roles: vec!["read".into()],
            indices: vec![],
            created_at_millis: 7,
        }
    }

    fn make_role(name: &str) -> crate::cluster::state::SecurityRoleDefinition {
        crate::cluster::state::SecurityRoleDefinition {
            name: name.into(),
            cluster: vec!["monitor".into()],
            indices: vec!["logs-*".into()],
            index_privileges: vec!["read".into()],
        }
    }

    #[test]
    fn apply_put_api_key_inserts_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.api_keys["key-1"].name, "key-1-name");
    }

    #[test]
    fn apply_put_api_key_replaces_existing() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });
        let mut updated = make_api_key("key-1");
        updated.name = "renamed".into();
        sm.apply_command(&ClusterCommand::PutApiKey { record: updated });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert_eq!(state.api_keys.len(), 1);
        assert_eq!(state.api_keys["key-1"].name, "renamed");
    }

    #[test]
    fn apply_delete_api_key_removes_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutApiKey {
            record: make_api_key("key-1"),
        });
        sm.apply_command(&ClusterCommand::DeleteApiKey {
            key_id: "key-1".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert!(state.api_keys.is_empty());
    }

    #[test]
    fn apply_delete_missing_api_key_still_bumps_version() {
        // Delete is idempotent like RemoveNode/DeleteIndex — it always bumps version.
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::DeleteApiKey {
            key_id: "ghost".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert!(state.api_keys.is_empty());
    }

    #[test]
    fn apply_put_role_inserts_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutRole {
            role: make_role("analyst"),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 1);
        assert_eq!(state.roles["analyst"].cluster, vec!["monitor".to_string()]);
    }

    #[test]
    fn apply_delete_role_removes_and_bumps_version() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::PutRole {
            role: make_role("analyst"),
        });
        sm.apply_command(&ClusterCommand::DeleteRole {
            name: "analyst".into(),
        });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.version, 2);
        assert!(state.roles.is_empty());
    }
}
