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

    fn apply_command(&self, cmd: &ClusterCommand) {
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        match cmd {
            ClusterCommand::AddNode { node } => {
                state.add_node(node.clone());
            }
            ClusterCommand::RemoveNode { node_id } => {
                state.remove_node(node_id);
            }
            ClusterCommand::CreateIndex { metadata } => {
                state.add_index(metadata.clone());
            }
            ClusterCommand::DeleteIndex { index_name } => {
                state.indices.remove(index_name);
                state.version += 1;
            }
            ClusterCommand::SetMaster { node_id } => {
                state.master_node = Some(node_id.clone());
                state.version += 1;
            }
            ClusterCommand::UpdateIndex { metadata } => {
                state.indices.insert(metadata.name.clone(), metadata.clone());
                state.version += 1;
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
        Strm: futures::Stream<Item = Result<(Entry, Option<openraft::storage::ApplyResponder<TypeConfig>>), io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;

        futures::pin_mut!(entries);

        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;

            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ClusterResponse::Ok,
                EntryPayload::Normal(cmd) => {
                    self.apply_command(&cmd);
                    ClusterResponse::Ok
                }
                EntryPayload::Membership(ref mem) => {
                    self.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
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
        let data = serde_json::to_vec(&state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let snapshot_id = format!(
            "snap-{}",
            self.last_applied.map(|l| l.index).unwrap_or(0)
        );

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
        let data = serde_json::to_vec(&self.state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let snapshot_id = format!(
            "snap-{}",
            self.last_applied.map(|l| l.index).unwrap_or(0)
        );

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
    use crate::cluster::state::{NodeInfo, NodeRole, IndexMetadata, ShardRoutingEntry};
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
        shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
            unassigned_replicas: 0,
        });
        IndexMetadata {
            name: name.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
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
        sm.apply_command(&ClusterCommand::AddNode { node: make_node("n1") });
        sm.apply_command(&ClusterCommand::RemoveNode { node_id: "n1".into() });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.nodes.is_empty());
    }

    #[test]
    fn apply_create_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        let idx = make_index("my-index");
        sm.apply_command(&ClusterCommand::CreateIndex { metadata: idx });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert!(state.indices.contains_key("my-index"));
        assert_eq!(state.indices["my-index"].number_of_shards, 1);
    }

    #[test]
    fn apply_delete_index_command() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex { metadata: make_index("idx") });
        sm.apply_command(&ClusterCommand::DeleteIndex { index_name: "idx".into() });

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
        sm.apply_command(&ClusterCommand::CreateIndex { metadata: idx.clone() });

        // Simulate allocator: assign replicas
        idx.allocate_unassigned_replicas(&["node-1".into(), "node-2".into(), "node-3".into()]);
        assert_eq!(idx.unassigned_replica_count(), 0);

        // Apply UpdateIndex
        sm.apply_command(&ClusterCommand::UpdateIndex { metadata: idx });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        let updated = &state.indices["products"];
        assert_eq!(updated.unassigned_replica_count(), 0);
        assert_eq!(updated.shard_routing[&0].replicas.len(), 2);
    }

    #[test]
    fn update_index_preserves_other_indices() {
        let sm = ClusterStateMachine::new("test".into());
        sm.apply_command(&ClusterCommand::CreateIndex { metadata: make_index("idx-a") });
        sm.apply_command(&ClusterCommand::CreateIndex { metadata: make_index("idx-b") });

        // Update only idx-a
        let mut updated_a = make_index("idx-a");
        updated_a.number_of_replicas = 99;
        sm.apply_command(&ClusterCommand::UpdateIndex { metadata: updated_a });

        let handle = sm.state_handle();
        let state = handle.read().unwrap();
        assert_eq!(state.indices["idx-a"].number_of_replicas, 99);
        assert_eq!(state.indices["idx-b"].number_of_replicas, 0, "idx-b should be untouched");
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
            sm.apply_command(&ClusterCommand::AddNode { node: make_node("n1") });
            sm.apply_command(&ClusterCommand::CreateIndex { metadata: make_index("idx1") });

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
            sm.apply_command(&ClusterCommand::AddNode { node: make_node("old") });

            // Build snapshot data from a different state
            let mut new_state = ClusterState::new("replaced".into());
            new_state.add_node(make_node("new-node"));
            let snap_data = serde_json::to_vec(&new_state).unwrap();

            let meta = SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "snap-install".into(),
            };

            sm.install_snapshot(&meta, Cursor::new(snap_data)).await.unwrap();

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
            sm.apply_command(&ClusterCommand::AddNode { node: make_node("b1") });

            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.unwrap();

            let data = snap.snapshot.into_inner();
            let restored: ClusterState = serde_json::from_slice(&data).unwrap();
            assert!(restored.nodes.contains_key("b1"));
        });
    }
}

