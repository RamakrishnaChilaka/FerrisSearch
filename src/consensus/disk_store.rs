//! Persistent Raft log storage backed by redb.
//!
//! Stores vote, committed marker, and log entries on disk so that Raft state
//! survives process restarts.  Uses the same shared-handle pattern as
//! `MemLogStore` so that the SM worker's log reader sees live data.

use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex};

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::{OptionalSend, RaftLogReader};
use redb::{Database, ReadableTable, TableDefinition};

use crate::consensus::types::{self, TypeConfig};

// ─── Table Definitions ──────────────────────────────────────────────────────

/// Meta table: stores "vote", "committed", "last_purged" as JSON blobs.
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Log table: maps log index (u64) → JSON-serialized Entry.
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log");

// ─── DiskLogStore ───────────────────────────────────────────────────────────

/// Shared handle to the redb database.
#[derive(Clone)]
pub struct DiskLogStore {
    db: Arc<Mutex<Database>>,
}

impl DiskLogStore {
    /// Open (or create) a persistent log store at the given path.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let db = Database::create(path.as_ref())
            .map_err(|e| io::Error::other(format!("redb open: {}", e)))?;

        // Ensure tables exist
        {
            let tx = db
                .begin_write()
                .map_err(|e| io::Error::other(format!("redb write: {}", e)))?;
            let _ = tx
                .open_table(META_TABLE)
                .map_err(|e| io::Error::other(format!("redb table: {}", e)))?;
            let _ = tx
                .open_table(LOG_TABLE)
                .map_err(|e| io::Error::other(format!("redb table: {}", e)))?;
            tx.commit()
                .map_err(|e| io::Error::other(format!("redb commit: {}", e)))?;
        }

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    fn io_err(msg: impl std::fmt::Display) -> io::Error {
        io::Error::other(msg.to_string())
    }

    fn read_meta<T: serde::de::DeserializeOwned>(&self, key: &str) -> io::Result<Option<T>> {
        let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
        let tx = db.begin_read().map_err(Self::io_err)?;
        let table = tx.open_table(META_TABLE).map_err(Self::io_err)?;
        match table.get(key).map_err(Self::io_err)? {
            Some(val) => {
                let v: T = serde_json::from_slice(val.value())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    fn write_meta<T: serde::Serialize>(&self, key: &str, value: &T) -> io::Result<()> {
        let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
        let data =
            serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tx = db.begin_write().map_err(Self::io_err)?;
        {
            let mut table = tx.open_table(META_TABLE).map_err(Self::io_err)?;
            table.insert(key, data.as_slice()).map_err(Self::io_err)?;
        }
        tx.commit().map_err(Self::io_err)?;
        Ok(())
    }

    fn delete_meta(&self, key: &str) -> io::Result<()> {
        let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
        let tx = db.begin_write().map_err(Self::io_err)?;
        {
            let mut table = tx.open_table(META_TABLE).map_err(Self::io_err)?;
            table.remove(key).map_err(Self::io_err)?;
        }
        tx.commit().map_err(Self::io_err)?;
        Ok(())
    }
}

// ─── RaftLogReader ──────────────────────────────────────────────────────────

impl RaftLogReader<TypeConfig> for DiskLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<types::Entry>, io::Error> {
        let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
        let tx = db.begin_read().map_err(Self::io_err)?;
        let table = tx.open_table(LOG_TABLE).map_err(Self::io_err)?;

        let mut entries = Vec::new();
        for item in table.range(range).map_err(Self::io_err)? {
            let (_, val) = item.map_err(Self::io_err)?;
            let entry: types::Entry = serde_json::from_slice(val.value())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<types::Vote>, io::Error> {
        self.read_meta("vote")
    }
}

// ─── RaftLogStorage ─────────────────────────────────────────────────────────

impl RaftLogStorage<TypeConfig> for DiskLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged: Option<types::LogId> = self.read_meta("last_purged")?;

        let last_log_id = {
            let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
            let tx = db.begin_read().map_err(Self::io_err)?;
            let table = tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
            match table.last().map_err(Self::io_err)? {
                Some((_, val)) => {
                    let entry: types::Entry = serde_json::from_slice(val.value())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    Some(entry.log_id)
                }
                None => None,
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log_id.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &types::Vote) -> Result<(), io::Error> {
        self.write_meta("vote", vote)
    }

    async fn save_committed(&mut self, committed: Option<types::LogId>) -> Result<(), io::Error> {
        match committed {
            Some(ref c) => self.write_meta("committed", c),
            None => self.delete_meta("committed"),
        }
    }

    async fn read_committed(&mut self) -> Result<Option<types::LogId>, io::Error> {
        self.read_meta("committed")
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = types::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
            let tx = db.begin_write().map_err(Self::io_err)?;
            {
                let mut table = tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
                for entry in entries {
                    let idx = entry.log_id.index;
                    let data = serde_json::to_vec(&entry)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    table.insert(idx, data.as_slice()).map_err(Self::io_err)?;
                }
            }
            tx.commit().map_err(Self::io_err)?;
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<types::LogId>) -> Result<(), io::Error> {
        let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
        let tx = db.begin_write().map_err(Self::io_err)?;
        {
            let mut table = tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
            match last_log_id {
                Some(id) => {
                    // Collect keys to remove (everything after id.index)
                    let keys: Vec<u64> = {
                        let read_tx = db.begin_read().map_err(Self::io_err)?;
                        let read_table = read_tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
                        read_table
                            .range((id.index + 1)..)
                            .map_err(Self::io_err)?
                            .map(|item| item.map(|(k, _)| k.value()))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(Self::io_err)?
                    };
                    for k in keys {
                        table.remove(k).map_err(Self::io_err)?;
                    }
                }
                None => {
                    // Remove all — drain the table
                    let keys: Vec<u64> = {
                        let read_tx = db.begin_read().map_err(Self::io_err)?;
                        let read_table = read_tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
                        read_table
                            .iter()
                            .map_err(Self::io_err)?
                            .map(|item| item.map(|(k, _)| k.value()))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(Self::io_err)?
                    };
                    for k in keys {
                        table.remove(k).map_err(Self::io_err)?;
                    }
                }
            }
        }
        tx.commit().map_err(Self::io_err)?;
        Ok(())
    }

    async fn purge(&mut self, upto: types::LogId) -> Result<(), io::Error> {
        {
            let db = self.db.lock().unwrap_or_else(|e| e.into_inner());
            let tx = db.begin_write().map_err(Self::io_err)?;
            {
                let mut table = tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
                let keys: Vec<u64> = {
                    let read_tx = db.begin_read().map_err(Self::io_err)?;
                    let read_table = read_tx.open_table(LOG_TABLE).map_err(Self::io_err)?;
                    read_table
                        .range(..=upto.index)
                        .map_err(Self::io_err)?
                        .map(|item| item.map(|(k, _)| k.value()))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(Self::io_err)?
                };
                for k in keys {
                    table.remove(k).map_err(Self::io_err)?;
                }
            }
            tx.commit().map_err(Self::io_err)?;
        }
        self.write_meta("last_purged", &upto)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{NodeInfo, NodeRole};
    use crate::consensus::types::ClusterCommand;
    use openraft::impls::leader_id_adv::LeaderId;
    use openraft::storage::RaftLogStorage;
    use openraft::{EntryPayload, RaftLogReader};

    fn make_leader_id(term: u64) -> LeaderId<u64, u64> {
        LeaderId { term, node_id: 1 }
    }

    fn make_entry(index: u64, term: u64) -> types::Entry {
        openraft::Entry {
            log_id: openraft::LogId::new(make_leader_id(term), index),
            payload: EntryPayload::Blank,
        }
    }

    fn make_normal_entry(index: u64, term: u64, cmd: ClusterCommand) -> types::Entry {
        openraft::Entry {
            log_id: openraft::LogId::new(make_leader_id(term), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    fn temp_store() -> DiskLogStore {
        let dir = tempfile::tempdir().unwrap();
        DiskLogStore::open(dir.path().join("raft.db")).unwrap()
    }

    #[test]
    fn open_creates_db_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        assert!(!path.exists());
        let _store = DiskLogStore::open(&path).unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn empty_store_state() {
        let mut store = temp_store();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());
        assert!(state.last_log_id.is_none());
        assert!(store.read_vote().await.unwrap().is_none());
        assert!(store.read_committed().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn append_and_read_entries() {
        let mut store = temp_store();
        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].log_id.index, 1);
        assert_eq!(read[2].log_id.index, 3);
    }

    #[tokio::test]
    async fn append_normal_entry() {
        let mut store = temp_store();
        let cmd = ClusterCommand::AddNode {
            node: NodeInfo {
                id: "n1".into(),
                name: "n1".into(),
                host: "127.0.0.1".into(),
                transport_port: 9300,
                http_port: 9200,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        };
        let entry = make_normal_entry(1, 1, cmd);
        store.append(vec![entry], IOFlushed::noop()).await.unwrap();

        let read = store.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(read.len(), 1);
        match &read[0].payload {
            EntryPayload::Normal(ClusterCommand::AddNode { node }) => {
                assert_eq!(node.id, "n1");
            }
            _ => panic!("Expected Normal(AddNode)"),
        }
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = temp_store();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = openraft::impls::Vote::new(1u64, 1u64);
        store.save_vote(&vote).await.unwrap();

        let read = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read, vote);
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let mut store = temp_store();
        assert!(store.read_committed().await.unwrap().is_none());

        let log_id = openraft::LogId::new(make_leader_id(1), 5);
        store.save_committed(Some(log_id)).await.unwrap();

        let read = store.read_committed().await.unwrap().unwrap();
        assert_eq!(read.index, 5);
    }

    #[tokio::test]
    async fn truncate_after_removes_tail() {
        let mut store = temp_store();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 1),
        ];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let log_id = openraft::LogId::new(make_leader_id(1), 2);
        store.truncate_after(Some(log_id)).await.unwrap();

        let read = store.try_get_log_entries(1..=4).await.unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].log_id.index, 1);
        assert_eq!(read[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn purge_removes_prefix() {
        let mut store = temp_store();
        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let log_id = openraft::LogId::new(make_leader_id(1), 2);
        store.purge(log_id).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);

        let read = store.try_get_log_entries(0..=10).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 3);
    }

    #[tokio::test]
    async fn data_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.db");

        // Write data
        {
            let mut store = DiskLogStore::open(&path).unwrap();
            let vote = openraft::impls::Vote::new(2u64, 5u64);
            store.save_vote(&vote).await.unwrap();
            store
                .append(vec![make_entry(1, 2), make_entry(2, 2)], IOFlushed::noop())
                .await
                .unwrap();
            let committed = openraft::LogId::new(make_leader_id(2), 2);
            store.save_committed(Some(committed)).await.unwrap();
        }

        // Reopen and verify
        {
            let mut store = DiskLogStore::open(&path).unwrap();

            let vote = store.read_vote().await.unwrap().unwrap();
            assert_eq!(vote, openraft::impls::Vote::new(2u64, 5u64));

            let entries = store.try_get_log_entries(1..=2).await.unwrap();
            assert_eq!(entries.len(), 2);

            let committed = store.read_committed().await.unwrap().unwrap();
            assert_eq!(committed.index, 2);

            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 2);
        }
    }

    #[tokio::test]
    async fn reader_shares_live_state() {
        let mut store = temp_store();
        store
            .append(vec![make_entry(1, 1), make_entry(2, 1)], IOFlushed::noop())
            .await
            .unwrap();

        let mut reader = store.get_log_reader().await;
        let read = reader.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(read.len(), 2);

        // Append more via the store — reader should see them
        store
            .append(vec![make_entry(3, 1)], IOFlushed::noop())
            .await
            .unwrap();
        let read = reader.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);
    }

    #[tokio::test]
    async fn truncate_after_none_clears_all() {
        let mut store = temp_store();
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        store.truncate_after(None).await.unwrap();

        let read = store.try_get_log_entries(0..=100).await.unwrap();
        assert!(read.is_empty());
    }

    #[tokio::test]
    async fn read_entries_range_subset() {
        let mut store = temp_store();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 2),
            make_entry(5, 2),
        ];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let read = store.try_get_log_entries(2..=4).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].log_id.index, 2);
        assert_eq!(read[2].log_id.index, 4);
    }

    #[tokio::test]
    async fn purge_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("purge.db");

        {
            let mut store = DiskLogStore::open(&path).unwrap();
            store
                .append(
                    vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)],
                    IOFlushed::noop(),
                )
                .await
                .unwrap();
            let purge_id = openraft::LogId::new(make_leader_id(1), 2);
            store.purge(purge_id).await.unwrap();
        }

        // Reopen and verify last_purged survived
        {
            let mut store = DiskLogStore::open(&path).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 2);

            // Only entry 3 should remain
            let entries = store.try_get_log_entries(0..=10).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id.index, 3);
        }
    }

    #[tokio::test]
    async fn raft_is_initialized_after_disk_recovery() {
        // Regression test: after writing data to a DiskLogStore and reopening,
        // Raft must report is_initialized=true so the node skips bootstrap/join.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("raft.db");

        // First run: create Raft, bootstrap, write an entry
        {
            let log_store = DiskLogStore::open(&db_path).unwrap();
            let sm = crate::consensus::state_machine::ClusterStateMachine::new("test".into());
            let config = openraft::Config {
                cluster_name: "test".into(),
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            };
            let raft = openraft::Raft::new(
                1u64,
                std::sync::Arc::new(config),
                crate::consensus::network::RaftNetworkFactoryImpl,
                log_store,
                sm,
            )
            .await
            .unwrap();

            // Bootstrap single-node
            let mut members = std::collections::BTreeMap::new();
            members.insert(
                1u64,
                openraft::BasicNode {
                    addr: "127.0.0.1:19400".into(),
                },
            );
            raft.initialize(members).await.unwrap();

            // Wait for leader
            for _ in 0..50 {
                if raft.current_leader().await.is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            // Write something
            raft.client_write(ClusterCommand::SetMaster {
                node_id: "n1".into(),
            })
            .await
            .unwrap();

            raft.shutdown().await.ok();
        }

        // Second run: reopen with same db — should be initialized
        {
            let log_store = DiskLogStore::open(&db_path).unwrap();
            let sm = crate::consensus::state_machine::ClusterStateMachine::new("test".into());
            let config = openraft::Config {
                cluster_name: "test".into(),
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            };
            let raft = openraft::Raft::new(
                1u64,
                std::sync::Arc::new(config),
                crate::consensus::network::RaftNetworkFactoryImpl,
                log_store,
                sm,
            )
            .await
            .unwrap();

            let initialized = raft.is_initialized().await.unwrap();
            assert!(
                initialized,
                "Raft must be initialized after recovering from disk"
            );

            raft.shutdown().await.ok();
        }
    }
}
