//! In-memory Raft log storage.
//!
//! Stores the Raft log entries and vote in memory. For a production system
//! this should be backed by disk, but for our cluster metadata (which is small
//! and snapshotted) in-memory is acceptable.
//!
//! The store uses `Arc<Mutex<...>>` for the inner state so that the log reader
//! returned by `get_log_reader()` shares the same data as the store itself.
//! This is required because openraft's SM worker holds a reader and must see
//! entries appended after the reader was created.

use std::collections::BTreeMap;
use std::io;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::{OptionalSend, RaftLogReader};

use crate::consensus::types::{self, TypeConfig};

/// Shared inner state for the log store and its readers.
struct Inner {
    vote: Option<types::Vote>,
    log: BTreeMap<u64, types::Entry>,
    last_purged: Option<types::LogId>,
    committed: Option<types::LogId>,
}

/// In-memory log storage for Raft consensus.
///
/// Cloning this struct (or calling `get_log_reader`) returns a handle that
/// shares the same underlying data.
#[derive(Clone)]
pub struct MemLogStore {
    inner: Arc<Mutex<Inner>>,
}

impl MemLogStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                vote: None,
                log: BTreeMap::new(),
                last_purged: None,
                committed: None,
            })),
        }
    }
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<types::Entry>, io::Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entries: Vec<types::Entry> = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<types::Vote>, io::Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.vote.clone())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let last_log_id = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        let last = last_log_id.or(inner.last_purged);

        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &types::Vote) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.vote = Some(vote.clone());
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<types::LogId>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<types::LogId>, io::Error> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.committed)
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
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            for entry in entries {
                let idx = entry.log_id.index;
                inner.log.insert(idx, entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<types::LogId>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match last_log_id {
            Some(id) => {
                let keys_to_remove: Vec<u64> =
                    inner.log.range((id.index + 1)..).map(|(k, _)| *k).collect();
                for k in keys_to_remove {
                    inner.log.remove(&k);
                }
            }
            None => {
                inner.log.clear();
            }
        }
        Ok(())
    }

    async fn purge(&mut self, upto: types::LogId) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let keys_to_remove: Vec<u64> = inner.log.range(..=upto.index).map(|(k, _)| *k).collect();
        for k in keys_to_remove {
            inner.log.remove(&k);
        }
        inner.last_purged = Some(upto);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{NodeInfo, NodeRole};
    use crate::consensus::types::ClusterCommand;
    use openraft::storage::RaftLogStorage;
    use openraft::{EntryPayload, RaftLogReader};

    use openraft::impls::leader_id_adv::LeaderId;

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

    #[test]
    fn new_store_is_empty() {
        let store = MemLogStore::new();
        let inner = store.inner.lock().unwrap();
        assert!(inner.vote.is_none());
        assert!(inner.log.is_empty());
        assert!(inner.last_purged.is_none());
        assert!(inner.committed.is_none());
    }

    #[tokio::test]
    async fn get_log_state_empty() {
        let mut store = MemLogStore::new();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn append_and_read_entries() {
        let mut store = MemLogStore::new();
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
        let mut store = MemLogStore::new();
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
    async fn read_entries_range() {
        let mut store = MemLogStore::new();
        let entries = vec![
            make_entry(1, 1),
            make_entry(2, 1),
            make_entry(3, 1),
            make_entry(4, 2),
            make_entry(5, 2),
        ];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        // Read only entries 2..4
        let read = store.try_get_log_entries(2..=4).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].log_id.index, 2);
        assert_eq!(read[2].log_id.index, 4);
    }

    #[tokio::test]
    async fn save_and_read_vote() {
        let mut store = MemLogStore::new();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = openraft::impls::Vote::new(1u64, 1u64);
        store.save_vote(&vote).await.unwrap();

        let read = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read, vote);
    }

    #[tokio::test]
    async fn save_and_read_committed() {
        let mut store = MemLogStore::new();
        assert!(store.read_committed().await.unwrap().is_none());

        let log_id = openraft::LogId::new(make_leader_id(1), 5);
        store.save_committed(Some(log_id)).await.unwrap();

        let read = store.read_committed().await.unwrap().unwrap();
        assert_eq!(read.index, 5);
    }

    #[tokio::test]
    async fn truncate_after_removes_tail() {
        let mut store = MemLogStore::new();
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
    async fn truncate_after_none_clears_all() {
        let mut store = MemLogStore::new();
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        store.truncate_after(None).await.unwrap();

        let read = store.try_get_log_entries(0..=100).await.unwrap();
        assert!(read.is_empty());
    }

    #[tokio::test]
    async fn purge_removes_prefix() {
        let mut store = MemLogStore::new();
        let entries = vec![make_entry(1, 1), make_entry(2, 1), make_entry(3, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let log_id = openraft::LogId::new(make_leader_id(1), 2);
        store.purge(log_id).await.unwrap();

        assert_eq!(store.inner.lock().unwrap().last_purged.unwrap().index, 2);
        let read = store.try_get_log_entries(0..=10).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 3);
    }

    #[tokio::test]
    async fn get_log_reader_shares_state() {
        let mut store = MemLogStore::new();
        let entries = vec![make_entry(1, 1), make_entry(2, 1)];
        store.append(entries, IOFlushed::noop()).await.unwrap();

        let mut reader = store.get_log_reader().await;
        let read = reader.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(read.len(), 2);

        // Append more entries via the store — reader should see them
        store
            .append(vec![make_entry(3, 1)], IOFlushed::noop())
            .await
            .unwrap();
        let read = reader.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);
    }
}
