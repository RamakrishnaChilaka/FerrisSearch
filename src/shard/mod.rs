//! Shard management.
//! Each index has N primary shards. Each shard is backed by a `SearchEngine` implementation.
//! The ShardManager owns all local shard engines on this node.

use crate::engine::{CompositeEngine, SearchEngine};
use crate::wal::TranslogDurability;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Key uniquely identifying a shard: (index_name, shard_id)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShardKey {
    pub index: String,
    pub shard_id: u32,
}

impl ShardKey {
    pub fn new(index: impl Into<String>, shard_id: u32) -> Self {
        Self {
            index: index.into(),
            shard_id,
        }
    }
    /// Returns the directory name for this shard's data
    pub fn data_dir(&self) -> String {
        format!("{}/shard_{}", self.index, self.shard_id)
    }
}

/// Per-replica checkpoint info for ISR tracking.
#[derive(Debug, Clone)]
pub struct ReplicaCheckpoint {
    /// The replica's last known local checkpoint (highest contiguous seq_no).
    pub checkpoint: u64,
    /// When we last heard from this replica.
    pub last_updated: std::time::Instant,
}

/// Tracks in-sync replicas for all primary shards on this node.
/// A replica is considered "in-sync" if its checkpoint is within
/// `max_lag` of the primary's local checkpoint.
pub struct IsrTracker {
    /// Per-shard, per-replica checkpoint tracking.
    /// Key: ShardKey, Value: HashMap<replica_node_id, ReplicaCheckpoint>
    replicas: RwLock<HashMap<ShardKey, HashMap<String, ReplicaCheckpoint>>>,
    /// Maximum allowed seq_no lag for a replica to be considered in-sync.
    max_lag: u64,
}

impl IsrTracker {
    pub fn new(max_lag: u64) -> Self {
        Self {
            replicas: RwLock::new(HashMap::new()),
            max_lag,
        }
    }

    /// Update a replica's checkpoint for a given shard.
    pub fn update_replica_checkpoint(
        &self,
        index: &str,
        shard_id: u32,
        replica_node_id: &str,
        checkpoint: u64,
    ) {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        let shard_replicas = replicas.entry(key).or_default();
        shard_replicas.insert(
            replica_node_id.to_string(),
            ReplicaCheckpoint {
                checkpoint,
                last_updated: std::time::Instant::now(),
            },
        );
    }

    /// Update multiple replica checkpoints from a replication round.
    pub fn update_replica_checkpoints(
        &self,
        index: &str,
        shard_id: u32,
        checkpoints: &[(String, u64)],
    ) {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        let shard_replicas = replicas.entry(key).or_default();
        let now = std::time::Instant::now();
        for (node_id, cp) in checkpoints {
            shard_replicas.insert(
                node_id.clone(),
                ReplicaCheckpoint {
                    checkpoint: *cp,
                    last_updated: now,
                },
            );
        }
    }

    /// Get the set of in-sync replica node IDs for a shard.
    /// A replica is in-sync if its checkpoint is within `max_lag` of the primary checkpoint.
    pub fn in_sync_replicas(
        &self,
        index: &str,
        shard_id: u32,
        primary_checkpoint: u64,
    ) -> Vec<String> {
        let key = ShardKey::new(index, shard_id);
        let replicas = self.replicas.read().unwrap_or_else(|e| e.into_inner());
        match replicas.get(&key) {
            Some(shard_replicas) => shard_replicas
                .iter()
                .filter(|(_, rc)| primary_checkpoint.saturating_sub(rc.checkpoint) <= self.max_lag)
                .map(|(node_id, _)| node_id.clone())
                .collect(),
            None => vec![],
        }
    }

    /// Get all replica checkpoints for a shard (for diagnostics / _cat/shards).
    pub fn replica_checkpoints(&self, index: &str, shard_id: u32) -> Vec<(String, u64)> {
        let key = ShardKey::new(index, shard_id);
        let replicas = self.replicas.read().unwrap_or_else(|e| e.into_inner());
        match replicas.get(&key) {
            Some(shard_replicas) => shard_replicas
                .iter()
                .map(|(node_id, rc)| (node_id.clone(), rc.checkpoint))
                .collect(),
            None => vec![],
        }
    }

    /// Remove tracking data for a shard (e.g., when index is deleted).
    pub fn remove_shard(&self, index: &str, shard_id: u32) {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        replicas.remove(&key);
    }

    /// Remove tracking for all shards of an index.
    pub fn remove_index(&self, index: &str) {
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        replicas.retain(|k, _| k.index != index);
    }
}

/// Manages all shard engines on this node.
/// Each shard is backed by a `CompositeEngine` (Tantivy text + USearch vector).
pub struct ShardManager {
    data_dir: PathBuf,
    refresh_interval: Duration,
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    /// ISR tracker for primary shards — tracks replica checkpoint lag.
    pub isr_tracker: IsrTracker,
    /// Translog durability mode for new shards.
    durability: TranslogDurability,
}

impl ShardManager {
    pub fn new(data_dir: impl Into<PathBuf>, refresh_interval: Duration) -> Self {
        Self::new_with_durability(data_dir, refresh_interval, TranslogDurability::Request)
    }

    pub fn new_with_durability(
        data_dir: impl Into<PathBuf>,
        refresh_interval: Duration,
        durability: TranslogDurability,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            refresh_interval,
            shards: RwLock::new(HashMap::new()),
            isr_tracker: IsrTracker::new(1000),
            durability,
        }
    }

    /// Get the base data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Open or create the engine for a specific shard.
    /// Uses CompositeEngine which handles both text and vector indexing.
    pub fn open_shard(&self, index: &str, shard_id: u32) -> Result<Arc<dyn SearchEngine>> {
        self.open_shard_with_mappings(index, shard_id, &HashMap::new())
    }

    /// Open or create the engine for a specific shard with explicit field mappings.
    pub fn open_shard_with_mappings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> Result<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                return Ok(engine.clone());
            }
        }

        let shard_dir = self.data_dir.join(&key.data_dir());
        std::fs::create_dir_all(&shard_dir)?;

        // Try to open the engine. If it fails with a schema mismatch (stale data
        // from a previous run whose index was already deleted/re-created), wipe the
        // orphaned directory and retry with a fresh index.
        let engine = match CompositeEngine::new_with_mappings(
            &shard_dir,
            self.refresh_interval,
            mappings,
            self.durability,
        ) {
            Ok(e) => Arc::new(e),
            Err(first_err) => {
                let err_msg = first_err.to_string();
                if err_msg.contains("schema does not match") {
                    tracing::warn!(
                        "Schema mismatch for {}/shard_{}, removing stale data and retrying",
                        index,
                        shard_id
                    );
                    std::fs::remove_dir_all(&shard_dir)?;
                    std::fs::create_dir_all(&shard_dir)?;
                    Arc::new(CompositeEngine::new_with_mappings(
                        &shard_dir,
                        self.refresh_interval,
                        mappings,
                        self.durability,
                    )?)
                } else {
                    return Err(first_err);
                }
            }
        };
        CompositeEngine::start_refresh_loop(engine.clone());

        // Rebuild vector index from persisted documents (covers crash recovery)
        if let Err(e) = engine.rebuild_vectors() {
            tracing::warn!(
                "Failed to rebuild vectors for {}/shard_{}: {}",
                index,
                shard_id,
                e
            );
        }

        tracing::info!(
            "Opened shard engine for {}/{} at {:?}",
            index,
            shard_id,
            shard_dir
        );

        let dyn_engine: Arc<dyn SearchEngine> = engine;
        let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
        shards.insert(key, dyn_engine.clone());
        Ok(dyn_engine)
    }

    /// Get an already-open shard engine.
    pub fn get_shard(&self, index: &str, shard_id: u32) -> Option<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);
        self.shards
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .cloned()
    }

    /// Return all local shard engines for a given index.
    pub fn get_index_shards(&self, index: &str) -> Vec<(u32, Arc<dyn SearchEngine>)> {
        self.shards
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(k, _)| k.index == index)
            .map(|(k, e)| (k.shard_id, e.clone()))
            .collect()
    }

    /// Return all local shard engines across all indices.
    pub fn all_shards(&self) -> Vec<(ShardKey, Arc<dyn SearchEngine>)> {
        self.shards
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect()
    }

    /// Close and remove all shard engines for an index, then delete the data directory.
    pub fn close_index_shards(&self, index: &str) -> Result<()> {
        let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
        let keys_to_remove: Vec<ShardKey> = shards
            .keys()
            .filter(|k| k.index == index)
            .cloned()
            .collect();
        for key in &keys_to_remove {
            shards.remove(key);
        }
        drop(shards);

        // Clean ISR tracking for this index
        self.isr_tracker.remove_index(index);

        let index_dir = self.data_dir.join(index);
        if index_dir.exists() {
            std::fs::remove_dir_all(&index_dir)?;
            tracing::info!(
                "Removed shard data for index '{}' at {:?}",
                index,
                index_dir
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_shard_manager() -> (tempfile::TempDir, ShardManager) {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::new(dir.path(), Duration::from_secs(60));
        (dir, mgr)
    }

    // ── ShardKey ─────────────────────────────────────────────────────────

    #[test]
    fn shard_key_data_dir() {
        let key = ShardKey::new("my-index", 2);
        assert_eq!(key.data_dir(), "my-index/shard_2");
    }

    #[test]
    fn shard_key_equality() {
        let a = ShardKey::new("idx", 0);
        let b = ShardKey::new("idx", 0);
        let c = ShardKey::new("idx", 1);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── open / get (need tokio runtime for refresh loop) ────────────────

    #[tokio::test]
    async fn open_shard_creates_engine() {
        let (_dir, mgr) = create_shard_manager();
        let engine = mgr.open_shard("test-index", 0).unwrap();
        engine
            .add_document("d1", json!({"hello": "world"}))
            .unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn get_shard_returns_none_for_unopened() {
        let (_dir, mgr) = create_shard_manager();
        assert!(mgr.get_shard("no-index", 0).is_none());
    }

    #[tokio::test]
    async fn get_shard_returns_opened_engine() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard("idx", 0).unwrap();
        assert!(mgr.get_shard("idx", 0).is_some());
    }

    #[tokio::test]
    async fn open_shard_is_idempotent() {
        let (_dir, mgr) = create_shard_manager();
        let e1 = mgr.open_shard("idx", 0).unwrap();
        let e2 = mgr.open_shard("idx", 0).unwrap();
        // Both should point to the same engine (Arc)
        assert!(std::sync::Arc::ptr_eq(&e1, &e2));
    }

    // ── get_index_shards / all_shards ───────────────────────────────────

    #[tokio::test]
    async fn get_index_shards_returns_correct_set() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard("idx-a", 0).unwrap();
        mgr.open_shard("idx-a", 1).unwrap();
        mgr.open_shard("idx-b", 0).unwrap();

        let shards_a = mgr.get_index_shards("idx-a");
        assert_eq!(shards_a.len(), 2);

        let shards_b = mgr.get_index_shards("idx-b");
        assert_eq!(shards_b.len(), 1);
    }

    #[tokio::test]
    async fn all_shards_returns_everything() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard("idx-a", 0).unwrap();
        mgr.open_shard("idx-b", 0).unwrap();
        assert_eq!(mgr.all_shards().len(), 2);
    }

    // ── close_index_shards ──────────────────────────────────────────────

    #[tokio::test]
    async fn close_index_shards_removes_and_cleans_up() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard("to-delete", 0).unwrap();
        mgr.open_shard("to-delete", 1).unwrap();
        mgr.open_shard("keep", 0).unwrap();

        mgr.close_index_shards("to-delete").unwrap();

        assert!(mgr.get_shard("to-delete", 0).is_none());
        assert!(mgr.get_shard("to-delete", 1).is_none());
        assert!(mgr.get_shard("keep", 0).is_some());
    }

    // ── ISR Tracker ─────────────────────────────────────────────────────

    #[test]
    fn isr_tracker_empty_returns_no_replicas() {
        let tracker = IsrTracker::new(100);
        let isr = tracker.in_sync_replicas("idx", 0, 10);
        assert!(isr.is_empty());
    }

    #[test]
    fn isr_tracker_update_and_query_checkpoint() {
        let tracker = IsrTracker::new(100);
        tracker.update_replica_checkpoint("idx", 0, "replica-1", 50);
        tracker.update_replica_checkpoint("idx", 0, "replica-2", 90);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 2);

        // Both are within max_lag=100 of primary_checkpoint=100
        let isr = tracker.in_sync_replicas("idx", 0, 100);
        assert_eq!(isr.len(), 2);
    }

    #[test]
    fn isr_tracker_lagging_replica_excluded() {
        let tracker = IsrTracker::new(10); // tight lag threshold
        tracker.update_replica_checkpoint("idx", 0, "replica-1", 95);
        tracker.update_replica_checkpoint("idx", 0, "replica-2", 50); // way behind

        let isr = tracker.in_sync_replicas("idx", 0, 100);
        assert_eq!(isr.len(), 1);
        assert_eq!(isr[0], "replica-1");
    }

    #[test]
    fn isr_tracker_update_batch() {
        let tracker = IsrTracker::new(100);
        let checkpoints = vec![("r1".to_string(), 10), ("r2".to_string(), 20)];
        tracker.update_replica_checkpoints("idx", 0, &checkpoints);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 2);
    }

    #[test]
    fn isr_tracker_update_overwrites_checkpoint() {
        let tracker = IsrTracker::new(100);
        tracker.update_replica_checkpoint("idx", 0, "r1", 10);
        tracker.update_replica_checkpoint("idx", 0, "r1", 50);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].1, 50);
    }

    #[test]
    fn isr_tracker_remove_shard() {
        let tracker = IsrTracker::new(100);
        tracker.update_replica_checkpoint("idx", 0, "r1", 10);
        tracker.update_replica_checkpoint("idx", 1, "r1", 20);

        tracker.remove_shard("idx", 0);

        assert!(tracker.replica_checkpoints("idx", 0).is_empty());
        assert_eq!(tracker.replica_checkpoints("idx", 1).len(), 1);
    }

    #[test]
    fn isr_tracker_remove_index() {
        let tracker = IsrTracker::new(100);
        tracker.update_replica_checkpoint("idx-a", 0, "r1", 10);
        tracker.update_replica_checkpoint("idx-a", 1, "r1", 20);
        tracker.update_replica_checkpoint("idx-b", 0, "r1", 30);

        tracker.remove_index("idx-a");

        assert!(tracker.replica_checkpoints("idx-a", 0).is_empty());
        assert!(tracker.replica_checkpoints("idx-a", 1).is_empty());
        assert_eq!(tracker.replica_checkpoints("idx-b", 0).len(), 1);
    }

    #[test]
    fn isr_tracker_different_shards_independent() {
        let tracker = IsrTracker::new(100);
        tracker.update_replica_checkpoint("idx", 0, "r1", 10);
        tracker.update_replica_checkpoint("idx", 1, "r2", 20);

        let cps0 = tracker.replica_checkpoints("idx", 0);
        let cps1 = tracker.replica_checkpoints("idx", 1);
        assert_eq!(cps0.len(), 1);
        assert_eq!(cps1.len(), 1);
        assert_eq!(cps0[0].0, "r1");
        assert_eq!(cps1[0].0, "r2");
    }

    #[test]
    fn close_index_cleans_isr_tracker() {
        let (_dir, mgr) = create_shard_manager();
        mgr.isr_tracker
            .update_replica_checkpoint("my-idx", 0, "r1", 10);
        mgr.isr_tracker
            .update_replica_checkpoint("other-idx", 0, "r1", 20);

        mgr.close_index_shards("my-idx").unwrap();

        assert!(mgr.isr_tracker.replica_checkpoints("my-idx", 0).is_empty());
        assert_eq!(mgr.isr_tracker.replica_checkpoints("other-idx", 0).len(), 1);
    }
}
