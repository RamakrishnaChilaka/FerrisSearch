//! Shard management.
//! Each index has N primary shards. Each shard is backed by a `SearchEngine` implementation.
//! The ShardManager owns all local shard engines on this node.

use crate::engine::{HotEngine, SearchEngine};
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
        Self { index: index.into(), shard_id }
    }
    /// Returns the directory name for this shard's data
    pub fn data_dir(&self) -> String {
        format!("{}/shard_{}", self.index, self.shard_id)
    }
}

/// Manages all shard engines on this node.
/// Each shard is backed by a `dyn SearchEngine` (currently `HotEngine`).
pub struct ShardManager {
    /// Base data directory for this node (e.g. `data/node-1`)
    data_dir: PathBuf,
    /// Refresh interval applied to each new shard engine
    refresh_interval: Duration,
    /// Map from (index, shard_id) → engine
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
}

impl ShardManager {
    pub fn new(data_dir: impl Into<PathBuf>, refresh_interval: Duration) -> Self {
        Self {
            data_dir: data_dir.into(),
            refresh_interval,
            shards: RwLock::new(HashMap::new()),
        }
    }

    /// Open or create the engine for a specific shard, starting its refresh loop.
    /// Called when an index is created or when the node recovers a shard on startup.
    pub fn open_shard(&self, index: &str, shard_id: u32) -> Result<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                return Ok(engine.clone());
            }
        }

        // Create the shard data directory: <node_data_dir>/<index>/shard_<id>/
        let shard_dir = self.data_dir.join(&key.data_dir());
        std::fs::create_dir_all(&shard_dir)?;

        let engine = Arc::new(HotEngine::new(&shard_dir, self.refresh_interval)?);
        HotEngine::start_refresh_loop(engine.clone());

        tracing::info!("Opened shard engine for {}/{} at {:?}", index, shard_id, shard_dir);

        let dyn_engine: Arc<dyn SearchEngine> = engine;
        let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
        shards.insert(key, dyn_engine.clone());
        Ok(dyn_engine)
    }

    /// Get an already-open shard engine. Returns None if this shard isn't local.
    pub fn get_shard(&self, index: &str, shard_id: u32) -> Option<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);
        self.shards.read().unwrap_or_else(|e| e.into_inner()).get(&key).cloned()
    }

    /// Return all local shard engines for a given index (for scatter-gather operations).
    pub fn get_index_shards(&self, index: &str) -> Vec<(u32, Arc<dyn SearchEngine>)> {
        self.shards.read().unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(k, _)| k.index == index)
            .map(|(k, e)| (k.shard_id, e.clone()))
            .collect()
    }

    /// Return all local shard engines across all indices.
    pub fn all_shards(&self) -> Vec<(ShardKey, Arc<dyn SearchEngine>)> {
        self.shards.read().unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect()
    }

    /// Close and remove all shard engines for an index, then delete the data directory.
    pub fn close_index_shards(&self, index: &str) -> Result<()> {
        let mut shards = self.shards.write().unwrap_or_else(|e| e.into_inner());
        let keys_to_remove: Vec<ShardKey> = shards.keys()
            .filter(|k| k.index == index)
            .cloned()
            .collect();
        for key in &keys_to_remove {
            shards.remove(key);
        }
        drop(shards);

        // Delete the index data directory on disk
        let index_dir = self.data_dir.join(index);
        if index_dir.exists() {
            std::fs::remove_dir_all(&index_dir)?;
            tracing::info!("Removed shard data for index '{}' at {:?}", index, index_dir);
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
        engine.add_document("d1", json!({"hello": "world"})).unwrap();
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
}
