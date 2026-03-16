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
