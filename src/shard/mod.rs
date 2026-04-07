//! Shard management.
//! Each index has N primary shards. Each shard is backed by a `SearchEngine` implementation.
//! The ShardManager owns all local shard engines on this node.

use crate::cluster::settings::SettingsManager;
use crate::cluster::state::IndexSettings;
use crate::engine::{CompositeEngine, SearchEngine};
use crate::wal::TranslogDurability;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
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
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    /// Per-index reactive settings managers.
    settings_managers: RwLock<HashMap<String, Arc<SettingsManager>>>,
    /// Maps index_name → index UUID (used for on-disk directory names).
    index_uuids: RwLock<HashMap<String, String>>,
    /// Serializes concurrent open attempts for the same shard key so only
    /// one thread performs the expensive CompositeEngine creation at a time.
    open_locks: Mutex<HashMap<ShardKey, Arc<Mutex<()>>>>,
    /// ISR tracker for primary shards — tracks replica checkpoint lag.
    pub isr_tracker: IsrTracker,
    /// Translog durability mode for new shards.
    durability: TranslogDurability,
    /// Shared column cache for SQL fast-field Arrow arrays.
    column_cache: Arc<crate::engine::column_cache::ColumnCache>,
}

impl ShardManager {
    pub fn new(data_dir: impl Into<PathBuf>, refresh_interval: Duration) -> Self {
        Self::new_with_durability(data_dir, refresh_interval, TranslogDurability::Request)
    }

    pub fn new_with_durability(
        data_dir: impl Into<PathBuf>,
        _refresh_interval: Duration,
        durability: TranslogDurability,
    ) -> Self {
        Self::new_full(
            data_dir,
            durability,
            Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
    }

    pub fn new_full(
        data_dir: impl Into<PathBuf>,
        durability: TranslogDurability,
        column_cache: Arc<crate::engine::column_cache::ColumnCache>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            shards: RwLock::new(HashMap::new()),
            settings_managers: RwLock::new(HashMap::new()),
            index_uuids: RwLock::new(HashMap::new()),
            open_locks: Mutex::new(HashMap::new()),
            isr_tracker: IsrTracker::new(1000),
            durability,
            column_cache,
        }
    }

    /// Get the base data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Open or create the engine for a specific shard.
    /// Uses CompositeEngine which handles both text and vector indexing.
    /// Generates a random UUID for the on-disk directory (suitable for tests).
    pub fn open_shard(&self, index: &str, shard_id: u32) -> Result<Arc<dyn SearchEngine>> {
        self.open_shard_with_mappings(index, shard_id, &HashMap::new())
    }

    /// Ensure a SettingsManager exists for this index, creating one if necessary.
    fn ensure_settings_manager(
        &self,
        index: &str,
        settings: &IndexSettings,
    ) -> Arc<SettingsManager> {
        {
            let managers = self
                .settings_managers
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(mgr) = managers.get(index) {
                return mgr.clone();
            }
        }
        let mgr = Arc::new(SettingsManager::new(settings));
        let mut managers = self
            .settings_managers
            .write()
            .unwrap_or_else(|e| e.into_inner());
        managers.entry(index.to_string()).or_insert(mgr).clone()
    }

    /// Open or create the engine for a specific shard with explicit field mappings.
    /// Local/test helpers reuse one generated UUID per index so all shard paths
    /// stay under the same index root.
    pub fn open_shard_with_mappings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> Result<Arc<dyn SearchEngine>> {
        let generated_uuid = self.get_or_generate_uuid(index);
        self.open_shard_with_settings(
            index,
            shard_id,
            mappings,
            &IndexSettings::default(),
            &generated_uuid,
        )
    }

    /// Open or create the engine for a specific shard with explicit field mappings
    /// and per-index settings. The settings manager provides a watch channel so
    /// the refresh loop automatically adjusts when settings change.
    ///
    /// `index_uuid` determines the on-disk directory: `<data_dir>/<uuid>/shard_<id>`.
    /// Callers must provide the authoritative UUID from cluster metadata.
    pub fn open_shard_with_settings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
    ) -> Result<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);

        // Fast path: shard already open.
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                return Ok(engine.clone());
            }
        }

        // Serialize concurrent open attempts for the same shard key.
        // This prevents two threads from both creating a CompositeEngine
        // on the same directory (which causes a Tantivy LockBusy error).
        let per_shard_lock = {
            let mut locks = self.open_locks.lock().unwrap_or_else(|e| e.into_inner());
            locks.entry(key.clone()).or_default().clone()
        };
        let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Re-check after acquiring the per-shard lock — a concurrent caller
        // may have finished opening this shard while we were waiting.
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                return Ok(engine.clone());
            }
        }

        if index_uuid.is_empty() {
            return Err(anyhow::anyhow!(
                "missing authoritative UUID for index '{}' — refusing to open shard {}/{}",
                index,
                index,
                shard_id
            ));
        }

        self.register_index_uuid(index, index_uuid);
        let uuid = index_uuid.to_string();

        // Ensure a settings manager exists for this index
        let settings_mgr = self.ensure_settings_manager(index, settings);
        let refresh_interval = settings_mgr.refresh_interval();
        let refresh_rx = settings_mgr.watch_refresh_interval();
        let flush_threshold_rx = settings_mgr.watch_flush_threshold();

        let shard_dir = self
            .data_dir
            .join(&uuid)
            .join(format!("shard_{}", shard_id));
        std::fs::create_dir_all(&shard_dir)?;

        // Try to open the engine. If it fails with a schema mismatch (stale data
        // from a previous run whose index was already deleted/re-created), wipe the
        // orphaned directory and retry with a fresh index.
        let engine = match CompositeEngine::new_with_mappings(
            &shard_dir,
            refresh_interval,
            mappings,
            self.durability,
            self.column_cache.clone(),
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
                        refresh_interval,
                        mappings,
                        self.durability,
                        self.column_cache.clone(),
                    )?)
                } else {
                    return Err(first_err);
                }
            }
        };
        CompositeEngine::start_refresh_loop_reactive(
            engine.clone(),
            refresh_rx,
            flush_threshold_rx,
        );

        // Only rebuild vectors when the index has knn_vector fields — otherwise
        // the 100K-doc MatchAll search is pure waste and can OOM on large indices.
        let has_vectors = mappings
            .values()
            .any(|m| matches!(m.field_type, crate::cluster::state::FieldType::KnnVector));
        if has_vectors && let Err(e) = engine.rebuild_vectors() {
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

    /// Async wrapper for shard open/create on Tokio call sites.
    /// Shard open can perform blocking filesystem recovery and engine startup work.
    pub async fn open_shard_with_settings_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        settings: IndexSettings,
        index_uuid: String,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            shard_manager.open_shard_with_settings(
                &index,
                shard_id,
                &mappings,
                &settings,
                &index_uuid,
            )
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking shard open task failed: {}", e))?
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
    /// Uses the stored UUID mapping to find the correct on-disk directory.
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

        // Clean settings manager for this index
        {
            let mut managers = self
                .settings_managers
                .write()
                .unwrap_or_else(|e| e.into_inner());
            managers.remove(index);
        }

        // Look up UUID for this index and delete the UUID-based directory
        let uuid = {
            let mut uuids = self.index_uuids.write().unwrap_or_else(|e| e.into_inner());
            uuids.remove(index)
        };

        if let Some(uuid) = uuid {
            let index_dir = self.data_dir.join(&uuid);
            if index_dir.exists() {
                Self::remove_dir_all_with_retry(&index_dir)?;
                tracing::info!(
                    "Removed shard data for index '{}' (uuid={}) at {:?}",
                    index,
                    uuid,
                    index_dir
                );
            }
        }
        Ok(())
    }

    /// Async wrapper for shard shutdown + directory deletion on Tokio call sites.
    pub async fn close_index_shards_blocking(self: &Arc<Self>, index: String) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || shard_manager.close_index_shards(&index))
            .await
            .map_err(|e| anyhow::anyhow!("blocking shard close task failed: {}", e))?
    }

    fn remove_dir_all_with_retry(path: &std::path::Path) -> Result<()> {
        const MAX_ATTEMPTS: usize = 10;
        const RETRY_DELAY: Duration = Duration::from_millis(20);

        for attempt in 0..MAX_ATTEMPTS {
            match std::fs::remove_dir_all(path) {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err)
                    if err.kind() == std::io::ErrorKind::DirectoryNotEmpty
                        && attempt + 1 < MAX_ATTEMPTS =>
                {
                    std::thread::sleep(RETRY_DELAY);
                }
                Err(err) => return Err(err.into()),
            }
        }

        unreachable!("directory removal retry loop must return or error");
    }

    /// Apply updated settings to a running index.
    /// This notifies all shard engines' consumers (e.g. refresh loop) via watch channels.
    pub fn apply_settings(&self, index: &str, new_settings: &IndexSettings) {
        let settings_mgr = self.ensure_settings_manager(index, new_settings);
        settings_mgr.update(new_settings);
    }

    /// Get the settings manager for an index, if one exists.
    pub fn get_settings_manager(&self, index: &str) -> Option<Arc<SettingsManager>> {
        self.settings_managers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(index)
            .cloned()
    }

    /// Register or update the UUID for an index.
    pub fn register_index_uuid(&self, index: &str, uuid: &str) {
        let mut uuids = self.index_uuids.write().unwrap_or_else(|e| e.into_inner());
        uuids.insert(index.to_string(), uuid.to_string());
    }

    /// Get the UUID for an index if one is registered, or generate and store one.
    /// Used only by local/test helpers that do not have cluster metadata yet.
    fn get_or_generate_uuid(&self, index: &str) -> String {
        if let Some(uuid) = self.index_uuid(index) {
            return uuid;
        }

        let new_uuid = uuid::Uuid::new_v4().to_string();
        let mut uuids = self.index_uuids.write().unwrap_or_else(|e| e.into_inner());
        uuids
            .entry(index.to_string())
            .or_insert_with(|| new_uuid.clone())
            .clone()
    }

    /// Get the UUID for an index if one is registered.
    pub fn index_uuid(&self, index: &str) -> Option<String> {
        self.index_uuids
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(index)
            .cloned()
    }

    /// Return the on-disk path for a shard, using the registered UUID.
    pub fn shard_data_dir(&self, index: &str, shard_id: u32) -> Option<PathBuf> {
        self.index_uuid(index).map(|uuid| {
            self.data_dir
                .join(&uuid)
                .join(format!("shard_{}", shard_id))
        })
    }

    /// Delete any directories under `data_dir` that don't correspond to a known
    /// index UUID. Called on startup to clean up stale data from deleted indices.
    pub fn cleanup_orphaned_data(&self, known_uuids: &std::collections::HashSet<String>) {
        let entries = match std::fs::read_dir(&self.data_dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let dir_name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            // Skip known directories (raft data, etc.)
            if dir_name == "raft" || dir_name == "raft-disk" {
                continue;
            }
            if !known_uuids.contains(&dir_name) {
                tracing::info!(
                    "Removing orphaned data directory: {:?} (not in known UUIDs)",
                    path
                );
                if let Err(e) = Self::remove_dir_all_with_retry(&path) {
                    tracing::warn!("Failed to remove orphaned directory {:?}: {}", path, e);
                }
            }
        }
    }

    /// Async wrapper for orphan cleanup on Tokio call sites.
    pub async fn cleanup_orphaned_data_blocking(
        self: &Arc<Self>,
        known_uuids: std::collections::HashSet<String>,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || shard_manager.cleanup_orphaned_data(&known_uuids))
            .await
            .map_err(|e| anyhow::anyhow!("blocking orphan cleanup task failed: {}", e))?;
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

    #[tokio::test(flavor = "current_thread")]
    async fn open_shard_with_settings_blocking_does_not_starve_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let manager = mgr.clone();
        let task = tokio::spawn(async move {
            let _ = started_tx.send(());
            manager
                .open_shard_with_settings_blocking(
                    "idx".to_string(),
                    0,
                    HashMap::new(),
                    IndexSettings::default(),
                    "uuid-1".to_string(),
                )
                .await
                .unwrap()
        });

        let start = std::time::Instant::now();
        started_rx.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "blocking shard-open wrapper stalled the async runtime for {:?}",
            elapsed
        );

        task.await.unwrap();
        assert!(mgr.get_shard("idx", 0).is_some());
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

    #[tokio::test]
    async fn open_shard_reuses_generated_uuid_for_same_index() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard("idx", 0).unwrap();
        let first_uuid = mgr.index_uuid("idx").unwrap();

        mgr.open_shard("idx", 1).unwrap();
        let second_uuid = mgr.index_uuid("idx").unwrap();

        assert_eq!(first_uuid, second_uuid);
        assert_eq!(
            mgr.shard_data_dir("idx", 0)
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
            mgr.shard_data_dir("idx", 1)
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        );
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

    // ── Settings manager integration ────────────────────────────────

    #[tokio::test]
    async fn open_shard_with_settings_creates_settings_manager() {
        let (_dir, mgr) = create_shard_manager();
        let settings = IndexSettings {
            refresh_interval_ms: Some(2000),
            ..Default::default()
        };
        mgr.open_shard_with_settings("idx", 0, &HashMap::new(), &settings, "uuid-1")
            .unwrap();

        let sm = mgr.get_settings_manager("idx");
        assert!(sm.is_some());
        assert_eq!(
            sm.unwrap().refresh_interval(),
            std::time::Duration::from_millis(2000)
        );
    }

    #[tokio::test]
    async fn get_settings_manager_returns_none_for_unknown_index() {
        let (_dir, mgr) = create_shard_manager();
        assert!(mgr.get_settings_manager("no-such-index").is_none());
    }

    #[tokio::test]
    async fn apply_settings_updates_refresh_interval() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
        )
        .unwrap();

        let sm = mgr.get_settings_manager("idx").unwrap();
        let rx = sm.watch_refresh_interval();
        assert_eq!(
            *rx.borrow(),
            std::time::Duration::from_millis(crate::cluster::settings::DEFAULT_REFRESH_INTERVAL_MS)
        );

        mgr.apply_settings(
            "idx",
            &IndexSettings {
                refresh_interval_ms: Some(3000),
                ..Default::default()
            },
        );
        assert_eq!(*rx.borrow(), std::time::Duration::from_millis(3000));
    }

    #[tokio::test]
    async fn apply_settings_for_new_index_creates_manager() {
        let (_dir, mgr) = create_shard_manager();
        assert!(mgr.get_settings_manager("new-idx").is_none());

        mgr.apply_settings(
            "new-idx",
            &IndexSettings {
                refresh_interval_ms: Some(7000),
                ..Default::default()
            },
        );

        let sm = mgr.get_settings_manager("new-idx").unwrap();
        assert_eq!(
            sm.refresh_interval(),
            std::time::Duration::from_millis(7000)
        );
    }

    #[tokio::test]
    async fn close_index_shards_removes_settings_manager() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
        )
        .unwrap();
        assert!(mgr.get_settings_manager("idx").is_some());

        mgr.close_index_shards("idx").unwrap();
        assert!(mgr.get_settings_manager("idx").is_none());
    }

    #[tokio::test]
    async fn open_shard_with_default_settings_uses_cluster_default() {
        let (_dir, mgr) = create_shard_manager();
        mgr.open_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
        )
        .unwrap();

        let sm = mgr.get_settings_manager("idx").unwrap();
        assert_eq!(
            sm.refresh_interval(),
            std::time::Duration::from_millis(crate::cluster::settings::DEFAULT_REFRESH_INTERVAL_MS)
        );
    }

    #[tokio::test]
    async fn multiple_shards_share_settings_manager() {
        let (_dir, mgr) = create_shard_manager();
        let settings = IndexSettings {
            refresh_interval_ms: Some(4000),
            ..Default::default()
        };
        mgr.open_shard_with_settings("idx", 0, &HashMap::new(), &settings, "uuid-1")
            .unwrap();
        mgr.open_shard_with_settings("idx", 1, &HashMap::new(), &settings, "uuid-1")
            .unwrap();

        let sm0 = mgr.get_settings_manager("idx").unwrap();
        // Both shards use the same settings manager
        assert_eq!(
            sm0.refresh_interval(),
            std::time::Duration::from_millis(4000)
        );

        // Updating settings affects both shards' watcher
        let rx = sm0.watch_refresh_interval();
        mgr.apply_settings(
            "idx",
            &IndexSettings {
                refresh_interval_ms: Some(9000),
                ..Default::default()
            },
        );
        assert_eq!(*rx.borrow(), std::time::Duration::from_millis(9000));
    }

    #[test]
    fn open_shard_with_settings_rejects_missing_uuid() {
        let (_dir, mgr) = create_shard_manager();
        let err = mgr
            .open_shard_with_settings("idx", 0, &HashMap::new(), &IndexSettings::default(), "")
            .err()
            .unwrap();

        assert!(err.to_string().contains("missing authoritative UUID"));
    }

    #[tokio::test]
    async fn open_shard_skips_rebuild_vectors_for_non_vector_mappings() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::new(dir.path(), Duration::from_secs(60));

        // Non-vector mappings only
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "score".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let engine = mgr
            .open_shard_with_settings("idx", 0, &mappings, &IndexSettings::default(), "test-uuid")
            .unwrap();

        // Index some docs and flush
        engine
            .add_document("d1", json!({"title": "hello", "score": 10}))
            .unwrap();
        engine.refresh().unwrap();

        // Engine opened successfully without triggering 100K-doc MatchAll
        assert_eq!(engine.doc_count(), 1);
    }
}
