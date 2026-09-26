//! Shard management.
//! Each index has N primary shards. Each shard is backed by a `SearchEngine` implementation.
//! The ShardManager owns all local shard engines on this node.

use crate::cluster::settings::SettingsManager;
use crate::cluster::state::IndexSettings;
use crate::engine::{CompositeEngine, SearchEngine};
use crate::wal::{HotTranslog, TranslogDurability};
use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

pub const SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX: &str = "api_delete_index";
pub const SHARD_DATA_REMOVE_REASON_TRANSPORT_DELETE_INDEX: &str = "transport_delete_index_rpc";
pub const SHARD_DATA_REMOVE_REASON_ORPHAN_CLEANUP: &str = "orphan_cleanup_unknown_uuid";
pub const PEER_RECOVERY_IN_PROGRESS_MARKER: &str = "PEER_RECOVERY_IN_PROGRESS";
pub const PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER: &str = "PEER_RECOVERY_AWAITING_MEMBERSHIP";
type SourceRecoveryIdentity = (String, u32);
type SourceRecoveryLock = Arc<tokio::sync::Mutex<()>>;
type SourceRecoveryLockMap = HashMap<SourceRecoveryIdentity, SourceRecoveryLock>;

#[derive(Debug, thiserror::Error)]
#[error("shard reopen aborted for [{index}][{shard_id}] with UUID [{expected_uuid}]: {reason}")]
pub(crate) struct ShardReopenAborted {
    index: String,
    shard_id: u32,
    expected_uuid: String,
    reason: String,
}

#[derive(Clone, Copy)]
enum CompositeOpenMode {
    CreateOrOpen { allow_schema_reset: bool },
    ExistingOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerRecoveryAwaitingMembership {
    pub index_uuid: String,
    pub primary_node_id: String,
    pub primary_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRecoveryTargetState {
    Recovering,
    FinalizedAwaitingMembership(PeerRecoveryAwaitingMembership),
}

pub(crate) trait SourceRecoverySessionCleanup: Send + Sync {
    fn abort_shard<'a>(
        &'a self,
        index_uuid: &'a str,
        shard_id: u32,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;

    fn abort_index<'a>(
        &'a self,
        index_uuid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + 'a>>;
}

pub struct PeerRecoveryTargetInstall {
    pub index: String,
    pub shard_id: u32,
    pub mappings: HashMap<String, crate::cluster::state::FieldMapping>,
    pub settings: IndexSettings,
    pub index_uuid: String,
    pub shard_dir: PathBuf,
    pub snapshot_next_seq_no: u64,
    pub expected_files: Vec<String>,
}

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
    /// The replica's last known applied sequence high-water mark.
    /// This tracker does not prove contiguous application below the watermark.
    pub checkpoint: u64,
    /// When we last heard from this replica.
    pub last_updated: std::time::Instant,
}

/// Tracks replica checkpoint observations for primary shards on this node.
///
/// Raft routing metadata owns authoritative in-sync membership. The lag-based
/// view here is diagnostic only and cannot grant acknowledgement or promotion
/// eligibility.
pub struct IsrTracker {
    /// Per-shard, per-replica checkpoint tracking.
    /// Key: ShardKey, Value: HashMap<replica_node_id, ReplicaCheckpoint>
    replicas: RwLock<HashMap<ShardKey, HashMap<String, ReplicaCheckpoint>>>,
    /// Maximum allowed seq_no lag for the diagnostic lag-eligible view.
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

    /// Get replica node IDs whose observed checkpoint is within `max_lag`.
    /// This is not the authoritative in-sync set.
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
    source_recovery_locks: Mutex<SourceRecoveryLockMap>,
    #[cfg(test)]
    open_before_lock_sender: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    open_before_lock_release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    #[cfg(test)]
    reopen_after_cleanup_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(test)]
    reopen_after_cleanup_release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    #[cfg(test)]
    reopen_after_remove_sender: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    reopen_after_remove_release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    #[cfg(test)]
    reopen_before_lifecycle_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(test)]
    close_lifecycle_waiting_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    peer_recovery_targets: RwLock<HashMap<ShardKey, PeerRecoveryTargetState>>,
    source_recovery_cleanup: RwLock<Option<Arc<dyn SourceRecoverySessionCleanup>>>,
    /// ISR tracker for primary shards — tracks replica checkpoint lag.
    pub isr_tracker: IsrTracker,
    /// Translog durability mode for new shards.
    durability: TranslogDurability,
    /// Shared column cache for SQL fast-field Arrow arrays and grouped-partials
    /// full-segment decoded columns.
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
            source_recovery_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            open_before_lock_sender: Mutex::new(None),
            #[cfg(test)]
            open_before_lock_release: Mutex::new(None),
            #[cfg(test)]
            reopen_after_cleanup_sender: Mutex::new(None),
            #[cfg(test)]
            reopen_after_cleanup_release: Mutex::new(None),
            #[cfg(test)]
            reopen_after_remove_sender: Mutex::new(None),
            #[cfg(test)]
            reopen_after_remove_release: Mutex::new(None),
            #[cfg(test)]
            reopen_before_lifecycle_sender: Mutex::new(None),
            #[cfg(test)]
            close_lifecycle_waiting_sender: Mutex::new(None),
            peer_recovery_targets: RwLock::new(HashMap::new()),
            source_recovery_cleanup: RwLock::new(None),
            isr_tracker: IsrTracker::new(1000),
            durability,
            column_cache,
        }
    }

    /// Get the base data directory.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Number of live entries in the shared column cache.
    pub fn column_cache_entry_count(&self) -> u64 {
        self.column_cache.entry_count()
    }

    /// Configured maximum capacity of the shared column cache.
    pub fn column_cache_max_capacity(&self) -> u64 {
        self.column_cache.max_capacity()
    }

    pub(crate) fn register_source_recovery_cleanup(
        &self,
        cleanup: Arc<dyn SourceRecoverySessionCleanup>,
    ) {
        *self
            .source_recovery_cleanup
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(cleanup);
    }

    pub(crate) async fn abort_source_recovery_for_shard(
        &self,
        index_uuid: &str,
        shard_id: u32,
    ) -> Result<bool> {
        let cleanup = self
            .source_recovery_cleanup
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        match cleanup {
            Some(cleanup) => cleanup.abort_shard(index_uuid, shard_id).await,
            None => Ok(false),
        }
    }

    pub(crate) fn source_recovery_lifecycle_lock(
        &self,
        index_uuid: &str,
        shard_id: u32,
    ) -> Arc<tokio::sync::Mutex<()>> {
        self.source_recovery_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry((index_uuid.to_string(), shard_id))
            .or_default()
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn set_reopen_after_cleanup_gate(
        &self,
        sender: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        *self
            .reopen_after_cleanup_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
        *self
            .reopen_after_cleanup_release
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(release);
    }

    #[cfg(test)]
    pub(crate) fn set_reopen_before_lifecycle_signal(
        &self,
        sender: tokio::sync::oneshot::Sender<()>,
    ) {
        *self
            .reopen_before_lifecycle_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
    }

    #[cfg(test)]
    pub(crate) fn set_reopen_after_remove_gate(
        &self,
        sender: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self
            .reopen_after_remove_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
        *self
            .reopen_after_remove_release
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(release);
    }

    #[cfg(test)]
    pub(crate) fn set_close_lifecycle_waiting_signal(
        &self,
        sender: tokio::sync::oneshot::Sender<()>,
    ) {
        *self
            .close_lifecycle_waiting_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
    }

    pub(crate) async fn abort_source_recoveries_for_index(
        &self,
        index_uuid: &str,
    ) -> Result<usize> {
        let cleanup = self
            .source_recovery_cleanup
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        match cleanup {
            Some(cleanup) => cleanup.abort_index(index_uuid).await,
            None => Ok(0),
        }
    }

    fn ensure_reopen_target(&self, index: &str, shard_id: u32, expected_uuid: &str) -> Result<()> {
        let registered_uuid = self.index_uuid(index);
        if registered_uuid.as_deref() != Some(expected_uuid) {
            return Err(ShardReopenAborted {
                index: index.to_string(),
                shard_id,
                expected_uuid: expected_uuid.to_string(),
                reason: format!("registered UUID is {registered_uuid:?}"),
            }
            .into());
        }
        let key = ShardKey::new(index, shard_id);
        if !self
            .shards
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&key)
        {
            return Err(ShardReopenAborted {
                index: index.to_string(),
                shard_id,
                expected_uuid: expected_uuid.to_string(),
                reason: "the shard engine is no longer open".to_string(),
            }
            .into());
        }
        let shard_dir = self
            .data_dir
            .join(expected_uuid)
            .join(format!("shard_{shard_id}"));
        if !shard_dir.is_dir() {
            return Err(ShardReopenAborted {
                index: index.to_string(),
                shard_id,
                expected_uuid: expected_uuid.to_string(),
                reason: format!("the shard directory {shard_dir:?} no longer exists"),
            }
            .into());
        }
        let meta_path = shard_dir.join("index").join("meta.json");
        if !meta_path.is_file() {
            return Err(ShardReopenAborted {
                index: index.to_string(),
                shard_id,
                expected_uuid: expected_uuid.to_string(),
                reason: format!("the existing Tantivy metadata {meta_path:?} is missing"),
            }
            .into());
        }
        Ok(())
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
    fn open_composite_engine(
        &self,
        index: &str,
        shard_id: u32,
        shard_dir: &std::path::Path,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        mode: CompositeOpenMode,
    ) -> Result<Arc<CompositeEngine>> {
        const LOCK_BUSY_RETRIES: usize = 50;
        const LOCK_BUSY_RETRY_DELAY: Duration = Duration::from_millis(20);

        let mut cleaned_stale_schema = false;
        for attempt in 0..=LOCK_BUSY_RETRIES {
            let open_result = match mode {
                CompositeOpenMode::ExistingOnly => CompositeEngine::open_existing_with_mappings(
                    shard_dir,
                    refresh_interval,
                    mappings,
                    self.durability,
                    self.column_cache.clone(),
                ),
                CompositeOpenMode::CreateOrOpen { .. } => CompositeEngine::new_with_mappings(
                    shard_dir,
                    refresh_interval,
                    mappings,
                    self.durability,
                    self.column_cache.clone(),
                ),
            };
            match open_result {
                Ok(engine) => return Ok(Arc::new(engine)),
                Err(err) => {
                    let err_msg = err.to_string();
                    if matches!(
                        mode,
                        CompositeOpenMode::CreateOrOpen {
                            allow_schema_reset: true
                        }
                    ) && err_msg.contains("schema does not match")
                        && !cleaned_stale_schema
                    {
                        tracing::warn!(
                            "Schema mismatch for {}/shard_{}, removing stale data and retrying",
                            index,
                            shard_id
                        );
                        std::fs::remove_dir_all(shard_dir)?;
                        std::fs::create_dir_all(shard_dir)?;
                        cleaned_stale_schema = true;
                        continue;
                    }
                    if err_msg.contains("Failed to acquire index lock")
                        && attempt < LOCK_BUSY_RETRIES
                    {
                        tracing::debug!(
                            "Index lock busy reopening {}/shard_{} (attempt {}/{}), retrying",
                            index,
                            shard_id,
                            attempt + 1,
                            LOCK_BUSY_RETRIES
                        );
                        std::thread::sleep(LOCK_BUSY_RETRY_DELAY);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        unreachable!("lock busy retry loop should return or error before exhaustion")
    }

    pub fn open_shard_with_settings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
    ) -> Result<Arc<dyn SearchEngine>> {
        self.open_shard_with_settings_mode(index, shard_id, mappings, settings, index_uuid, true)
    }

    pub fn open_shard_with_settings_strict(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
    ) -> Result<Arc<dyn SearchEngine>> {
        self.open_shard_with_settings_mode(index, shard_id, mappings, settings, index_uuid, false)
    }

    fn open_shard_with_settings_mode(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
        allow_schema_reset: bool,
    ) -> Result<Arc<dyn SearchEngine>> {
        let key = ShardKey::new(index, shard_id);
        let shard_dir = self
            .data_dir
            .join(index_uuid)
            .join(format!("shard_{shard_id}"));
        if shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists() {
            anyhow::bail!("shard {index}/{shard_id} has an incomplete peer recovery installation");
        }

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
        #[cfg(test)]
        {
            if let Some(sender) = self
                .open_before_lock_sender
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            if let Some(release) = self
                .open_before_lock_release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = release.recv();
            }
        }
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

        if shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists() {
            anyhow::bail!("shard {index}/{shard_id} has an incomplete peer recovery installation");
        }
        let awaiting_membership_path = shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER);
        let awaiting_membership = if awaiting_membership_path.exists() {
            let pending: PeerRecoveryAwaitingMembership =
                serde_json::from_slice(&std::fs::read(&awaiting_membership_path)?)?;
            if pending.index_uuid != index_uuid {
                anyhow::bail!(
                    "peer recovery awaiting-membership marker UUID does not match shard metadata"
                );
            }
            Some(pending)
        } else {
            None
        };

        self.register_index_uuid(index, index_uuid);

        // Ensure a settings manager exists for this index
        let settings_mgr = self.ensure_settings_manager(index, settings);
        let refresh_interval = settings_mgr.refresh_interval();
        let refresh_rx = settings_mgr.watch_refresh_interval();
        let flush_threshold_rx = settings_mgr.watch_flush_threshold();

        std::fs::create_dir_all(&shard_dir)?;
        let stale_snapshot_dir = shard_dir.join("peer-recovery");
        if stale_snapshot_dir.exists() {
            Self::remove_dir_all_with_retry(&stale_snapshot_dir)?;
        }

        let engine = self.open_composite_engine(
            index,
            shard_id,
            &shard_dir,
            refresh_interval,
            mappings,
            CompositeOpenMode::CreateOrOpen { allow_schema_reset },
        )?;
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
        shards.insert(key.clone(), dyn_engine.clone());
        drop(shards);
        if let Some(pending) = awaiting_membership {
            self.peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(
                    key,
                    PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                );
        }
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
        index_uuid: impl Into<String> + Send + 'static,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        let uuid_str = index_uuid.into();
        tokio::task::spawn_blocking(move || {
            shard_manager
                .open_shard_with_settings(&index, shard_id, &mappings, &settings, &uuid_str)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking shard open task failed: {e}"))?
    }

    pub async fn open_shard_with_settings_strict_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        settings: IndexSettings,
        index_uuid: impl Into<String> + Send + 'static,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        let uuid_str = index_uuid.into();
        tokio::task::spawn_blocking(move || {
            shard_manager
                .open_shard_with_settings_strict(&index, shard_id, &mappings, &settings, &uuid_str)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking strict shard open task failed: {e}"))?
    }

    pub fn begin_peer_recovery_target(&self, index: &str, shard_id: u32) -> bool {
        let key = ShardKey::new(index, shard_id);
        let mut targets = self
            .peer_recovery_targets
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let std::collections::hash_map::Entry::Vacant(entry) = targets.entry(key) {
            entry.insert(PeerRecoveryTargetState::Recovering);
            true
        } else {
            false
        }
    }

    pub fn end_peer_recovery_target(&self, index: &str, shard_id: u32) {
        if let Some(shard_dir) = self.shard_data_dir(index, shard_id) {
            let _ = std::fs::remove_file(shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER));
        }
        self.peer_recovery_targets
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&ShardKey::new(index, shard_id));
    }

    pub fn is_peer_recovery_target(&self, index: &str, shard_id: u32) -> bool {
        self.peer_recovery_targets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&ShardKey::new(index, shard_id))
    }

    pub fn rejects_live_replication(&self, index: &str, shard_id: u32) -> bool {
        matches!(
            self.peer_recovery_targets
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(&ShardKey::new(index, shard_id)),
            Some(PeerRecoveryTargetState::Recovering)
        )
    }

    pub fn peer_recovery_target_states(&self) -> Vec<(ShardKey, PeerRecoveryTargetState)> {
        self.peer_recovery_targets
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect()
    }

    pub async fn mark_peer_recovery_awaiting_membership_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        pending: PeerRecoveryAwaitingMembership,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            if !matches!(
                shard_manager
                    .peer_recovery_targets
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&key),
                Some(PeerRecoveryTargetState::Recovering)
            ) {
                anyhow::bail!(
                    "peer recovery target is not in the recovering state before completion"
                );
            }
            let shard_dir = shard_manager
                .data_dir
                .join(&pending.index_uuid)
                .join(format!("shard_{shard_id}"));
            let marker_path = shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER);
            let temporary_path = marker_path.with_extension("tmp");
            let bytes = serde_json::to_vec(&pending)?;
            let mut marker = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary_path)?;
            use std::io::Write;
            marker.write_all(&bytes)?;
            marker.sync_all()?;
            std::fs::rename(&temporary_path, &marker_path)?;
            std::fs::File::open(&shard_dir)?.sync_all()?;
            shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(
                    key,
                    PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                );
            Ok(())
        })
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "blocking peer recovery awaiting-membership publication failed: {error}"
            )
        })?
    }

    pub async fn clear_peer_recovery_awaiting_membership_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let shard_dir = shard_manager
                .data_dir
                .join(index_uuid)
                .join(format!("shard_{shard_id}"));
            match std::fs::remove_file(shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)) {
                Ok(()) => std::fs::File::open(&shard_dir)?.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&ShardKey::new(&index, shard_id));
            Ok(())
        })
        .await
        .map_err(|error| {
            anyhow::anyhow!("blocking peer recovery awaiting-membership cleanup failed: {error}")
        })?
    }

    pub async fn prepare_peer_recovery_target_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
    ) -> Result<PathBuf> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = {
                let mut locks = shard_manager
                    .open_locks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                locks.entry(key.clone()).or_default().clone()
            };
            let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());
            shard_manager
                .shards
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
            shard_manager.isr_tracker.remove_shard(&index, shard_id);
            shard_manager.register_index_uuid(&index, &index_uuid);

            let shard_dir = shard_manager
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            if shard_dir.exists() {
                Self::remove_dir_all_with_retry(&shard_dir)?;
            }
            std::fs::create_dir_all(shard_dir.join("index"))?;
            let marker_path = shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER);
            let marker = std::fs::File::create(&marker_path)?;
            marker.sync_all()?;
            std::fs::File::open(&shard_dir)?.sync_all()?;
            Ok(shard_dir)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking peer recovery target preparation failed: {e}"))?
    }

    pub async fn finalize_peer_recovery_target_blocking(
        self: &Arc<Self>,
        install: PeerRecoveryTargetInstall,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let PeerRecoveryTargetInstall {
                index,
                shard_id,
                mappings,
                settings,
                index_uuid,
                shard_dir,
                snapshot_next_seq_no,
                mut expected_files,
            } = install;
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = {
                let mut locks = shard_manager
                    .open_locks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                locks.entry(key.clone()).or_default().clone()
            };
            let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());
            if !shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists() {
                anyhow::bail!("peer recovery marker disappeared before install finalization");
            }

            HotTranslog::initialize_empty_at(
                &shard_dir,
                shard_manager.durability,
                snapshot_next_seq_no,
            )?;
            let committed_path = shard_dir.join("translog.committed");
            let mut committed = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&committed_path)?;
            use std::io::Write;
            write!(committed, "{snapshot_next_seq_no}")?;
            committed.sync_all()?;
            std::fs::File::open(shard_dir.join("index"))?.sync_all()?;

            shard_manager.register_index_uuid(&index, &index_uuid);
            let settings_manager = shard_manager.ensure_settings_manager(&index, &settings);
            let refresh_interval = settings_manager.refresh_interval();
            let refresh_rx = settings_manager.watch_refresh_interval();
            let flush_threshold_rx = settings_manager.watch_flush_threshold();
            let engine = shard_manager.open_composite_engine(
                &index,
                shard_id,
                &shard_dir,
                refresh_interval,
                &mappings,
                CompositeOpenMode::CreateOrOpen {
                    allow_schema_reset: false,
                },
            )?;
            let mut actual_files = engine.peer_recovery_commit_files()?;
            expected_files.sort();
            actual_files.sort();
            if actual_files != expected_files {
                anyhow::bail!(
                    "installed peer recovery commit file set does not match the source snapshot"
                );
            }

            let has_vectors = mappings.values().any(|mapping| {
                matches!(
                    mapping.field_type,
                    crate::cluster::state::FieldType::KnnVector
                )
            });
            if has_vectors {
                engine.rebuild_vectors()?;
            }

            std::fs::remove_file(shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER))?;
            std::fs::File::open(&shard_dir)?.sync_all()?;

            CompositeEngine::start_refresh_loop_reactive(
                engine.clone(),
                refresh_rx,
                flush_threshold_rx,
            );
            let dynamic_engine: Arc<dyn SearchEngine> = engine;
            shard_manager
                .shards
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, dynamic_engine.clone());
            Ok(dynamic_engine)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking peer recovery target finalization failed: {e}"))?
    }

    pub async fn abort_peer_recovery_target_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            shard_manager
                .shards
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
            shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            let shard_dir = shard_manager
                .data_dir
                .join(index_uuid)
                .join(format!("shard_{shard_id}"));
            std::fs::create_dir_all(&shard_dir)?;
            match std::fs::remove_file(shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let marker = std::fs::File::create(shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER))?;
            marker.sync_all()?;
            std::fs::File::open(shard_dir)?.sync_all()?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking peer recovery abort failed: {e}"))?
    }

    /// Close an existing shard engine and reopen it with updated mappings.
    ///
    /// Dynamic mapping uses this after the Raft AddMappings commit succeeds so
    /// the live shard immediately picks up the new typed fields instead of
    /// waiting for a later restart or maintenance reopen.
    pub async fn reopen_shard(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        settings: IndexSettings,
        index_uuid: String,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        tokio::spawn(async move {
            #[cfg(test)]
            if let Some(sender) = shard_manager
                .reopen_before_lifecycle_sender
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            let source_recovery_lock =
                shard_manager.source_recovery_lifecycle_lock(&index_uuid, shard_id);
            let _source_recovery_guard = source_recovery_lock.lock_owned().await;
            shard_manager.ensure_reopen_target(&index, shard_id, &index_uuid)?;
            shard_manager
                .abort_source_recovery_for_shard(&index_uuid, shard_id)
                .await?;
            #[cfg(test)]
            {
                if let Some(sender) = shard_manager
                    .reopen_after_cleanup_sender
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                {
                    let _ = sender.send(());
                }
                let release = shard_manager
                    .reopen_after_cleanup_release
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
                if let Some(release) = release {
                    let _ = release.await;
                }
            }
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = {
                let mut locks = shard_manager
                    .open_locks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                locks.entry(key.clone()).or_default().clone()
            };
            let blocking_manager = shard_manager.clone();
            tokio::task::spawn_blocking(move || {
                let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());
                blocking_manager.ensure_reopen_target(&index, shard_id, &index_uuid)?;
                let old_engine = {
                    let shards = blocking_manager
                        .shards
                        .read()
                        .unwrap_or_else(|e| e.into_inner());
                    shards
                        .get(&key)
                        .cloned()
                        .ok_or_else(|| ShardReopenAborted {
                            index: index.clone(),
                            shard_id,
                            expected_uuid: index_uuid.clone(),
                            reason: "the shard engine disappeared before replacement".to_string(),
                        })?
                };
                let _ = old_engine.flush();
                drop(old_engine);
                let removed = blocking_manager
                    .shards
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&key);
                if removed.is_none() {
                    return Err(ShardReopenAborted {
                        index: index.clone(),
                        shard_id,
                        expected_uuid: index_uuid.clone(),
                        reason: "the shard engine disappeared before replacement".to_string(),
                    }
                    .into());
                }
                drop(removed);
                #[cfg(test)]
                {
                    if let Some(sender) = blocking_manager
                        .reopen_after_remove_sender
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        let _ = sender.send(());
                    }
                    if let Some(release) = blocking_manager
                        .reopen_after_remove_release
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take()
                    {
                        let _ = release.recv();
                    }
                }
                let settings_mgr = blocking_manager.ensure_settings_manager(&index, &settings);
                let refresh_interval = settings_mgr.refresh_interval();
                let refresh_rx = settings_mgr.watch_refresh_interval();
                let flush_threshold_rx = settings_mgr.watch_flush_threshold();
                let shard_dir = blocking_manager
                    .data_dir
                    .join(&index_uuid)
                    .join(format!("shard_{shard_id}"));
                let meta_path = shard_dir.join("index").join("meta.json");
                if !meta_path.is_file() {
                    return Err(ShardReopenAborted {
                        index: index.clone(),
                        shard_id,
                        expected_uuid: index_uuid.clone(),
                        reason: format!("the existing Tantivy metadata {meta_path:?} is missing"),
                    }
                    .into());
                }
                let engine = blocking_manager.open_composite_engine(
                    &index,
                    shard_id,
                    &shard_dir,
                    refresh_interval,
                    &mappings,
                    CompositeOpenMode::ExistingOnly,
                )?;
                CompositeEngine::start_refresh_loop_reactive(
                    engine.clone(),
                    refresh_rx,
                    flush_threshold_rx,
                );
                tracing::info!(
                    "Reopened shard engine for {}/{} at {:?}",
                    index,
                    shard_id,
                    shard_dir
                );
                let dyn_engine: Arc<dyn SearchEngine> = engine;
                blocking_manager
                    .shards
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(key, dyn_engine.clone());
                Ok(dyn_engine)
            })
            .await
            .map_err(|e| anyhow::anyhow!("blocking shard reopen task failed: {e}"))?
        })
        .await
        .map_err(|e| anyhow::anyhow!("shard reopen task failed: {e}"))?
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

    #[cfg(test)]
    pub(crate) fn insert_shard_for_test(
        &self,
        index: &str,
        shard_id: u32,
        engine: Arc<dyn SearchEngine>,
    ) {
        let key = ShardKey::new(index, shard_id);
        self.shards
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key, engine);
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
        self.close_index_shards_with_reason(index, "unspecified")
    }

    /// Close and remove all shard engines for an index, then delete the data directory.
    /// `reason` is logged with any on-disk removal so destructive paths can be
    /// distinguished in restart diagnostics.
    pub fn close_index_shards_with_reason(&self, index: &str, reason: &'static str) -> Result<()> {
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
                tracing::warn!(
                    reason = reason,
                    index = index,
                    uuid = uuid.as_str(),
                    path = ?index_dir,
                    "Removing shard data directory"
                );
                Self::remove_dir_all_with_retry(&index_dir)?;
                tracing::info!(
                    reason = reason,
                    index = index,
                    uuid = uuid.as_str(),
                    path = ?index_dir,
                    "Removed shard data directory"
                );
            }
        }
        Ok(())
    }

    /// Async wrapper for shard shutdown + directory deletion on Tokio call sites.
    pub async fn close_index_shards_blocking(self: &Arc<Self>, index: String) -> Result<()> {
        self.close_index_shards_blocking_with_reason(index, "unspecified")
            .await
    }

    /// Async wrapper for shard shutdown + directory deletion on Tokio call sites.
    pub async fn close_index_shards_blocking_with_reason(
        self: &Arc<Self>,
        index: String,
        reason: &'static str,
    ) -> Result<()> {
        let mut source_recovery_guards = Vec::new();
        if let Some(index_uuid) = self.index_uuid(&index) {
            let mut lifecycle_locks = self
                .source_recovery_locks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter(|((uuid, _), _)| uuid == &index_uuid)
                .map(|((_, shard_id), lock)| (*shard_id, lock.clone()))
                .collect::<Vec<_>>();
            lifecycle_locks.sort_unstable_by_key(|(shard_id, _)| *shard_id);
            for (_, lock) in lifecycle_locks {
                let guard = match lock.clone().try_lock_owned() {
                    Ok(guard) => guard,
                    Err(_) => {
                        #[cfg(test)]
                        if let Some(sender) = self
                            .close_lifecycle_waiting_sender
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .take()
                        {
                            let _ = sender.send(());
                        }
                        lock.lock_owned().await
                    }
                };
                source_recovery_guards.push(guard);
            }
            self.abort_source_recoveries_for_index(&index_uuid).await?;
        }
        let mut open_locks = self
            .open_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter(|(key, _)| key.index == index)
            .map(|(key, lock)| (key.shard_id, lock.clone()))
            .collect::<Vec<_>>();
        open_locks.sort_unstable_by_key(|(shard_id, _)| *shard_id);
        let shard_manager = self.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _open_guards = open_locks
                .iter()
                .map(|(_, lock)| lock.lock().unwrap_or_else(|error| error.into_inner()))
                .collect::<Vec<_>>();
            shard_manager.close_index_shards_with_reason(&index, reason)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking shard close task failed: {e}"))?;
        drop(source_recovery_guards);
        result
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
        self.index_uuid(index)
            .map(|uuid| self.data_dir.join(&uuid).join(format!("shard_{shard_id}")))
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
            if dir_name == "raft"
                || dir_name == "raft-disk"
                || dir_name == crate::storage::REMOTE_STORE_DIR_NAME
            {
                continue;
            }
            if !known_uuids.contains(&dir_name) {
                tracing::warn!(
                    reason = SHARD_DATA_REMOVE_REASON_ORPHAN_CLEANUP,
                    uuid = dir_name.as_str(),
                    path = ?path,
                    "Removing orphaned data directory because it is not present in authoritative known UUIDs"
                );
                if let Err(e) = Self::remove_dir_all_with_retry(&path) {
                    tracing::warn!(
                        reason = SHARD_DATA_REMOVE_REASON_ORPHAN_CLEANUP,
                        uuid = dir_name.as_str(),
                        path = ?path,
                        error = %e,
                        "Failed to remove orphaned data directory"
                    );
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
            .map_err(|e| anyhow::anyhow!("blocking orphan cleanup task failed: {e}"))?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopen_rechecks_identity_after_waiting_for_open_lock() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-old",
            )
            .unwrap();

        let (reopen_entered_tx, reopen_entered_rx) = tokio::sync::oneshot::channel();
        let (reopen_release_tx, reopen_release_rx) = tokio::sync::oneshot::channel();
        manager.set_reopen_after_cleanup_gate(reopen_entered_tx, reopen_release_rx);
        let reopen_manager = manager.clone();
        let reopen = tokio::spawn(async move {
            reopen_manager
                .reopen_shard(
                    "idx".into(),
                    0,
                    HashMap::new(),
                    IndexSettings::default(),
                    "uuid-old".into(),
                )
                .await
        });
        reopen_entered_rx.await.unwrap();

        manager
            .close_index_shards_with_reason("idx", SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX)
            .unwrap();
        reopen_release_tx.send(()).unwrap();
        let error = match reopen.await.unwrap() {
            Ok(_) => panic!("reopen recreated a shard that was deleted while it waited"),
            Err(error) => error,
        };
        assert!(error.is::<ShardReopenAborted>());
        assert!(manager.get_shard("idx", 0).is_none());
        assert!(!dir.path().join("uuid-old").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_during_reopen_open_window_does_not_resurrect_directory() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let held = manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-old",
            )
            .unwrap();
        held.add_document("before", json!({"value": 0})).unwrap();

        let (removed_tx, removed_rx) = std::sync::mpsc::channel();
        let (reopen_release_tx, reopen_release_rx) = std::sync::mpsc::channel();
        manager.set_reopen_after_remove_gate(removed_tx, reopen_release_rx);
        let reopen_manager = manager.clone();
        let reopen = tokio::spawn(async move {
            reopen_manager
                .reopen_shard(
                    "idx".into(),
                    0,
                    HashMap::new(),
                    IndexSettings::default(),
                    "uuid-old".into(),
                )
                .await
        });
        tokio::task::spawn_blocking(move || {
            removed_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("reopen did not enter the engine-open window")
        })
        .await
        .unwrap();

        let (close_waiting_tx, close_waiting_rx) = tokio::sync::oneshot::channel();
        manager.set_close_lifecycle_waiting_signal(close_waiting_tx);
        let close_manager = manager.clone();
        let close = tokio::spawn(async move {
            close_manager
                .close_index_shards_blocking_with_reason(
                    "idx".into(),
                    SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX,
                )
                .await
        });
        let close_waited_for_reopen =
            tokio::time::timeout(Duration::from_secs(2), close_waiting_rx).await;

        drop(held);
        reopen_release_tx.send(()).unwrap();
        let reopen_result = tokio::time::timeout(Duration::from_secs(5), reopen)
            .await
            .expect("reopen did not finish")
            .unwrap();
        let close_result = tokio::time::timeout(Duration::from_secs(5), close)
            .await
            .expect("delete did not finish")
            .unwrap();

        assert!(
            close_waited_for_reopen.is_ok(),
            "delete did not wait on the registered lifecycle lock"
        );
        assert!(reopen_result.is_ok());
        close_result.unwrap();
        assert!(manager.get_shard("idx", 0).is_none());
        assert!(!dir.path().join("uuid-old").exists());

        let recreated = manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-new",
            )
            .unwrap();
        recreated.add_document("new", json!({"value": 1})).unwrap();
        recreated.flush().unwrap();
        assert_eq!(
            manager.shard_data_dir("idx", 0),
            Some(dir.path().join("uuid-new/shard_0"))
        );
        assert!(dir.path().join("uuid-new/shard_0").exists());
        assert!(!dir.path().join("uuid-old").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reopen_refuses_missing_existing_tantivy_index() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let held = manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-old",
            )
            .unwrap();

        let (removed_tx, removed_rx) = std::sync::mpsc::channel();
        let (reopen_release_tx, reopen_release_rx) = std::sync::mpsc::channel();
        manager.set_reopen_after_remove_gate(removed_tx, reopen_release_rx);
        let reopen_manager = manager.clone();
        let reopen = tokio::spawn(async move {
            reopen_manager
                .reopen_shard(
                    "idx".into(),
                    0,
                    HashMap::new(),
                    IndexSettings::default(),
                    "uuid-old".into(),
                )
                .await
        });
        tokio::task::spawn_blocking(move || {
            removed_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("reopen did not enter the engine-open window")
        })
        .await
        .unwrap();

        std::fs::remove_dir_all(dir.path().join("uuid-old")).unwrap();
        drop(held);
        reopen_release_tx.send(()).unwrap();
        let error = match tokio::time::timeout(Duration::from_secs(5), reopen)
            .await
            .expect("reopen did not finish")
            .unwrap()
        {
            Ok(_) => panic!("reopen created a fresh index after its directory disappeared"),
            Err(error) => error,
        };
        assert!(error.is::<ShardReopenAborted>());
        assert!(manager.get_shard("idx", 0).is_none());
        assert!(!dir.path().join("uuid-old").exists());
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
            "blocking shard-open wrapper stalled the async runtime for {elapsed:?}"
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
    #[should_panic(expected = "IndexUuid must not be empty")]
    fn index_uuid_rejects_empty_construction() {
        crate::cluster::state::IndexUuid::new("");
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

    #[test]
    fn cleanup_orphaned_data_skips_remote_store_dir() {
        let (_dir, mgr) = create_shard_manager();
        let data_dir = mgr.data_dir().to_path_buf();

        // Simulate what `StorageManager::new_in_path` creates on a live node
        // and some per-index content under it.
        let remote_store = data_dir.join(crate::storage::REMOTE_STORE_DIR_NAME);
        let remote_index_uuid = remote_store.join("idx-uuid-1");
        std::fs::create_dir_all(remote_index_uuid.join("manifests")).unwrap();
        std::fs::write(
            remote_index_uuid.join("manifest.current.json"),
            "{\"version\":1}",
        )
        .unwrap();

        // Also create an unrelated orphan UUID directory that SHOULD be removed.
        let orphan_uuid = data_dir.join("dead-beef-1234");
        std::fs::create_dir_all(&orphan_uuid).unwrap();
        std::fs::write(orphan_uuid.join("marker"), b"x").unwrap();

        // And a known UUID directory that should be retained because it appears
        // in the authoritative known_uuids set.
        let known_uuid = data_dir.join("live-uuid-9999");
        std::fs::create_dir_all(&known_uuid).unwrap();
        std::fs::write(known_uuid.join("marker"), b"x").unwrap();

        let mut known = std::collections::HashSet::new();
        known.insert("live-uuid-9999".to_string());

        mgr.cleanup_orphaned_data(&known);

        assert!(
            remote_store.exists(),
            "_remote_store must never be treated as an orphan UUID directory"
        );
        assert!(
            remote_index_uuid.join("manifest.current.json").exists(),
            "remote_store contents must survive orphan cleanup"
        );
        assert!(
            !orphan_uuid.exists(),
            "unknown UUID directories should still be removed"
        );
        assert!(
            known_uuid.exists(),
            "known UUID directories must be retained"
        );
    }

    #[tokio::test]
    async fn peer_recovery_marker_blocks_normal_shard_open() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let shard_dir = manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into())
            .await
            .unwrap();

        assert!(shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists());
        let error = match manager.open_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
        ) {
            Ok(_) => panic!("normal shard open must reject an in-progress recovery marker"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("incomplete peer recovery installation")
        );
    }

    #[test]
    fn peer_recovery_marker_created_while_open_waits_is_rechecked() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *manager.open_before_lock_sender.lock().unwrap() = Some(entered_tx);
        *manager.open_before_lock_release.lock().unwrap() = Some(release_rx);

        let open_manager = manager.clone();
        let open = std::thread::spawn(move || {
            open_manager.open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let shard_dir = dir.path().join("uuid-1/shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::File::create(shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER)).unwrap();
        release_tx.send(()).unwrap();

        let error = match open.join().unwrap() {
            Ok(_) => panic!("open must reject a marker created while waiting"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("incomplete peer recovery installation")
        );
        assert!(manager.get_shard("idx", 0).is_none());
    }

    #[tokio::test]
    async fn strict_recovery_open_refuses_schema_mismatch_without_wiping() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let shard_dir = dir.path().join("uuid-1").join("shard_0");
        let original_mappings = HashMap::from([(
            "value".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        {
            let engine = CompositeEngine::new_with_mappings(
                &shard_dir,
                Duration::from_secs(60),
                &original_mappings,
                TranslogDurability::Request,
                Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
            )
            .unwrap();
            engine
                .add_document("doc-1", json!({"value": "preserved"}))
                .unwrap();
            engine.refresh().unwrap();
        }
        let meta_before = std::fs::read(shard_dir.join("index/meta.json")).unwrap();

        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let incompatible = HashMap::from([(
            "value".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        )]);
        let error = match manager.open_shard_with_settings_strict(
            "idx",
            0,
            &incompatible,
            &IndexSettings::default(),
            "uuid-1",
        ) {
            Ok(_) => panic!("strict recovery open must reject an incompatible schema"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("schema does not match"));
        assert_eq!(
            std::fs::read(shard_dir.join("index/meta.json")).unwrap(),
            meta_before,
            "strict recovery open must not invoke the schema-mismatch wipe fallback"
        );

        let reopened = manager
            .open_shard_with_settings_strict(
                "idx",
                0,
                &original_mappings,
                &IndexSettings::default(),
                "uuid-1",
            )
            .unwrap();
        assert_eq!(
            reopened.get_document("doc-1").unwrap().unwrap()["value"],
            json!("preserved")
        );
    }

    #[tokio::test]
    async fn finalized_peer_recovery_install_opens_exact_snapshot() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = CompositeEngine::new(source_dir.path(), Duration::from_secs(60)).unwrap();
        source.add_document("doc-1", json!({"value": 1})).unwrap();
        source.add_document("doc-2", json!({"value": 2})).unwrap();
        let snapshot_dir = source_dir.path().join("peer-recovery/session");
        let snapshot = source.create_peer_recovery_snapshot(&snapshot_dir).unwrap();

        let target_dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let shard_dir = manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into())
            .await
            .unwrap();
        for file in &snapshot.files {
            let destination = shard_dir.join("index").join(&file.name);
            std::fs::copy(snapshot_dir.join(&file.name), &destination).unwrap();
            std::fs::File::open(destination)
                .unwrap()
                .sync_all()
                .unwrap();
        }

        let engine = manager
            .finalize_peer_recovery_target_blocking(PeerRecoveryTargetInstall {
                index: "idx".into(),
                shard_id: 0,
                mappings: HashMap::new(),
                settings: IndexSettings::default(),
                index_uuid: "uuid-1".into(),
                shard_dir: shard_dir.clone(),
                snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
                expected_files: snapshot
                    .files
                    .iter()
                    .map(|file| file.name.clone())
                    .collect(),
            })
            .await
            .unwrap();
        assert_eq!(engine.doc_count(), 2);
        assert!(!shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists());
        source
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
    }
}
