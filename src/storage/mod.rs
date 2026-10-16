//! Object-store-backed storage abstraction.
//!
//! Dispatches at construction time between a local filesystem backend and a
//! remote S3 backend based on the URL scheme of the storage root.
//! All manifest I/O flows through `Arc<dyn ObjectStore>` and works against
//! either backend.
//!
//! Supported schemes:
//!   * bare path (e.g. `/var/data/_remote_store`) — `LocalFileSystem`
//!   * `file://<path>` — `LocalFileSystem`
//!   * `s3://<bucket>[/prefix]` — `AmazonS3` (requires AWS credentials in env)
//!
//! AWS credentials for s3:// backends are read from the standard environment
//! variables supported by `object_store::aws::AmazonS3Builder::from_env`
//! (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_SESSION_TOKEN,
//! AWS_ENDPOINT_URL, etc). The `AWS_ENDPOINT_URL` env var is how a MinIO or
//! RustFS dev server is pointed at.

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::{
    GetResult, ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder,
    local::LocalFileSystem, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Mutex;
use url::Url;

mod conditional;
pub mod inventory;
pub mod janitor;
pub mod publication;

#[cfg(test)]
use conditional::LocalCasCrashStage;
use conditional::{CapabilityProbeState, detect_local_mount_fstype, is_uncertified_network_fstype};
pub use conditional::{
    CompareAndSwapOutcome, CompareToken, ConditionalRead, ConditionalStorageError,
    CreateImmutableOutcome, ImmutableObjectIdentity, PublicationCapabilityStatus,
    run_shared_conditional_race_and_contract_suite,
};
pub use inventory::{
    DEFAULT_INVENTORY_ACTIVE_GRACE_MS, DEFAULT_INVENTORY_MAX_REPORTED_OBJECTS,
    DEFAULT_INVENTORY_RETENTION_GENERATIONS, DEFAULT_INVENTORY_RETENTION_HORIZON_MS,
    InventoryObjectEntry, InventoryObjectKind, InventorySummaryCounts, ObjectReachabilityClass,
    ReachabilityInventoryOptions, ReachabilityInventoryReport,
};
pub use janitor::{
    DEFAULT_JANITOR_MAX_DELETE_BYTES_PER_RUN, DEFAULT_JANITOR_MAX_DELETES_PER_RUN,
    DEFAULT_JANITOR_SWEEP_GRACE_MS, JANITOR_MARKS_FORMAT_VERSION, JanitorCandidateAction,
    JanitorCandidateDecision, JanitorMarkEntry, JanitorMarksDocument, JanitorOptions,
    JanitorRunReport, janitor_marks_key,
};
pub use publication::{
    FormatCapability, MAX_LINEAGE_WALK_GENERATIONS, PUBLICATION_RECORD_FORMAT_CAPABILITY,
    PUBLICATION_RECORD_FORMAT_VERSION, PublicationAttemptOutcome, PublicationAttemptRecord,
    PublicationCommitOutcome, PublicationCommittedRecord, PublicationCrashStage, PublicationError,
    PublicationIntentRecord, PublicationPhase, PublicationPreparedRecord, PublicationResolution,
    REMOTE_MANIFEST_FORMAT_CAPABILITY, REMOTE_MANIFEST_POINTER_FORMAT_CAPABILITY,
    SPLIT_BUNDLE_FORMAT_CAPABILITY, SPLIT_BUNDLE_FORMAT_VERSION, SequencerCommitRequest,
    ShardPublishedWatermark, SplitDocVersionRecord, SplitSealRangeMetadata, SplitTombstoneRecord,
    compute_publication_request_fingerprint, deterministic_publication_doc_id,
    deterministic_publication_split_id, is_canonical_manifest_path, is_canonical_split_bundle_path,
    manifest_operation_key, publication_attempt_key, publication_committed_key,
    publication_intent_key, publication_prepared_key, validate_publication_operation_id,
};

/// Directory name used under `<data_dir>/` for the node-local remote_store root.
/// Exported so orphan cleanup (which otherwise treats unknown top-level data_dir
/// entries as UUID directories) can exclude it.
pub const REMOTE_STORE_DIR_NAME: &str = "_remote_store";

/// Directory name used under `<data_dir>/` for the node-local split cache.
pub const REMOTE_STORE_CACHE_DIR_NAME: &str = "_remote_store_cache";

/// Directory name used under `<data_dir>/` for in-progress split builds.
pub const REMOTE_STORE_STAGING_DIR_NAME: &str = "_remote_store_staging";

/// Magic header prefix written at the start of every packed split bundle.
/// `FSBND` + version byte. Bumping the version byte reserves room for a
/// future format change without needing a separate checksum scheme.
const BUNDLE_MAGIC: &[u8; 6] = b"FSBND\x01";
pub const REMOTE_MANIFEST_MIN_READ_FORMAT_VERSION: u32 = 2;
pub const REMOTE_MANIFEST_FORMAT_VERSION: u32 = 3;
pub const REMOTE_MANIFEST_POINTER_MIN_READ_FORMAT_VERSION: u32 = 2;
pub const REMOTE_MANIFEST_POINTER_FORMAT_VERSION: u32 = 3;

/// Backend behind a `StorageManager`. Remote backends have no local root.
#[derive(Debug, Clone)]
enum Backend {
    Local {
        root: PathBuf,
        local: Arc<LocalFileSystem>,
    },
    Remote {
        /// The object_store trait object (e.g. `AmazonS3`).
        store: Arc<dyn ObjectStore>,
        /// The original URI (e.g. `s3://bucket/prefix`) — kept for diagnostics.
        uri: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedSplitStatus {
    pub artifact_cached: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteStoreLoadSnapshot {
    pub inflight_bytes: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Clone)]
struct CachedSplitEntry {
    index_uuid: String,
    split_id: String,
    size_bytes: u64,
    last_access_epoch_ms: u64,
    reservation: Option<Arc<StdMutex<crate::worker::ResourceReservation>>>,
}

fn cache_entry_key(index_uuid: &str, split_id: &str) -> String {
    format!("{index_uuid}/{split_id}")
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn dir_size_bytes(path: &Path) -> u64 {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }

    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            total += dir_size_bytes(&entry.path());
        }
    }
    total
}

pub struct SplitCachePin {
    key: String,
    pin_counts: Arc<StdMutex<HashMap<String, usize>>>,
}

impl Drop for SplitCachePin {
    fn drop(&mut self) {
        let mut pin_counts = self.pin_counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = pin_counts.get_mut(&self.key) {
            if *count <= 1 {
                pin_counts.remove(&self.key);
            } else {
                *count -= 1;
            }
        }
    }
}

pub struct RemoteStoreLoadGuard {
    bytes: u64,
    inflight_bytes: Arc<AtomicU64>,
    queue_depth: Arc<AtomicUsize>,
}

impl Drop for RemoteStoreLoadGuard {
    fn drop(&mut self) {
        self.inflight_bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct StorageManager {
    backend: Backend,
    /// Node-local directory for split download/extract cache and for
    /// in-progress split builds staged before upload. Always set.
    local_workdir: PathBuf,
    /// Serializes concurrent `publish_manifest` calls on this process so
    /// two in-process publishers cannot race on the generation pointer.
    /// The fs/object_store backend does not give us compare-and-set on the
    /// pointer object, so we serialize in-process for correctness and leave
    /// cross-process publish to a dedicated indexer role in a later PR.
    publish_lock: Arc<Mutex<()>>,
    /// Per-split async mutex used to single-flight concurrent download+
    /// extract operations against the local cache. Key format:
    /// `"<index_uuid>/<split_id>"`.
    split_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Per-index async mutex used by the active sequencer to serialize
    /// single-sequencer manifest commits on this node.
    sequencer_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    cache_entries: Arc<StdMutex<HashMap<String, CachedSplitEntry>>>,
    pin_counts: Arc<StdMutex<HashMap<String, usize>>>,
    remote_store_inflight_bytes: Arc<AtomicU64>,
    remote_store_queue_depth: Arc<AtomicUsize>,
    capability_state: Arc<StdMutex<CapabilityProbeState>>,
    capability_probe_lock: Arc<Mutex<()>>,
    cached_writer_authorities:
        Arc<StdMutex<HashMap<String, crate::cluster::state::ManifestWriterAuthority>>>,
    publication_crash_stage: Arc<StdMutex<Option<PublicationCrashStage>>>,
    janitor_fail_delete_paths: Arc<StdMutex<HashSet<String>>>,
    resource_governor: Arc<crate::worker::ResourceGovernor>,
    admission: Arc<crate::worker::AdmissionController>,
    #[cfg(test)]
    local_cas_crash_stage: Arc<StdMutex<Option<LocalCasCrashStage>>>,
}

impl std::fmt::Debug for crate::worker::ResourceGovernor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceGovernor").finish()
    }
}

impl std::fmt::Debug for crate::worker::AdmissionController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionController").finish()
    }
}

impl StorageManager {
    /// Construct a StorageManager from a storage URI and a node-local working
    /// directory for split build-staging and cache.
    ///
    /// Accepts bare paths, `file://` URLs, and `s3://bucket[/prefix]` URLs for
    /// the storage URI. `local_workdir` must be a writable local filesystem
    /// path; it's created if missing.
    pub fn new(path: String, local_workdir: PathBuf) -> Result<Self> {
        let (backend, initial_capability) = match storage_scheme(&path) {
            StorageScheme::Local => {
                let root = normalize_local_root(&path)?;
                std::fs::create_dir_all(&root)
                    .with_context(|| format!("failed to create storage root {root:?}"))?;
                let root = root
                    .canonicalize()
                    .with_context(|| format!("failed to canonicalize storage root {root:?}"))?;
                let capability = match detect_local_mount_fstype(&root) {
                    Some(fstype) if is_uncertified_network_fstype(&fstype) => {
                        CapabilityProbeState::Unsupported(format!(
                            "shared or network filesystem '{fstype}' is not certified for conditional manifest publication (requires atomic local lock, rename, and fsync semantics)"
                        ))
                    }
                    _ => CapabilityProbeState::Unprobed,
                };
                let local =
                    Arc::new(LocalFileSystem::new_with_prefix(&root).with_context(|| {
                        format!("failed to create object_store backend for {root:?}")
                    })?);
                (Backend::Local { root, local }, capability)
            }
            StorageScheme::S3 => {
                let (store, unsupported_reason) = build_s3_backend(&path)?;
                let capability = match unsupported_reason {
                    Some(reason) => CapabilityProbeState::Unsupported(reason),
                    None => CapabilityProbeState::Unprobed,
                };
                (
                    Backend::Remote {
                        store,
                        uri: path.clone(),
                    },
                    capability,
                )
            }
            StorageScheme::Unsupported(scheme) => {
                anyhow::bail!(
                    "unsupported storage URL scheme '{scheme}'; supported schemes: file, s3, or bare path"
                );
            }
        };

        std::fs::create_dir_all(&local_workdir).with_context(|| {
            format!("failed to create remote_store local workdir {local_workdir:?}")
        })?;
        let local_workdir = local_workdir.canonicalize().with_context(|| {
            format!("failed to canonicalize remote_store local workdir {local_workdir:?}")
        })?;

        Ok(Self {
            backend,
            local_workdir,
            publish_lock: Arc::new(Mutex::new(())),
            split_locks: Arc::new(Mutex::new(HashMap::new())),
            sequencer_locks: Arc::new(Mutex::new(HashMap::new())),
            cache_entries: Arc::new(StdMutex::new(HashMap::new())),
            pin_counts: Arc::new(StdMutex::new(HashMap::new())),
            remote_store_inflight_bytes: Arc::new(AtomicU64::new(0)),
            remote_store_queue_depth: Arc::new(AtomicUsize::new(0)),
            capability_state: Arc::new(StdMutex::new(initial_capability)),
            capability_probe_lock: Arc::new(Mutex::new(())),
            cached_writer_authorities: Arc::new(StdMutex::new(HashMap::new())),
            publication_crash_stage: Arc::new(StdMutex::new(None)),
            janitor_fail_delete_paths: Arc::new(StdMutex::new(HashSet::new())),
            resource_governor: Arc::new(crate::worker::ResourceGovernor::default()),
            admission: Arc::new(crate::worker::AdmissionController::new(4, 4)),
            #[cfg(test)]
            local_cas_crash_stage: Arc::new(StdMutex::new(None)),
        })
    }

    pub fn with_governance(
        mut self,
        resource_governor: Arc<crate::worker::ResourceGovernor>,
        admission: Arc<crate::worker::AdmissionController>,
    ) -> Self {
        self.resource_governor = resource_governor;
        self.admission = admission;
        self
    }

    pub fn resource_governor(&self) -> &Arc<crate::worker::ResourceGovernor> {
        &self.resource_governor
    }

    pub fn admission(&self) -> &Arc<crate::worker::AdmissionController> {
        &self.admission
    }

    /// Construct a `StorageManager` wrapping a caller-supplied `ObjectStore`
    /// implementation (used by integration and fault-injection tests to exercise
    /// remote conditional-put and capability-probe paths deterministically).
    pub fn new_with_object_store(
        uri: String,
        store: Arc<dyn ObjectStore>,
        local_workdir: PathBuf,
        unsupported_reason: Option<String>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&local_workdir).with_context(|| {
            format!("failed to create remote_store local workdir {local_workdir:?}")
        })?;
        let local_workdir = local_workdir.canonicalize().with_context(|| {
            format!("failed to canonicalize remote_store local workdir {local_workdir:?}")
        })?;
        let initial_capability = match unsupported_reason {
            Some(reason) => CapabilityProbeState::Unsupported(reason),
            None => CapabilityProbeState::Unprobed,
        };
        Ok(Self {
            backend: Backend::Remote { store, uri },
            local_workdir,
            publish_lock: Arc::new(Mutex::new(())),
            split_locks: Arc::new(Mutex::new(HashMap::new())),
            sequencer_locks: Arc::new(Mutex::new(HashMap::new())),
            cache_entries: Arc::new(StdMutex::new(HashMap::new())),
            pin_counts: Arc::new(StdMutex::new(HashMap::new())),
            remote_store_inflight_bytes: Arc::new(AtomicU64::new(0)),
            remote_store_queue_depth: Arc::new(AtomicUsize::new(0)),
            capability_state: Arc::new(StdMutex::new(initial_capability)),
            capability_probe_lock: Arc::new(Mutex::new(())),
            cached_writer_authorities: Arc::new(StdMutex::new(HashMap::new())),
            publication_crash_stage: Arc::new(StdMutex::new(None)),
            janitor_fail_delete_paths: Arc::new(StdMutex::new(HashSet::new())),
            resource_governor: Arc::new(crate::worker::ResourceGovernor::default()),
            admission: Arc::new(crate::worker::AdmissionController::new(4, 4)),
            #[cfg(test)]
            local_cas_crash_stage: Arc::new(StdMutex::new(None)),
        })
    }

    #[cfg(test)]
    pub(crate) fn set_local_cas_crash_stage_for_test(&self, stage: Option<LocalCasCrashStage>) {
        *self
            .local_cas_crash_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = stage;
    }

    #[cfg(test)]
    pub(crate) fn take_local_cas_crash_stage_for_test(&self) -> Option<LocalCasCrashStage> {
        self.local_cas_crash_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
    }

    #[cfg(test)]
    pub(crate) fn set_unsupported_capability_for_test(&self, reason: impl Into<String>) {
        *self
            .capability_state
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = CapabilityProbeState::Unsupported(reason.into());
    }

    /// Returns the on-disk root when the backend is local, `None` for remote.
    pub fn root(&self) -> Option<&Path> {
        match &self.backend {
            Backend::Local { root, .. } => Some(root.as_path()),
            Backend::Remote { .. } => None,
        }
    }

    /// URI describing the storage backend (e.g. `s3://bucket/prefix` for
    /// remote, canonical local path for local). Useful for log / error
    /// messages so operators can tell at a glance which backend is active.
    pub fn uri(&self) -> String {
        match &self.backend {
            Backend::Local { root, .. } => root.to_string_lossy().into_owned(),
            Backend::Remote { uri, .. } => uri.clone(),
        }
    }

    /// True when the backend is a local filesystem. Callers that still need
    /// direct disk access (for example, the initial remote_store publish path
    /// that builds a Tantivy index in place) gate behind this.
    pub fn is_local(&self) -> bool {
        matches!(&self.backend, Backend::Local { .. })
    }

    /// Construct a StorageManager rooted inside an existing directory
    /// (uses `<parent>/_remote_store` for the storage backend and
    /// `<parent>/_remote_store_cache` for the node-local split cache).
    /// Convenient for tests and for the node startup path that roots the
    /// store under `<data_dir>/_remote_store`.
    pub fn new_in_path(parent: &Path) -> Result<Self> {
        let root = parent.join(REMOTE_STORE_DIR_NAME);
        let workdir = parent.join(REMOTE_STORE_CACHE_DIR_NAME);
        Self::new(root.to_string_lossy().into_owned(), workdir)
    }

    /// Node-local working directory root (cache + staging live under this).
    pub fn local_workdir(&self) -> &Path {
        &self.local_workdir
    }

    /// Cache directory for a specific (index, split). The path is always
    /// computed from trusted IDs (never from untrusted manifest fields).
    fn cache_dir(&self, index_uuid: &str, split_id: &str) -> PathBuf {
        self.local_workdir
            .join("splits")
            .join(index_uuid)
            .join(split_id)
    }

    pub fn cached_split_size_bytes(&self, index_uuid: &str, split_id: &str) -> u64 {
        dir_size_bytes(&self.cache_dir(index_uuid, split_id))
    }

    pub fn record_cached_split_access(&self, index_uuid: &str, split_id: &str, size_bytes: u64) {
        self.record_cached_split_access_with_reservation(index_uuid, split_id, size_bytes, None);
    }

    fn record_cached_split_access_with_reservation(
        &self,
        index_uuid: &str,
        split_id: &str,
        size_bytes: u64,
        reservation: Option<crate::worker::ResourceReservation>,
    ) {
        let key = cache_entry_key(index_uuid, split_id);
        let mut cache_entries = self.cache_entries.lock().unwrap_or_else(|e| e.into_inner());
        let existing_reservation = cache_entries
            .get(&key)
            .and_then(|entry| entry.reservation.clone());
        let reservation = reservation
            .map(|res| Arc::new(StdMutex::new(res)))
            .or(existing_reservation)
            .or_else(|| {
                (size_bytes > 0)
                    .then(|| {
                        self.resource_governor
                            .try_reserve(
                                crate::worker::ResourcePoolKind::PersistentCache,
                                size_bytes,
                            )
                            .ok()
                            .map(|res| Arc::new(StdMutex::new(res)))
                    })
                    .flatten()
            });
        cache_entries.insert(
            key,
            CachedSplitEntry {
                index_uuid: index_uuid.to_string(),
                split_id: split_id.to_string(),
                size_bytes,
                last_access_epoch_ms: now_epoch_ms(),
                reservation,
            },
        );
    }

    pub fn cached_split_status(
        &self,
        index_uuid: &str,
        split_id: &str,
        checksum: &str,
    ) -> CachedSplitStatus {
        let cache_dir = self.cache_dir(index_uuid, split_id);
        let done_marker = cache_dir.join(".done");
        let artifact_cached = cache_dir.exists()
            && std::fs::read(&done_marker)
                .map(|bytes| bytes == checksum.as_bytes())
                .unwrap_or(false);
        CachedSplitStatus { artifact_cached }
    }

    pub fn pin_cached_split(&self, index_uuid: &str, split_id: &str) -> SplitCachePin {
        let key = cache_entry_key(index_uuid, split_id);
        let mut pin_counts = self.pin_counts.lock().unwrap_or_else(|e| e.into_inner());
        *pin_counts.entry(key.clone()).or_insert(0) += 1;
        SplitCachePin {
            key,
            pin_counts: Arc::clone(&self.pin_counts),
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_split_pin_count(&self, index_uuid: &str, split_id: &str) -> usize {
        let key = cache_entry_key(index_uuid, split_id);
        self.pin_counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .copied()
            .unwrap_or(0)
    }

    pub fn begin_remote_store_batch(&self, bytes: u64) -> RemoteStoreLoadGuard {
        self.remote_store_queue_depth
            .fetch_add(1, Ordering::Relaxed);
        self.remote_store_inflight_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        RemoteStoreLoadGuard {
            bytes,
            inflight_bytes: Arc::clone(&self.remote_store_inflight_bytes),
            queue_depth: Arc::clone(&self.remote_store_queue_depth),
        }
    }

    pub fn remote_store_load_snapshot(&self) -> RemoteStoreLoadSnapshot {
        RemoteStoreLoadSnapshot {
            inflight_bytes: self.remote_store_inflight_bytes.load(Ordering::Relaxed),
            queue_depth: self.remote_store_queue_depth.load(Ordering::Relaxed),
        }
    }

    pub fn reap_split_cache(
        &self,
        index_uuid: &str,
        live_split_ids: &HashSet<String>,
        budget_bytes: Option<u64>,
    ) -> Result<()> {
        let cache_root = self.local_workdir.join("splits").join(index_uuid);

        let mut entries: Vec<(String, CachedSplitEntry)> = {
            let cache_entries = self.cache_entries.lock().unwrap_or_else(|e| e.into_inner());
            cache_entries
                .iter()
                .filter(|(_, entry)| entry.index_uuid == index_uuid)
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect()
        };

        if cache_root.exists() {
            for entry in std::fs::read_dir(&cache_root)
                .with_context(|| format!("read cache root {cache_root:?}"))?
            {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let split_id = entry.file_name().to_string_lossy().to_string();
                let key = cache_entry_key(index_uuid, &split_id);
                if entries.iter().any(|(existing_key, _)| existing_key == &key) {
                    continue;
                }

                entries.push((
                    key,
                    CachedSplitEntry {
                        index_uuid: index_uuid.to_string(),
                        split_id,
                        size_bytes: dir_size_bytes(&entry.path()),
                        last_access_epoch_ms: 0,
                        reservation: None,
                    },
                ));
            }
        }

        let mut total_bytes: u64 = entries.iter().map(|(_, entry)| entry.size_bytes).sum();
        entries.sort_by_key(|(_, entry)| {
            (
                live_split_ids.contains(&entry.split_id),
                entry.last_access_epoch_ms,
            )
        });

        for (key, entry) in entries {
            let is_stale = !live_split_ids.contains(&entry.split_id);
            let over_budget = budget_bytes.is_some_and(|budget| total_bytes > budget);
            if !is_stale && !over_budget {
                continue;
            }

            let is_pinned = self
                .pin_counts
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .copied()
                .unwrap_or(0)
                > 0;
            if is_pinned {
                continue;
            }

            let cache_dir = self.cache_dir(index_uuid, &entry.split_id);
            if cache_dir.exists() {
                std::fs::remove_dir_all(&cache_dir)
                    .with_context(|| format!("remove stale cache dir {cache_dir:?}"))?;
            }
            self.cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
            total_bytes = total_bytes.saturating_sub(entry.size_bytes);
        }

        Ok(())
    }

    /// Staging directory for building a split in-progress. Used by the
    /// publish path; the staging tree never appears in the published
    /// storage backend.
    pub fn staging_dir(&self, index_uuid: &str, split_id: &str) -> PathBuf {
        self.local_workdir
            .join("staging")
            .join(index_uuid)
            .join(split_id)
    }

    /// Object-store key where a split's bundle is published. Derived from
    /// trusted IDs so the key is never attacker-controllable.
    pub fn split_bundle_key(index_uuid: &str, split_id: &str) -> String {
        format!("{index_uuid}/splits/{split_id}/bundle")
    }

    /// Acquire a per-split async mutex so concurrent downloads of the same
    /// split coalesce into a single fetch.
    async fn split_lock(&self, cache_key: &str) -> Arc<Mutex<()>> {
        let mut map = self.split_locks.lock().await;
        map.entry(cache_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Download a split's bundle from the object store, verify its checksum,
    /// and unpack it into the node-local cache. Reuses the cached copy on
    /// subsequent calls when the `.done` marker matches `expected_checksum`.
    ///
    /// `split` is trusted only for its `split_id`, `bundle_path`, and
    /// `checksum` fields; `bundle_path` is validated against the expected
    /// canonical key shape so a malicious manifest cannot escape the key
    /// namespace or reference another index's splits.
    pub async fn fetch_split_into_cache(
        &self,
        index_uuid: &str,
        split: &RemoteSplitManifest,
    ) -> Result<PathBuf> {
        let expected_key = Self::split_bundle_key(index_uuid, &split.split_id);
        let op_id = split
            .tags
            .get("operation_id")
            .map(String::as_str)
            .unwrap_or("");
        if !is_canonical_split_bundle_path(index_uuid, &split.split_id, op_id, &split.bundle_path) {
            anyhow::bail!(
                "bundle_path {} does not match canonical key {} for split {}",
                split.bundle_path,
                expected_key,
                split.split_id
            );
        }

        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;

        let cache_dir = self.cache_dir(index_uuid, &split.split_id);
        let done_marker = cache_dir.join(".done");
        if let Ok(bytes) = std::fs::read(&done_marker)
            && bytes == split.checksum.as_bytes()
        {
            self.record_cached_split_access(
                index_uuid,
                &split.split_id,
                self.cached_split_size_bytes(index_uuid, &split.split_id),
            );
            return Ok(cache_dir);
        }

        let current_ctx = crate::worker::current_request_context();
        let _hydration_timer =
            crate::worker::start_current_stage_timer(crate::worker::TraceStage::Hydration);
        let _hydration_lease = self
            .admission
            .acquire(crate::worker::WorkClass::Hydration, current_ctx.as_ref())
            .await?;

        let cache_key = format!("{}/{}", index_uuid, split.split_id);
        let guard_arc = self.split_lock(&cache_key).await;
        let _guard = match &current_ctx {
            Some(ctx) => {
                tokio::select! {
                    biased;
                    _ = ctx.wait_cancelled(crate::worker::ExecutionStage::Hydration) => {
                        ctx.check_active(crate::worker::ExecutionStage::Hydration)?;
                        return Err(crate::worker::RequestCancelledError {
                            stage: crate::worker::ExecutionStage::Hydration,
                            reason: crate::worker::CancellationReason::ClientDisconnected,
                        }.into());
                    }
                    guard = guard_arc.lock() => guard,
                }
            }
            None => guard_arc.lock().await,
        };

        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;

        // Re-check inside the lock: a concurrent caller may have just finished.
        if let Ok(bytes) = std::fs::read(&done_marker)
            && bytes == split.checksum.as_bytes()
        {
            self.record_cached_split_access(
                index_uuid,
                &split.split_id,
                self.cached_split_size_bytes(index_uuid, &split.split_id),
            );
            return Ok(cache_dir);
        }

        let estimated_inflight = split.size_bytes.max(1);
        let estimated_unpacked = split.uncompressed_bytes.max(split.size_bytes).max(1);
        let mut inflight_reservation = self.resource_governor.try_reserve(
            crate::worker::ResourcePoolKind::ObjectStoreInflight,
            estimated_inflight,
        )?;
        let mut temp_disk_reservation = self.resource_governor.try_reserve(
            crate::worker::ResourcePoolKind::TempDisk,
            estimated_unpacked,
        )?;
        let mut cache_reservation = self.resource_governor.try_reserve(
            crate::worker::ResourcePoolKind::PersistentCache,
            estimated_unpacked,
        )?;

        // Clean up any partial state from a previous failed extract.
        if cache_dir.exists() {
            std::fs::remove_dir_all(&cache_dir)
                .with_context(|| format!("failed to clear stale cache dir {cache_dir:?}"))?;
        }
        let tmp_dir = cache_dir.with_extension("tmp");
        if tmp_dir.exists() {
            std::fs::remove_dir_all(&tmp_dir)
                .with_context(|| format!("failed to clear stale tmp dir {tmp_dir:?}"))?;
        }
        std::fs::create_dir_all(&tmp_dir)
            .with_context(|| format!("failed to create tmp cache dir {tmp_dir:?}"))?;

        struct TempDirCleanupGuard {
            path: Option<PathBuf>,
        }
        impl Drop for TempDirCleanupGuard {
            fn drop(&mut self) {
                if let Some(path) = self.path.take()
                    && path.exists()
                {
                    let _ = std::fs::remove_dir_all(&path);
                }
            }
        }
        let mut tmp_cleanup = TempDirCleanupGuard {
            path: Some(tmp_dir.clone()),
        };

        let object_path = self.object_path(&split.bundle_path)?;
        let (bundle_bytes, actual_checksum) = match &current_ctx {
            Some(ctx) => {
                tokio::select! {
                    biased;
                    _ = ctx.wait_cancelled(crate::worker::ExecutionStage::Hydration) => {
                        ctx.check_active(crate::worker::ExecutionStage::Hydration)?;
                        return Err(crate::worker::RequestCancelledError {
                            stage: crate::worker::ExecutionStage::Hydration,
                            reason: crate::worker::CancellationReason::ClientDisconnected,
                        }.into());
                    }
                    res = self.download_and_hash_with_reservation(&object_path, &mut inflight_reservation) => res?,
                }
            }
            None => {
                self.download_and_hash_with_reservation(&object_path, &mut inflight_reservation)
                    .await?
            }
        };
        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;

        if actual_checksum != split.checksum {
            anyhow::bail!(
                "bundle checksum mismatch for split {}: expected {}, got {}",
                split.split_id,
                split.checksum,
                actual_checksum
            );
        }

        // Unpack in a blocking task so we don't stall the reactor on large
        // bundles.
        let tmp_dir_for_unpack = tmp_dir.clone();
        let unpack_result = tokio::task::spawn_blocking(move || {
            unpack_bundle_into(&bundle_bytes, &tmp_dir_for_unpack)
        })
        .await;
        drop(inflight_reservation);

        match unpack_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(e) => {
                return Err(anyhow::anyhow!("unpack task panicked: {e}"));
            }
        }

        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;
        let unpacked_bytes = dir_size_bytes(&tmp_dir);
        temp_disk_reservation.reconcile(unpacked_bytes)?;

        if let Some(parent) = cache_dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create cache parent {parent:?}"))?;
        }
        tmp_cleanup.path = None;
        std::fs::rename(&tmp_dir, &cache_dir).with_context(|| {
            format!("failed to promote tmp cache dir {tmp_dir:?} to final {cache_dir:?}")
        })?;
        drop(temp_disk_reservation);

        std::fs::write(&done_marker, split.checksum.as_bytes())
            .with_context(|| format!("failed to write done marker {done_marker:?}"))?;
        let actual_cached_bytes = self.cached_split_size_bytes(index_uuid, &split.split_id);
        cache_reservation.reconcile(actual_cached_bytes)?;
        self.record_cached_split_access_with_reservation(
            index_uuid,
            &split.split_id,
            actual_cached_bytes,
            Some(cache_reservation),
        );

        Ok(cache_dir)
    }

    /// Stream-download a split's bundle and compute its sha256 without
    /// writing anything to disk. Used by `verify_splits`.
    pub async fn hash_split_bundle(
        &self,
        index_uuid: &str,
        split: &RemoteSplitManifest,
    ) -> Result<String> {
        let expected_key = Self::split_bundle_key(index_uuid, &split.split_id);
        let op_id = split
            .tags
            .get("operation_id")
            .map(String::as_str)
            .unwrap_or("");
        if !is_canonical_split_bundle_path(index_uuid, &split.split_id, op_id, &split.bundle_path) {
            anyhow::bail!(
                "bundle_path {} does not match canonical key {} for split {}",
                split.bundle_path,
                expected_key,
                split.split_id
            );
        }
        let object_path = self.object_path(&split.bundle_path)?;
        let mut stream = self.get_object(&object_path).await?.into_stream();
        let mut hasher = Sha256::new();
        let mut downloaded_bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    crate::metrics::record_object_store_request("get", "error");
                    return Err(anyhow::Error::from(error));
                }
            };
            downloaded_bytes += chunk.len();
            hasher.update(&chunk);
        }
        crate::metrics::record_object_store_request("get", "success");
        crate::metrics::record_object_store_transfer("download", downloaded_bytes);
        Ok(format!("sha256:{:x}", hasher.finalize()))
    }

    /// Upload an already-built local split directory as a packed bundle
    /// to the object store. Returns the `(bundle_key, checksum, size_bytes)`
    /// the caller needs to fill in the manifest entry.
    pub async fn upload_split_bundle(
        &self,
        index_uuid: &str,
        split_id: &str,
        staging_dir: &Path,
    ) -> Result<(String, String, u64)> {
        self.ensure_publication_supported().await?;
        let staging_dir = staging_dir.to_path_buf();
        let (bundle_bytes, checksum) =
            tokio::task::spawn_blocking(move || pack_bundle_from_dir(&staging_dir))
                .await
                .map_err(|e| anyhow::anyhow!("pack task panicked: {e}"))??;
        let size_bytes = bundle_bytes.len() as u64;

        let bundle_key = Self::split_bundle_key(index_uuid, split_id);
        let object_path = self.object_path(&bundle_key)?;
        let outcome = self.create_immutable(&object_path, bundle_bytes).await?;
        outcome.ensure_matching_content(&object_path)?;
        Ok((bundle_key, checksum, size_bytes))
    }

    /// Stream-download an object into a `Vec<u8>` and compute its sha256 in
    /// the same pass. For MVP we buffer the whole bundle in memory; a
    /// future PR can switch to a file-backed staging buffer for very large
    /// bundles.
    async fn download_and_hash(&self, key: &ObjectPath) -> Result<(Vec<u8>, String)> {
        let mut stream = self.get_object(key).await?.into_stream();
        let mut buf: Vec<u8> = Vec::new();
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    crate::metrics::record_object_store_request("get", "error");
                    return Err(anyhow::Error::from(error));
                }
            };
            hasher.update(&chunk);
            buf.extend_from_slice(&chunk);
        }
        let checksum = format!("sha256:{:x}", hasher.finalize());
        crate::metrics::record_object_store_request("get", "success");
        crate::metrics::record_object_store_transfer("download", buf.len());
        Ok((buf, checksum))
    }

    async fn download_and_hash_with_reservation(
        &self,
        key: &ObjectPath,
        inflight_reservation: &mut crate::worker::ResourceReservation,
    ) -> Result<(Vec<u8>, String)> {
        let mut stream = self.get_object(key).await?.into_stream();
        let mut buf: Vec<u8> = Vec::new();
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    crate::metrics::record_object_store_request("get", "error");
                    return Err(anyhow::Error::from(error));
                }
            };
            let next_len = (buf.len() + chunk.len()) as u64;
            if next_len > inflight_reservation.reserved_bytes() {
                inflight_reservation.reconcile(next_len)?;
            }
            let current_ctx = crate::worker::current_request_context();
            self.admission
                .account_io_bytes(
                    crate::worker::WorkClass::Hydration,
                    chunk.len() as u64,
                    current_ctx.as_ref(),
                )
                .await?;
            hasher.update(&chunk);
            buf.extend_from_slice(&chunk);
        }
        inflight_reservation.reconcile(buf.len() as u64)?;
        let checksum = format!("sha256:{:x}", hasher.finalize());
        crate::metrics::record_object_store_request("get", "success");
        crate::metrics::record_object_store_transfer("download", buf.len());
        Ok((buf, checksum))
    }

    /// Upcast the concrete backend to the generic `ObjectStore` trait object.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        match &self.backend {
            Backend::Local { local, .. } => local.clone(),
            Backend::Remote { store, .. } => store.clone(),
        }
    }

    pub fn object_path(&self, location: &str) -> Result<ObjectPath> {
        ObjectPath::parse(location).map_err(anyhow::Error::from)
    }

    /// Resolves an `ObjectPath` to a filesystem path. Fails for remote backends.
    pub fn filesystem_path(&self, location: &ObjectPath) -> Result<PathBuf> {
        match &self.backend {
            Backend::Local { local, .. } => local
                .path_to_filesystem(location)
                .map_err(anyhow::Error::from),
            Backend::Remote { .. } => anyhow::bail!(
                "filesystem_path is only supported for local storage backends; \
                 current backend is remote ({})",
                self.uri()
            ),
        }
    }

    pub async fn put(&self, location: &ObjectPath, payload: impl Into<PutPayload>) -> Result<()> {
        let _storage_timer =
            crate::worker::start_current_stage_timer(crate::worker::TraceStage::Storage);
        let payload = payload.into();
        let content_length = payload.content_length();
        match self.object_store().put(location, payload).await {
            Ok(_) => {
                crate::metrics::record_object_store_request("put", "success");
                crate::metrics::record_object_store_transfer("upload", content_length);
                Ok(())
            }
            Err(error) => {
                crate::metrics::record_object_store_request("put", "error");
                Err(anyhow::Error::from(error))
            }
        }
    }

    async fn get_object(&self, location: &ObjectPath) -> object_store::Result<GetResult> {
        let _storage_timer =
            crate::worker::start_current_stage_timer(crate::worker::TraceStage::Storage);
        match self.object_store().get(location).await {
            Ok(result) => Ok(result),
            Err(error @ object_store::Error::NotFound { .. }) => {
                crate::metrics::record_object_store_request("get", "not_found");
                Err(error)
            }
            Err(error) => {
                crate::metrics::record_object_store_request("get", "error");
                Err(error)
            }
        }
    }

    pub async fn get_bytes(&self, location: &ObjectPath) -> Result<Vec<u8>> {
        let result = self.get_object(location).await?;
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                crate::metrics::record_object_store_request("get", "error");
                return Err(anyhow::Error::from(error));
            }
        };
        crate::metrics::record_object_store_request("get", "success");
        crate::metrics::record_object_store_transfer("download", bytes.len());
        Ok(bytes.to_vec())
    }

    // ── Manifest I/O ─────────────────────────────────────────────────
    //
    // The remote-store engine uses a two-layer manifest: a mutable
    // `manifest.current.json` pointer plus immutable `manifests/<gen>.json`
    // generations. See `docs/remote-store-one-pager.md`.

    /// Load the mutable manifest pointer for an index.
    ///
    /// Returns `Ok(None)` when the pointer does not exist (fresh/empty index).
    /// Corrupt JSON, unsupported versions, or invalid pointer fields surface as `Err`.
    pub async fn load_manifest_pointer(
        &self,
        index_uuid: &str,
    ) -> Result<Option<RemoteManifestPointer>> {
        let location = self.object_path(&manifest_pointer_key(index_uuid))?;
        match self.get_object(&location).await {
            Ok(result) => {
                let bytes = match result.bytes().await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        crate::metrics::record_object_store_request("get", "error");
                        return Err(anyhow::Error::from(error));
                    }
                };
                crate::metrics::record_object_store_request("get", "success");
                crate::metrics::record_object_store_transfer("download", bytes.len());
                let pointer: RemoteManifestPointer =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        crate::common::unsupported_index_format(
                            "remote manifest pointer",
                            format!("cannot decode current format at {location}: {error}"),
                        )
                    })?;
                pointer.validate_for_index(index_uuid)?;
                Ok(Some(pointer))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(anyhow::Error::from(err)),
        }
    }

    pub(crate) async fn load_manifest_at_path(
        &self,
        index_uuid: &str,
        manifest_path: &str,
        expected_generation: u64,
        expected_schema_hash: Option<&str>,
    ) -> Result<(RemoteStoreManifest, String)> {
        if !is_canonical_manifest_path(index_uuid, expected_generation, None, manifest_path) {
            anyhow::bail!(
                "manifest_path '{manifest_path}' is not a canonical manifest key for index '{index_uuid}' generation {expected_generation}"
            );
        }
        let location = self.object_path(manifest_path)?;
        let result = self.get_object(&location).await.with_context(|| {
            format!("manifest generation {expected_generation} not found at {location}")
        })?;
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                crate::metrics::record_object_store_request("get", "error");
                return Err(anyhow::Error::from(error));
            }
        };
        crate::metrics::record_object_store_request("get", "success");
        crate::metrics::record_object_store_transfer("download", bytes.len());
        let actual_checksum = publication::sha256_prefixed(&bytes);
        let manifest: RemoteStoreManifest = serde_json::from_slice(&bytes).map_err(|error| {
            crate::common::unsupported_index_format(
                "remote store manifest",
                format!("cannot decode current format at {location}: {error}"),
            )
        })?;
        manifest.validate_for_read(index_uuid, expected_generation, expected_schema_hash)?;
        if manifest.version >= 3
            && !is_canonical_manifest_path(
                index_uuid,
                expected_generation,
                Some(&manifest.operation_id),
                manifest_path,
            )
        {
            anyhow::bail!(
                "manifest_path '{}' does not match operation_id '{}' for generation {}",
                manifest_path,
                manifest.operation_id,
                expected_generation
            );
        }
        Ok((manifest, actual_checksum))
    }

    pub(crate) async fn load_manifest_for_pointer(
        &self,
        index_uuid: &str,
        pointer: &RemoteManifestPointer,
        expected_schema_hash: Option<&str>,
    ) -> Result<RemoteStoreManifest> {
        pointer.validate_for_index(index_uuid)?;
        if pointer.current_generation == 0 {
            anyhow::bail!(
                "cannot load manifest for generation-0 empty pointer on index '{index_uuid}'"
            );
        }
        let (manifest, actual_checksum) = self
            .load_manifest_at_path(
                index_uuid,
                &pointer.manifest_path,
                pointer.current_generation,
                expected_schema_hash,
            )
            .await?;
        pointer.validate_manifest_agreement(&manifest, &actual_checksum)?;
        Ok(manifest)
    }

    pub async fn verify_manifest_lineage_chain(
        &self,
        index_uuid: &str,
        head: &RemoteStoreManifest,
        expected_schema_hash: Option<&str>,
    ) -> Result<()> {
        let mut current = head.clone();
        let mut steps = 0usize;
        while current.parent_generation > 0 {
            steps += 1;
            if steps > MAX_LINEAGE_WALK_GENERATIONS {
                anyhow::bail!(
                    "manifest lineage chain exceeded {MAX_LINEAGE_WALK_GENERATIONS} generations for index '{index_uuid}'"
                );
            }
            let Some(ref parent_path) = current.parent_manifest_path else {
                anyhow::bail!(
                    "manifest generation {} for index '{}' is missing parent_manifest_path for parent_generation {}",
                    current.generation,
                    index_uuid,
                    current.parent_generation
                );
            };
            let (parent, _) = self
                .load_manifest_at_path(
                    index_uuid,
                    parent_path,
                    current.parent_generation,
                    expected_schema_hash,
                )
                .await
                .with_context(|| {
                    format!(
                        "broken manifest lineage for index '{}' at generation {}: parent generation {} at '{}' could not be loaded",
                        index_uuid, current.generation, current.parent_generation, parent_path
                    )
                })?;
            if parent.writer_epoch > current.writer_epoch {
                anyhow::bail!(
                    "broken manifest lineage for index '{}': parent generation {} has writer_epoch {} > child generation {} writer_epoch {}",
                    index_uuid,
                    parent.generation,
                    parent.writer_epoch,
                    current.generation,
                    current.writer_epoch
                );
            }
            if parent.schema_generation > current.schema_generation {
                anyhow::bail!(
                    "broken manifest lineage for index '{}': parent generation {} has schema_generation {} > child schema_generation {}",
                    index_uuid,
                    parent.generation,
                    parent.schema_generation,
                    current.schema_generation
                );
            }
            current = parent;
        }
        Ok(())
    }

    /// Load the immutable manifest for a specific generation.
    ///
    /// Fails closed if the manifest's `generation` does not match the
    /// generation we asked for (guards against stale overwrites) or if its
    /// `schema_hash` does not match `expected_schema_hash` when supplied.
    pub async fn load_manifest(
        &self,
        index_uuid: &str,
        generation: u64,
        expected_schema_hash: Option<&str>,
    ) -> Result<RemoteStoreManifest> {
        let legacy_key = manifest_generation_key(index_uuid, generation);
        let legacy_loc = self.object_path(&legacy_key)?;
        match self.get_object(&legacy_loc).await {
            Ok(result) => {
                let bytes = match result.bytes().await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        crate::metrics::record_object_store_request("get", "error");
                        return Err(anyhow::Error::from(error));
                    }
                };
                crate::metrics::record_object_store_request("get", "success");
                crate::metrics::record_object_store_transfer("download", bytes.len());
                let manifest: RemoteStoreManifest =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        crate::common::unsupported_index_format(
                            "remote store manifest",
                            format!("cannot decode current format at {legacy_loc}: {error}"),
                        )
                    })?;
                manifest.validate_for_read(index_uuid, generation, expected_schema_hash)?;
                return Ok(manifest);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(anyhow::Error::from(err)),
        }

        if let Ok(Some(pointer)) = self.load_manifest_pointer(index_uuid).await
            && pointer.current_generation >= generation
            && !pointer.manifest_path.is_empty()
        {
            if pointer.current_generation == generation {
                return self
                    .load_manifest_for_pointer(index_uuid, &pointer, expected_schema_hash)
                    .await;
            }
            let mut next_path = Some(pointer.manifest_path.clone());
            let mut expected_gen = pointer.current_generation;
            let mut steps = 0usize;
            while let Some(m_path) = next_path.take() {
                steps += 1;
                if steps > MAX_LINEAGE_WALK_GENERATIONS {
                    break;
                }
                let Ok((manifest, _)) = self
                    .load_manifest_at_path(index_uuid, &m_path, expected_gen, expected_schema_hash)
                    .await
                else {
                    break;
                };
                if manifest.generation == generation {
                    return Ok(manifest);
                }
                if manifest.parent_generation == 0 || manifest.parent_generation < generation {
                    break;
                }
                expected_gen = manifest.parent_generation;
                next_path = manifest.parent_manifest_path;
            }
        }

        let gen_prefix = format!("{index_uuid}/manifests/{generation:012}");
        let prefix_path = self.object_path(&gen_prefix)?;
        let mut list_stream = self.object_store().list(Some(&prefix_path));
        while let Some(item) = list_stream.next().await {
            let meta = item?;
            let path_str = meta.location.to_string();
            if path_str.contains(".ferris.") && path_str.ends_with("#0") {
                continue;
            }
            if path_str.ends_with(".json") {
                let (manifest, _) = self
                    .load_manifest_at_path(index_uuid, &path_str, generation, expected_schema_hash)
                    .await?;
                return Ok(manifest);
            }
        }

        anyhow::bail!("manifest generation {generation} not found")
    }

    /// Convenience: resolve the pointer, validate pointer-manifest agreement and
    /// parent lineage, and load the matching manifest.
    /// Returns `Ok(None)` when the pointer does not exist or is a generation-0
    /// empty fenced pointer.
    pub async fn load_current_manifest(
        &self,
        index_uuid: &str,
        expected_schema_hash: Option<&str>,
    ) -> Result<Option<RemoteStoreManifest>> {
        let Some(pointer) = self.load_manifest_pointer(index_uuid).await? else {
            return Ok(None);
        };
        if pointer.current_generation == 0 {
            return Ok(None);
        }
        let manifest = self
            .load_manifest_for_pointer(index_uuid, &pointer, expected_schema_hash)
            .await?;
        self.verify_manifest_lineage_chain(index_uuid, &manifest, expected_schema_hash)
            .await?;
        Ok(Some(manifest))
    }

    /// Publish a manifest atomically: write the immutable generation first
    /// via `create_immutable`, then conditionally update `manifest.current.json`
    /// via `compare_and_swap_optional`. Returns the new pointer.
    ///
    /// If the pointer CAS fails or conflicts, the immutable generation object
    /// is left in place and the conflict is surfaced to the caller.
    pub async fn publish_manifest(
        &self,
        manifest: &RemoteStoreManifest,
    ) -> Result<RemoteManifestPointer> {
        let _guard = self.publish_lock.lock().await;
        self.ensure_publication_supported().await?;
        let current = self
            .load_manifest_pointer_with_token(&manifest.index_uuid)
            .await?;
        self.publish_manifest_conditional(manifest, current.as_ref())
            .await
    }

    /// Append a single new split to the currently-published manifest and
    /// publish a new generation. Load-modify-publish runs under the per-
    /// storage `publish_lock` and commits `manifest.current.json` via
    /// conditional compare-and-swap against the observed pointer token so
    /// cross-process or independent writers cannot silently overwrite each
    /// other's generations.
    ///
    /// - If no manifest exists yet, generation 1 is published with the new
    ///   split as the only entry (`expected_token = None`).
    /// - If a manifest exists, its `schema_hash` must match `schema_hash`
    ///   (we fail closed on mismatch to avoid publishing splits against a
    ///   stale schema assumption).
    ///
    /// Returns the newly-published manifest.
    pub async fn append_split_and_publish(
        &self,
        index_uuid: &str,
        index_name: &str,
        schema_hash: &str,
        settings: Option<&crate::cluster::state::RemoteStoreSettings>,
        mut new_split: RemoteSplitManifest,
    ) -> Result<RemoteStoreManifest> {
        let _guard = self.publish_lock.lock().await;
        self.ensure_publication_supported().await?;

        // Load current manifest + pointer token (if any) WITHOUT schema
        // validation — we want to surface a mismatch as a clear, targeted
        // error rather than a generic load failure.
        let current = self
            .load_current_manifest_unchecked_with_token(index_uuid)
            .await?;

        let (
            generation,
            parent_generation,
            parent_manifest_path,
            writer_epoch,
            mut splits,
            existing_settings,
            mut published_watermarks,
            expected_pointer,
        ) = match current {
            Some((Some(existing), pointer, token)) => {
                if existing.schema_hash != schema_hash {
                    anyhow::bail!(
                        "schema_hash mismatch while appending split: manifest={} local={}",
                        existing.schema_hash,
                        schema_hash
                    );
                }
                (
                    existing.generation + 1,
                    existing.generation,
                    Some(pointer.manifest_path.clone()),
                    pointer.writer_epoch.max(existing.writer_epoch).max(1),
                    existing.splits,
                    existing.settings,
                    existing.published_watermarks,
                    Some((pointer, token)),
                )
            }
            Some((None, pointer, token)) => (
                1,
                0,
                None,
                pointer.writer_epoch.max(1),
                Vec::new(),
                None,
                BTreeMap::new(),
                Some((pointer, token)),
            ),
            None => (1, 0, None, 1, Vec::new(), None, BTreeMap::new(), None),
        };

        // Reject duplicate split_id within the same manifest — `published_splits`
        // would otherwise return two entries for the same bundle.
        if splits.iter().any(|s| s.split_id == new_split.split_id) {
            anyhow::bail!(
                "split_id {} already exists in manifest for index {}",
                new_split.split_id,
                index_uuid
            );
        }
        let operation_id = format!("op-gen-{generation}-{}", new_split.split_id);
        let request_fingerprint = publication::sha256_prefixed(
            format!(
                "{index_uuid}:{schema_hash}:{operation_id}:{}",
                new_split.checksum
            )
            .as_bytes(),
        );
        new_split
            .tags
            .entry("operation_id".into())
            .or_insert_with(|| operation_id.clone());
        if let Some(range) = new_split.seal_range()? {
            publication::advance_shard_watermark(
                &mut published_watermarks,
                &range,
                &new_split.split_id,
                &operation_id,
                generation,
            )?;
        }
        splits.push(new_split);

        let manifest = RemoteStoreManifest {
            version: REMOTE_MANIFEST_FORMAT_VERSION,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: index_uuid.to_string(),
            index_name: index_name.to_string(),
            schema_generation: 1,
            schema_hash: schema_hash.to_string(),
            generation,
            parent_generation,
            parent_manifest_path,
            writer_epoch,
            operation_id,
            request_fingerprint,
            published_at: chrono::Utc::now().to_rfc3339(),
            settings: settings.cloned().or(existing_settings),
            published_watermarks,
            splits,
        };

        self.publish_manifest_conditional(&manifest, expected_pointer.as_ref())
            .await?;
        Ok(manifest)
    }

    pub(crate) async fn load_manifest_pointer_with_token(
        &self,
        index_uuid: &str,
    ) -> Result<Option<(RemoteManifestPointer, CompareToken)>> {
        let location = self.object_path(&manifest_pointer_key(index_uuid))?;
        let Some(read) = self.read_with_token(&location).await? else {
            return Ok(None);
        };
        let pointer: RemoteManifestPointer =
            serde_json::from_slice(&read.bytes).map_err(|error| {
                crate::common::unsupported_index_format(
                    "remote manifest pointer",
                    format!("cannot decode current format at {location}: {error}"),
                )
            })?;
        pointer.validate_for_index(index_uuid)?;
        Ok(Some((pointer, read.token)))
    }

    /// Load the current manifest and its pointer compare token without schema
    /// validation. Used by `append_split_and_publish` so we can produce a
    /// targeted error on schema mismatch and conditionally CAS the pointer.
    async fn load_current_manifest_unchecked_with_token(
        &self,
        index_uuid: &str,
    ) -> Result<
        Option<(
            Option<RemoteStoreManifest>,
            RemoteManifestPointer,
            CompareToken,
        )>,
    > {
        let Some((pointer, token)) = self.load_manifest_pointer_with_token(index_uuid).await?
        else {
            return Ok(None);
        };
        if pointer.current_generation == 0 {
            return Ok(Some((None, pointer, token)));
        }
        let manifest = self
            .load_manifest_for_pointer(index_uuid, &pointer, None)
            .await?;
        Ok(Some((Some(manifest), pointer, token)))
    }

    /// Internal conditional publish that writes the immutable generation via
    /// `create_immutable` and updates `manifest.current.json` via
    /// `compare_and_swap_optional`.
    async fn publish_manifest_conditional(
        &self,
        manifest: &RemoteStoreManifest,
        expected_pointer: Option<&(RemoteManifestPointer, CompareToken)>,
    ) -> Result<RemoteManifestPointer> {
        if manifest.version != REMOTE_MANIFEST_FORMAT_VERSION {
            return Err(crate::common::unsupported_index_format(
                "remote store manifest",
                format!(
                    "version {} is not supported for publication; expected {}",
                    manifest.version, REMOTE_MANIFEST_FORMAT_VERSION
                ),
            ));
        }
        manifest.validate_for_read(&manifest.index_uuid, manifest.generation, None)?;
        let generation_key = manifest.canonical_manifest_path();
        let generation_path = self.object_path(&generation_key)?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)
            .context("failed to serialize manifest before publish")?;
        let manifest_checksum = publication::sha256_prefixed(&manifest_bytes);
        let create_outcome = self
            .create_immutable(&generation_path, manifest_bytes)
            .await?;
        create_outcome.ensure_matching_content(&generation_path)?;

        let next_revision = expected_pointer
            .map(|(ptr, _)| ptr.pointer_revision.saturating_add(1).max(1))
            .unwrap_or(1);
        let pointer = RemoteManifestPointer {
            version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
            index_uuid: manifest.index_uuid.clone(),
            pointer_revision: next_revision,
            writer_epoch: manifest.writer_epoch.max(1),
            current_generation: manifest.generation,
            manifest_path: generation_key,
            manifest_checksum: Some(manifest_checksum),
            last_operation_id: (!manifest.operation_id.is_empty())
                .then(|| manifest.operation_id.clone()),
            manifest_etag: None,
        };
        pointer.validate_for_index(&manifest.index_uuid)?;
        let pointer_path = self.object_path(&manifest_pointer_key(&manifest.index_uuid))?;
        let pointer_bytes = serde_json::to_vec_pretty(&pointer)
            .context("failed to serialize manifest pointer before publish")?;
        match self
            .compare_and_swap_optional(
                &pointer_path,
                expected_pointer.map(|(_, token)| token),
                pointer_bytes,
            )
            .await?
        {
            CompareAndSwapOutcome::Swapped { .. } => Ok(pointer),
            CompareAndSwapOutcome::Conflict => {
                Err(anyhow::Error::from(ConditionalStorageError::Conflict {
                    path: pointer_path.to_string(),
                    reason: format!(
                        "manifest pointer for index {} changed concurrently before generation {} could commit",
                        manifest.index_uuid, manifest.generation
                    ),
                }))
            }
        }
    }
}

/// Compute a deterministic content fingerprint over an index's field mappings.
///
/// The hash covers `(field_name, field_type, dimension)` for every declared
/// field, sorted by field name. Two mappings that differ in any of those
/// dimensions produce different hashes; two mappings that are logically
/// identical but iterated in different orders produce the same hash.
///
/// Format: `"sha256:<64 hex chars>"` so the prefix identifies the algorithm
/// for future algorithm bumps.
pub fn compute_schema_hash(
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> String {
    // BTreeMap for deterministic iteration order.
    let ordered: BTreeMap<&str, &crate::cluster::state::FieldMapping> =
        mappings.iter().map(|(k, v)| (k.as_str(), v)).collect();

    let mut hasher = Sha256::new();
    for (name, mapping) in ordered {
        hasher.update(name.as_bytes());
        hasher.update(b"\x1f"); // unit separator to avoid name/type bleed
        hasher.update(format!("{:?}", mapping.field_type).as_bytes());
        hasher.update(b"\x1f");
        if let Some(dim) = mapping.dimension {
            hasher.update(dim.to_le_bytes());
        }
        hasher.update(b"\x1e"); // record separator
    }
    let digest = hasher.finalize();
    format!("sha256:{}", hex_lower(&digest))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ── Split bundle format ───────────────────────────────────────────────
//
// A split is uploaded as a single opaque object in the object store.
// The bundle layout is:
//
//   MAGIC:       6 bytes  (b"FSBND\x01")  — format identifier + version
//   file_count:  u64 LE
//   for each file (in POSIX-path sorted order):
//     rel_path_len:    u64 LE
//     rel_path_bytes:  utf8 (never contains "..", never absolute)
//     content_len:     u64 LE
//     content_bytes
//
// The checksum stored in the manifest is the SHA-256 of the entire bundle
// byte stream (`"sha256:<hex>"`). Packing is deterministic (sorted paths +
// explicit length framing) so two bundles with identical contents always
// produce identical bytes and identical checksums, regardless of directory
// iteration order on the builder host.
pub(crate) fn pack_bundle_from_dir(dir: &Path) -> Result<(Vec<u8>, String)> {
    use std::path::Component;

    // Collect every regular file under `dir` with its POSIX relative path.
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries =
            std::fs::read_dir(&next).with_context(|| format!("failed to read_dir {next:?}"))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("dir entry in {next:?}"))?;
            let ft = entry
                .file_type()
                .with_context(|| format!("file_type of {:?}", entry.path()))?;
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let rel = path
                    .strip_prefix(dir)
                    .with_context(|| format!("path {path:?} not under bundle root {dir:?}"))?;
                let rel_posix = rel
                    .components()
                    .filter_map(|c| match c {
                        Component::Normal(s) => s.to_str(),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                if rel_posix.is_empty() {
                    anyhow::bail!("empty relative path produced for {path:?}");
                }
                files.push((rel_posix, path));
            }
            // Skip symlinks and other non-regular entries on purpose.
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(BUNDLE_MAGIC);
    buf.extend_from_slice(&(files.len() as u64).to_le_bytes());
    for (rel_posix, abs_path) in &files {
        let rel_bytes = rel_posix.as_bytes();
        buf.extend_from_slice(&(rel_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(rel_bytes);

        let content = std::fs::read(abs_path)
            .with_context(|| format!("failed to read bundle file {abs_path:?}"))?;
        buf.extend_from_slice(&(content.len() as u64).to_le_bytes());
        buf.extend_from_slice(&content);
    }

    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let checksum = format!("sha256:{:x}", hasher.finalize());
    Ok((buf, checksum))
}

/// Inverse of `pack_bundle_from_dir`: parse `bytes` as a bundle and write
/// each embedded file under `dir`. Rejects malformed frames and any
/// relative path that would escape the target directory.
pub(crate) fn unpack_bundle_into(bytes: &[u8], dir: &Path) -> Result<()> {
    use std::path::Component;

    if bytes.len() < BUNDLE_MAGIC.len() + 8 {
        anyhow::bail!("bundle too short to contain header ({} bytes)", bytes.len());
    }
    if &bytes[..BUNDLE_MAGIC.len()] != BUNDLE_MAGIC {
        anyhow::bail!(
            "bundle magic mismatch: expected {:?}, got {:?}",
            BUNDLE_MAGIC,
            &bytes[..BUNDLE_MAGIC.len()]
        );
    }
    let mut off = BUNDLE_MAGIC.len();

    let file_count = read_u64_le(bytes, &mut off)?;
    // Defensive upper bound: splits realistically have thousands of files
    // at most. Reject values that would let a malformed bundle trigger huge
    // allocations.
    if file_count > 1_000_000 {
        anyhow::bail!("bundle declares implausibly large file_count {file_count}");
    }

    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create bundle extract dir {dir:?}"))?;
    let dir_canon = dir
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {dir:?}"))?;

    for _ in 0..file_count {
        let rel_len = read_u64_le(bytes, &mut off)? as usize;
        if off + rel_len > bytes.len() {
            anyhow::bail!("bundle truncated while reading relative path");
        }
        let rel_bytes = &bytes[off..off + rel_len];
        off += rel_len;
        let rel_str =
            std::str::from_utf8(rel_bytes).context("bundle relative path is not valid utf-8")?;

        let rel_path = Path::new(rel_str);
        if rel_path.is_absolute() {
            anyhow::bail!("bundle contains absolute path {rel_str:?}");
        }
        for component in rel_path.components() {
            match component {
                Component::Normal(_) => {}
                _ => anyhow::bail!("bundle contains disallowed path component in {rel_str:?}"),
            }
        }

        let content_len = read_u64_le(bytes, &mut off)? as usize;
        if off + content_len > bytes.len() {
            anyhow::bail!("bundle truncated while reading file content for {rel_str:?}");
        }
        let content = &bytes[off..off + content_len];
        off += content_len;

        let abs = dir_canon.join(rel_path);
        // Belt-and-suspenders: ensure the joined path still lives under `dir_canon`.
        if !abs.starts_with(&dir_canon) {
            anyhow::bail!("bundle file path {abs:?} would escape extract dir {dir_canon:?}");
        }
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create parent dir {parent:?}"))?;
        }
        std::fs::write(&abs, content)
            .with_context(|| format!("failed to write bundle file {abs:?}"))?;
    }

    if off != bytes.len() {
        anyhow::bail!(
            "bundle has {} trailing bytes after last file",
            bytes.len() - off
        );
    }
    Ok(())
}

fn read_u64_le(bytes: &[u8], off: &mut usize) -> Result<u64> {
    if *off + 8 > bytes.len() {
        anyhow::bail!("bundle truncated while reading u64");
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[*off..*off + 8]);
    *off += 8;
    Ok(u64::from_le_bytes(b))
}

fn manifest_pointer_key(index_uuid: &str) -> String {
    format!("{index_uuid}/manifest.current.json")
}

fn manifest_generation_key(index_uuid: &str, generation: u64) -> String {
    // Zero-padded generations keep the object-store listing naturally sorted.
    format!("{index_uuid}/manifests/{generation:012}.json")
}

fn normalize_local_root(path: &str) -> Result<PathBuf> {
    if path.starts_with("file://") {
        let url =
            Url::parse(path).with_context(|| format!("invalid file:// storage URL '{path}'"))?;
        url.to_file_path().map_err(|_| {
            anyhow::anyhow!("storage URL '{path}' could not be converted to a filesystem path")
        })
    } else {
        Ok(PathBuf::from(path))
    }
}

/// Classification of a storage URI for dispatch in `StorageManager::new`.
enum StorageScheme {
    Local,
    S3,
    Unsupported(String),
}

fn storage_scheme(path: &str) -> StorageScheme {
    if path.starts_with("file://") {
        return StorageScheme::Local;
    }
    if path.starts_with("s3://") {
        return StorageScheme::S3;
    }
    if let Some(scheme_end) = path.find("://") {
        return StorageScheme::Unsupported(path[..scheme_end].to_string());
    }
    StorageScheme::Local
}

/// Build an `AmazonS3` object store from an `s3://bucket[/prefix]` URI.
///
/// Reads credentials and region from environment variables via
/// `AmazonS3Builder::from_env`. `AWS_ENDPOINT_URL` overrides the default
/// AWS endpoint (needed for MinIO / RustFS dev servers).
///
/// When a prefix is present in the URI (e.g. `s3://bucket/indexes`), all
/// manifest keys are scoped under that prefix automatically by the
/// object_store crate's `new_with_prefix` construction.
fn build_s3_backend(uri: &str) -> Result<(Arc<dyn ObjectStore>, Option<String>)> {
    let url = Url::parse(uri).with_context(|| format!("invalid s3:// storage URL '{uri}'"))?;
    let bucket = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("s3:// URL '{uri}' is missing a bucket name"))?;
    let prefix = url.path().trim_start_matches('/').to_string();

    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    let unsupported_reason = builder
        .get_config_value(&object_store::aws::AmazonS3ConfigKey::ConditionalPut)
        .filter(|value| value.trim().eq_ignore_ascii_case("disabled"))
        .map(|_| {
            "S3 conditional put (If-Match / If-None-Match) is disabled in configuration".to_string()
        });

    // `from_env` reads AWS_ENDPOINT_URL, but only if set. For MinIO/RustFS
    // deployments using path-style addressing over plain HTTP, we must also
    // enable virtual_hosted_style=false and allow_http=true. Both are inferred
    // from the endpoint URL scheme when we hand it to the builder explicitly.
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        let allow_http = endpoint.starts_with("http://");
        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(false)
            .with_allow_http(allow_http);
    }

    // Region is required by the AWS SDK even for MinIO; default to us-east-1
    // when unset so dev servers work out of the box.
    if std::env::var("AWS_REGION").is_err() && std::env::var("AWS_DEFAULT_REGION").is_err() {
        builder = builder.with_region("us-east-1");
    }

    let s3 = builder
        .build()
        .with_context(|| format!("failed to build S3 backend for '{uri}'"))?;

    let store: Arc<dyn ObjectStore> = if prefix.is_empty() {
        Arc::new(s3)
    } else {
        // `object_store`'s PrefixStore wraps any ObjectStore with a fixed prefix.
        let prefix_path = ObjectPath::parse(&prefix)
            .with_context(|| format!("invalid s3 prefix '{prefix}' in URL '{uri}'"))?;
        let prefixed = object_store::prefix::PrefixStore::new(s3, prefix_path);
        Arc::new(prefixed)
    };
    Ok((store, unsupported_reason))
}

const fn default_schema_generation() -> u64 {
    1
}

/// Mutable pointer to the currently published immutable manifest generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteManifestPointer {
    pub version: u32,
    #[serde(default)]
    pub index_uuid: String,
    #[serde(default)]
    pub pointer_revision: u64,
    #[serde(default)]
    pub writer_epoch: u64,
    pub current_generation: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub manifest_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_etag: Option<String>,
}

impl RemoteManifestPointer {
    pub fn validate_for_index(&self, expected_index_uuid: &str) -> Result<()> {
        if !REMOTE_MANIFEST_POINTER_FORMAT_CAPABILITY.supports_read(self.version) {
            return Err(crate::common::unsupported_index_format(
                "remote manifest pointer",
                format!(
                    "version {} is not supported; supported read window is {}..={}",
                    self.version,
                    REMOTE_MANIFEST_POINTER_FORMAT_CAPABILITY.min_read_version,
                    REMOTE_MANIFEST_POINTER_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if self.version == 2 {
            if self.current_generation == 0 || self.manifest_path.is_empty() {
                anyhow::bail!(
                    "v2 remote manifest pointer for index '{expected_index_uuid}' requires positive current_generation and non-empty manifest_path"
                );
            }
            if !is_canonical_manifest_path(
                expected_index_uuid,
                self.current_generation,
                None,
                &self.manifest_path,
            ) {
                anyhow::bail!(
                    "v2 remote manifest pointer path '{}' is not canonical for index '{}' generation {}",
                    self.manifest_path,
                    expected_index_uuid,
                    self.current_generation
                );
            }
            return Ok(());
        }

        if self.index_uuid != expected_index_uuid {
            anyhow::bail!(
                "manifest pointer index_uuid mismatch: expected '{}', pointer declares '{}'",
                expected_index_uuid,
                self.index_uuid
            );
        }
        if self.pointer_revision == 0 || self.writer_epoch == 0 {
            anyhow::bail!(
                "manifest pointer for index '{expected_index_uuid}' requires positive pointer_revision and writer_epoch"
            );
        }
        if self.current_generation == 0 {
            if !self.manifest_path.is_empty()
                || self.manifest_checksum.is_some()
                || self.last_operation_id.is_some()
            {
                anyhow::bail!(
                    "generation-0 empty manifest pointer for index '{expected_index_uuid}' must not carry manifest_path, manifest_checksum, or last_operation_id"
                );
            }
            return Ok(());
        }

        if self.manifest_path.is_empty() {
            anyhow::bail!(
                "manifest pointer for index '{}' generation {} is missing manifest_path",
                expected_index_uuid,
                self.current_generation
            );
        }
        if !is_canonical_manifest_path(
            expected_index_uuid,
            self.current_generation,
            self.last_operation_id.as_deref(),
            &self.manifest_path,
        ) {
            anyhow::bail!(
                "manifest pointer path '{}' is not canonical for index '{}' generation {}",
                self.manifest_path,
                expected_index_uuid,
                self.current_generation
            );
        }
        let Some(ref checksum) = self.manifest_checksum else {
            anyhow::bail!(
                "manifest pointer for index '{}' generation {} is missing manifest_checksum",
                expected_index_uuid,
                self.current_generation
            );
        };
        if !checksum.starts_with("sha256:") || checksum.len() != 71 {
            anyhow::bail!(
                "manifest pointer for index '{expected_index_uuid}' has malformed manifest_checksum '{checksum}'"
            );
        }
        let Some(ref op_id) = self.last_operation_id else {
            anyhow::bail!(
                "manifest pointer for index '{}' generation {} is missing last_operation_id",
                expected_index_uuid,
                self.current_generation
            );
        };
        validate_publication_operation_id(op_id).map_err(anyhow::Error::msg)?;
        Ok(())
    }

    pub fn validate_manifest_agreement(
        &self,
        manifest: &RemoteStoreManifest,
        actual_manifest_checksum: &str,
    ) -> Result<()> {
        if manifest.index_uuid != self.index_uuid && !self.index_uuid.is_empty() {
            anyhow::bail!(
                "pointer/manifest index_uuid mismatch: pointer='{}' manifest='{}'",
                self.index_uuid,
                manifest.index_uuid
            );
        }
        if manifest.generation != self.current_generation {
            anyhow::bail!(
                "manifest generation mismatch: pointer requested {}, payload declares {}",
                self.current_generation,
                manifest.generation
            );
        }
        if let Some(ref expected_checksum) = self.manifest_checksum
            && expected_checksum != actual_manifest_checksum
        {
            anyhow::bail!(
                "manifest checksum mismatch at '{}': pointer expected {}, actual {}",
                self.manifest_path,
                expected_checksum,
                actual_manifest_checksum
            );
        }
        if self.version >= 3 {
            if manifest.version < 3 {
                anyhow::bail!(
                    "v3 manifest pointer at '{}' cannot reference legacy v{} manifest",
                    self.manifest_path,
                    manifest.version
                );
            }
            if self.last_operation_id.as_deref() != Some(manifest.operation_id.as_str()) {
                anyhow::bail!(
                    "pointer/manifest operation_id mismatch: pointer={:?}, manifest='{}'",
                    self.last_operation_id,
                    manifest.operation_id
                );
            }
            if self.writer_epoch < manifest.writer_epoch {
                anyhow::bail!(
                    "pointer writer_epoch {} is older than referenced manifest writer_epoch {}",
                    self.writer_epoch,
                    manifest.writer_epoch
                );
            }
        }
        Ok(())
    }
}

/// Immutable manifest for one remote-store generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteStoreManifest {
    pub version: u32,
    pub engine: crate::cluster::state::IndexEngine,
    pub index_uuid: String,
    pub index_name: String,
    #[serde(default = "default_schema_generation")]
    pub schema_generation: u64,
    pub schema_hash: String,
    pub generation: u64,
    #[serde(default)]
    pub parent_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_path: Option<String>,
    #[serde(default)]
    pub writer_epoch: u64,
    #[serde(default)]
    pub operation_id: String,
    #[serde(default)]
    pub request_fingerprint: String,
    pub published_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<crate::cluster::state::RemoteStoreSettings>,
    /// Contiguous per-shard hot-delta publication visibility watermarks (`FS-036`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub published_watermarks: BTreeMap<u32, ShardPublishedWatermark>,
    #[serde(default)]
    pub splits: Vec<RemoteSplitManifest>,
}

impl RemoteStoreManifest {
    pub fn published_splits(&self) -> impl Iterator<Item = &RemoteSplitManifest> {
        self.splits
            .iter()
            .filter(|split| matches!(split.state, RemoteSplitState::Published))
    }

    pub fn canonical_manifest_path(&self) -> String {
        if self.version >= 3 && !self.operation_id.is_empty() {
            manifest_operation_key(&self.index_uuid, self.generation, &self.operation_id)
        } else {
            manifest_generation_key(&self.index_uuid, self.generation)
        }
    }

    pub fn validate_for_read(
        &self,
        expected_index_uuid: &str,
        expected_generation: u64,
        expected_schema_hash: Option<&str>,
    ) -> Result<()> {
        if !REMOTE_MANIFEST_FORMAT_CAPABILITY.supports_read(self.version) {
            return Err(crate::common::unsupported_index_format(
                "remote store manifest",
                format!(
                    "version {} is not supported; supported read window is {}..={}",
                    self.version,
                    REMOTE_MANIFEST_FORMAT_CAPABILITY.min_read_version,
                    REMOTE_MANIFEST_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if let Some(split) = self
            .published_splits()
            .find(|split| split.checksum.starts_with("sha256:pending:"))
        {
            return Err(crate::common::unsupported_index_format(
                "remote store manifest",
                format!("split '{}' contains a placeholder checksum", split.split_id),
            ));
        }
        if self.index_uuid != expected_index_uuid {
            anyhow::bail!(
                "manifest index_uuid mismatch: expected '{}', payload declares '{}'",
                expected_index_uuid,
                self.index_uuid
            );
        }
        if self.generation == 0 || self.generation != expected_generation {
            anyhow::bail!(
                "manifest generation mismatch: pointer requested {}, payload declares {}",
                expected_generation,
                self.generation
            );
        }
        if self.schema_generation == 0 {
            anyhow::bail!(
                "manifest generation {} for index '{}' has invalid zero schema_generation",
                self.generation,
                self.index_uuid
            );
        }
        if let Some(expected) = expected_schema_hash
            && self.schema_hash != expected
        {
            anyhow::bail!(
                "manifest schema_hash mismatch: expected {}, payload declares {}",
                expected,
                self.schema_hash
            );
        }
        let mut seen_splits = HashSet::new();
        for split in &self.splits {
            if !seen_splits.insert(split.split_id.as_str()) {
                anyhow::bail!(
                    "duplicate split_id '{}' in manifest generation {} for index '{}'",
                    split.split_id,
                    self.generation,
                    self.index_uuid
                );
            }
            let _ = split.seal_range()?;
        }
        for (shard_id, wm) in &self.published_watermarks {
            if *shard_id != wm.shard_id {
                anyhow::bail!(
                    "manifest generation {} for index '{}' has mismatched published_watermarks key {} != shard_id {}",
                    self.generation,
                    self.index_uuid,
                    shard_id,
                    wm.shard_id
                );
            }
            if wm.primary_term == 0
                || wm.updated_at_generation == 0
                || wm.updated_at_generation > self.generation
                || wm.last_split_id.is_empty()
                || wm.last_operation_id.is_empty()
            {
                anyhow::bail!(
                    "manifest generation {} for index '{}' has invalid published watermark for shard {}",
                    self.generation,
                    self.index_uuid,
                    shard_id
                );
            }
        }
        if self.version >= 3 {
            if self.writer_epoch == 0 {
                anyhow::bail!(
                    "v3 manifest generation {} for index '{}' requires positive writer_epoch",
                    self.generation,
                    self.index_uuid
                );
            }
            validate_publication_operation_id(&self.operation_id).map_err(|err| {
                anyhow::anyhow!(
                    "v3 manifest generation {} for index '{}' has invalid operation_id '{}': {}",
                    self.generation,
                    self.index_uuid,
                    self.operation_id,
                    err
                )
            })?;
            if !self.request_fingerprint.starts_with("sha256:") {
                anyhow::bail!(
                    "v3 manifest generation {} for index '{}' requires sha256 request_fingerprint, got '{}'",
                    self.generation,
                    self.index_uuid,
                    self.request_fingerprint
                );
            }
            if self.generation == 1 {
                if self.parent_generation != 0 || self.parent_manifest_path.is_some() {
                    anyhow::bail!(
                        "manifest generation 1 for index '{}' must have parent_generation 0 and no parent_manifest_path",
                        self.index_uuid
                    );
                }
            } else {
                if self.parent_generation != self.generation - 1 {
                    anyhow::bail!(
                        "broken manifest lineage at generation {} for index '{}': expected parent_generation {}, got {}",
                        self.generation,
                        self.index_uuid,
                        self.generation - 1,
                        self.parent_generation
                    );
                }
                let Some(ref parent_path) = self.parent_manifest_path else {
                    anyhow::bail!(
                        "broken manifest lineage at generation {} for index '{}': missing parent_manifest_path",
                        self.generation,
                        self.index_uuid
                    );
                };
                if !is_canonical_manifest_path(
                    &self.index_uuid,
                    self.parent_generation,
                    None,
                    parent_path,
                ) {
                    anyhow::bail!(
                        "broken manifest lineage at generation {} for index '{}': parent_manifest_path '{}' is not canonical for parent_generation {}",
                        self.generation,
                        self.index_uuid,
                        parent_path,
                        self.parent_generation
                    );
                }
            }
        }
        Ok(())
    }
}

/// State of a split within a remote manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSplitState {
    Staged,
    Published,
    MarkedForDeletion,
}

/// Immutable metadata for one searchable split.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteSplitManifest {
    pub split_id: String,
    pub state: RemoteSplitState,
    pub bundle_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hotcache_path: Option<String>,
    pub checksum: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hotcache_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<SplitTimeRange>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_ranges: BTreeMap<String, SplitFieldRange>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_terms: BTreeMap<String, SplitFieldTerms>,
}

impl RemoteSplitManifest {
    pub fn set_seal_range(&mut self, range: &SplitSealRangeMetadata) {
        self.tags
            .insert("seal_shard_id".into(), range.shard_id.to_string());
        self.tags
            .insert("seal_start_seq_no".into(), range.start_seq_no.to_string());
        self.tags
            .insert("seal_end_seq_no".into(), range.end_seq_no.to_string());
        self.tags
            .insert("seal_primary_term".into(), range.primary_term.to_string());
        if !range.doc_versions.is_empty() {
            let versions_json =
                serde_json::to_string(&range.doc_versions).expect("doc_versions must serialize");
            self.tags
                .insert("seal_doc_versions_json".into(), versions_json);
        } else {
            self.tags.remove("seal_doc_versions_json");
        }
        if !range.tombstones.is_empty() {
            let tombstones_json =
                serde_json::to_string(&range.tombstones).expect("tombstones must serialize");
            self.tags
                .insert("seal_tombstones_json".into(), tombstones_json);
        } else {
            self.tags.remove("seal_tombstones_json");
        }
    }

    pub fn seal_range(&self) -> Result<Option<SplitSealRangeMetadata>> {
        let shard_raw = self.tags.get("seal_shard_id");
        let start_raw = self.tags.get("seal_start_seq_no");
        let end_raw = self.tags.get("seal_end_seq_no");
        let term_raw = self.tags.get("seal_primary_term");
        if shard_raw.is_none() && start_raw.is_none() && end_raw.is_none() && term_raw.is_none() {
            return Ok(None);
        }
        let (Some(shard_raw), Some(start_raw), Some(end_raw), Some(term_raw)) =
            (shard_raw, start_raw, end_raw, term_raw)
        else {
            anyhow::bail!(
                "split '{}' has incomplete seal range metadata tags",
                self.split_id
            );
        };
        let shard_id = shard_raw.parse::<u32>().with_context(|| {
            format!(
                "split '{}' has invalid seal_shard_id '{shard_raw}'",
                self.split_id
            )
        })?;
        let start_seq_no = start_raw.parse::<u64>().with_context(|| {
            format!(
                "split '{}' has invalid seal_start_seq_no '{start_raw}'",
                self.split_id
            )
        })?;
        let end_seq_no = end_raw.parse::<u64>().with_context(|| {
            format!(
                "split '{}' has invalid seal_end_seq_no '{end_raw}'",
                self.split_id
            )
        })?;
        let primary_term = term_raw.parse::<u64>().with_context(|| {
            format!(
                "split '{}' has invalid seal_primary_term '{term_raw}'",
                self.split_id
            )
        })?;
        if end_seq_no < start_seq_no || primary_term == 0 {
            anyhow::bail!(
                "split '{}' has invalid seal range ({start_seq_no}..={end_seq_no}, primary_term={primary_term})",
                self.split_id
            );
        }
        let doc_versions = match self.tags.get("seal_doc_versions_json") {
            Some(raw) if !raw.is_empty() => serde_json::from_str::<Vec<SplitDocVersionRecord>>(raw)
                .with_context(|| {
                    format!(
                        "split '{}' has invalid seal_doc_versions_json",
                        self.split_id
                    )
                })?,
            _ => Vec::new(),
        };
        let tombstones = match self.tags.get("seal_tombstones_json") {
            Some(raw) if !raw.is_empty() => serde_json::from_str::<Vec<SplitTombstoneRecord>>(raw)
                .with_context(|| {
                    format!("split '{}' has invalid seal_tombstones_json", self.split_id)
                })?,
            _ => Vec::new(),
        };
        Ok(Some(SplitSealRangeMetadata {
            shard_id,
            start_seq_no,
            end_seq_no,
            primary_term,
            doc_versions,
            tombstones,
        }))
    }
}

/// Optional timestamp range summary used for split pruning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitTimeRange {
    pub field: String,
    pub min: String,
    pub max: String,
}

/// Exact min/max summary for a mapped numeric or date field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitFieldRange {
    pub min: String,
    pub max: String,
}

/// Exact small distinct-set summary for a mapped keyword or boolean field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SplitFieldTerms {
    pub values: Vec<String>,
}

#[cfg(test)]
mod fs017_021_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn remote_manifest_pointer_roundtrip() {
        let pointer = RemoteManifestPointer {
            version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
            index_uuid: "idx-uuid".into(),
            pointer_revision: 3,
            writer_epoch: 2,
            current_generation: 42,
            manifest_path: "idx-uuid/manifests/000000000042/op-42.json".into(),
            manifest_checksum: Some(format!("sha256:{}", "a".repeat(64))),
            last_operation_id: Some("op-42".into()),
            manifest_etag: Some("etag-42".into()),
        };

        let json = serde_json::to_string(&pointer).unwrap();
        let decoded: RemoteManifestPointer = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, pointer);
    }

    #[test]
    fn remote_store_manifest_roundtrip_preserves_published_splits() {
        let manifest = RemoteStoreManifest {
            version: REMOTE_MANIFEST_FORMAT_VERSION,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: "idx-uuid".into(),
            index_name: "events".into(),
            schema_generation: 1,
            schema_hash: "sha256:schema".into(),
            generation: 7,
            parent_generation: 6,
            parent_manifest_path: Some("idx-uuid/manifests/000000000006/op-6.json".into()),
            writer_epoch: 2,
            operation_id: "op-7".into(),
            request_fingerprint: format!("sha256:{}", "b".repeat(64)),
            published_at: "2026-04-16T00:00:00Z".into(),
            settings: Some(crate::cluster::state::RemoteStoreSettings {
                object_store_uri: Some("s3://bucket/indexes".into()),
                manifest_path: Some("manifests/7.json".into()),
                manifest_generation: Some(7),
                manifest_checksum: Some("sha256:manifest".into()),
                manifest_refresh_ms: Some(1000),
                hotcache_bytes: Some(4096),
                split_cache_bytes: Some(8192),
                hot_delta: None,
            }),
            published_watermarks: BTreeMap::new(),
            splits: vec![
                RemoteSplitManifest {
                    split_id: "split-a".into(),
                    state: RemoteSplitState::Published,
                    bundle_path: "splits/split-a/bundle.tar.zst".into(),
                    bundle_etag: Some("bundle-a".into()),
                    hotcache_path: Some("splits/split-a/hotcache.bin".into()),
                    checksum: "sha256:split-a".into(),
                    doc_count: 10,
                    size_bytes: 2048,
                    uncompressed_bytes: 4096,
                    hotcache_bytes: Some(256),
                    time_range: Some(SplitTimeRange {
                        field: "created_at".into(),
                        min: "2026-04-01T00:00:00Z".into(),
                        max: "2026-04-02T00:00:00Z".into(),
                    }),
                    tags: BTreeMap::from([("tenant".into(), "blue".into())]),
                    field_ranges: BTreeMap::from([(
                        "created_at".into(),
                        SplitFieldRange {
                            min: "1711929600000".into(),
                            max: "1712016000000".into(),
                        },
                    )]),
                    field_terms: BTreeMap::from([(
                        "status".into(),
                        SplitFieldTerms {
                            values: vec!["error".into(), "warn".into()],
                        },
                    )]),
                },
                RemoteSplitManifest {
                    split_id: "split-b".into(),
                    state: RemoteSplitState::Staged,
                    bundle_path: "splits/split-b/bundle.tar.zst".into(),
                    bundle_etag: None,
                    hotcache_path: None,
                    checksum: "sha256:split-b".into(),
                    doc_count: 3,
                    size_bytes: 1024,
                    uncompressed_bytes: 2048,
                    hotcache_bytes: None,
                    time_range: None,
                    tags: BTreeMap::new(),
                    field_ranges: BTreeMap::new(),
                    field_terms: BTreeMap::new(),
                },
            ],
        };

        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: RemoteStoreManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, manifest);

        let published: Vec<_> = decoded
            .published_splits()
            .map(|split| split.split_id.as_str())
            .collect();
        assert_eq!(published, vec!["split-a"]);
    }

    #[tokio::test]
    async fn storage_manager_uses_object_store_local_backend_for_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager.object_path("manifests/1.json").unwrap();

        manager
            .put(&path, "{\"version\":1}".to_string())
            .await
            .unwrap();

        let bytes = manager.get_bytes(&path).await.unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), "{\"version\":1}");
        assert!(manager.filesystem_path(&path).unwrap().exists());
    }

    #[test]
    fn storage_manager_accepts_file_url_root() {
        let temp_dir = TempDir::new().unwrap();
        let root_url = Url::from_file_path(temp_dir.path())
            .expect("temp dir path should convert to a file:// URL");
        let workdir = temp_dir.path().join("workdir");

        let manager = StorageManager::new(root_url.to_string(), workdir).unwrap();

        assert_eq!(
            manager.root().unwrap(),
            temp_dir.path().canonicalize().unwrap()
        );
        assert!(manager.is_local());
    }

    #[test]
    fn storage_manager_rejects_unsupported_scheme() {
        let temp_dir = TempDir::new().unwrap();
        let error = StorageManager::new(
            "gs://bucket/indexes".into(),
            temp_dir.path().join("workdir"),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported storage URL scheme 'gs'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn storage_manager_s3_url_build_requires_env() {
        // When neither AWS_ACCESS_KEY_ID nor an explicit builder is configured
        // and AWS_ENDPOINT_URL is unset, AmazonS3Builder::from_env returns the
        // unconfigured builder which can still build (object_store defers
        // credential resolution until first request). So construction
        // succeeds; we just verify it dispatched to the remote backend and
        // that `filesystem_path` errors instead of returning a bogus path.
        //
        // NOTE: we do NOT set AWS env vars here to avoid racing other tests.
        // The real S3 roundtrip is covered by the env-gated
        // `tests/remote_store_s3_integration.rs` suite.
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new(
            "s3://test-bucket/indexes".into(),
            temp_dir.path().join("workdir"),
        )
        .unwrap();
        assert!(!manager.is_local());
        assert!(manager.root().is_none());
        assert_eq!(manager.uri(), "s3://test-bucket/indexes");

        let path = manager.object_path("foo").unwrap();
        let err = manager.filesystem_path(&path).unwrap_err();
        assert!(
            err.to_string().contains("only supported for local"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn storage_manager_s3_url_rejects_missing_bucket() {
        let temp_dir = TempDir::new().unwrap();
        let err = StorageManager::new("s3:///prefix-only".into(), temp_dir.path().join("workdir"))
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("missing a bucket name") || msg.contains("failed to build S3 backend"),
            "unexpected error: {msg}"
        );
    }

    fn sample_manifest(
        index_uuid: &str,
        generation: u64,
        schema_hash: &str,
    ) -> RemoteStoreManifest {
        let parent_generation = generation.saturating_sub(1);
        let operation_id = format!("op-gen-{generation}");
        RemoteStoreManifest {
            version: REMOTE_MANIFEST_FORMAT_VERSION,
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            index_uuid: index_uuid.into(),
            index_name: "events".into(),
            schema_generation: 1,
            schema_hash: schema_hash.into(),
            generation,
            parent_generation,
            parent_manifest_path: (parent_generation > 0).then(|| {
                manifest_operation_key(
                    index_uuid,
                    parent_generation,
                    &format!("op-gen-{parent_generation}"),
                )
            }),
            writer_epoch: 1,
            operation_id,
            request_fingerprint: format!("sha256:{}", "1".repeat(64)),
            published_at: "2026-04-16T00:00:00Z".into(),
            settings: None,
            published_watermarks: BTreeMap::new(),
            splits: vec![RemoteSplitManifest {
                split_id: "split-a".into(),
                state: RemoteSplitState::Published,
                bundle_path: "splits/split-a/bundle.tar.zst".into(),
                bundle_etag: None,
                hotcache_path: None,
                checksum: "sha256:a".into(),
                doc_count: 1,
                size_bytes: 16,
                uncompressed_bytes: 32,
                hotcache_bytes: None,
                time_range: None,
                tags: BTreeMap::new(),
                field_ranges: BTreeMap::new(),
                field_terms: BTreeMap::new(),
            }],
        }
    }

    #[tokio::test]
    async fn load_manifest_pointer_returns_none_for_fresh_index() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let pointer = manager.load_manifest_pointer("idx-1").await.unwrap();
        assert!(pointer.is_none());
    }

    #[tokio::test]
    async fn no_compat_old_manifest_pointer_requires_recreate() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager.object_path(&manifest_pointer_key("idx-1")).unwrap();
        manager
            .put(
                &path,
                serde_json::to_vec(&RemoteManifestPointer {
                    version: 1,
                    index_uuid: "idx-1".into(),
                    pointer_revision: 1,
                    writer_epoch: 1,
                    current_generation: 1,
                    manifest_path: "idx-1/manifests/000000000001.json".into(),
                    manifest_checksum: None,
                    last_operation_id: None,
                    manifest_etag: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();

        let error = manager.load_manifest_pointer("idx-1").await.unwrap_err();
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
    }

    #[tokio::test]
    async fn publish_then_load_current_manifest_roundtrips() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let manifest = sample_manifest("idx-1", 1, "sha256:v1");

        let pointer = manager.publish_manifest(&manifest).await.unwrap();
        assert_eq!(pointer.current_generation, 1);
        assert_eq!(
            pointer.manifest_path,
            "idx-1/manifests/000000000001/op-gen-1.json"
        );

        let loaded_pointer = manager
            .load_manifest_pointer("idx-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded_pointer, pointer);

        let loaded = manager
            .load_current_manifest("idx-1", Some("sha256:v1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded, manifest);
    }

    #[tokio::test]
    async fn load_manifest_rejects_schema_hash_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let manifest = sample_manifest("idx-1", 3, "sha256:v1");
        manager.publish_manifest(&manifest).await.unwrap();

        let error = manager
            .load_manifest("idx-1", 3, Some("sha256:different"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("schema_hash mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_manifest_rejects_generation_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        // Publish a manifest whose pointer says generation 5 but whose payload declares 6.
        // We simulate this by writing the manifest by hand under a different generation key.
        let mut manifest = sample_manifest("idx-1", 6, "sha256:v1");
        // Publish generation 6 correctly first, then overwrite the generation-5 object
        // with the generation-6 payload to trigger the mismatch guard.
        manifest.generation = 6;
        let path_5 = manager
            .object_path(&manifest_generation_key("idx-1", 5))
            .unwrap();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        manager.put(&path_5, bytes).await.unwrap();

        let error = manager.load_manifest("idx-1", 5, None).await.unwrap_err();
        assert!(
            error.to_string().contains("manifest generation mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn load_manifest_rejects_corrupt_json() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager
            .object_path(&manifest_generation_key("idx-1", 1))
            .unwrap();
        manager.put(&path, "not-json".to_string()).await.unwrap();

        let error = manager.load_manifest("idx-1", 1, None).await.unwrap_err();
        assert!(
            error.is::<crate::common::UnsupportedIndexFormatError>()
                && error.to_string().contains("recreate the index"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn no_compat_old_remote_manifest_requires_recreate() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let mut manifest = sample_manifest("idx-1", 1, "sha256:v1");
        manifest.version = 1;
        let path = manager
            .object_path(&manifest_generation_key("idx-1", 1))
            .unwrap();
        manager
            .put(&path, serde_json::to_vec(&manifest).unwrap())
            .await
            .unwrap();

        let error = manager.load_manifest("idx-1", 1, None).await.unwrap_err();
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
    }

    #[tokio::test]
    async fn load_current_manifest_returns_none_when_pointer_missing() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let loaded = manager.load_current_manifest("idx-1", None).await.unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn compute_schema_hash_is_deterministic_regardless_of_insertion_order() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        a.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let mut b = HashMap::new();
        b.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        b.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        assert_eq!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_differs_on_type_change() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );
        assert_ne!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_differs_on_dimension_change() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut a = HashMap::new();
        a.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(384),
            },
        );
        let mut b = HashMap::new();
        b.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(768),
            },
        );
        assert_ne!(compute_schema_hash(&a), compute_schema_hash(&b));
    }

    #[test]
    fn compute_schema_hash_is_stable_across_calls() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut m = HashMap::new();
        m.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        let h1 = compute_schema_hash(&m);
        let h2 = compute_schema_hash(&m);
        assert_eq!(h1, h2);
        assert!(h1.starts_with("sha256:"));
        assert_eq!(h1.len(), "sha256:".len() + 64);
    }

    #[tokio::test]
    async fn append_split_and_publish_bootstraps_generation_1() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let manifest = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split)
            .await
            .unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.splits.len(), 1);

        let loaded = manager
            .load_current_manifest("idx-1", Some("sha256:abc"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.generation, 1);
        assert_eq!(loaded.splits[0].split_id, "split-a");
    }

    #[tokio::test]
    async fn append_split_and_publish_bumps_generation_and_appends() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split_a = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split_a)
            .await
            .unwrap();

        let split_b = RemoteSplitManifest {
            split_id: "split-b".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-b".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:b".into(),
            doc_count: 2,
            size_bytes: 32,
            uncompressed_bytes: 32,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let second = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split_b)
            .await
            .unwrap();

        assert_eq!(second.generation, 2);
        assert_eq!(second.splits.len(), 2);
        assert_eq!(second.splits[0].split_id, "split-a");
        assert_eq!(second.splits[1].split_id, "split-b");
    }

    #[tokio::test]
    async fn append_split_and_publish_rejects_schema_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split.clone())
            .await
            .unwrap();

        let mut split_b = split.clone();
        split_b.split_id = "split-b".into();
        let err = manager
            .append_split_and_publish("idx-1", "events", "sha256:DIFFERENT", None, split_b)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("schema_hash mismatch"));
    }

    #[tokio::test]
    async fn append_split_and_publish_rejects_duplicate_split_id() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split.clone())
            .await
            .unwrap();
        let err = manager
            .append_split_and_publish("idx-1", "events", "sha256:abc", None, split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // ── Bundle pack/unpack ────────────────────────────────────────────

    fn write_file(root: &Path, rel: &str, content: &[u8]) {
        let abs = root.join(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(abs, content).unwrap();
    }

    #[test]
    fn pack_bundle_produces_deterministic_sha256() {
        // Same contents, different creation order → identical bytes + checksum.
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        write_file(a.path(), "index/meta.json", b"{\"v\":1}");
        write_file(a.path(), "index/000.store", b"\x00\x01\x02");
        write_file(b.path(), "index/000.store", b"\x00\x01\x02");
        write_file(b.path(), "index/meta.json", b"{\"v\":1}");

        let (bytes_a, sum_a) = pack_bundle_from_dir(a.path()).unwrap();
        let (bytes_b, sum_b) = pack_bundle_from_dir(b.path()).unwrap();
        assert_eq!(bytes_a, bytes_b);
        assert_eq!(sum_a, sum_b);
        assert!(sum_a.starts_with("sha256:"));
        assert_eq!(sum_a.len(), "sha256:".len() + 64);
    }

    #[test]
    fn pack_then_unpack_roundtrip_preserves_every_file() {
        let src = TempDir::new().unwrap();
        write_file(src.path(), "index/meta.json", b"{\"hello\":true}");
        write_file(src.path(), "index/nested/dir/blob.bin", b"\xaa\xbb\xcc\xdd");
        write_file(src.path(), "top.txt", b"root-level");

        let (bytes, checksum) = pack_bundle_from_dir(src.path()).unwrap();
        assert!(checksum.starts_with("sha256:"));

        let dst = TempDir::new().unwrap();
        unpack_bundle_into(&bytes, dst.path()).unwrap();

        assert_eq!(
            std::fs::read(dst.path().join("index/meta.json")).unwrap(),
            b"{\"hello\":true}"
        );
        assert_eq!(
            std::fs::read(dst.path().join("index/nested/dir/blob.bin")).unwrap(),
            b"\xaa\xbb\xcc\xdd"
        );
        assert_eq!(
            std::fs::read(dst.path().join("top.txt")).unwrap(),
            b"root-level"
        );
    }

    #[test]
    fn pack_then_unpack_then_pack_yields_identical_bytes() {
        let src = TempDir::new().unwrap();
        write_file(src.path(), "a.bin", b"hello");
        write_file(src.path(), "b/c.bin", b"world");
        let (bytes1, checksum1) = pack_bundle_from_dir(src.path()).unwrap();

        let mid = TempDir::new().unwrap();
        unpack_bundle_into(&bytes1, mid.path()).unwrap();
        let (bytes2, checksum2) = pack_bundle_from_dir(mid.path()).unwrap();

        assert_eq!(bytes1, bytes2);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn unpack_rejects_missing_magic() {
        let dst = TempDir::new().unwrap();
        // 14+ bytes so the length check passes, but the magic prefix is wrong.
        let bogus: Vec<u8> = b"BOGUS!!!\x00\x00\x00\x00\x00\x00\x00\x00".to_vec();
        let err = unpack_bundle_into(&bogus, dst.path()).unwrap_err();
        assert!(err.to_string().contains("magic"), "{err}");
    }

    #[test]
    fn unpack_rejects_truncated_header() {
        let dst = TempDir::new().unwrap();
        // First 5 bytes of the magic are valid but the header is incomplete.
        let err = unpack_bundle_into(b"FSBND", dst.path()).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn unpack_rejects_absolute_path_in_bundle() {
        // Hand-craft a bundle with one file whose rel path is absolute.
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(BUNDLE_MAGIC);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        let evil = "/etc/passwd";
        bytes.extend_from_slice(&(evil.len() as u64).to_le_bytes());
        bytes.extend_from_slice(evil.as_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let dst = TempDir::new().unwrap();
        let err = unpack_bundle_into(&bytes, dst.path()).unwrap_err();
        assert!(err.to_string().contains("absolute"), "{err}");
    }

    #[test]
    fn unpack_rejects_parent_dir_component() {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(BUNDLE_MAGIC);
        bytes.extend_from_slice(&1u64.to_le_bytes());
        let evil = "../escape.txt";
        bytes.extend_from_slice(&(evil.len() as u64).to_le_bytes());
        bytes.extend_from_slice(evil.as_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        let dst = TempDir::new().unwrap();
        let err = unpack_bundle_into(&bytes, dst.path()).unwrap_err();
        assert!(
            err.to_string().contains("disallowed path component"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn upload_and_fetch_split_bundle_roundtrips_via_local_backend() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        // Build a local staging dir that looks like a mini split.
        let stage = manager.staging_dir("idx-uuid", "split-42");
        std::fs::create_dir_all(&stage).unwrap();
        write_file(&stage, "index/meta.json", b"{\"meta\":1}");
        write_file(&stage, "index/000.store", b"\x11\x22\x33");

        let (bundle_key, checksum, size) = manager
            .upload_split_bundle("idx-uuid", "split-42", &stage)
            .await
            .unwrap();
        assert_eq!(bundle_key, "idx-uuid/splits/split-42/bundle");
        assert!(checksum.starts_with("sha256:"));
        assert!(size > 0);

        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum: checksum.clone(),
            doc_count: 0,
            size_bytes: size,
            uncompressed_bytes: size,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };

        let hashed = manager.hash_split_bundle("idx-uuid", &split).await.unwrap();
        assert_eq!(hashed, checksum);

        let cache_dir = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(cache_dir.join("index/meta.json")).unwrap(),
            b"{\"meta\":1}"
        );
        assert!(cache_dir.join(".done").exists());
        let cached_size = manager.cached_split_size_bytes("idx-uuid", "split-42");
        assert_eq!(cached_size, dir_size_bytes(&cache_dir));
        assert_ne!(cached_size, size);
        assert_eq!(
            manager
                .cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get("idx-uuid/split-42")
                .map(|entry| entry.size_bytes),
            Some(cached_size)
        );

        // Second fetch should short-circuit without re-downloading (same cache dir).
        let cache_dir_2 = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap();
        assert_eq!(cache_dir, cache_dir_2);
        assert_eq!(
            manager.cached_split_size_bytes("idx-uuid", "split-42"),
            cached_size
        );
    }

    #[tokio::test]
    async fn fetch_split_rejects_unexpected_bundle_path() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: "attacker/splits/split-42/bundle".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:zz".into(),
            doc_count: 0,
            size_bytes: 0,
            uncompressed_bytes: 0,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let err = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not match canonical key"));

        let err = manager
            .hash_split_bundle("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not match canonical key"));
    }

    #[tokio::test]
    async fn fetch_split_rejects_checksum_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let stage = manager.staging_dir("idx-uuid", "split-42");
        std::fs::create_dir_all(&stage).unwrap();
        write_file(&stage, "index/meta.json", b"{}");

        let (bundle_key, checksum, size) = manager
            .upload_split_bundle("idx-uuid", "split-42", &stage)
            .await
            .unwrap();
        let bogus_checksum = format!("sha256:{}", "0".repeat(64));
        assert_ne!(bogus_checksum, checksum);

        let split = RemoteSplitManifest {
            split_id: "split-42".into(),
            state: RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum: bogus_checksum,
            doc_count: 0,
            size_bytes: size,
            uncompressed_bytes: size,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };
        let err = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("bundle checksum mismatch"));
    }

    #[test]
    fn remote_store_load_snapshot_tracks_active_batches() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        assert_eq!(manager.remote_store_load_snapshot().inflight_bytes, 0);
        assert_eq!(manager.remote_store_load_snapshot().queue_depth, 0);

        let guard = manager.begin_remote_store_batch(4096);
        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 4096);
        assert_eq!(snapshot.queue_depth, 1);

        {
            let _nested = manager.begin_remote_store_batch(1024);
            let nested_snapshot = manager.remote_store_load_snapshot();
            assert_eq!(nested_snapshot.inflight_bytes, 5120);
            assert_eq!(nested_snapshot.queue_depth, 2);
        }

        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 4096);
        assert_eq!(snapshot.queue_depth, 1);
        drop(guard);

        let snapshot = manager.remote_store_load_snapshot();
        assert_eq!(snapshot.inflight_bytes, 0);
        assert_eq!(snapshot.queue_depth, 0);
    }

    #[test]
    fn reap_split_cache_respects_live_set_budget_and_pins() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();

        for split_id in ["stale", "live-pinned", "live-budget"] {
            let cache_dir = manager.cache_dir("idx-1", split_id);
            std::fs::create_dir_all(&cache_dir).unwrap();
            std::fs::write(cache_dir.join("blob.bin"), vec![b'x'; 20]).unwrap();
            std::fs::write(cache_dir.join(".done"), b"sha256:test").unwrap();
            manager.record_cached_split_access("idx-1", split_id, dir_size_bytes(&cache_dir));
        }

        {
            let mut entries = manager
                .cache_entries
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            entries.get_mut("idx-1/stale").unwrap().last_access_epoch_ms = 1;
            entries
                .get_mut("idx-1/live-pinned")
                .unwrap()
                .last_access_epoch_ms = 2;
            entries
                .get_mut("idx-1/live-budget")
                .unwrap()
                .last_access_epoch_ms = 3;
        }

        let _pin = manager.pin_cached_split("idx-1", "live-pinned");
        let live_split_ids = HashSet::from(["live-pinned".to_string(), "live-budget".to_string()]);

        manager
            .reap_split_cache("idx-1", &live_split_ids, Some(40))
            .unwrap();

        assert!(!manager.cache_dir("idx-1", "stale").exists());
        assert!(manager.cache_dir("idx-1", "live-pinned").exists());
        assert!(!manager.cache_dir("idx-1", "live-budget").exists());
        assert!(
            manager
                .cached_split_status("idx-1", "live-pinned", "sha256:test")
                .artifact_cached
        );
        assert!(
            !manager
                .cached_split_status("idx-1", "live-budget", "sha256:test")
                .artifact_cached
        );
    }

    #[tokio::test]
    async fn fs028_fs031_split_hydration_governor_and_cancellation_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let stage = manager.staging_dir("idx-uuid", "split-gov");
        std::fs::create_dir_all(&stage).unwrap();
        write_file(&stage, "index/meta.json", b"{\"meta\":12345}");

        let (bundle_key, checksum, size) = manager
            .upload_split_bundle("idx-uuid", "split-gov", &stage)
            .await
            .unwrap();

        let split = RemoteSplitManifest {
            split_id: "split-gov".into(),
            state: RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum,
            doc_count: 1,
            size_bytes: size,
            uncompressed_bytes: size * 2,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
        };

        // 1. Oversized split rejected before download/staging
        manager.resource_governor().set_capacity(
            crate::worker::ResourcePoolKind::ObjectStoreInflight,
            size - 1,
        );
        let err = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap_err();
        assert!(err.is::<crate::worker::ResourceExhaustedError>());
        assert!(!manager.cache_dir("idx-uuid", "split-gov").exists());
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::ObjectStoreInflight),
            0
        );
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::TempDisk),
            0
        );

        // 2. Cancelled context rejects hydration before creating cache/tmp dirs
        manager.resource_governor().set_capacity(
            crate::worker::ResourcePoolKind::ObjectStoreInflight,
            1024 * 1024,
        );
        let cancelled_ctx = crate::worker::RequestCancellationContext::new();
        cancelled_ctx.cancel(crate::worker::CancellationReason::ClientDisconnected);
        let cancel_err = crate::worker::with_task_request_context(
            cancelled_ctx,
            manager.fetch_split_into_cache("idx-uuid", &split),
        )
        .await
        .unwrap_err();
        assert!(cancel_err.is::<crate::worker::RequestCancelledError>());
        assert!(!manager.cache_dir("idx-uuid", "split-gov").exists());

        // 3. Normal hydration reserves and reconciles PersistentCache, and reaping releases it
        let cache_dir = manager
            .fetch_split_into_cache("idx-uuid", &split)
            .await
            .unwrap();
        assert!(cache_dir.exists());
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::ObjectStoreInflight),
            0
        );
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::TempDisk),
            0
        );
        let cached_bytes = manager.cached_split_size_bytes("idx-uuid", "split-gov");
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::PersistentCache),
            cached_bytes
        );

        manager
            .reap_split_cache("idx-uuid", &HashSet::new(), Some(0))
            .unwrap();
        assert_eq!(
            manager
                .resource_governor()
                .reserved_bytes(crate::worker::ResourcePoolKind::PersistentCache),
            0,
            "evicting split from cache must release PersistentCache reservation"
        );
    }
}
