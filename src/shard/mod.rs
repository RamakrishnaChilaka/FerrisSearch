//! Shard management.
//! Each index has N primary shards. Each shard is backed by a `SearchEngine` implementation.
//! The ShardManager owns all local shard engines on this node.

use crate::cluster::settings::SettingsManager;
use crate::cluster::state::{AllocationId, IndexSettings};
use crate::engine::{CompositeEngine, SearchEngine};
use crate::wal::{HotTranslog, TranslogDurability};
use anyhow::Result;
use std::collections::HashMap;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

pub const SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX: &str = "api_delete_index";
pub const SHARD_DATA_REMOVE_REASON_TRANSPORT_DELETE_INDEX: &str = "transport_delete_index_rpc";
pub const SHARD_DATA_REMOVE_REASON_ORPHAN_CLEANUP: &str = "orphan_cleanup_unknown_uuid";
pub const SHARD_DATA_REMOVE_REASON_STALE_UUID_REPLACEMENT: &str = "stale_index_uuid_replacement";
pub const PEER_RECOVERY_IN_PROGRESS_MARKER: &str = "PEER_RECOVERY_IN_PROGRESS";
pub const PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER: &str = "PEER_RECOVERY_AWAITING_MEMBERSHIP";
pub const SHARD_COPY_IDENTITY_FILE: &str = "SHARD_COPY_IDENTITY.json";
const SHARD_COPY_IDENTITY_VERSION: u32 = 2;
type SourceRecoveryIdentity = (String, u32);
type SourceRecoveryLock = Arc<tokio::sync::Mutex<()>>;
type SourceRecoveryLockMap = HashMap<SourceRecoveryIdentity, SourceRecoveryLock>;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct DefinitiveShardCopyFailure {
    message: String,
}

fn definitive_shard_copy_failure(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(DefinitiveShardCopyFailure {
        message: message.into(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ShardCopyIoOperation {
    Open,
    Fence,
    Apply,
    Recovery,
    PendingMarker,
    InstallMarker,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShardCopyIoKey {
    index_uuid: String,
    shard_id: u32,
    allocation_id: AllocationId,
    operation: ShardCopyIoOperation,
}

#[derive(Debug, Clone, Copy)]
struct ShardCopyRetryPolicy {
    max_attempts: u32,
    escalation_window: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for ShardCopyRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            escalation_window: Duration::from_secs(60),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ShardCopyRetryState {
    attempts: u32,
    first_failure: Instant,
    next_attempt: Instant,
    delay: Duration,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "assigned shard copy operation is backing off after {attempts} failures; retry in {retry_after_ms} ms"
)]
pub(crate) struct ShardCopyBackoff {
    attempts: u32,
    retry_after_ms: u128,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "persistent shard copy I/O failure during {operation:?} after {attempts} attempts over {elapsed_ms} ms: {source}"
)]
pub(crate) struct PersistentShardCopyIoFailure {
    operation: ShardCopyIoOperation,
    attempts: u32,
    elapsed_ms: u128,
    #[source]
    source: anyhow::Error,
}

#[derive(Debug, thiserror::Error)]
#[error("local shard storage I/O failed: {source}")]
pub(crate) struct LocalShardStorageFailure {
    #[source]
    source: anyhow::Error,
}

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

#[derive(Clone, Copy)]
enum ShardOpenAuthority {
    Local { allow_schema_reset: bool },
    Assigned { assignment: AssignedShardOpen },
}

struct AssignedOpenRequest<'a> {
    index: &'a str,
    shard_id: u32,
    mappings: &'a HashMap<String, crate::cluster::state::FieldMapping>,
    settings: &'a IndexSettings,
    index_uuid: &'a str,
    assignment: AssignedShardOpen,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerRecoveryAwaitingMembership {
    pub index_uuid: String,
    pub allocation_id: AllocationId,
    pub primary_node_id: String,
    pub primary_term: u64,
}

impl PeerRecoveryAwaitingMembership {
    fn validate(&self) -> Result<()> {
        if self.index_uuid.is_empty() {
            return Err(definitive_shard_copy_failure(
                "peer recovery awaiting-membership marker has an empty index UUID",
            ));
        }
        if self.allocation_id == 0 {
            return Err(definitive_shard_copy_failure(
                "peer recovery awaiting-membership marker has a zero allocation ID",
            ));
        }
        if self.primary_node_id.is_empty() {
            return Err(definitive_shard_copy_failure(
                "peer recovery awaiting-membership marker has an empty primary node",
            ));
        }
        if self.primary_term == 0 {
            return Err(definitive_shard_copy_failure(
                "peer recovery awaiting-membership marker has a zero primary term",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRecoveryTargetState {
    Recovering {
        index_uuid: String,
        allocation_id: AllocationId,
    },
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

pub(crate) struct PeerRecoveryTargetInstall {
    pub index: String,
    pub shard_id: u32,
    pub mappings: HashMap<String, crate::cluster::state::FieldMapping>,
    pub settings: IndexSettings,
    pub index_uuid: String,
    pub allocation_id: AllocationId,
    pub primary_term: u64,
    pub shard_dir: PathBuf,
    pub committed_boundary: crate::engine::sequence::CommittedBoundaryRecord,
    pub expected_files: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct AssignedShardOpen {
    pub allocation_id: AllocationId,
    pub primary_term: u64,
    pub allow_empty_creation: bool,
}

pub(crate) struct ReplicaApplyContext<'a> {
    pub index_uuid: &'a str,
    pub allocation_id: AllocationId,
    pub applied_view_term: u64,
    pub message_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardCopyIdentity {
    pub version: u32,
    pub index_uuid: String,
    pub allocation_id: AllocationId,
    pub replica_fence: u64,
    pub fence_max_seq_no: Option<u64>,
}

#[derive(serde::Deserialize)]
struct ShardCopyIdentityVersionHeader {
    version: u32,
}

impl ShardCopyIdentity {
    fn new(
        index_uuid: &str,
        allocation_id: AllocationId,
        replica_fence: u64,
        fence_max_seq_no: Option<u64>,
    ) -> Result<Self> {
        let identity = Self {
            version: SHARD_COPY_IDENTITY_VERSION,
            index_uuid: index_uuid.to_string(),
            allocation_id,
            replica_fence,
            fence_max_seq_no,
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<()> {
        if self.version != SHARD_COPY_IDENTITY_VERSION {
            return Err(crate::common::unsupported_index_format(
                "shard copy identity",
                format!(
                    "version {} is not supported; expected {}",
                    self.version, SHARD_COPY_IDENTITY_VERSION
                ),
            ));
        }
        if self.index_uuid.is_empty() {
            return Err(definitive_shard_copy_failure(
                "shard copy identity has an empty index UUID",
            ));
        }
        if self.allocation_id == 0 {
            return Err(definitive_shard_copy_failure(
                "shard copy identity has a zero allocation ID",
            ));
        }
        if self.replica_fence == 0 {
            return Err(definitive_shard_copy_failure(
                "shard copy identity has a zero replica fence",
            ));
        }
        Ok(())
    }

    fn validate_expected(&self, index_uuid: &str, allocation_id: AllocationId) -> Result<()> {
        self.validate()?;
        if self.index_uuid != index_uuid {
            return Err(definitive_shard_copy_failure(format!(
                "local shard copy UUID mismatch: expected {index_uuid}, found {}",
                self.index_uuid
            )));
        }
        if self.allocation_id != allocation_id {
            return Err(definitive_shard_copy_failure(format!(
                "local shard copy allocation mismatch: expected {allocation_id}, found {}",
                self.allocation_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerRecoveryInstallMarker {
    version: u32,
    index_uuid: String,
    allocation_id: AllocationId,
}

impl PeerRecoveryInstallMarker {
    fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(definitive_shard_copy_failure(format!(
                "unsupported peer recovery install marker version {}",
                self.version
            )));
        }
        if self.index_uuid.is_empty() {
            return Err(definitive_shard_copy_failure(
                "peer recovery install marker has an empty index UUID",
            ));
        }
        if self.allocation_id == 0 {
            return Err(definitive_shard_copy_failure(
                "peer recovery install marker has a zero allocation ID",
            ));
        }
        Ok(())
    }
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
    pub allocation_id: AllocationId,
    /// Highest contiguous processed checkpoint observed for this allocation.
    pub processed_checkpoint: Option<u64>,
    /// Highest contiguous persisted checkpoint observed for this allocation.
    pub persisted_checkpoint: Option<u64>,
    /// When we last heard from this replica.
    pub last_updated: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaCheckpointUpdate {
    pub node_id: String,
    pub allocation_id: AllocationId,
    pub processed_checkpoint: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicaCheckpointContext<'a> {
    pub index_uuid: &'a str,
    pub primary_term: u64,
    pub primary_processed_checkpoint: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ReplicaGapObservation {
    pub index_uuid: String,
    pub replica_node_id: String,
    pub allocation_id: AllocationId,
    pub primary_term: u64,
    pub first_seen: Instant,
    pub target_checkpoint: u64,
    pub max_reported_checkpoint: Option<u64>,
}

impl ReplicaGapObservation {
    fn same_probe_identity(&self, other: &Self) -> bool {
        self.index_uuid == other.index_uuid
            && self.replica_node_id == other.replica_node_id
            && self.allocation_id == other.allocation_id
            && self.primary_term == other.primary_term
            && self.first_seen == other.first_seen
            && self.target_checkpoint == other.target_checkpoint
    }
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
    gap_observations: RwLock<HashMap<ShardKey, HashMap<String, ReplicaGapObservation>>>,
    /// Maximum allowed seq_no lag for the diagnostic lag-eligible view.
    max_lag: u64,
}

impl IsrTracker {
    pub fn new(max_lag: u64) -> Self {
        Self {
            replicas: RwLock::new(HashMap::new()),
            gap_observations: RwLock::new(HashMap::new()),
            max_lag,
        }
    }

    fn max_checkpoint(current: Option<u64>, reported: Option<u64>) -> Option<u64> {
        match (current, reported) {
            (Some(current), Some(reported)) => Some(current.max(reported)),
            (Some(current), None) => Some(current),
            (None, Some(reported)) => Some(reported),
            (None, None) => None,
        }
    }

    pub fn update_replica_checkpoint(
        &self,
        index: &str,
        index_uuid: &str,
        shard_id: u32,
        primary_term: u64,
        primary_processed_checkpoint: Option<u64>,
        checkpoint: ReplicaCheckpointUpdate,
    ) {
        self.update_replica_checkpoints_at(
            index,
            shard_id,
            ReplicaCheckpointContext {
                index_uuid,
                primary_term,
                primary_processed_checkpoint,
            },
            std::slice::from_ref(&checkpoint),
            Instant::now(),
        );
    }

    /// Update multiple replica checkpoints from a replication round.
    pub fn update_replica_checkpoints(
        &self,
        index: &str,
        index_uuid: &str,
        shard_id: u32,
        primary_term: u64,
        primary_processed_checkpoint: Option<u64>,
        checkpoints: &[ReplicaCheckpointUpdate],
    ) {
        self.update_replica_checkpoints_at(
            index,
            shard_id,
            ReplicaCheckpointContext {
                index_uuid,
                primary_term,
                primary_processed_checkpoint,
            },
            checkpoints,
            Instant::now(),
        );
    }

    pub(crate) fn update_replica_checkpoints_at(
        &self,
        index: &str,
        shard_id: u32,
        context: ReplicaCheckpointContext<'_>,
        checkpoints: &[ReplicaCheckpointUpdate],
        now: Instant,
    ) {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        let mut gaps = self
            .gap_observations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let shard_replicas = replicas.entry(key.clone()).or_default();
        let shard_gaps = gaps.entry(key).or_default();
        for checkpoint in checkpoints {
            let stored = shard_replicas
                .entry(checkpoint.node_id.clone())
                .or_insert_with(|| ReplicaCheckpoint {
                    allocation_id: checkpoint.allocation_id,
                    processed_checkpoint: None,
                    persisted_checkpoint: None,
                    last_updated: now,
                });
            if stored.allocation_id != checkpoint.allocation_id {
                *stored = ReplicaCheckpoint {
                    allocation_id: checkpoint.allocation_id,
                    processed_checkpoint: None,
                    persisted_checkpoint: None,
                    last_updated: now,
                };
                shard_gaps.remove(&checkpoint.node_id);
            }
            stored.processed_checkpoint =
                Self::max_checkpoint(stored.processed_checkpoint, checkpoint.processed_checkpoint);
            stored.persisted_checkpoint =
                Self::max_checkpoint(stored.persisted_checkpoint, checkpoint.persisted_checkpoint);
            stored.last_updated = now;

            if let Some(observation) = shard_gaps.get_mut(&checkpoint.node_id) {
                if observation.index_uuid != context.index_uuid
                    || observation.allocation_id != checkpoint.allocation_id
                    || observation.primary_term != context.primary_term
                {
                    shard_gaps.remove(&checkpoint.node_id);
                } else {
                    observation.max_reported_checkpoint = stored.processed_checkpoint;
                    if stored
                        .processed_checkpoint
                        .is_some_and(|reported| reported >= observation.target_checkpoint)
                    {
                        shard_gaps.remove(&checkpoint.node_id);
                    }
                }
            }

            if !shard_gaps.contains_key(&checkpoint.node_id)
                && let Some(target_checkpoint) = context.primary_processed_checkpoint
                && stored
                    .processed_checkpoint
                    .is_none_or(|reported| reported < target_checkpoint)
            {
                shard_gaps.insert(
                    checkpoint.node_id.clone(),
                    ReplicaGapObservation {
                        index_uuid: context.index_uuid.to_string(),
                        replica_node_id: checkpoint.node_id.clone(),
                        allocation_id: checkpoint.allocation_id,
                        primary_term: context.primary_term,
                        first_seen: now,
                        target_checkpoint,
                        max_reported_checkpoint: stored.processed_checkpoint,
                    },
                );
            }
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
                .filter(|(_, rc)| {
                    rc.processed_checkpoint.is_some_and(|checkpoint| {
                        primary_checkpoint.saturating_sub(checkpoint) <= self.max_lag
                    })
                })
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
                .filter_map(|(node_id, rc)| {
                    rc.processed_checkpoint
                        .map(|checkpoint| (node_id.clone(), checkpoint))
                })
                .collect(),
            None => vec![],
        }
    }

    pub fn expired_gap_observations(
        &self,
        timeout: Duration,
    ) -> Vec<(ShardKey, ReplicaGapObservation)> {
        self.expired_gap_observations_at(timeout, Instant::now())
    }

    fn expired_gap_observations_at(
        &self,
        timeout: Duration,
        now: Instant,
    ) -> Vec<(ShardKey, ReplicaGapObservation)> {
        self.gap_observations
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .flat_map(|(key, observations)| {
                observations
                    .values()
                    .filter(|observation| now.duration_since(observation.first_seen) >= timeout)
                    .map(|observation| (key.clone(), observation.clone()))
            })
            .collect()
    }

    pub fn record_gap_probe_checkpoint(
        &self,
        index: &str,
        shard_id: u32,
        expected: &ReplicaGapObservation,
        processed_checkpoint: Option<u64>,
    ) -> bool {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        let mut gaps = self
            .gap_observations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let Some(observation) = gaps
            .get_mut(&key)
            .and_then(|observations| observations.get_mut(&expected.replica_node_id))
        else {
            return false;
        };
        if !observation.same_probe_identity(expected) {
            return false;
        }
        let max_reported =
            Self::max_checkpoint(observation.max_reported_checkpoint, processed_checkpoint);
        observation.max_reported_checkpoint = max_reported;
        if let Some(stored) = replicas
            .get_mut(&key)
            .and_then(|replicas| replicas.get_mut(&expected.replica_node_id))
            .filter(|stored| stored.allocation_id == expected.allocation_id)
        {
            stored.processed_checkpoint =
                Self::max_checkpoint(stored.processed_checkpoint, processed_checkpoint);
            stored.last_updated = Instant::now();
        }
        if max_reported.is_some_and(|reported| reported >= observation.target_checkpoint) {
            if let Some(observations) = gaps.get_mut(&key) {
                observations.remove(&expected.replica_node_id);
            }
            return true;
        }
        false
    }

    pub fn remove_gap_observation(
        &self,
        index: &str,
        shard_id: u32,
        expected: &ReplicaGapObservation,
    ) {
        let key = ShardKey::new(index, shard_id);
        let mut gaps = self
            .gap_observations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let Some(observations) = gaps.get_mut(&key) else {
            return;
        };
        if observations
            .get(&expected.replica_node_id)
            .is_some_and(|observation| observation.same_probe_identity(expected))
        {
            observations.remove(&expected.replica_node_id);
        }
    }

    pub fn has_gap_observation(
        &self,
        index: &str,
        shard_id: u32,
        expected: &ReplicaGapObservation,
    ) -> bool {
        self.gap_observations
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ShardKey::new(index, shard_id))
            .and_then(|observations| observations.get(&expected.replica_node_id))
            .is_some_and(|observation| observation.same_probe_identity(expected))
    }

    #[cfg(test)]
    pub(crate) fn gap_observations(
        &self,
        index: &str,
        shard_id: u32,
    ) -> Vec<ReplicaGapObservation> {
        self.gap_observations
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ShardKey::new(index, shard_id))
            .map(|observations| observations.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove tracking data for a shard (e.g., when index is deleted).
    pub fn remove_shard(&self, index: &str, shard_id: u32) {
        let key = ShardKey::new(index, shard_id);
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        replicas.remove(&key);
        self.gap_observations
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
    }

    /// Remove tracking for all shards of an index.
    pub fn remove_index(&self, index: &str) {
        let mut replicas = self.replicas.write().unwrap_or_else(|e| e.into_inner());
        replicas.retain(|k, _| k.index != index);
        self.gap_observations
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, _| k.index != index);
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
    /// Validated durable identity for each currently open shard copy.
    copy_identities: RwLock<HashMap<ShardKey, ShardCopyIdentity>>,
    copy_io_retries: Mutex<HashMap<ShardCopyIoKey, ShardCopyRetryState>>,
    copy_io_attempt_locks: Mutex<HashMap<ShardCopyIoKey, Arc<Mutex<()>>>>,
    copy_retry_policy: RwLock<ShardCopyRetryPolicy>,
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
    #[cfg(test)]
    assigned_open_io_failure: Mutex<Option<(i32, usize)>>,
    #[cfg(test)]
    assigned_open_attempts: AtomicUsize,
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
            copy_identities: RwLock::new(HashMap::new()),
            copy_io_retries: Mutex::new(HashMap::new()),
            copy_io_attempt_locks: Mutex::new(HashMap::new()),
            copy_retry_policy: RwLock::new(ShardCopyRetryPolicy::default()),
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
            #[cfg(test)]
            assigned_open_io_failure: Mutex::new(None),
            #[cfg(test)]
            assigned_open_attempts: AtomicUsize::new(0),
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

    fn shard_open_lock(&self, key: &ShardKey) -> Arc<Mutex<()>> {
        self.open_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(key.clone())
            .or_default()
            .clone()
    }

    fn copy_io_key(
        index_uuid: &str,
        shard_id: u32,
        allocation_id: AllocationId,
        operation: ShardCopyIoOperation,
    ) -> ShardCopyIoKey {
        ShardCopyIoKey {
            index_uuid: index_uuid.to_string(),
            shard_id,
            allocation_id,
            operation,
        }
    }

    fn ensure_copy_io_attempt_allowed(&self, key: &ShardCopyIoKey) -> Result<()> {
        let now = Instant::now();
        let retries = self
            .copy_io_retries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(retry) = retries.get(key) else {
            return Ok(());
        };
        if now >= retry.next_attempt {
            return Ok(());
        }
        Err(ShardCopyBackoff {
            attempts: retry.attempts,
            retry_after_ms: retry.next_attempt.duration_since(now).as_millis(),
        }
        .into())
    }

    fn copy_io_attempt_lock(&self, key: &ShardCopyIoKey) -> Arc<Mutex<()>> {
        self.copy_io_attempt_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(key.clone())
            .or_default()
            .clone()
    }

    fn record_copy_io_failure(&self, key: ShardCopyIoKey, error: anyhow::Error) -> anyhow::Error {
        let now = Instant::now();
        let operation = key.operation;
        let policy = *self
            .copy_retry_policy
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let mut retries = self
            .copy_io_retries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let retry = retries.entry(key).or_insert(ShardCopyRetryState {
            attempts: 0,
            first_failure: now,
            next_attempt: now,
            delay: policy.initial_backoff,
        });
        retry.attempts = retry.attempts.saturating_add(1);
        if retry.attempts > 1 {
            retry.delay = (retry.delay * 2).min(policy.max_backoff);
        }
        retry.next_attempt = now + retry.delay;
        let elapsed = now.duration_since(retry.first_failure);
        let attempts = retry.attempts;
        drop(retries);

        if Self::is_retryable_io_failure(&error)
            && attempts >= policy.max_attempts
            && elapsed >= policy.escalation_window
        {
            PersistentShardCopyIoFailure {
                operation,
                attempts,
                elapsed_ms: elapsed.as_millis(),
                source: error,
            }
            .into()
        } else {
            error
        }
    }

    fn clear_copy_io_failure(&self, key: &ShardCopyIoKey) {
        self.copy_io_retries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(key);
    }

    fn is_retryable_io_failure(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            if cause.downcast_ref::<std::io::Error>().is_some() {
                return true;
            }
            if cause
                .downcast_ref::<crate::engine::tantivy::TantivyWriterUnavailableError>()
                .is_some()
            {
                return true;
            }
            if cause
                .downcast_ref::<crate::engine::tantivy::TantivyCommitFailureError>()
                .is_some()
            {
                return true;
            }
            let Some(tantivy_error) = cause.downcast_ref::<tantivy::TantivyError>() else {
                return false;
            };
            matches!(
                tantivy_error,
                tantivy::TantivyError::IoError(_)
                    | tantivy::TantivyError::OpenDirectoryError(
                        tantivy::directory::error::OpenDirectoryError::FailedToCreateTempDir(_)
                            | tantivy::directory::error::OpenDirectoryError::IoError { .. }
                    )
                    | tantivy::TantivyError::OpenReadError(
                        tantivy::directory::error::OpenReadError::IoError { .. }
                    )
                    | tantivy::TantivyError::OpenWriteError(
                        tantivy::directory::error::OpenWriteError::IoError { .. }
                    )
                    | tantivy::TantivyError::LockFailure(
                        tantivy::directory::error::LockError::IoError(_),
                        _
                    )
            )
        })
    }

    fn tantivy_failure_is_definitive(error: &tantivy::TantivyError) -> bool {
        matches!(
            error,
            tantivy::TantivyError::DataCorruption(_)
                | tantivy::TantivyError::FieldNotFound(_)
                | tantivy::TantivyError::InvalidArgument(_)
                | tantivy::TantivyError::IndexBuilderMissingArgument(_)
                | tantivy::TantivyError::SchemaError(_)
                | tantivy::TantivyError::IncompatibleIndex(_)
                | tantivy::TantivyError::DeserializeError(_)
                | tantivy::TantivyError::OpenDirectoryError(
                    tantivy::directory::error::OpenDirectoryError::DoesNotExist(_)
                        | tantivy::directory::error::OpenDirectoryError::NotADirectory(_)
                )
                | tantivy::TantivyError::OpenReadError(
                    tantivy::directory::error::OpenReadError::FileDoesNotExist(_)
                        | tantivy::directory::error::OpenReadError::IncompatibleIndex(_)
                )
        )
    }

    #[cfg(test)]
    pub(crate) fn set_copy_retry_policy_for_test(
        &self,
        max_attempts: u32,
        escalation_window: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) {
        *self
            .copy_retry_policy
            .write()
            .unwrap_or_else(|error| error.into_inner()) = ShardCopyRetryPolicy {
            max_attempts,
            escalation_window,
            initial_backoff,
            max_backoff,
        };
    }

    pub fn configure_copy_retry_policy(&self, max_attempts: u32, escalation_window: Duration) {
        let mut policy = self
            .copy_retry_policy
            .write()
            .unwrap_or_else(|error| error.into_inner());
        policy.max_attempts = max_attempts;
        policy.escalation_window = escalation_window;
    }

    #[cfg(test)]
    pub(crate) fn inject_assigned_open_io_failures(&self, raw_os_error: i32, attempts: usize) {
        *self
            .assigned_open_io_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((raw_os_error, attempts));
        self.assigned_open_attempts
            .store(0, AtomicOrdering::Release);
    }

    #[cfg(test)]
    pub(crate) fn assigned_open_attempts_for_test(&self) -> usize {
        self.assigned_open_attempts.load(AtomicOrdering::Acquire)
    }

    #[cfg(test)]
    fn maybe_inject_assigned_open_io_failure(&self) -> Result<()> {
        self.assigned_open_attempts
            .fetch_add(1, AtomicOrdering::AcqRel);
        let mut injection = self
            .assigned_open_io_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some((raw_os_error, remaining)) = injection.as_mut() else {
            return Ok(());
        };
        if *remaining == 0 {
            *injection = None;
            return Ok(());
        }
        *remaining -= 1;
        Err(std::io::Error::from_raw_os_error(*raw_os_error).into())
    }

    fn copy_identity_path(shard_dir: &std::path::Path) -> PathBuf {
        shard_dir.join(SHARD_COPY_IDENTITY_FILE)
    }

    fn persist_copy_identity(
        shard_dir: &std::path::Path,
        identity: &ShardCopyIdentity,
    ) -> Result<()> {
        identity.validate()?;
        std::fs::create_dir_all(shard_dir)?;
        let path = Self::copy_identity_path(shard_dir);
        let temporary_path = shard_dir.join(format!("{SHARD_COPY_IDENTITY_FILE}.tmp"));
        let bytes = serde_json::to_vec(identity)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary_path, &path)?;
        std::fs::File::open(shard_dir)?.sync_all()?;
        Ok(())
    }

    fn load_copy_identity(shard_dir: &std::path::Path) -> Result<ShardCopyIdentity> {
        let path = Self::copy_identity_path(shard_dir);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(definitive_shard_copy_failure(format!(
                    "assigned shard copy is missing durable identity {path:?}"
                )));
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "read shard copy identity {path:?}: {error}"
                ));
            }
        };
        let header: ShardCopyIdentityVersionHeader =
            serde_json::from_slice(&bytes).map_err(|error| {
                crate::common::unsupported_index_format(
                    "shard copy identity",
                    format!("cannot decode version header at {path:?}: {error}"),
                )
            })?;
        if header.version != SHARD_COPY_IDENTITY_VERSION {
            return Err(crate::common::unsupported_index_format(
                "shard copy identity",
                format!(
                    "version {} is not supported; expected {}",
                    header.version, SHARD_COPY_IDENTITY_VERSION
                ),
            ));
        }
        let identity: ShardCopyIdentity = serde_json::from_slice(&bytes).map_err(|error| {
            crate::common::unsupported_index_format(
                "shard copy identity",
                format!("cannot decode current format at {path:?}: {error}"),
            )
        })?;
        identity.validate()?;
        Ok(identity)
    }

    fn load_peer_recovery_install_marker(
        shard_dir: &std::path::Path,
    ) -> Result<Option<PeerRecoveryInstallMarker>> {
        let marker_path = shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER);
        let bytes = match std::fs::read(&marker_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "read peer recovery install marker {marker_path:?}: {error}"
                ));
            }
        };
        let marker: PeerRecoveryInstallMarker =
            serde_json::from_slice(&bytes).map_err(|error| {
                definitive_shard_copy_failure(format!(
                    "decode peer recovery install marker {marker_path:?}: {error}"
                ))
            })?;
        marker.validate()?;
        Ok(Some(marker))
    }

    fn ensure_no_current_peer_recovery_install(
        shard_dir: &std::path::Path,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        assignment: Option<AssignedShardOpen>,
    ) -> Result<()> {
        let Some(marker) = Self::load_peer_recovery_install_marker(shard_dir)? else {
            return Ok(());
        };
        if let Some(assignment) = assignment {
            let reason = if marker.index_uuid == index_uuid
                && marker.allocation_id == assignment.allocation_id
            {
                "its current allocation".to_string()
            } else {
                format!(
                    "a different allocation (marker UUID {}, allocation {})",
                    marker.index_uuid, marker.allocation_id
                )
            };
            return Err(definitive_shard_copy_failure(format!(
                "shard {index}/{shard_id} has an incomplete peer recovery installation for {reason}"
            )));
        }
        anyhow::bail!(
            "shard {index}/{shard_id} has an incomplete peer recovery installation for a different allocation"
        );
    }

    fn load_peer_recovery_awaiting_membership(
        shard_dir: &std::path::Path,
    ) -> Result<Option<PeerRecoveryAwaitingMembership>> {
        let marker_path = shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER);
        let bytes = match std::fs::read(&marker_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "read peer recovery awaiting-membership marker {marker_path:?}: {error}"
                ));
            }
        };
        let pending: PeerRecoveryAwaitingMembership =
            serde_json::from_slice(&bytes).map_err(|error| {
                definitive_shard_copy_failure(format!(
                    "decode peer recovery awaiting-membership marker {marker_path:?}: {error}"
                ))
            })?;
        pending.validate()?;
        Ok(Some(pending))
    }

    fn matching_peer_recovery_awaiting_membership(
        shard_dir: &std::path::Path,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) -> Result<Option<PeerRecoveryAwaitingMembership>> {
        Ok(
            Self::load_peer_recovery_awaiting_membership(shard_dir)?.filter(|pending| {
                pending.index_uuid == index_uuid && pending.allocation_id == allocation_id
            }),
        )
    }

    fn remove_stale_copy_identity_temp(shard_dir: &std::path::Path) -> Result<()> {
        let temporary_path = shard_dir.join(format!("{SHARD_COPY_IDENTITY_FILE}.tmp"));
        match std::fs::remove_file(&temporary_path) {
            Ok(()) => std::fs::File::open(shard_dir)?.sync_all()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "remove stale shard copy identity temporary file {temporary_path:?}: {error}"
                ));
            }
        }
        Ok(())
    }

    fn directory_tree_is_empty(path: &std::path::Path) -> Result<bool> {
        if !path.try_exists()? {
            return Ok(true);
        }
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();
            if entry.file_type()?.is_dir() {
                if !Self::directory_tree_is_empty(&entry_path)? {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn cache_copy_identity(&self, key: &ShardKey, identity: ShardCopyIdentity) {
        self.copy_identities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.clone(), identity);
    }

    fn validated_cached_copy_identity(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) -> Result<ShardCopyIdentity> {
        let identities = self
            .copy_identities
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let identity = identities.get(key).ok_or_else(|| {
            definitive_shard_copy_failure(format!(
                "open shard {}/{} has no validated local copy identity",
                key.index, key.shard_id
            ))
        })?;
        identity.validate_expected(index_uuid, allocation_id)?;
        Ok(identity.clone())
    }

    fn prepare_assigned_copy_identity(
        &self,
        key: &ShardKey,
        shard_dir: &std::path::Path,
        index_uuid: &str,
        assignment: AssignedShardOpen,
    ) -> Result<ShardCopyIdentity> {
        if assignment.allocation_id == 0 {
            anyhow::bail!("assigned shard copy has a zero allocation ID");
        }

        if assignment.primary_term == 0 {
            anyhow::bail!("assigned shard copy has a zero primary term");
        }
        if !assignment.allow_empty_creation {
            match std::fs::metadata(shard_dir) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(definitive_shard_copy_failure(format!(
                        "assigned shard copy {}/{} path {shard_dir:?} is not a directory",
                        key.index, key.shard_id
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(definitive_shard_copy_failure(format!(
                        "assigned shard copy {}/{} is missing its shard directory {shard_dir:?}",
                        key.index, key.shard_id
                    )));
                }
                Err(error) => return Err(error.into()),
            }
        }
        let identity_path = Self::copy_identity_path(shard_dir);
        let identity = if identity_path.try_exists()? {
            let identity = Self::load_copy_identity(shard_dir)?;
            identity.validate_expected(index_uuid, assignment.allocation_id)?;
            identity
        } else {
            if !assignment.allow_empty_creation {
                return Err(definitive_shard_copy_failure(format!(
                    "assigned shard copy {}/{} is missing durable identity",
                    key.index, key.shard_id
                )));
            }
            Self::remove_stale_copy_identity_temp(shard_dir)?;
            if !Self::directory_tree_is_empty(shard_dir)? {
                return Err(definitive_shard_copy_failure(format!(
                    "assigned shard copy {}/{} has data but no durable identity",
                    key.index, key.shard_id
                )));
            }
            let identity = ShardCopyIdentity::new(
                index_uuid,
                assignment.allocation_id,
                assignment.primary_term,
                None,
            )?;
            Self::persist_copy_identity(shard_dir, &identity)?;
            identity
        };
        if !assignment.allow_empty_creation {
            let meta_path = shard_dir.join("index").join("meta.json");
            match std::fs::metadata(&meta_path) {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => {
                    return Err(definitive_shard_copy_failure(format!(
                        "assigned shard copy {}/{} Tantivy metadata path {meta_path:?} is not a file",
                        key.index, key.shard_id
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(definitive_shard_copy_failure(format!(
                        "assigned shard copy {}/{} is missing Tantivy metadata {meta_path:?}",
                        key.index, key.shard_id
                    )));
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.cache_copy_identity(key, identity.clone());
        Ok(identity)
    }

    fn ensure_local_test_identity(
        &self,
        key: &ShardKey,
        shard_dir: &std::path::Path,
        index_uuid: &str,
    ) -> Result<ShardCopyIdentity> {
        if let Some(identity) = self
            .copy_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
            .cloned()
        {
            identity.validate()?;
            if identity.index_uuid != index_uuid {
                anyhow::bail!(
                    "local test shard copy UUID mismatch: expected {index_uuid}, found {}",
                    identity.index_uuid
                );
            }
            return Ok(identity);
        }
        if Self::copy_identity_path(shard_dir).try_exists()? {
            let identity = Self::load_copy_identity(shard_dir)?;
            if identity.index_uuid != index_uuid {
                anyhow::bail!(
                    "local test shard copy UUID mismatch: expected {index_uuid}, found {}",
                    identity.index_uuid
                );
            }
            self.cache_copy_identity(key, identity.clone());
            return Ok(identity);
        }
        Self::remove_stale_copy_identity_temp(shard_dir)?;
        if !Self::directory_tree_is_empty(shard_dir)? {
            anyhow::bail!(
                "local test shard copy {}/{} has data but no durable identity",
                key.index,
                key.shard_id
            );
        }
        let identity = ShardCopyIdentity::new(index_uuid, 1, 1, None)?;
        Self::persist_copy_identity(shard_dir, &identity)?;
        self.cache_copy_identity(key, identity.clone());
        Ok(identity)
    }

    pub fn copy_identity(&self, index: &str, shard_id: u32) -> Option<ShardCopyIdentity> {
        self.copy_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&ShardKey::new(index, shard_id))
            .cloned()
    }

    pub fn durability(&self) -> TranslogDurability {
        self.durability
    }

    pub fn validate_open_copy_identity(
        &self,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) -> Result<()> {
        self.validated_cached_copy_identity(
            &ShardKey::new(index, shard_id),
            index_uuid,
            allocation_id,
        )
        .map(|_| ())
    }

    pub(crate) fn is_definitive_copy_failure(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            cause.downcast_ref::<DefinitiveShardCopyFailure>().is_some()
                || cause
                    .downcast_ref::<crate::common::UnsupportedIndexFormatError>()
                    .is_some()
                || cause
                    .downcast_ref::<crate::wal::WalCorruptionError>()
                    .is_some()
                || cause.downcast_ref::<serde_json::Error>().is_some()
                || cause.downcast_ref::<std::num::ParseIntError>().is_some()
                || cause
                    .downcast_ref::<crate::engine::tantivy::AuthoritativeSchemaError>()
                    .is_some()
                || cause
                    .downcast_ref::<crate::engine::sequence::PrimaryTermSequenceCollisionError>()
                    .is_some()
                || cause
                    .downcast_ref::<crate::engine::tantivy::SequenceOperationCollisionError>()
                    .is_some()
                || cause
                    .downcast_ref::<crate::engine::version_map::VersionMapCollisionError>()
                    .is_some()
                || cause
                    .downcast_ref::<tantivy::TantivyError>()
                    .is_some_and(Self::tantivy_failure_is_definitive)
        })
    }

    pub(crate) fn is_sequence_collision_failure(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            cause
                .downcast_ref::<crate::engine::sequence::PrimaryTermSequenceCollisionError>()
                .is_some()
                || cause
                    .downcast_ref::<crate::engine::tantivy::SequenceOperationCollisionError>()
                    .is_some()
                || cause
                    .downcast_ref::<crate::engine::version_map::VersionMapCollisionError>()
                    .is_some()
        })
    }

    pub(crate) fn is_persistent_io_failure(error: &anyhow::Error) -> bool {
        error.is::<PersistentShardCopyIoFailure>()
    }

    pub(crate) fn should_report_copy_failure(error: &anyhow::Error) -> bool {
        Self::is_definitive_copy_failure(error) || Self::is_persistent_io_failure(error)
    }

    pub(crate) fn should_quarantine_copy_failure(error: &anyhow::Error) -> bool {
        if Self::is_definitive_copy_failure(error) {
            return true;
        }
        error.chain().any(|cause| {
            cause
                .downcast_ref::<PersistentShardCopyIoFailure>()
                .is_some_and(|failure| failure.operation != ShardCopyIoOperation::Apply)
        })
    }

    pub(crate) fn ensure_local_apply_allowed(
        &self,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: AllocationId,
    ) -> Result<()> {
        self.ensure_copy_io_attempt_allowed(&Self::copy_io_key(
            index_uuid,
            shard_id,
            allocation_id,
            ShardCopyIoOperation::Apply,
        ))
    }

    pub(crate) fn record_local_apply_result<T>(
        &self,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: AllocationId,
        result: Result<T>,
    ) -> Result<T> {
        let retry_key = Self::copy_io_key(
            index_uuid,
            shard_id,
            allocation_id,
            ShardCopyIoOperation::Apply,
        );
        match result {
            Ok(value) => {
                self.clear_copy_io_failure(&retry_key);
                Ok(value)
            }
            Err(error) if Self::is_definitive_copy_failure(&error) => Err(error),
            Err(error) if Self::is_retryable_io_failure(&error) => {
                Err(self.record_copy_io_failure(retry_key, error))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn record_peer_recovery_failure(
        &self,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: AllocationId,
        error: anyhow::Error,
    ) -> anyhow::Error {
        if Self::is_definitive_copy_failure(&error) {
            return error;
        }
        if !error.is::<LocalShardStorageFailure>() {
            return error;
        }
        self.record_copy_io_failure(
            Self::copy_io_key(
                index_uuid,
                shard_id,
                allocation_id,
                ShardCopyIoOperation::Recovery,
            ),
            error,
        )
    }

    pub(crate) fn local_storage_failure(error: impl Into<anyhow::Error>) -> anyhow::Error {
        anyhow::Error::new(LocalShardStorageFailure {
            source: error.into(),
        })
    }

    pub(crate) fn clear_peer_recovery_failure(
        &self,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: AllocationId,
    ) {
        self.clear_copy_io_failure(&Self::copy_io_key(
            index_uuid,
            shard_id,
            allocation_id,
            ShardCopyIoOperation::Recovery,
        ));
    }

    pub fn quarantine_shard_copy(&self, index: &str, shard_id: u32) {
        let key = ShardKey::new(index, shard_id);
        let per_shard_lock = self.shard_open_lock(&key);
        let _guard = per_shard_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.shards
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&key);
        self.copy_identities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&key);
        self.isr_tracker.remove_shard(index, shard_id);
    }

    pub async fn quarantine_shard_copy_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            shard_manager.quarantine_shard_copy(&index, shard_id);
        })
        .await
        .map_err(|error| anyhow::anyhow!("blocking shard quarantine failed: {error}"))?;
        Ok(())
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

    fn ensure_reopen_target(
        &self,
        index: &str,
        shard_id: u32,
        expected_uuid: &str,
        expected_allocation_id: AllocationId,
    ) -> Result<()> {
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
        let identity = Self::load_copy_identity(&shard_dir)?;
        identity.validate_expected(expected_uuid, expected_allocation_id)?;
        self.cache_copy_identity(&key, identity);
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
        self.open_shard_with_settings_mode(
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            ShardOpenAuthority::Local {
                allow_schema_reset: true,
            },
        )
    }

    pub fn open_shard_with_settings_strict(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
    ) -> Result<Arc<dyn SearchEngine>> {
        self.open_shard_with_settings_mode(
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            ShardOpenAuthority::Local {
                allow_schema_reset: false,
            },
        )
    }

    pub fn open_assigned_shard_with_settings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
        assignment: AssignedShardOpen,
    ) -> Result<Arc<dyn SearchEngine>> {
        self.open_assigned_shard(AssignedOpenRequest {
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            assignment,
        })
    }

    pub fn open_primary_assigned_shard_with_settings(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
        assignment: AssignedShardOpen,
    ) -> Result<Arc<dyn SearchEngine>> {
        self.open_assigned_shard(AssignedOpenRequest {
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            assignment,
        })
    }

    fn open_assigned_shard(
        &self,
        request: AssignedOpenRequest<'_>,
    ) -> Result<Arc<dyn SearchEngine>> {
        let AssignedOpenRequest {
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            assignment,
        } = request;
        let retry_key = Self::copy_io_key(
            index_uuid,
            shard_id,
            assignment.allocation_id,
            ShardCopyIoOperation::PendingMarker,
        );
        let attempt_lock = self.copy_io_attempt_lock(&retry_key);
        let _attempt_guard = attempt_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.ensure_copy_io_attempt_allowed(&retry_key)?;
        #[cfg(test)]
        if let Err(error) = self.maybe_inject_assigned_open_io_failure() {
            return Err(self.record_copy_io_failure(retry_key, error));
        }
        match self.open_shard_with_settings_mode(
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            ShardOpenAuthority::Assigned { assignment },
        ) {
            Ok(engine) => {
                self.clear_copy_io_failure(&retry_key);
                Ok(engine)
            }
            Err(error) => Err(self.record_copy_io_failure(retry_key, error)),
        }
    }

    fn open_shard_with_settings_mode(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
        authority: ShardOpenAuthority,
    ) -> Result<Arc<dyn SearchEngine>> {
        let (assignment, open_mode) = match authority {
            ShardOpenAuthority::Local { allow_schema_reset } => {
                (None, CompositeOpenMode::CreateOrOpen { allow_schema_reset })
            }
            ShardOpenAuthority::Assigned { assignment } => {
                let open_mode = if assignment.allow_empty_creation {
                    CompositeOpenMode::CreateOrOpen {
                        allow_schema_reset: false,
                    }
                } else {
                    CompositeOpenMode::ExistingOnly
                };
                (Some(assignment), open_mode)
            }
        };
        let key = ShardKey::new(index, shard_id);
        let shard_dir = self
            .data_dir
            .join(index_uuid)
            .join(format!("shard_{shard_id}"));
        Self::ensure_no_current_peer_recovery_install(
            &shard_dir, index, shard_id, index_uuid, assignment,
        )?;

        // Fast path: shard already open.
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                if let Some(assignment) = assignment {
                    self.validated_cached_copy_identity(
                        &key,
                        index_uuid,
                        assignment.allocation_id,
                    )?;
                } else {
                    self.ensure_local_test_identity(&key, &shard_dir, index_uuid)?;
                }
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
        let per_shard_lock = self.shard_open_lock(&key);
        let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Re-check after acquiring the per-shard lock — a concurrent caller
        // may have finished opening this shard while we were waiting.
        {
            let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
            if let Some(engine) = shards.get(&key) {
                if let Some(assignment) = assignment {
                    self.validated_cached_copy_identity(
                        &key,
                        index_uuid,
                        assignment.allocation_id,
                    )?;
                } else {
                    self.ensure_local_test_identity(&key, &shard_dir, index_uuid)?;
                }
                return Ok(engine.clone());
            }
        }

        Self::ensure_no_current_peer_recovery_install(
            &shard_dir, index, shard_id, index_uuid, assignment,
        )?;
        let awaiting_membership = if let Some(pending) =
            Self::load_peer_recovery_awaiting_membership(&shard_dir)?
        {
            if pending.index_uuid != index_uuid {
                anyhow::bail!(
                    "peer recovery awaiting-membership marker UUID does not match shard metadata"
                );
            }
            if let Some(assignment) = assignment
                && pending.allocation_id != assignment.allocation_id
            {
                anyhow::bail!(
                    "peer recovery awaiting-membership marker allocation does not match shard metadata"
                );
            }
            Some(pending)
        } else {
            None
        };

        let mut prepared_identity = if let Some(assignment) = assignment {
            Some(self.prepare_assigned_copy_identity(&key, &shard_dir, index_uuid, assignment)?)
        } else {
            None
        };

        self.register_index_uuid(index, index_uuid);

        // Ensure a settings manager exists for this index
        let settings_mgr = self.ensure_settings_manager(index, settings);
        let refresh_interval = settings_mgr.refresh_interval();
        let refresh_rx = settings_mgr.watch_refresh_interval();
        let flush_threshold_rx = settings_mgr.watch_flush_threshold();

        if assignment.is_none() {
            std::fs::create_dir_all(&shard_dir)?;
            prepared_identity =
                Some(self.ensure_local_test_identity(&key, &shard_dir, index_uuid)?);
        }
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
            open_mode,
        )?;
        if let Some(identity) = prepared_identity {
            engine
                .reconcile_term_sequence_state(identity.replica_fence, identity.fence_max_seq_no)?;
        }
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
        if has_vectors {
            if assignment.is_some() {
                engine.rebuild_vectors()?;
            } else if let Err(e) = engine.rebuild_vectors() {
                tracing::warn!(
                    "Failed to rebuild vectors for {}/shard_{}: {}",
                    index,
                    shard_id,
                    e
                );
            }
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

    pub async fn open_assigned_shard_with_settings_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        settings: IndexSettings,
        index_uuid: impl Into<String> + Send + 'static,
        assignment: AssignedShardOpen,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        let uuid_str = index_uuid.into();
        tokio::task::spawn_blocking(move || {
            shard_manager.open_assigned_shard_with_settings(
                &index, shard_id, &mappings, &settings, &uuid_str, assignment,
            )
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking assigned shard open task failed: {e}"))?
    }

    pub async fn open_primary_assigned_shard_with_settings_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        settings: IndexSettings,
        index_uuid: impl Into<String> + Send + 'static,
        assignment: AssignedShardOpen,
    ) -> Result<Arc<dyn SearchEngine>> {
        let shard_manager = self.clone();
        let uuid_str = index_uuid.into();
        tokio::task::spawn_blocking(move || {
            shard_manager.open_primary_assigned_shard_with_settings(
                &index, shard_id, &mappings, &settings, &uuid_str, assignment,
            )
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking primary shard open task failed: {e}"))?
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

    pub(crate) fn apply_replica_operation<T, F>(
        &self,
        index: &str,
        shard_id: u32,
        context: ReplicaApplyContext<'_>,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce(Arc<dyn SearchEngine>) -> Result<T>,
    {
        let key = ShardKey::new(index, shard_id);
        let per_shard_lock = self.shard_open_lock(&key);
        let _guard = per_shard_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut identity =
            self.validated_cached_copy_identity(&key, context.index_uuid, context.allocation_id)?;
        if self.rejects_live_replication(index, shard_id) {
            anyhow::bail!("replica is installing a peer recovery snapshot");
        }
        let required_term = context.applied_view_term.max(identity.replica_fence);
        if context.message_term < required_term {
            anyhow::bail!(
                "replication primary term {} is below local fence {required_term}",
                context.message_term
            );
        }
        let engine = self
            .shards
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("replica shard engine is not open"))?;
        if context.message_term > identity.replica_fence {
            let fence_max_seq_no = engine
                .sequence_stats()
                .max_seq_no
                .into_iter()
                .chain(engine.wal_max_seq_no())
                .max();
            identity.replica_fence = context.message_term;
            identity.fence_max_seq_no = fence_max_seq_no;
            let shard_dir = self
                .data_dir
                .join(context.index_uuid)
                .join(format!("shard_{shard_id}"));
            let retry_key = Self::copy_io_key(
                context.index_uuid,
                shard_id,
                context.allocation_id,
                ShardCopyIoOperation::Fence,
            );
            self.ensure_copy_io_attempt_allowed(&retry_key)?;
            if let Err(error) = Self::persist_copy_identity(&shard_dir, &identity) {
                return Err(self.record_copy_io_failure(retry_key, error));
            }
            self.clear_copy_io_failure(&retry_key);
            self.cache_copy_identity(&key, identity.clone());
            engine
                .reconcile_term_sequence_state(identity.replica_fence, identity.fence_max_seq_no)?;
        }
        self.ensure_local_apply_allowed(context.index_uuid, shard_id, context.allocation_id)?;
        let result = operation(engine);
        self.record_local_apply_result(context.index_uuid, shard_id, context.allocation_id, result)
    }

    pub async fn raise_copy_fence_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
        allocation_id: AllocationId,
        term: u64,
    ) -> Result<()> {
        if term == 0 {
            anyhow::bail!("replica fence term must be greater than zero");
        }
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = shard_manager.shard_open_lock(&key);
            let _guard = per_shard_lock
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut identity =
                shard_manager.validated_cached_copy_identity(&key, &index_uuid, allocation_id)?;
            if term > identity.replica_fence {
                let engine = shard_manager
                    .shards
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!("shard engine is not open during fence raise")
                    })?;
                let fence_max_seq_no = engine
                    .sequence_stats()
                    .max_seq_no
                    .into_iter()
                    .chain(engine.wal_max_seq_no())
                    .max();
                identity.replica_fence = term;
                identity.fence_max_seq_no = fence_max_seq_no;
                let shard_dir = shard_manager
                    .data_dir
                    .join(&index_uuid)
                    .join(format!("shard_{shard_id}"));
                let retry_key = Self::copy_io_key(
                    &index_uuid,
                    shard_id,
                    allocation_id,
                    ShardCopyIoOperation::Fence,
                );
                shard_manager.ensure_copy_io_attempt_allowed(&retry_key)?;
                if let Err(error) = Self::persist_copy_identity(&shard_dir, &identity) {
                    return Err(shard_manager.record_copy_io_failure(retry_key, error));
                }
                shard_manager.clear_copy_io_failure(&retry_key);
                shard_manager.cache_copy_identity(&key, identity.clone());
                engine.reconcile_term_sequence_state(
                    identity.replica_fence,
                    identity.fence_max_seq_no,
                )?;
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("blocking replica fence update failed: {error}"))?
    }

    pub async fn begin_peer_recovery_target_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
        allocation_id: AllocationId,
    ) -> Result<bool> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = shard_manager.shard_open_lock(&key);
            let _guard = per_shard_lock
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let shard_dir = shard_manager
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            if let Some(pending) = Self::matching_peer_recovery_awaiting_membership(
                &shard_dir,
                &index_uuid,
                allocation_id,
            )? {
                shard_manager
                    .peer_recovery_targets
                    .write()
                    .unwrap_or_else(|error| error.into_inner())
                    .entry(key)
                    .or_insert(PeerRecoveryTargetState::FinalizedAwaitingMembership(
                        pending,
                    ));
                return Ok(false);
            }
            let mut targets = shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner());
            if let std::collections::hash_map::Entry::Vacant(entry) = targets.entry(key) {
                entry.insert(PeerRecoveryTargetState::Recovering {
                    index_uuid,
                    allocation_id,
                });
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await
        .map_err(|error| anyhow::anyhow!("blocking peer recovery target start failed: {error}"))?
    }

    pub fn restore_peer_recovery_awaiting_membership(
        &self,
        index: &str,
        shard_id: u32,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        settings: &IndexSettings,
        index_uuid: &str,
        assignment: AssignedShardOpen,
    ) -> Result<bool> {
        let retry_key = Self::copy_io_key(
            index_uuid,
            shard_id,
            assignment.allocation_id,
            ShardCopyIoOperation::Open,
        );
        self.ensure_copy_io_attempt_allowed(&retry_key)?;
        let key = ShardKey::new(index, shard_id);
        let per_shard_lock = self.shard_open_lock(&key);
        let guard = per_shard_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let shard_dir = self
            .data_dir
            .join(index_uuid)
            .join(format!("shard_{shard_id}"));
        let pending = match Self::matching_peer_recovery_awaiting_membership(
            &shard_dir,
            index_uuid,
            assignment.allocation_id,
        ) {
            Ok(Some(pending)) => pending,
            Ok(None) => {
                self.clear_copy_io_failure(&retry_key);
                return Ok(false);
            }
            Err(error) => return Err(self.record_copy_io_failure(retry_key, error)),
        };
        {
            let mut targets = self
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner());
            match targets.get(&key) {
                Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(existing))
                    if existing == &pending => {}
                Some(PeerRecoveryTargetState::Recovering { .. }) => return Ok(false),
                Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(_)) => {
                    anyhow::bail!(
                        "in-memory peer recovery pending state does not match its durable marker"
                    );
                }
                None => {
                    targets.insert(
                        key,
                        PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                    );
                }
            }
        }
        drop(guard);
        self.clear_copy_io_failure(&retry_key);
        self.open_assigned_shard_with_settings(
            index,
            shard_id,
            mappings,
            settings,
            index_uuid,
            AssignedShardOpen {
                allocation_id: assignment.allocation_id,
                primary_term: assignment.primary_term,
                allow_empty_creation: false,
            },
        )?;
        Ok(true)
    }

    pub fn begin_peer_recovery_target(&self, index: &str, shard_id: u32) -> bool {
        let key = ShardKey::new(index, shard_id);
        let Some(identity) = self.copy_identity(index, shard_id) else {
            return false;
        };
        let mut targets = self
            .peer_recovery_targets
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let std::collections::hash_map::Entry::Vacant(entry) = targets.entry(key) {
            entry.insert(PeerRecoveryTargetState::Recovering {
                index_uuid: identity.index_uuid,
                allocation_id: identity.allocation_id,
            });
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

    pub fn failed_peer_recovery_install_matches(
        &self,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) -> Result<bool> {
        let retry_key = Self::copy_io_key(
            index_uuid,
            shard_id,
            allocation_id,
            ShardCopyIoOperation::InstallMarker,
        );
        self.ensure_copy_io_attempt_allowed(&retry_key)?;
        let key = ShardKey::new(index, shard_id);
        let per_shard_lock = self.shard_open_lock(&key);
        let _guard = per_shard_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self
            .peer_recovery_targets
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&key)
        {
            return Ok(false);
        }
        let shard_dir = self
            .data_dir
            .join(index_uuid)
            .join(format!("shard_{shard_id}"));
        let marker = match Self::load_peer_recovery_install_marker(&shard_dir) {
            Ok(Some(marker)) => marker,
            Ok(None) => {
                self.clear_copy_io_failure(&retry_key);
                return Ok(false);
            }
            Err(error) => return Err(self.record_copy_io_failure(retry_key, error)),
        };
        self.clear_copy_io_failure(&retry_key);
        Ok(marker.index_uuid == index_uuid && marker.allocation_id == allocation_id)
    }

    pub fn rejects_live_replication(&self, index: &str, shard_id: u32) -> bool {
        matches!(
            self.peer_recovery_targets
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(&ShardKey::new(index, shard_id)),
            Some(PeerRecoveryTargetState::Recovering { .. })
        )
    }

    pub fn accepts_live_replication_while_pending(&self, index: &str, shard_id: u32) -> bool {
        matches!(
            self.peer_recovery_targets
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(&ShardKey::new(index, shard_id)),
            Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(_))
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

    fn peer_recovery_target_matches(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) -> bool {
        match self
            .peer_recovery_targets
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
        {
            Some(PeerRecoveryTargetState::Recovering {
                index_uuid: active_uuid,
                allocation_id: active_allocation,
            }) => active_uuid == index_uuid && *active_allocation == allocation_id,
            Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(pending)) => {
                pending.index_uuid == index_uuid && pending.allocation_id == allocation_id
            }
            None => false,
        }
    }

    fn remove_peer_recovery_target_if_matches(
        &self,
        key: &ShardKey,
        index_uuid: &str,
        allocation_id: AllocationId,
    ) {
        let mut targets = self
            .peer_recovery_targets
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let matches = match targets.get(key) {
            Some(PeerRecoveryTargetState::Recovering {
                index_uuid: active_uuid,
                allocation_id: active_allocation,
            }) => active_uuid == index_uuid && *active_allocation == allocation_id,
            Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(pending)) => {
                pending.index_uuid == index_uuid && pending.allocation_id == allocation_id
            }
            None => false,
        };
        if matches {
            targets.remove(key);
        }
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
                Some(PeerRecoveryTargetState::Recovering { .. })
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
            let directory_sync = std::fs::File::open(&shard_dir).and_then(|dir| dir.sync_all());
            shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(
                    key,
                    PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                );
            directory_sync?;
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
        allocation_id: AllocationId,
    ) -> Result<PathBuf> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            if allocation_id == 0 {
                anyhow::bail!("peer recovery target has a zero allocation ID");
            }
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = shard_manager.shard_open_lock(&key);
            let _guard = per_shard_lock.lock().unwrap_or_else(|e| e.into_inner());
            let shard_dir = shard_manager
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            if Self::matching_peer_recovery_awaiting_membership(
                &shard_dir,
                &index_uuid,
                allocation_id,
            )?
            .is_some()
            {
                anyhow::bail!("peer recovery target is already finalized and awaiting membership");
            }
            shard_manager
                .shards
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
            shard_manager
                .copy_identities
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            shard_manager.isr_tracker.remove_shard(&index, shard_id);
            shard_manager.register_index_uuid(&index, &index_uuid);

            if shard_dir.try_exists()? {
                Self::remove_dir_all_with_retry(&shard_dir)?;
            }
            std::fs::create_dir_all(shard_dir.join("index"))?;
            let marker_path = shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER);
            let temporary_path = marker_path.with_extension("tmp");
            let marker = PeerRecoveryInstallMarker {
                version: 1,
                index_uuid,
                allocation_id,
            };
            let bytes = serde_json::to_vec(&marker)?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary_path, &marker_path)?;
            std::fs::File::open(&shard_dir)?.sync_all()?;
            Ok(shard_dir)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking peer recovery target preparation failed: {e}"))?
    }

    pub(crate) async fn finalize_peer_recovery_target_blocking(
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
                allocation_id,
                primary_term,
                shard_dir,
                committed_boundary,
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
            let marker_path = shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER);
            if !marker_path.try_exists()? {
                return Err(definitive_shard_copy_failure(
                    "peer recovery marker disappeared before install finalization",
                ));
            }
            let marker: PeerRecoveryInstallMarker =
                serde_json::from_slice(&std::fs::read(&marker_path)?).map_err(|error| {
                    definitive_shard_copy_failure(format!("decode peer recovery marker: {error}"))
                })?;
            marker.validate()?;
            if marker.index_uuid != index_uuid || marker.allocation_id != allocation_id {
                return Err(definitive_shard_copy_failure(
                    "peer recovery install marker does not match the target allocation",
                ));
            }

            committed_boundary.validate()?;
            if committed_boundary.term_sequence_state.current_term != primary_term {
                return Err(definitive_shard_copy_failure(
                    "peer recovery committed boundary term does not match the source term",
                ));
            }
            let allocator_next = committed_boundary
                .max_seq_no
                .map(|max_seq_no| {
                    max_seq_no.checked_add(1).ok_or_else(|| {
                        definitive_shard_copy_failure(
                            "peer recovery maximum sequence exhausts the allocator",
                        )
                    })
                })
                .transpose()?
                .unwrap_or(0);
            HotTranslog::initialize_empty_at(&shard_dir, shard_manager.durability, allocator_next)?;
            let committed_path = shard_dir.join("translog.committed");
            committed_boundary.persist(&committed_path)?;
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
                CompositeOpenMode::ExistingOnly,
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

            let fence_max_seq_no = committed_boundary
                .term_sequence_state
                .max_seq_no_at_term_start;
            let identity =
                ShardCopyIdentity::new(&index_uuid, allocation_id, primary_term, fence_max_seq_no)?;
            Self::persist_copy_identity(&shard_dir, &identity)?;
            engine
                .reconcile_term_sequence_state(identity.replica_fence, identity.fence_max_seq_no)?;
            std::fs::remove_file(&marker_path)?;
            std::fs::File::open(&shard_dir)?.sync_all()?;

            CompositeEngine::start_refresh_loop_reactive(
                engine.clone(),
                refresh_rx,
                flush_threshold_rx,
            );
            let dynamic_engine: Arc<dyn SearchEngine> = engine;
            shard_manager.cache_copy_identity(&key, identity);
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
        allocation_id: AllocationId,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = shard_manager.shard_open_lock(&key);
            let _guard = per_shard_lock
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if shard_manager.index_uuid(&index).as_deref() != Some(index_uuid.as_str())
                || !shard_manager.peer_recovery_target_matches(&key, &index_uuid, allocation_id)
            {
                shard_manager.remove_peer_recovery_target_if_matches(
                    &key,
                    &index_uuid,
                    allocation_id,
                );
                return Ok(());
            }
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
            shard_manager
                .copy_identities
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            let shard_dir = shard_manager
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            std::fs::create_dir_all(&shard_dir)?;
            match std::fs::remove_file(shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let marker_path = shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER);
            let temporary_path = marker_path.with_extension("tmp");
            let marker = PeerRecoveryInstallMarker {
                version: 1,
                index_uuid,
                allocation_id,
            };
            let bytes = serde_json::to_vec(&marker)?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary_path, &marker_path)?;
            std::fs::File::open(shard_dir)?.sync_all()?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking peer recovery abort failed: {e}"))?
    }

    pub async fn reset_peer_recovery_target_for_retry_blocking(
        self: &Arc<Self>,
        index: String,
        shard_id: u32,
        index_uuid: String,
        allocation_id: AllocationId,
    ) -> Result<()> {
        let shard_manager = self.clone();
        tokio::task::spawn_blocking(move || {
            let key = ShardKey::new(&index, shard_id);
            let per_shard_lock = shard_manager.shard_open_lock(&key);
            let _guard = per_shard_lock
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if shard_manager.index_uuid(&index).as_deref() != Some(index_uuid.as_str()) {
                shard_manager.remove_peer_recovery_target_if_matches(
                    &key,
                    &index_uuid,
                    allocation_id,
                );
                return Ok(());
            }
            let shard_dir = shard_manager
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            if let Some(pending) = Self::matching_peer_recovery_awaiting_membership(
                &shard_dir,
                &index_uuid,
                allocation_id,
            )? {
                let mut targets = shard_manager
                    .peer_recovery_targets
                    .write()
                    .unwrap_or_else(|error| error.into_inner());
                match targets.get(&key) {
                    Some(PeerRecoveryTargetState::Recovering {
                        index_uuid: active_uuid,
                        allocation_id: active_allocation,
                    }) if active_uuid == &index_uuid && *active_allocation == allocation_id => {
                        targets.insert(
                            key,
                            PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                        );
                        shard_manager.clear_peer_recovery_failure(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                        );
                        return Ok(());
                    }
                    Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(existing))
                        if existing == &pending =>
                    {
                        shard_manager.clear_peer_recovery_failure(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                        );
                        return Ok(());
                    }
                    Some(_) => {
                        anyhow::bail!(
                            "refusing retry cleanup for a different recovery allocation"
                        );
                    }
                    None => {
                        targets.insert(
                            key,
                            PeerRecoveryTargetState::FinalizedAwaitingMembership(pending),
                        );
                        shard_manager.clear_peer_recovery_failure(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                        );
                        return Ok(());
                    }
                }
            }
            {
                let targets = shard_manager
                    .peer_recovery_targets
                    .read()
                    .unwrap_or_else(|error| error.into_inner());
                match targets.get(&key) {
                    Some(PeerRecoveryTargetState::Recovering {
                        index_uuid: active_uuid,
                        allocation_id: active_allocation,
                    }) if active_uuid == &index_uuid && *active_allocation == allocation_id => {}
                    Some(PeerRecoveryTargetState::FinalizedAwaitingMembership(_)) => {
                        anyhow::bail!(
                            "refusing retry cleanup for a finalized target awaiting membership"
                        );
                    }
                    Some(PeerRecoveryTargetState::Recovering { .. }) => {
                        anyhow::bail!(
                            "refusing retry cleanup for a different recovery allocation"
                        );
                    }
                    None => return Ok(()),
                }
            }
            if let Some(marker) = Self::load_peer_recovery_install_marker(&shard_dir)?
                && (marker.index_uuid != index_uuid || marker.allocation_id != allocation_id)
            {
                anyhow::bail!(
                    "refusing retry cleanup because the install marker belongs to another allocation"
                );
            }
            shard_manager
                .shards
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            shard_manager
                .copy_identities
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            shard_manager.isr_tracker.remove_shard(&index, shard_id);
            if shard_dir.try_exists()? {
                Self::remove_dir_all_with_retry(&shard_dir)?;
            }
            shard_manager
                .peer_recovery_targets
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&key);
            Ok(())
        })
        .await
        .map_err(|error| anyhow::anyhow!("blocking peer recovery retry cleanup failed: {error}"))?
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
        allocation_id: AllocationId,
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
            shard_manager.ensure_reopen_target(&index, shard_id, &index_uuid, allocation_id)?;
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
                blocking_manager.ensure_reopen_target(
                    &index,
                    shard_id,
                    &index_uuid,
                    allocation_id,
                )?;
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
    pub(crate) fn initialize_copy_identity_for_test(
        &self,
        index: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: AllocationId,
        primary_term: u64,
    ) -> Result<()> {
        let key = ShardKey::new(index, shard_id);
        let per_shard_lock = self.shard_open_lock(&key);
        let _guard = per_shard_lock
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let shard_dir = self
            .data_dir
            .join(index_uuid)
            .join(format!("shard_{shard_id}"));
        std::fs::create_dir_all(&shard_dir)?;
        self.prepare_assigned_copy_identity(
            &key,
            &shard_dir,
            index_uuid,
            AssignedShardOpen {
                allocation_id,
                primary_term,
                allow_empty_creation: true,
            },
        )?;
        self.register_index_uuid(index, index_uuid);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn insert_shard_for_test(
        &self,
        index: &str,
        shard_id: u32,
        engine: Arc<dyn SearchEngine>,
    ) {
        let key = ShardKey::new(index, shard_id);
        if let Some(index_uuid) = self.index_uuid(index) {
            let shard_dir = self
                .data_dir
                .join(&index_uuid)
                .join(format!("shard_{shard_id}"));
            self.ensure_local_test_identity(&key, &shard_dir, &index_uuid)
                .expect("test shard identity should persist");
        }
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
        self.copy_identities
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|key, _| key.index != index);

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
            self.copy_io_retries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|key, _| key.index_uuid != uuid);
            self.copy_io_attempt_locks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|key, _| key.index_uuid != uuid);
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

    fn apply_index(
        engine: &Arc<dyn SearchEngine>,
        doc_id: &str,
        source: serde_json::Value,
        seq_no: u64,
        primary_term: u64,
    ) -> Result<crate::engine::ReplicaApplyReceipt> {
        engine.apply_replica_operation(crate::engine::SequencedOperation {
            seq_no,
            primary_term,
            mutation: crate::engine::DocumentMutation::Index {
                doc_id: doc_id.to_string(),
                source,
            },
        })
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
                    1,
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
                    1,
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
                    1,
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

    fn replica_checkpoint(
        node_id: &str,
        allocation_id: u64,
        processed_checkpoint: Option<u64>,
        persisted_checkpoint: Option<u64>,
    ) -> ReplicaCheckpointUpdate {
        ReplicaCheckpointUpdate {
            node_id: node_id.to_string(),
            allocation_id,
            processed_checkpoint,
            persisted_checkpoint,
        }
    }

    fn update_replica_checkpoint(
        tracker: &IsrTracker,
        index: &str,
        shard_id: u32,
        node_id: &str,
        checkpoint: u64,
    ) {
        tracker.update_replica_checkpoint(
            index,
            &format!("{index}-uuid"),
            shard_id,
            1,
            Some(checkpoint),
            replica_checkpoint(node_id, 1, Some(checkpoint), Some(checkpoint)),
        );
    }

    #[test]
    fn isr_tracker_empty_returns_no_replicas() {
        let tracker = IsrTracker::new(100);
        let isr = tracker.in_sync_replicas("idx", 0, 10);
        assert!(isr.is_empty());
    }

    #[test]
    fn isr_tracker_update_and_query_checkpoint() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx", 0, "replica-1", 50);
        update_replica_checkpoint(&tracker, "idx", 0, "replica-2", 90);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 2);

        // Both are within max_lag=100 of primary_checkpoint=100
        let isr = tracker.in_sync_replicas("idx", 0, 100);
        assert_eq!(isr.len(), 2);
    }

    #[test]
    fn isr_tracker_lagging_replica_excluded() {
        let tracker = IsrTracker::new(10); // tight lag threshold
        update_replica_checkpoint(&tracker, "idx", 0, "replica-1", 95);
        update_replica_checkpoint(&tracker, "idx", 0, "replica-2", 50);

        let isr = tracker.in_sync_replicas("idx", 0, 100);
        assert_eq!(isr.len(), 1);
        assert_eq!(isr[0], "replica-1");
    }

    #[test]
    fn isr_tracker_update_batch() {
        let tracker = IsrTracker::new(100);
        let checkpoints = vec![
            replica_checkpoint("r1", 1, Some(10), Some(10)),
            replica_checkpoint("r2", 2, Some(20), Some(20)),
        ];
        tracker.update_replica_checkpoints("idx", "idx-uuid", 0, 1, Some(20), &checkpoints);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 2);
    }

    #[test]
    fn d1_commit3_replica_checkpoint_observation_never_regresses() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 10);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 3);

        let cps = tracker.replica_checkpoints("idx", 0);
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].1, 10);
    }

    #[test]
    fn replica_gap_target_stays_fixed_until_progress_reaches_it() {
        let tracker = IsrTracker::new(100);
        let first_seen = Instant::now();
        tracker.update_replica_checkpoints_at(
            "idx",
            0,
            ReplicaCheckpointContext {
                index_uuid: "idx-uuid",
                primary_term: 4,
                primary_processed_checkpoint: Some(5),
            },
            &[replica_checkpoint("r1", 7, Some(0), Some(0))],
            first_seen,
        );
        tracker.update_replica_checkpoints_at(
            "idx",
            0,
            ReplicaCheckpointContext {
                index_uuid: "idx-uuid",
                primary_term: 4,
                primary_processed_checkpoint: Some(10),
            },
            &[replica_checkpoint("r1", 7, Some(2), Some(2))],
            first_seen + Duration::from_secs(30),
        );

        let observations = tracker.gap_observations("idx", 0);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].target_checkpoint, 5);
        assert_eq!(observations[0].first_seen, first_seen);
        assert_eq!(observations[0].max_reported_checkpoint, Some(2));
    }

    #[test]
    fn reordered_lower_checkpoint_cannot_reopen_a_closed_gap() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 10);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 3);

        assert!(tracker.gap_observations("idx", 0).is_empty());
        assert_eq!(
            tracker.replica_checkpoints("idx", 0),
            vec![("r1".to_string(), 10)]
        );
    }

    #[test]
    fn deadline_probe_clears_an_idle_gap_at_the_fixed_target() {
        let tracker = IsrTracker::new(100);
        let first_seen = Instant::now();
        tracker.update_replica_checkpoints_at(
            "idx",
            0,
            ReplicaCheckpointContext {
                index_uuid: "idx-uuid",
                primary_term: 4,
                primary_processed_checkpoint: Some(5),
            },
            &[replica_checkpoint("r1", 7, Some(0), Some(0))],
            first_seen,
        );
        let mut expired = tracker.expired_gap_observations_at(
            Duration::from_secs(60),
            first_seen + Duration::from_secs(61),
        );
        assert_eq!(expired.len(), 1);
        let (_, observation) = expired.pop().unwrap();

        assert!(tracker.record_gap_probe_checkpoint("idx", 0, &observation, Some(5)));
        assert!(tracker.gap_observations("idx", 0).is_empty());
    }

    #[test]
    fn review_c3_stale_probe_cannot_mutate_reopened_gap_observation() {
        let tracker = IsrTracker::new(100);
        let first_seen = Instant::now();
        tracker.update_replica_checkpoints_at(
            "idx",
            0,
            ReplicaCheckpointContext {
                index_uuid: "idx-uuid",
                primary_term: 4,
                primary_processed_checkpoint: Some(5),
            },
            &[replica_checkpoint("r1", 7, Some(0), Some(0))],
            first_seen,
        );
        let old = tracker.gap_observations("idx", 0).pop().unwrap();
        tracker.remove_gap_observation("idx", 0, &old);

        let reopened_at = first_seen + Duration::from_secs(30);
        tracker.update_replica_checkpoints_at(
            "idx",
            0,
            ReplicaCheckpointContext {
                index_uuid: "idx-uuid",
                primary_term: 4,
                primary_processed_checkpoint: Some(10),
            },
            &[replica_checkpoint("r1", 7, Some(0), Some(0))],
            reopened_at,
        );

        assert!(!tracker.record_gap_probe_checkpoint("idx", 0, &old, Some(10)));
        let observations = tracker.gap_observations("idx", 0);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].first_seen, reopened_at);
        assert_eq!(observations[0].target_checkpoint, 10);
    }

    #[test]
    fn isr_tracker_remove_shard() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 10);
        update_replica_checkpoint(&tracker, "idx", 1, "r1", 20);

        tracker.remove_shard("idx", 0);

        assert!(tracker.replica_checkpoints("idx", 0).is_empty());
        assert_eq!(tracker.replica_checkpoints("idx", 1).len(), 1);
    }

    #[test]
    fn isr_tracker_remove_index() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx-a", 0, "r1", 10);
        update_replica_checkpoint(&tracker, "idx-a", 1, "r1", 20);
        update_replica_checkpoint(&tracker, "idx-b", 0, "r1", 30);

        tracker.remove_index("idx-a");

        assert!(tracker.replica_checkpoints("idx-a", 0).is_empty());
        assert!(tracker.replica_checkpoints("idx-a", 1).is_empty());
        assert_eq!(tracker.replica_checkpoints("idx-b", 0).len(), 1);
    }

    #[test]
    fn isr_tracker_different_shards_independent() {
        let tracker = IsrTracker::new(100);
        update_replica_checkpoint(&tracker, "idx", 0, "r1", 10);
        update_replica_checkpoint(&tracker, "idx", 1, "r2", 20);

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
        update_replica_checkpoint(&mgr.isr_tracker, "my-idx", 0, "r1", 10);
        update_replica_checkpoint(&mgr.isr_tracker, "other-idx", 0, "r1", 20);

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
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 1)
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
        std::fs::write(
            shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "index_uuid": "uuid-1",
                "allocation_id": 1,
            }))
            .unwrap(),
        )
        .unwrap();
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
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager
            .initialize_copy_identity_for_test("idx", 0, "uuid-1", 1, 1)
            .unwrap();
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
        let source: Arc<dyn SearchEngine> = Arc::new(source);
        apply_index(&source, "doc-1", json!({"value": 1}), 0, 1).unwrap();
        apply_index(&source, "doc-2", json!({"value": 2}), 2, 1).unwrap();
        let snapshot_dir = source_dir.path().join("peer-recovery/session");
        let snapshot = source.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        assert_eq!(snapshot.committed_boundary.processed_checkpoint, Some(0));
        assert_eq!(snapshot.committed_boundary.max_seq_no, Some(2));

        let target_dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let shard_dir = manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 1)
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
                allocation_id: 1,
                primary_term: 1,
                shard_dir: shard_dir.clone(),
                committed_boundary: snapshot.committed_boundary.clone(),
                expected_files: snapshot
                    .files
                    .iter()
                    .map(|file| file.name.clone())
                    .collect(),
            })
            .await
            .unwrap();
        assert_eq!(engine.doc_count(), 2);
        assert_eq!(engine.sequence_stats().processed_checkpoint, Some(0));
        assert_eq!(engine.sequence_stats().max_seq_no, Some(2));
        assert!(!shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER).exists());
        source
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
    }

    #[tokio::test]
    async fn assigned_copy_identity_and_fence_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        assert_eq!(
            manager.copy_identity("idx", 0),
            Some(ShardCopyIdentity {
                version: SHARD_COPY_IDENTITY_VERSION,
                index_uuid: "uuid-1".into(),
                allocation_id: 7,
                replica_fence: 2,
                fence_max_seq_no: None,
            })
        );
        apply_index(&engine, "before-raise", json!({"value": 0}), 0, 2).unwrap();
        manager
            .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), 7, 5)
            .await
            .unwrap();
        drop(engine);
        drop(manager);

        let restarted = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        restarted
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: false,
                },
            )
            .unwrap();
        let identity = restarted.copy_identity("idx", 0).unwrap();
        assert_eq!(identity.replica_fence, 5);
        assert_eq!(identity.fence_max_seq_no, Some(0));
    }

    #[test]
    fn assigned_copy_missing_or_malformed_identity_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let shard_dir = dir.path().join("uuid-1/shard_0");
        std::fs::create_dir_all(shard_dir.join("index")).unwrap();
        std::fs::write(shard_dir.join("legacy-data"), b"legacy").unwrap();

        let error = match manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: true,
            },
        ) {
            Ok(_) => panic!("legacy data without identity must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("data but no durable identity"));
        assert!(manager.get_shard("idx", 0).is_none());

        std::fs::remove_file(shard_dir.join("legacy-data")).unwrap();
        std::fs::write(shard_dir.join(SHARD_COPY_IDENTITY_FILE), b"{not-json").unwrap();
        let error = match manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("malformed identity must fail closed"),
            Err(error) => error,
        };
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
        assert!(manager.get_shard("idx", 0).is_none());
    }

    #[test]
    fn no_compat_v1_identity_requires_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let shard_dir = dir.path();
        std::fs::write(
            shard_dir.join(SHARD_COPY_IDENTITY_FILE),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "index_uuid": "uuid-1",
                "allocation_id": 7,
                "replica_fence": 2,
            }))
            .unwrap(),
        )
        .unwrap();

        let error = ShardManager::load_copy_identity(shard_dir).unwrap_err();
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
        assert!(ShardManager::should_report_copy_failure(&error));
        assert!(ShardManager::should_quarantine_copy_failure(&error));
    }

    #[test]
    fn initialized_assignment_never_creates_a_missing_empty_copy() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let shard_dir = dir.path().join("uuid-1/shard_0");

        let error = match manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("initialized missing copy must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("missing its shard directory"));
        assert!(!shard_dir.exists());
    }

    #[tokio::test]
    async fn stale_identity_temp_does_not_block_initial_primary_creation() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let shard_dir = dir.path().join("uuid-1/shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(
            shard_dir.join(format!("{SHARD_COPY_IDENTITY_FILE}.tmp")),
            b"{\"version\":1",
        )
        .unwrap();

        let result = manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 1,
                allow_empty_creation: true,
            },
        );
        assert!(
            result.is_ok(),
            "a stale identity temp file must not block initial creation: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn local_test_open_preserves_an_existing_allocation_identity() {
        let dir = tempfile::tempdir().unwrap();
        {
            let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
            manager
                .open_assigned_shard_with_settings(
                    "idx",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "uuid-1",
                    AssignedShardOpen {
                        allocation_id: 7,
                        primary_term: 3,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
        }

        let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
        restarted
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
            )
            .unwrap();
        assert_eq!(
            restarted.copy_identity("idx", 0).unwrap().allocation_id,
            7,
            "local test helpers must not overwrite a durable allocation identity with 1"
        );
    }

    #[tokio::test]
    async fn matching_pending_marker_refuses_new_recovery_begin_and_prepare() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 3,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        apply_index(&engine, "preserved", serde_json::json!({"value": 1}), 0, 3).unwrap();
        engine.refresh().unwrap();
        assert!(manager.begin_peer_recovery_target("idx", 0));
        manager
            .mark_peer_recovery_awaiting_membership_blocking(
                "idx".into(),
                0,
                PeerRecoveryAwaitingMembership {
                    index_uuid: "uuid-1".into(),
                    allocation_id: 7,
                    primary_node_id: "primary".into(),
                    primary_term: 3,
                },
            )
            .await
            .unwrap();

        assert!(
            !manager
                .begin_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 7)
                .await
                .unwrap()
        );
        let prepare_error = manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 7)
            .await
            .unwrap_err();
        assert!(
            prepare_error
                .to_string()
                .contains("already finalized and awaiting membership")
        );
        assert!(
            manager
                .get_shard("idx", 0)
                .unwrap()
                .get_document("preserved")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn authoritative_open_classifies_a_stale_install_marker_as_definitive() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let shard_dir = dir.path().join("uuid-1/shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(
            shard_dir.join(PEER_RECOVERY_IN_PROGRESS_MARKER),
            serde_json::to_vec(&serde_json::json!({
                "version": 1,
                "index_uuid": "old-uuid",
                "allocation_id": 3,
            }))
            .unwrap(),
        )
        .unwrap();

        let error = match manager.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 3,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("an authoritative copy with a stale install marker must fail closed"),
            Err(error) => error,
        };
        assert!(ShardManager::is_definitive_copy_failure(&error));
        assert!(error.to_string().contains("a different allocation"));
    }

    #[test]
    fn malformed_pending_marker_and_tantivy_metadata_are_definitive() {
        let marker_dir = tempfile::tempdir().unwrap();
        let marker_manager = ShardManager::new(marker_dir.path(), Duration::from_secs(60));
        let shard_dir = marker_dir.path().join("uuid-1/shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(
            shard_dir.join(PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER),
            b"{not-json",
        )
        .unwrap();
        let marker_error = marker_manager
            .restore_peer_recovery_awaiting_membership(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: false,
                },
            )
            .unwrap_err();
        assert!(ShardManager::is_definitive_copy_failure(&marker_error));

        let index_dir = tempfile::tempdir().unwrap();
        {
            let manager = ShardManager::new(index_dir.path(), Duration::from_secs(60));
            manager
                .initialize_copy_identity_for_test("idx", 0, "uuid-1", 7, 2)
                .unwrap();
            CompositeEngine::new(
                index_dir.path().join("uuid-1/shard_0"),
                Duration::from_secs(60),
            )
            .unwrap();
        }
        std::fs::write(
            index_dir.path().join("uuid-1/shard_0/index/meta.json"),
            b"{not-json",
        )
        .unwrap();
        let restarted = ShardManager::new(index_dir.path(), Duration::from_secs(60));
        let meta_error = match restarted.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("corrupt Tantivy metadata must fail closed"),
            Err(error) => error,
        };
        assert!(
            ShardManager::is_definitive_copy_failure(&meta_error),
            "{meta_error:#}"
        );
    }

    #[tokio::test]
    async fn structural_tantivy_schema_failures_are_definitive() {
        let missing_schema_dir = tempfile::tempdir().unwrap();
        {
            let manager = ShardManager::new(missing_schema_dir.path(), Duration::from_secs(60));
            manager
                .open_assigned_shard_with_settings(
                    "idx",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "uuid-1",
                    AssignedShardOpen {
                        allocation_id: 7,
                        primary_term: 2,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
        }
        std::fs::write(
            missing_schema_dir
                .path()
                .join("uuid-1/shard_0/index/meta.json"),
            br#"{"segments":[]}"#,
        )
        .unwrap();
        let restarted = ShardManager::new(missing_schema_dir.path(), Duration::from_secs(60));
        let missing_schema = match restarted.open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("missing Tantivy schema must fail closed"),
            Err(error) => error,
        };
        assert!(
            ShardManager::is_definitive_copy_failure(&missing_schema),
            "{missing_schema:#}"
        );

        let mapping_conflict_dir = tempfile::tempdir().unwrap();
        let text_mapping = HashMap::from([(
            "value".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Text,
                dimension: None,
            },
        )]);
        {
            let manager = ShardManager::new(mapping_conflict_dir.path(), Duration::from_secs(60));
            manager
                .open_assigned_shard_with_settings(
                    "idx",
                    0,
                    &text_mapping,
                    &IndexSettings::default(),
                    "uuid-1",
                    AssignedShardOpen {
                        allocation_id: 7,
                        primary_term: 2,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
        }
        let integer_mapping = HashMap::from([(
            "value".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Integer,
                dimension: None,
            },
        )]);
        let restarted = ShardManager::new(mapping_conflict_dir.path(), Duration::from_secs(60));
        let mapping_conflict = match restarted.open_assigned_shard_with_settings(
            "idx",
            0,
            &integer_mapping,
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: false,
            },
        ) {
            Ok(_) => panic!("authoritative mapping conflict must fail closed"),
            Err(error) => error,
        };
        assert!(
            ShardManager::is_definitive_copy_failure(&mapping_conflict),
            "{mapping_conflict:#}"
        );
    }

    #[tokio::test]
    async fn retryable_recovery_cleanup_does_not_leave_a_failed_install_marker() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 3,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        assert!(
            manager
                .begin_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 7)
                .await
                .unwrap()
        );
        manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-1".into(), 7)
            .await
            .unwrap();
        assert!(
            dir.path()
                .join("uuid-1/shard_0")
                .join(PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        );

        manager
            .reset_peer_recovery_target_for_retry_blocking("idx".into(), 0, "uuid-1".into(), 7)
            .await
            .unwrap();
        assert!(!manager.is_peer_recovery_target("idx", 0));
        assert!(
            !manager
                .failed_peer_recovery_install_matches("idx", 0, "uuid-1", 7)
                .unwrap(),
            "handled retryable recovery failures must not look like crashed installs"
        );
    }

    #[tokio::test]
    async fn abort_after_delete_recreate_does_not_recreate_old_uuid_directory() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "old-uuid",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        assert!(manager.begin_peer_recovery_target("idx", 0));
        manager
            .mark_peer_recovery_awaiting_membership_blocking(
                "idx".into(),
                0,
                PeerRecoveryAwaitingMembership {
                    index_uuid: "old-uuid".into(),
                    allocation_id: 7,
                    primary_node_id: "node-1".into(),
                    primary_term: 2,
                },
            )
            .await
            .unwrap();
        manager
            .close_index_shards_with_reason("idx", SHARD_DATA_REMOVE_REASON_API_DELETE_INDEX)
            .unwrap();
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "new-uuid",
                AssignedShardOpen {
                    allocation_id: 9,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();

        manager
            .abort_peer_recovery_target_blocking("idx".into(), 0, "old-uuid".into(), 7)
            .await
            .unwrap();
        assert!(!dir.path().join("old-uuid").exists());
        assert!(dir.path().join("new-uuid/shard_0").is_dir());
        assert_eq!(manager.copy_identity("idx", 0).unwrap().allocation_id, 9);
    }

    #[cfg(unix)]
    #[test]
    fn shard_directory_metadata_io_error_is_not_classified_as_missing() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let uuid_dir = dir.path().join("uuid-1");
        std::fs::create_dir_all(&uuid_dir).unwrap();
        let shard_dir = uuid_dir.join("shard_0");
        symlink("shard_0", &shard_dir).unwrap();

        let error = manager
            .prepare_assigned_copy_identity(
                &ShardKey::new("idx", 0),
                &shard_dir,
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: false,
                },
            )
            .unwrap_err();
        assert!(
            !ShardManager::is_definitive_copy_failure(&error),
            "filesystem metadata I/O must remain retryable: {error}"
        );
    }

    #[tokio::test]
    async fn persistent_replica_wal_io_escalates_under_apply_key() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine.inject_wal_write_failures_for_test(28, 3);

        let mut reportable = Vec::new();
        for seq_no in 0..3 {
            let error = manager
                .apply_replica_operation(
                    "idx",
                    0,
                    ReplicaApplyContext {
                        index_uuid: "uuid-1",
                        allocation_id: 7,
                        applied_view_term: 2,
                        message_term: 2,
                    },
                    |engine| {
                        apply_index(
                            &engine,
                            "doc",
                            serde_json::json!({"value": seq_no}),
                            seq_no,
                            2,
                        )
                        .map(|_| ())
                    },
                )
                .unwrap_err();
            reportable.push(ShardManager::should_report_copy_failure(&error));
        }
        assert_eq!(reportable, [false, false, true]);
    }

    #[tokio::test]
    async fn transient_force_merge_writer_failure_rebuilds_on_next_write() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .add_document_with_receipt("baseline", serde_json::json!({"value": 0}))
            .unwrap();
        engine.refresh().unwrap();
        engine.inject_writer_replacement_failures_for_test(28, 1);
        assert!(engine.force_merge(1).is_err());

        let rebuilt = manager
            .record_local_apply_result(
                "uuid-1",
                0,
                7,
                engine.add_document_with_receipt("after-rebuild", serde_json::json!({"value": 1})),
            )
            .unwrap();
        assert_eq!(rebuilt.doc_id, "after-rebuild");
    }

    #[tokio::test]
    async fn writer_rebuild_does_not_append_while_another_instance_holds_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let first = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        first
            .add_document_with_receipt("baseline", serde_json::json!({"value": 0}))
            .unwrap();
        first.refresh().unwrap();
        first.inject_writer_replacement_failures_for_test(28, 1);
        assert!(first.force_merge(1).is_err());
        let wal_before = first
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .len();

        let second = crate::engine::CompositeEngine::open_existing_with_mappings(
            dir.path().join("uuid-1/shard_0"),
            Duration::from_secs(60),
            &HashMap::new(),
            crate::wal::TranslogDurability::Request,
            Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        let blocked = first
            .add_document_with_receipt("blocked", serde_json::json!({"value": 1}))
            .unwrap_err();
        assert!(!ShardManager::should_report_copy_failure(&blocked));
        assert_eq!(
            first
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .len(),
            wal_before,
            "a failed writer rebuild must not append a WAL entry"
        );

        drop(second);
        first
            .add_document_with_receipt("after-release", serde_json::json!({"value": 2}))
            .unwrap();
    }

    #[tokio::test]
    async fn persistent_force_merge_writer_rebuild_failure_escalates_under_apply_key() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .add_document_with_receipt("baseline", serde_json::json!({"value": 0}))
            .unwrap();
        engine.refresh().unwrap();
        engine.inject_writer_replacement_failures_for_test(28, usize::MAX);
        assert!(engine.force_merge(1).is_err());

        let mut reportable = Vec::new();
        let mut quarantine = Vec::new();
        for attempt in 0..3 {
            let result = engine.add_document_with_receipt(
                &format!("after-{attempt}"),
                serde_json::json!({"value": attempt}),
            );
            let error = manager
                .record_local_apply_result("uuid-1", 0, 7, result)
                .unwrap_err();
            reportable.push(ShardManager::should_report_copy_failure(&error));
            quarantine.push(ShardManager::should_quarantine_copy_failure(&error));
        }
        assert_eq!(reportable, [false, false, true]);
        assert_eq!(quarantine, [false, false, false]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn transient_commit_failure_replays_acknowledged_writes_before_next_commit_and_restart() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let assignment = AssignedShardOpen {
            allocation_id: 7,
            primary_term: 2,
            allow_empty_creation: true,
        };
        {
            let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
            let engine = manager
                .open_assigned_shard_with_settings(
                    "idx",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "uuid-1",
                    assignment,
                )
                .unwrap();
            engine
                .add_document_with_receipt("pre-fault", serde_json::json!({"value": -1}))
                .unwrap();
            engine.refresh().unwrap();
            let committed_path = dir.path().join("uuid-1/shard_0/translog.committed");
            let committed = crate::engine::sequence::CommittedBoundaryRecord::load(&committed_path)
                .unwrap()
                .unwrap();
            assert_eq!(committed.processed_checkpoint, Some(0));
            assert_eq!(committed.persisted_checkpoint, Some(0));

            let index_dir = dir.path().join("uuid-1/shard_0/index");
            std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
            let during_fault = engine
                .add_document_with_receipt("during-fault", serde_json::json!({"value": 0}))
                .unwrap();
            std::thread::sleep(Duration::from_millis(300));
            let failed_commit = engine.refresh();
            std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
            let failed_commit = failed_commit.unwrap_err();
            assert!(
                failed_commit
                    .chain()
                    .any(|cause| cause.is::<crate::engine::tantivy::TantivyCommitFailureError>()),
                "{failed_commit:#}"
            );
            assert!(
                engine.writer_is_failed_for_test(),
                "a failed commit must remove the writer before another write can queue"
            );
            assert_eq!(
                crate::engine::sequence::CommittedBoundaryRecord::load(&committed_path)
                    .unwrap()
                    .unwrap(),
                committed,
                "a failed commit must not advance the persisted checkpoint"
            );

            let first_recovered_engine = engine.clone();
            let first_recovered = tokio::time::timeout(
                Duration::from_secs(5),
                tokio::task::spawn_blocking(move || {
                    first_recovered_engine
                        .add_document_with_receipt("acked-0", serde_json::json!({"value": 0}))
                }),
            )
            .await
            .expect("the first write after a failed commit must not hang")
            .expect("rebuild write task must not panic")
            .unwrap();
            assert_eq!(first_recovered.doc_id, "acked-0");
            let mut acknowledged = vec!["acked-0".to_string()];
            for attempt in 1..5 {
                let id = format!("acked-{attempt}");
                engine
                    .add_document_with_receipt(&id, serde_json::json!({"value": attempt}))
                    .unwrap();
                acknowledged.push(id);
            }
            assert!(
                !engine.writer_is_failed_for_test(),
                "the first recovered write must replace the failed writer"
            );
            engine.refresh().unwrap();
            assert!(engine.get_document("during-fault").unwrap().is_some());
            for id in &acknowledged {
                assert!(engine.get_document(id).unwrap().is_some(), "{id}");
            }
            engine.flush().unwrap();
            let committed = crate::engine::sequence::CommittedBoundaryRecord::load(&committed_path)
                .unwrap()
                .unwrap();
            let expected_checkpoint = during_fault.seq_no + acknowledged.len() as u64;
            assert_eq!(committed.processed_checkpoint, Some(expected_checkpoint));
            assert_eq!(committed.persisted_checkpoint, Some(expected_checkpoint));
        }

        let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = restarted
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allow_empty_creation: false,
                    ..assignment
                },
            )
            .unwrap();
        assert!(engine.get_document("during-fault").unwrap().is_some());
        for attempt in 0..5 {
            assert!(
                engine
                    .get_document(&format!("acked-{attempt}"))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_survives_runtime_commit_failure_rebuild_and_replay() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .add_document_with_receipt("victim", serde_json::json!({"value": 1}))
            .unwrap();
        engine.refresh().unwrap();
        engine.delete_document_with_receipt("victim").unwrap();

        let index_dir = dir.path().join("uuid-1/shard_0/index");
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let failed_commit = engine.refresh();
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(failed_commit.is_err());
        assert!(engine.writer_is_failed_for_test());

        engine
            .add_document_with_receipt("trigger", serde_json::json!({"value": 2}))
            .unwrap();
        engine.refresh().unwrap();
        assert!(
            engine.get_document("victim").unwrap().is_none(),
            "runtime WAL replay must not resurrect an acknowledged delete"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn idle_failed_writer_recovers_on_refresh_and_snapshot_without_client_write() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .add_document_with_receipt("committed", serde_json::json!({"value": 0}))
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document_with_receipt("acked-before-fault", serde_json::json!({"value": 1}))
            .unwrap();

        let index_dir = dir.path().join("uuid-1/shard_0/index");
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let failed_commit = engine.refresh();
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(failed_commit.is_err());
        assert!(engine.writer_is_failed_for_test());

        engine
            .refresh()
            .expect("idle refresh must rebuild the writer and replay the WAL suffix");
        assert!(engine.get_document("acked-before-fault").unwrap().is_some());
        let snapshot_dir = dir.path().join("idle-recovery-snapshot");
        let snapshot = engine
            .prepare_peer_recovery_snapshot(&snapshot_dir)
            .expect("peer recovery snapshot must succeed after idle writer repair");
        assert_eq!(snapshot.snapshot_next_seq_no, 2);
    }

    #[tokio::test]
    async fn successful_replica_apply_clears_a_transient_apply_failure() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(2, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let engine = manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine.inject_wal_write_failures_for_test(28, 1);
        let first = manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |engine| {
                    apply_index(&engine, "doc", serde_json::json!({"value": 0}), 0, 2).map(|_| ())
                },
            )
            .unwrap_err();
        assert!(!ShardManager::should_report_copy_failure(&first));

        manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |engine| {
                    apply_index(&engine, "doc", serde_json::json!({"value": 1}), 0, 2).map(|_| ())
                },
            )
            .unwrap();

        engine.inject_wal_write_failures_for_test(28, 1);
        let after_success = manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |engine| {
                    apply_index(&engine, "doc", serde_json::json!({"value": 2}), 1, 2).map(|_| ())
                },
            )
            .unwrap_err();
        assert!(!ShardManager::should_report_copy_failure(&after_success));

        engine.inject_wal_write_failures_for_test(28, 1);
        let persistent_after_reset = manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |engine| {
                    apply_index(&engine, "doc", serde_json::json!({"value": 3}), 1, 2).map(|_| ())
                },
            )
            .unwrap_err();
        assert!(ShardManager::should_report_copy_failure(
            &persistent_after_reset
        ));
    }

    #[tokio::test]
    async fn primary_term_sequence_collision_is_definitive() {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 1,
                    message_term: 1,
                },
                |engine| apply_index(&engine, "doc", json!({"value": 1}), 0, 1).map(|_| ()),
            )
            .unwrap();
        manager
            .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), 7, 2)
            .await
            .unwrap();

        let error = manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |engine| apply_index(&engine, "doc", json!({"value": 2}), 0, 2).map(|_| ()),
            )
            .unwrap_err();

        assert!(ShardManager::is_definitive_copy_failure(&error));
        assert!(ShardManager::should_report_copy_failure(&error));
    }

    #[tokio::test]
    async fn apply_backoff_suppresses_repeated_engine_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
                AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let apply = || {
            manager.apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 7,
                    applied_view_term: 2,
                    message_term: 2,
                },
                |_engine| -> Result<()> {
                    attempts.fetch_add(1, AtomicOrdering::AcqRel);
                    Err(std::io::Error::from_raw_os_error(28).into())
                },
            )
        };
        assert!(apply().is_err());
        let backoff = apply().unwrap_err();
        assert!(backoff.is::<ShardCopyBackoff>(), "{backoff:#}");
        assert_eq!(attempts.load(AtomicOrdering::Acquire), 1);
    }

    #[test]
    fn network_recovery_errors_do_not_consume_local_storage_budget() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.set_copy_retry_policy_for_test(2, Duration::ZERO, Duration::ZERO, Duration::ZERO);

        for _ in 0..3 {
            let error = manager.record_peer_recovery_failure(
                "uuid-1",
                0,
                7,
                anyhow::anyhow!("recovery transport timed out"),
            );
            assert!(!ShardManager::should_report_copy_failure(&error));
        }

        let first_local = manager.record_peer_recovery_failure(
            "uuid-1",
            0,
            7,
            ShardManager::local_storage_failure(std::io::Error::from_raw_os_error(28)),
        );
        assert!(!ShardManager::should_report_copy_failure(&first_local));
        let second_local = manager.record_peer_recovery_failure(
            "uuid-1",
            0,
            7,
            ShardManager::local_storage_failure(std::io::Error::from_raw_os_error(28)),
        );
        assert!(ShardManager::should_report_copy_failure(&second_local));
    }
}
