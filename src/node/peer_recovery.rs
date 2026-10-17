use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{ClusterState, IndexMetadata, NodeInfo};
use crate::engine::SearchEngine;
use crate::shard::{
    PeerRecoveryAwaitingMembership, PeerRecoveryTargetInstall, PeerRecoveryTargetState,
    ReplicaCopyState, ShardKey, ShardManager,
};
use crate::transport::TransportClient;
use crate::transport::proto::{
    CancelPeerRecoveryRequest, CompleteFinalizeRecoveryRequest, FetchRecoveryFileChunkRequest,
    FetchRecoveryOpsRequest, PrepareFinalizeRecoveryRequest, RecoveryFileMetadata,
    RecoveryOperation, StartPeerRecoveryRequest,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

const MAX_RECOVERY_FILES: usize = 10_000;
const MAX_RECOVERY_TOTAL_BYTES: u64 = 1 << 40;

#[derive(Clone)]
struct RecoveryCandidate {
    index_name: String,
    metadata: IndexMetadata,
    shard_id: u32,
    primary: NodeInfo,
    allocation_id: u64,
}

#[derive(Clone, Copy)]
struct RetryState {
    delay: Duration,
    next_attempt: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetMembershipObservation {
    Admitted,
    Rejected,
    Unknown,
}

pub(super) struct PeerRecoveryDriver {
    enabled: bool,
    permits: Arc<Semaphore>,
    active: Mutex<HashSet<ShardKey>>,
    retries: Mutex<HashMap<ShardKey, RetryState>>,
}

impl PeerRecoveryDriver {
    pub(super) fn new(max_concurrent: usize) -> Arc<Self> {
        Arc::new(Self {
            enabled: max_concurrent > 0,
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            active: Mutex::new(HashSet::new()),
            retries: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn reconcile(
        self: &Arc<Self>,
        state: &ClusterState,
        local_node_id: &str,
        cluster_manager: Arc<ClusterManager>,
        shard_manager: Arc<ShardManager>,
        transport_client: TransportClient,
    ) {
        shard_manager.reconcile_applied_settings(state);
        let pending_targets =
            self.reconcile_pending_targets(state, local_node_id, shard_manager.clone());

        if !self.enabled {
            return;
        }
        let now = Instant::now();
        for candidate in recovery_candidates(state, local_node_id) {
            let key = ShardKey::new(&candidate.index_name, candidate.shard_id);
            let primary_term = candidate
                .metadata
                .shard_routing
                .get(&candidate.shard_id)
                .map(|r| r.primary_term)
                .unwrap_or(1);
            if shard_manager
                .replica_states
                .snapshot(&key, local_node_id, now)
                .is_none_or(|snap| {
                    snap.index_uuid != candidate.metadata.uuid.as_str()
                        || snap.allocation_id != Some(candidate.allocation_id)
                })
            {
                let _ = shard_manager.replica_states.transition(
                    &key,
                    candidate.metadata.uuid.as_str(),
                    local_node_id,
                    Some(candidate.allocation_id),
                    primary_term,
                    ReplicaCopyState::Initializing,
                    false,
                    None,
                );
            }
            if pending_targets.contains(&key) {
                continue;
            }
            if self
                .retries
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&key)
                .is_some_and(|retry| now < retry.next_attempt)
            {
                continue;
            }
            {
                let mut active = self
                    .active
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if !active.insert(key.clone()) {
                    continue;
                }
            }
            let permit = match self.permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.active
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .remove(&key);
                    break;
                }
            };

            let driver = self.clone();
            let local_node_id = local_node_id.to_string();
            let cluster_manager = cluster_manager.clone();
            let shard_manager = shard_manager.clone();
            let transport_client = transport_client.clone();
            let destructive_started = Arc::new(AtomicBool::new(false));
            let destructive_started_for_run = destructive_started.clone();
            tokio::spawn(async move {
                let started = Instant::now();
                let result = run_peer_recovery(
                    &candidate,
                    &local_node_id,
                    cluster_manager.clone(),
                    shard_manager.clone(),
                    transport_client,
                    destructive_started_for_run,
                )
                .await;
                drop(permit);

                match result {
                    Ok(RecoveryRunOutcome::Admitted(stats)) => {
                        let _ = shard_manager.replica_states.transition(
                            &key,
                            candidate.metadata.uuid.as_str(),
                            &local_node_id,
                            Some(candidate.allocation_id),
                            primary_term,
                            ReplicaCopyState::InSync,
                            false,
                            None,
                        );
                        shard_manager.clear_peer_recovery_failure(
                            candidate.metadata.uuid.as_str(),
                            candidate.shard_id,
                            candidate.allocation_id,
                        );
                        driver
                            .retries
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .remove(&key);
                        tracing::info!(
                            session_id = stats.session_id,
                            index = candidate.index_name,
                            shard_id = candidate.shard_id,
                            source = candidate.primary.id,
                            bytes = stats.bytes,
                            operations = stats.operations,
                            duration_ms = started.elapsed().as_millis(),
                            "Peer recovery completed"
                        );
                    }
                    Ok(RecoveryRunOutcome::AwaitingMembership(stats)) => {
                        let _ = shard_manager.replica_states.transition(
                            &key,
                            candidate.metadata.uuid.as_str(),
                            &local_node_id,
                            Some(candidate.allocation_id),
                            primary_term,
                            ReplicaCopyState::CatchingUp,
                            true,
                            None,
                        );
                        shard_manager.clear_peer_recovery_failure(
                            candidate.metadata.uuid.as_str(),
                            candidate.shard_id,
                            candidate.allocation_id,
                        );
                        driver
                            .retries
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .remove(&key);
                        tracing::warn!(
                            session_id = stats.session_id,
                            index = candidate.index_name,
                            shard_id = candidate.shard_id,
                            source = candidate.primary.id,
                            bytes = stats.bytes,
                            operations = stats.operations,
                            duration_ms = started.elapsed().as_millis(),
                            "Peer recovery is finalized and awaiting membership settlement"
                        );
                    }
                    Err(error) => {
                        let destructive = destructive_started.load(Ordering::Acquire);
                        let error = if destructive {
                            shard_manager.record_peer_recovery_failure(
                                candidate.metadata.uuid.as_str(),
                                candidate.shard_id,
                                candidate.allocation_id,
                                error,
                            )
                        } else {
                            error
                        };
                        let policy = candidate.metadata.settings.effective_replica_policy();
                        let initial_backoff =
                            Duration::from_millis(policy.recovery_retry_initial_backoff_ms);
                        let max_backoff =
                            Duration::from_millis(policy.recovery_retry_max_backoff_ms);
                        if destructive {
                            let cleanup_result = if ShardManager::should_report_copy_failure(&error)
                            {
                                shard_manager
                                    .abort_peer_recovery_target_blocking(
                                        candidate.index_name.clone(),
                                        candidate.shard_id,
                                        candidate.metadata.uuid.to_string(),
                                        candidate.allocation_id,
                                    )
                                    .await
                            } else {
                                shard_manager
                                    .reset_peer_recovery_target_for_retry_blocking(
                                        candidate.index_name.clone(),
                                        candidate.shard_id,
                                        candidate.metadata.uuid.to_string(),
                                        candidate.allocation_id,
                                    )
                                    .await
                            };
                            if let Err(cleanup_error) = cleanup_result {
                                tracing::warn!(
                                    index = candidate.index_name,
                                    shard_id = candidate.shard_id,
                                    allocation_id = candidate.allocation_id,
                                    error = %cleanup_error,
                                    "Peer recovery cleanup failed; retaining the active target gate"
                                );
                            }
                        }
                        if ShardManager::should_report_copy_failure(&error) {
                            let _ = shard_manager.replica_states.transition(
                                &key,
                                candidate.metadata.uuid.as_str(),
                                &local_node_id,
                                Some(candidate.allocation_id),
                                primary_term,
                                ReplicaCopyState::Failed,
                                false,
                                Some(error.to_string()),
                            );
                        } else {
                            let _ = shard_manager.replica_states.record_retry_backoff(
                                &key,
                                candidate.metadata.uuid.as_str(),
                                &local_node_id,
                                candidate.allocation_id,
                                primary_term,
                                initial_backoff,
                                max_backoff,
                                error.to_string(),
                            );
                        }
                        let mut retries = driver
                            .retries
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let delay = retries
                            .get(&key)
                            .map(|retry| (retry.delay * 2).min(max_backoff))
                            .unwrap_or_else(|| initial_backoff.min(max_backoff));
                        retries.insert(
                            key.clone(),
                            RetryState {
                                delay,
                                next_attempt: Instant::now() + delay,
                            },
                        );
                        tracing::warn!(
                            index = candidate.index_name,
                            shard_id = candidate.shard_id,
                            source = candidate.primary.id,
                            retry_after_ms = delay.as_millis(),
                            error = %error,
                            "Peer recovery failed"
                        );
                    }
                }
                driver
                    .active
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&key);
            });
        }
    }

    fn reconcile_pending_targets(
        self: &Arc<Self>,
        state: &ClusterState,
        local_node_id: &str,
        shard_manager: Arc<ShardManager>,
    ) -> HashSet<ShardKey> {
        let mut pending_keys = HashSet::new();
        for (key, target_state) in shard_manager.peer_recovery_target_states() {
            pending_keys.insert(key.clone());
            let pending = match target_state {
                PeerRecoveryTargetState::Recovering {
                    index_uuid,
                    allocation_id,
                } => {
                    {
                        let mut active = self
                            .active
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        if !active.insert(key.clone()) {
                            continue;
                        }
                    }
                    let driver = self.clone();
                    let shard_manager = shard_manager.clone();
                    tokio::spawn(async move {
                        if let Err(error) = shard_manager
                            .reset_peer_recovery_target_for_retry_blocking(
                                key.index.clone(),
                                key.shard_id,
                                index_uuid,
                                allocation_id,
                            )
                            .await
                        {
                            tracing::warn!(
                                index = key.index,
                                shard_id = key.shard_id,
                                allocation_id,
                                error = %error,
                                "Failed to clean a retryable peer recovery target"
                            );
                        }
                        driver
                            .active
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .remove(&key);
                    });
                    continue;
                }
                PeerRecoveryTargetState::FinalizedAwaitingMembership(pending) => pending,
            };
            let observation = observe_target_membership(state, &key, local_node_id, &pending);
            if observation == TargetMembershipObservation::Unknown {
                continue;
            }
            {
                let mut active = self
                    .active
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if !active.insert(key.clone()) {
                    continue;
                }
            }
            let driver = self.clone();
            let shard_manager = shard_manager.clone();
            let local_node_for_task = local_node_id.to_string();
            tokio::spawn(async move {
                let result = match observation {
                    TargetMembershipObservation::Admitted => {
                        let _ = shard_manager.replica_states.transition(
                            &key,
                            &pending.index_uuid,
                            &local_node_for_task,
                            Some(pending.allocation_id),
                            pending.primary_term,
                            ReplicaCopyState::InSync,
                            false,
                            None,
                        );
                        shard_manager
                            .clear_peer_recovery_awaiting_membership_blocking(
                                key.index.clone(),
                                key.shard_id,
                                pending.index_uuid.clone(),
                            )
                            .await
                    }
                    TargetMembershipObservation::Rejected => {
                        shard_manager
                            .abort_peer_recovery_target_blocking(
                                key.index.clone(),
                                key.shard_id,
                                pending.index_uuid.clone(),
                                pending.allocation_id,
                            )
                            .await
                    }
                    TargetMembershipObservation::Unknown => Ok(()),
                };
                if let Err(error) = result {
                    tracing::error!(
                        index = key.index,
                        shard_id = key.shard_id,
                        ?observation,
                        error = %error,
                        "Failed to reconcile finalized peer recovery membership"
                    );
                }
                driver
                    .active
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&key);
            });
        }
        pending_keys
    }
}

fn recovery_candidates(state: &ClusterState, local_node_id: &str) -> Vec<RecoveryCandidate> {
    let mut candidates = Vec::new();
    for (index_name, metadata) in &state.indices {
        if !metadata.uses_local_shards() {
            continue;
        }
        for (shard_id, routing) in &metadata.shard_routing {
            if !routing.replicas.iter().any(|node| node == local_node_id)
                || routing
                    .in_sync_replicas
                    .iter()
                    .any(|node| node == local_node_id)
            {
                continue;
            }
            let Some(primary) = state.nodes.get(&routing.primary).cloned() else {
                continue;
            };
            let Some(allocation_id) =
                state.shard_allocation_id(index_name, *shard_id, local_node_id)
            else {
                continue;
            };
            candidates.push(RecoveryCandidate {
                index_name: index_name.clone(),
                metadata: metadata.clone(),
                shard_id: *shard_id,
                primary,
                allocation_id,
            });
        }
    }
    candidates
}

fn observe_target_membership(
    state: &ClusterState,
    key: &ShardKey,
    local_node_id: &str,
    pending: &PeerRecoveryAwaitingMembership,
) -> TargetMembershipObservation {
    let Some(metadata) = state.indices.get(&key.index) else {
        return TargetMembershipObservation::Rejected;
    };
    if metadata.uuid.as_str() != pending.index_uuid {
        return TargetMembershipObservation::Rejected;
    }
    let Some(routing) = metadata.shard_routing.get(&key.shard_id) else {
        return TargetMembershipObservation::Rejected;
    };
    let local_allocation = state.shard_allocation_id(&key.index, key.shard_id, local_node_id);
    if local_allocation != Some(pending.allocation_id) {
        return TargetMembershipObservation::Rejected;
    }
    if routing.primary == local_node_id
        || routing
            .in_sync_replicas
            .iter()
            .any(|node| node == local_node_id)
    {
        return TargetMembershipObservation::Admitted;
    }
    if routing.primary != pending.primary_node_id || routing.primary_term > pending.primary_term {
        return TargetMembershipObservation::Rejected;
    }
    TargetMembershipObservation::Unknown
}

struct RecoveryStats {
    session_id: String,
    bytes: u64,
    operations: u64,
}

enum RecoveryRunOutcome {
    Admitted(RecoveryStats),
    AwaitingMembership(RecoveryStats),
}

struct CompletionSettlementContext<'a> {
    candidate: &'a RecoveryCandidate,
    local_node_id: &'a str,
    cluster_manager: Arc<ClusterManager>,
    transport_client: &'a TransportClient,
    session_id: &'a str,
    pending: &'a PeerRecoveryAwaitingMembership,
    applied_cursor: crate::wal::WalCursor,
    processed_checkpoint: Option<u64>,
    retry_timeout: Duration,
}

#[cfg_attr(not(feature = "protocol-trace"), allow(dead_code))]
#[derive(Clone)]
struct RecoveryApplyContext {
    node: String,
    index_uuid: String,
    shard: u32,
    allocation: u64,
}

struct TargetRecoverySessionGuard {
    active: bool,
    reset_target_on_drop: bool,
    session_id: String,
    primary: NodeInfo,
    transport_client: TransportClient,
    shard_manager: Arc<ShardManager>,
    index_name: String,
    shard_id: u32,
    index_uuid: String,
    allocation_id: u64,
    local_node_id: String,
    primary_term: u64,
}

impl TargetRecoverySessionGuard {
    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for TargetRecoverySessionGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        crate::metrics::record_peer_recovery_cancellation();
        let reset_target = self.reset_target_on_drop;
        let session_id = self.session_id.clone();
        let primary = self.primary.clone();
        let transport_client = self.transport_client.clone();
        let shard_manager = self.shard_manager.clone();
        let index_name = self.index_name.clone();
        let shard_id = self.shard_id;
        let index_uuid = self.index_uuid.clone();
        let allocation_id = self.allocation_id;
        let local_node_id = self.local_node_id.clone();
        let primary_term = self.primary_term;
        let _ = shard_manager.replica_states.transition(
            &ShardKey::new(&index_name, shard_id),
            &index_uuid,
            &local_node_id,
            Some(allocation_id),
            primary_term,
            ReplicaCopyState::Initializing,
            false,
            Some("peer recovery session cancelled before finalization".into()),
        );
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let _ = transport_client
                    .cancel_peer_recovery(
                        &primary,
                        CancelPeerRecoveryRequest {
                            session_id,
                            reason: "target recovery cancelled or failed before finalization"
                                .into(),
                        },
                    )
                    .await;
                if reset_target {
                    let _ = shard_manager
                        .reset_peer_recovery_target_for_retry_blocking(
                            index_name,
                            shard_id,
                            index_uuid,
                            allocation_id,
                        )
                        .await;
                }
            });
        }
    }
}

fn require_recovery_cursor(
    cursor: Option<crate::transport::proto::RecoveryWalCursor>,
    label: &'static str,
) -> Result<crate::wal::WalCursor> {
    let cursor = cursor.ok_or_else(|| anyhow::anyhow!("{label} is missing"))?;
    Ok(crate::wal::WalCursor {
        generation_id: cursor.generation_id,
        byte_offset: cursor.byte_offset,
    })
}

fn recovery_cursor_proto(
    cursor: crate::wal::WalCursor,
) -> crate::transport::proto::RecoveryWalCursor {
    crate::transport::proto::RecoveryWalCursor {
        generation_id: cursor.generation_id,
        byte_offset: cursor.byte_offset,
    }
}

async fn run_peer_recovery(
    candidate: &RecoveryCandidate,
    local_node_id: &str,
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: TransportClient,
    destructive_started: Arc<AtomicBool>,
) -> Result<RecoveryRunOutcome> {
    let start_request = StartPeerRecoveryRequest {
        index_name: candidate.index_name.clone(),
        index_uuid: candidate.metadata.uuid.to_string(),
        shard_id: candidate.shard_id,
        target_node_id: local_node_id.to_string(),
        target_allocation_id: Some(candidate.allocation_id),
    };
    let start = loop {
        let response = transport_client
            .start_peer_recovery(&candidate.primary, start_request.clone())
            .await
            .context("start peer recovery")?;
        if !response.preparing {
            break response;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if start.session_id.is_empty() || start.primary_term == 0 {
        anyhow::bail!("peer recovery source returned an invalid session identity");
    }
    if start.target_allocation_id != Some(candidate.allocation_id) {
        anyhow::bail!("peer recovery source returned a different target allocation ID");
    }
    let mut session_guard = TargetRecoverySessionGuard {
        active: true,
        reset_target_on_drop: false,
        session_id: start.session_id.clone(),
        primary: candidate.primary.clone(),
        transport_client: transport_client.clone(),
        shard_manager: shard_manager.clone(),
        index_name: candidate.index_name.clone(),
        shard_id: candidate.shard_id,
        index_uuid: candidate.metadata.uuid.to_string(),
        allocation_id: candidate.allocation_id,
        local_node_id: local_node_id.to_string(),
        primary_term: start.primary_term,
    };
    let _ = shard_manager.replica_states.transition(
        &ShardKey::new(&candidate.index_name, candidate.shard_id),
        candidate.metadata.uuid.as_str(),
        local_node_id,
        Some(candidate.allocation_id),
        start.primary_term,
        ReplicaCopyState::Snapshotting,
        false,
        None,
    );
    let snapshot_cursor = require_recovery_cursor(start.snapshot_cursor, "snapshot WAL cursor")?;
    let committed_boundary: crate::engine::sequence::CommittedBoundaryRecord =
        serde_json::from_slice(&start.committed_boundary_json)
            .context("decode peer recovery committed boundary")?;
    committed_boundary.validate()?;
    if committed_boundary.processed_checkpoint != start.snapshot_processed_checkpoint
        || committed_boundary.persisted_checkpoint != start.snapshot_persisted_checkpoint
        || committed_boundary.max_seq_no != start.snapshot_max_seq_no
    {
        anyhow::bail!("peer recovery committed boundary summary mismatch");
    }
    if committed_boundary.term_sequence_state.current_term != start.primary_term {
        anyhow::bail!("peer recovery committed boundary term mismatch");
    }
    validate_file_manifest(&start.files)?;
    #[cfg(feature = "protocol-trace")]
    let snapshot_next_seq_no = committed_boundary
        .max_seq_no
        .and_then(|seq_no| seq_no.checked_add(1))
        .unwrap_or(0);
    #[cfg(feature = "protocol-trace")]
    let trace_context = crate::protocol_trace::RecoveryTraceContext {
        source_node: candidate.primary.id.clone(),
        target_node: local_node_id.to_string(),
        index_uuid: candidate.metadata.uuid.to_string(),
        shard: candidate.shard_id,
        allocation: candidate.allocation_id,
        session_id: start.session_id.clone(),
        snapshot_next_seq_no,
    };

    if !shard_manager
        .begin_peer_recovery_target_blocking(
            candidate.index_name.clone(),
            candidate.shard_id,
            candidate.metadata.uuid.to_string(),
            candidate.allocation_id,
        )
        .await?
    {
        anyhow::bail!("peer recovery target is already active");
    }
    destructive_started.store(true, Ordering::Release);
    #[cfg(feature = "protocol-trace")]
    let shard_dir = shard_manager
        .prepare_peer_recovery_target_blocking_traced(
            candidate.index_name.clone(),
            candidate.shard_id,
            candidate.metadata.uuid.to_string(),
            candidate.allocation_id,
            trace_context.clone(),
        )
        .await
        .map_err(ShardManager::local_storage_failure)?;
    #[cfg(not(feature = "protocol-trace"))]
    let shard_dir = shard_manager
        .prepare_peer_recovery_target_blocking(
            candidate.index_name.clone(),
            candidate.shard_id,
            candidate.metadata.uuid.to_string(),
            candidate.allocation_id,
        )
        .await
        .map_err(ShardManager::local_storage_failure)?;
    let index_dir = shard_dir.join("index");
    let mut transferred_bytes = 0u64;
    for file in &start.files {
        transferred_bytes = transferred_bytes
            .checked_add(
                download_recovery_file(
                    &transport_client,
                    &candidate.primary,
                    &start.session_id,
                    file,
                    &index_dir,
                )
                .await?,
            )
            .ok_or_else(|| anyhow::anyhow!("peer recovery byte counter overflow"))?;
    }
    let index_dir_for_sync = index_dir.clone();
    tokio::task::spawn_blocking(move || std::fs::File::open(index_dir_for_sync)?.sync_all())
        .await
        .map_err(|error| anyhow::anyhow!("index directory sync task failed: {error}"))?
        .map_err(ShardManager::local_storage_failure)?;

    let install = PeerRecoveryTargetInstall {
        index: candidate.index_name.clone(),
        shard_id: candidate.shard_id,
        mappings: candidate.metadata.mappings.clone(),
        settings: candidate.metadata.settings.clone(),
        index_uuid: candidate.metadata.uuid.to_string(),
        allocation_id: candidate.allocation_id,
        primary_term: start.primary_term,
        shard_dir,
        committed_boundary: committed_boundary.clone(),
        expected_files: start.files.iter().map(|file| file.name.clone()).collect(),
    };
    #[cfg(feature = "protocol-trace")]
    let engine = shard_manager
        .finalize_peer_recovery_target_blocking_traced(install, trace_context.clone())
        .await
        .map_err(ShardManager::local_storage_failure)?;
    #[cfg(not(feature = "protocol-trace"))]
    let engine = shard_manager
        .finalize_peer_recovery_target_blocking(install)
        .await
        .map_err(ShardManager::local_storage_failure)?;
    let recovery_apply = RecoveryApplyContext {
        node: local_node_id.to_string(),
        index_uuid: candidate.metadata.uuid.to_string(),
        shard: candidate.shard_id,
        allocation: candidate.allocation_id,
    };
    let mut cursor = snapshot_cursor;
    let mut operations = 0u64;
    loop {
        let response = transport_client
            .fetch_recovery_ops(
                &candidate.primary,
                FetchRecoveryOpsRequest {
                    session_id: start.session_id.clone(),
                    cursor: Some(recovery_cursor_proto(cursor)),
                    max_ops: crate::transport::server::MAX_RECOVERY_OPS as u32,
                    snapshot_processed_checkpoint: committed_boundary.processed_checkpoint,
                },
            )
            .await
            .context("fetch peer recovery operations")?;
        let next_cursor =
            require_recovery_cursor(response.next_cursor, "peer recovery next cursor")?;
        if !response.complete && next_cursor == cursor && response.operations.is_empty() {
            // The source encountered a durable WAL entry that its engine has not
            // applied yet. Finalization takes the exclusive barrier and rebuilds
            // the source writer before serving the remaining suffix.
            break;
        }
        let (new_cursor, applied) = apply_recovery_operations_cursor(
            engine.clone(),
            Some(recovery_apply.clone()),
            cursor,
            next_cursor,
            committed_boundary.processed_checkpoint,
            response.source_max_seq_no,
            response.complete,
            response.operations,
        )
        .await?;
        operations += applied;
        cursor = new_cursor;
        if response.complete {
            break;
        }
    }

    loop {
        let prepared = transport_client
            .prepare_finalize_recovery(
                &candidate.primary,
                PrepareFinalizeRecoveryRequest {
                    session_id: start.session_id.clone(),
                    cursor: Some(recovery_cursor_proto(cursor)),
                    processed_checkpoint: engine.sequence_stats().processed_checkpoint,
                },
            )
            .await
            .context("prepare peer recovery finalization")?;
        if prepared.retry_catch_up {
            let response = transport_client
                .fetch_recovery_ops(
                    &candidate.primary,
                    FetchRecoveryOpsRequest {
                        session_id: start.session_id.clone(),
                        cursor: Some(recovery_cursor_proto(cursor)),
                        max_ops: crate::transport::server::MAX_RECOVERY_OPS as u32,
                        snapshot_processed_checkpoint: committed_boundary.processed_checkpoint,
                    },
                )
                .await?;
            let next_cursor =
                require_recovery_cursor(response.next_cursor, "peer recovery next cursor")?;
            let (new_cursor, applied) = apply_recovery_operations_cursor(
                engine.clone(),
                Some(recovery_apply.clone()),
                cursor,
                next_cursor,
                committed_boundary.processed_checkpoint,
                response.source_max_seq_no,
                response.complete,
                response.operations,
            )
            .await?;
            cursor = new_cursor;
            operations += applied;
            continue;
        }
        if !prepared.complete {
            anyhow::bail!("peer recovery finalize response was incomplete");
        }
        let next_cursor = require_recovery_cursor(prepared.next_cursor, "finalize next cursor")?;
        let barrier_wal_end =
            require_recovery_cursor(prepared.barrier_wal_end, "finalize barrier WAL end")?;
        let (new_cursor, applied) = apply_recovery_operations_cursor(
            engine.clone(),
            Some(recovery_apply.clone()),
            cursor,
            next_cursor,
            committed_boundary.processed_checkpoint,
            prepared.barrier_max_seq_no,
            true,
            prepared.operations,
        )
        .await?;
        cursor = new_cursor;
        operations += applied;
        if cursor != barrier_wal_end {
            anyhow::bail!("peer recovery target did not reach the physical barrier");
        }
        let target_stats = engine.sequence_stats();
        if target_stats.processed_checkpoint != prepared.barrier_processed_checkpoint {
            anyhow::bail!(
                "peer recovery target processed checkpoint does not match the source barrier"
            );
        }
        if target_stats.processed_checkpoint != target_stats.max_seq_no
            || target_stats.processed_checkpoint != prepared.barrier_max_seq_no
        {
            anyhow::bail!(
                "peer recovery target final checkpoint {:?} is not contiguous through max_seq_no {:?}",
                target_stats.processed_checkpoint,
                prepared.barrier_max_seq_no
            );
        }
        #[cfg(feature = "protocol-trace")]
        crate::protocol_trace::record_recovery_barrier(
            &candidate.primary.id,
            local_node_id,
            candidate.metadata.uuid.as_str(),
            candidate.shard_id,
            candidate.allocation_id,
            &start.session_id,
            prepared
                .barrier_max_seq_no
                .and_then(|seq_no| seq_no.checked_add(1))
                .unwrap_or(0),
            engine.protocol_trace_processed_sequences()?,
        );
        session_guard.disarm();
        break;
    }

    let _ = shard_manager.replica_states.transition(
        &ShardKey::new(&candidate.index_name, candidate.shard_id),
        candidate.metadata.uuid.as_str(),
        local_node_id,
        Some(candidate.allocation_id),
        start.primary_term,
        ReplicaCopyState::CatchingUp,
        false,
        None,
    );
    crate::metrics::record_peer_recovery_progress(transferred_bytes, operations);

    let engine_for_refresh = engine.clone();
    tokio::task::spawn_blocking(move || engine_for_refresh.refresh())
        .await
        .map_err(|error| anyhow::anyhow!("peer recovery refresh task failed: {error}"))?
        .map_err(ShardManager::local_storage_failure)?;

    let pending = PeerRecoveryAwaitingMembership {
        index_uuid: candidate.metadata.uuid.to_string(),
        allocation_id: candidate.allocation_id,
        primary_node_id: candidate.primary.id.clone(),
        primary_term: start.primary_term,
    };
    shard_manager
        .mark_peer_recovery_awaiting_membership_blocking(
            candidate.index_name.clone(),
            candidate.shard_id,
            pending.clone(),
        )
        .await
        .map_err(ShardManager::local_storage_failure)?;
    let policy = candidate.metadata.settings.effective_replica_policy();
    let observation = complete_with_observed_settlement(CompletionSettlementContext {
        candidate,
        local_node_id,
        cluster_manager: cluster_manager.clone(),
        transport_client: &transport_client,
        session_id: &start.session_id,
        pending: &pending,
        applied_cursor: cursor,
        processed_checkpoint: engine.sequence_stats().processed_checkpoint,
        retry_timeout: Duration::from_millis(policy.recovery_finalize_timeout_ms),
    })
    .await;
    #[cfg(feature = "protocol-trace")]
    if observation == TargetMembershipObservation::Unknown {
        crate::protocol_trace::record_recovery_membership(
            &candidate.primary.id,
            local_node_id,
            candidate.metadata.uuid.as_str(),
            candidate.shard_id,
            candidate.allocation_id,
            &start.session_id,
            "unknown",
        );
    }

    let stats = RecoveryStats {
        session_id: start.session_id,
        bytes: transferred_bytes,
        operations,
    };
    match observation {
        TargetMembershipObservation::Admitted => {
            match shard_manager
                .clear_peer_recovery_awaiting_membership_blocking(
                    candidate.index_name.clone(),
                    candidate.shard_id,
                    pending.index_uuid,
                )
                .await
            {
                Ok(()) => Ok(RecoveryRunOutcome::Admitted(stats)),
                Err(error) => {
                    tracing::error!(
                        index = candidate.index_name,
                        shard_id = candidate.shard_id,
                        error = %error,
                        "Peer recovery was admitted but pending-state cleanup failed"
                    );
                    Ok(RecoveryRunOutcome::AwaitingMembership(stats))
                }
            }
        }
        TargetMembershipObservation::Rejected => {
            shard_manager
                .abort_peer_recovery_target_blocking(
                    candidate.index_name.clone(),
                    candidate.shard_id,
                    pending.index_uuid,
                    pending.allocation_id,
                )
                .await?;
            destructive_started.store(false, Ordering::Release);
            anyhow::bail!("peer recovery membership was definitively rejected")
        }
        TargetMembershipObservation::Unknown => Ok(RecoveryRunOutcome::AwaitingMembership(stats)),
    }
}

fn validate_file_manifest(files: &[RecoveryFileMetadata]) -> Result<()> {
    if files.is_empty() || files.len() > MAX_RECOVERY_FILES {
        anyhow::bail!("peer recovery file manifest has an invalid file count");
    }
    let mut names = HashSet::new();
    let mut total = 0u64;
    for file in files {
        validate_file_name(&file.name)?;
        if !names.insert(&file.name) {
            anyhow::bail!("peer recovery file manifest contains duplicate names");
        }
        if file.sha256.len() != 64
            || !file
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            anyhow::bail!("peer recovery file has an invalid SHA-256 digest");
        }
        total = total
            .checked_add(file.length)
            .ok_or_else(|| anyhow::anyhow!("peer recovery file lengths overflow"))?;
        if total > MAX_RECOVERY_TOTAL_BYTES {
            anyhow::bail!("peer recovery snapshot exceeds the configured byte limit");
        }
    }
    Ok(())
}

fn validate_file_name(name: &str) -> Result<()> {
    let path = Path::new(name);
    let mut components = path.components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(component)), None)
            if !component.is_empty() && component != "." && component != ".." =>
        {
            Ok(())
        }
        _ => anyhow::bail!("peer recovery file name is not a safe plain file name"),
    }
}

fn verify_recovery_sha256(file_name: &str, actual_hash: &str, expected_hash: &str) -> Result<()> {
    if actual_hash != expected_hash {
        anyhow::bail!("peer recovery checksum mismatch for file '{file_name}'");
    }
    Ok(())
}

async fn download_recovery_file(
    transport_client: &TransportClient,
    primary: &NodeInfo,
    session_id: &str,
    metadata: &RecoveryFileMetadata,
    index_dir: &Path,
) -> Result<u64> {
    validate_file_name(&metadata.name)?;
    let destination = index_dir.join(&metadata.name);
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&destination)
        .await
        .map_err(ShardManager::local_storage_failure)?;
    let mut hasher = Sha256::new();
    let mut offset = 0u64;
    while offset < metadata.length {
        let response = transport_client
            .fetch_recovery_file_chunk(
                primary,
                FetchRecoveryFileChunkRequest {
                    session_id: session_id.to_string(),
                    file_name: metadata.name.clone(),
                    offset,
                    max_len: crate::transport::server::MAX_RECOVERY_FILE_CHUNK_BYTES as u32,
                },
            )
            .await?;
        if response.data.is_empty() {
            anyhow::bail!("peer recovery source returned an empty non-terminal file chunk");
        }
        if offset + response.data.len() as u64 > metadata.length {
            anyhow::bail!("peer recovery file chunk exceeds the declared length");
        }
        hasher.update(&response.data);
        file.write_all(&response.data)
            .await
            .map_err(ShardManager::local_storage_failure)?;
        offset += response.data.len() as u64;
        if response.eof != (offset == metadata.length) {
            anyhow::bail!("peer recovery file EOF flag does not match the declared length");
        }
    }
    file.sync_all()
        .await
        .map_err(ShardManager::local_storage_failure)?;
    let actual_hash: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    verify_recovery_sha256(&metadata.name, &actual_hash, &metadata.sha256)?;
    Ok(offset)
}

#[allow(clippy::too_many_arguments)]
async fn apply_recovery_operations_cursor(
    engine: Arc<dyn SearchEngine>,
    trace: Option<RecoveryApplyContext>,
    cursor: crate::wal::WalCursor,
    next_cursor: crate::wal::WalCursor,
    snapshot_processed_checkpoint: Option<u64>,
    source_max_seq_no: Option<u64>,
    complete: bool,
    operations: Vec<RecoveryOperation>,
) -> Result<(crate::wal::WalCursor, u64)> {
    if next_cursor.position() < cursor.position() {
        anyhow::bail!("peer recovery physical cursor regressed");
    }
    if !complete && next_cursor == cursor {
        if operations.is_empty() {
            return Ok((cursor, 0));
        }
        anyhow::bail!(
            "bounded peer recovery response returned operations without physical progress"
        );
    }
    let mut decoded = Vec::with_capacity(operations.len());
    let mut seen_in_batch: HashMap<u64, crate::engine::SequencedOperation> =
        HashMap::with_capacity(operations.len());
    for operation in operations {
        if operation.primary_term == 0 {
            anyhow::bail!("peer recovery operation has a zero primary term");
        }
        if source_max_seq_no.is_none_or(|maximum| operation.seq_no > maximum) {
            anyhow::bail!("peer recovery operation exceeds the captured source maximum");
        }
        if snapshot_processed_checkpoint.is_some_and(|checkpoint| operation.seq_no <= checkpoint) {
            continue;
        }
        let (mutation, document_version) = match operation.op.as_str() {
            "index" => (
                crate::engine::DocumentMutation::Index {
                    doc_id: operation.doc_id,
                    source: serde_json::from_slice(&operation.payload_json)
                        .context("decode peer recovery index payload")?,
                },
                operation
                    .document_version
                    .filter(|version| *version > 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!("peer recovery index operation has no document version")
                    })?,
            ),
            "delete" => (
                crate::engine::DocumentMutation::Delete {
                    doc_id: operation.doc_id,
                },
                operation
                    .document_version
                    .filter(|version| *version > 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!("peer recovery delete operation has no document version")
                    })?,
            ),
            "noop" => {
                if operation.document_version.is_some() {
                    anyhow::bail!("peer recovery no-op has a document version");
                }
                let payload: serde_json::Value = serde_json::from_slice(&operation.payload_json)
                    .context("decode peer recovery no-op payload")?;
                let reason = payload
                    .get("_reason")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow::anyhow!("peer recovery no-op has no _reason"))?
                    .to_string();
                (crate::engine::DocumentMutation::NoOp { reason }, 0)
            }
            other => anyhow::bail!("unknown peer recovery operation '{other}'"),
        };
        let client_operation =
            crate::transport::client_operation::record_from_proto(operation.client_operation)
                .context("decode peer recovery client operation")?;
        let sequenced = crate::engine::SequencedOperation {
            seq_no: operation.seq_no,
            primary_term: operation.primary_term,
            document_version,
            client_operation,
            mutation,
        };
        if let Some(existing) = seen_in_batch.get(&sequenced.seq_no) {
            if existing == &sequenced {
                // Exact duplicate within the same recovery chunk is idempotent.
                continue;
            }
            anyhow::bail!(
                "peer recovery chunk contains conflicting duplicate sequence {}",
                sequenced.seq_no
            );
        }
        seen_in_batch.insert(sequenced.seq_no, sequenced.clone());
        decoded.push(sequenced);
    }

    let engine_for_apply = engine.clone();
    #[cfg(feature = "protocol-trace")]
    let trace_copy = trace
        .as_ref()
        .map(|trace| crate::protocol_trace::TraceCopy {
            node: trace.node.clone(),
            index_uuid: trace.index_uuid.clone(),
            shard: trace.shard,
            allocation: trace.allocation,
        });
    let receipt = tokio::task::spawn_blocking(move || {
        #[cfg(feature = "protocol-trace")]
        let result = match trace_copy {
            Some(trace_copy) => crate::protocol_trace::with_open_copy(trace_copy, || {
                crate::protocol_trace::with_apply_scope(
                    crate::protocol_trace::ApplyOrigin::Recovery,
                    decoded.clone(),
                    || {
                        engine_for_apply
                            .apply_replica_batch(decoded)
                            .map_err(ShardManager::local_storage_failure)
                    },
                )
            }),
            None => engine_for_apply
                .apply_replica_batch(decoded)
                .map_err(ShardManager::local_storage_failure),
        };
        #[cfg(not(feature = "protocol-trace"))]
        let result = {
            let _ = trace;
            engine_for_apply
                .apply_replica_batch(decoded)
                .map_err(ShardManager::local_storage_failure)
        };
        result
    })
    .await
    .map_err(|error| anyhow::anyhow!("peer recovery apply task failed: {error}"))??;
    let applied = receipt
        .outcomes
        .iter()
        .filter(|outcome| **outcome != crate::engine::ApplyOutcome::Redelivery)
        .count() as u64;
    Ok((next_cursor, applied))
}

/// Apply a sequence-ordered WAL suffix chunk during recovery (`FS-024`).
///
/// Enforces:
/// - Duplicate chunks or duplicate identical operations are idempotent (`applied = 0` for redeliveries).
/// - Unexpected sequence gaps (`operation.seq_no > expected_next_seq_no`) are rejected.
/// - Out-of-order sequences within the chunk (`operation.seq_no < previous_seq_no`) are rejected.
#[cfg(test)]
async fn apply_ordered_recovery_suffix_chunk(
    engine: Arc<dyn SearchEngine>,
    expected_next_seq_no: u64,
    snapshot_processed_checkpoint: Option<u64>,
    source_max_seq_no: Option<u64>,
    operations: Vec<RecoveryOperation>,
) -> Result<(u64, u64)> {
    for window in operations.windows(2) {
        if window[1].seq_no < window[0].seq_no {
            anyhow::bail!(
                "peer recovery WAL suffix chunk is out of sequence order: {} follows {}",
                window[1].seq_no,
                window[0].seq_no
            );
        }
    }
    let mut next_expected = expected_next_seq_no;
    for op in &operations {
        if snapshot_processed_checkpoint.is_some_and(|cp| op.seq_no <= cp) {
            continue;
        }
        if op.seq_no > next_expected {
            anyhow::bail!(
                "peer recovery WAL suffix chunk has an unexpected sequence gap: expected {next_expected}, got {}",
                op.seq_no
            );
        }
        if op.seq_no == next_expected {
            next_expected = next_expected
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("peer recovery sequence overflow"))?;
        }
    }
    let cursor = crate::wal::WalCursor {
        generation_id: 0,
        byte_offset: expected_next_seq_no,
    };
    let next_cursor = crate::wal::WalCursor {
        generation_id: 0,
        byte_offset: next_expected,
    };
    let (_, applied) = apply_recovery_operations_cursor(
        engine,
        None,
        cursor,
        next_cursor,
        snapshot_processed_checkpoint,
        source_max_seq_no,
        true,
        operations,
    )
    .await?;
    Ok((next_expected, applied))
}

#[cfg(test)]
async fn apply_recovery_operations(
    engine: Arc<dyn SearchEngine>,
    from_seq_no: u64,
    primary_next_seq_no: u64,
    complete: bool,
    operations: Vec<RecoveryOperation>,
) -> Result<(u64, u64)> {
    let (_, applied) = apply_recovery_operations_cursor(
        engine,
        None,
        crate::wal::WalCursor {
            generation_id: 0,
            byte_offset: from_seq_no,
        },
        crate::wal::WalCursor {
            generation_id: 0,
            byte_offset: primary_next_seq_no,
        },
        None,
        primary_next_seq_no.checked_sub(1),
        complete,
        operations,
    )
    .await?;
    Ok((primary_next_seq_no, applied))
}

async fn complete_with_observed_settlement(
    context: CompletionSettlementContext<'_>,
) -> TargetMembershipObservation {
    let deadline = tokio::time::Instant::now() + context.retry_timeout;
    let key = ShardKey::new(&context.candidate.index_name, context.candidate.shard_id);
    loop {
        match context
            .transport_client
            .complete_finalize_recovery(
                &context.candidate.primary,
                CompleteFinalizeRecoveryRequest {
                    session_id: context.session_id.to_string(),
                    applied_cursor: Some(recovery_cursor_proto(context.applied_cursor)),
                    processed_checkpoint: context.processed_checkpoint,
                },
            )
            .await
        {
            Ok(_) => return TargetMembershipObservation::Admitted,
            Err(error) => {
                let state = context.cluster_manager.get_state();
                let observation =
                    observe_target_membership(&state, &key, context.local_node_id, context.pending);
                if observation != TargetMembershipObservation::Unknown {
                    return observation;
                }
                if tokio::time::Instant::now() >= deadline {
                    tracing::warn!(
                        index = context.candidate.index_name,
                        shard_id = context.candidate.shard_id,
                        error = %error,
                        "Peer recovery completion remains unknown after retry timeout"
                    );
                    return TargetMembershipObservation::Unknown;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    fn forwarding_request<T>(message: T) -> tonic::Request<T> {
        crate::transport::request_with_cluster_state_version(message, 0)
    }
    use super::*;
    use crate::cluster::state::{
        DynamicMapping, IndexMetadata, IndexSettings, IndexUuid, NodeRole, ShardRoutingEntry,
    };
    use crate::consensus;
    use crate::consensus::types::{ClusterCommand, ClusterResponse};
    use crate::tasks::TaskManager;
    use crate::transport::proto::internal_transport_client::InternalTransportClient;
    use crate::transport::proto::{
        FetchRecoveryOpsRequest, ReplicateDocRequest, ShardDeleteRequest, ShardDocRequest,
    };
    use crate::transport::server::{
        RemoteStoreTransportResources, create_transport_service_for_test,
        create_transport_service_with_raft, create_transport_service_with_raft_and_storage_handle,
    };
    use std::collections::HashMap;

    fn apply_index(
        engine: &Arc<dyn SearchEngine>,
        doc_id: &str,
        source: serde_json::Value,
        seq_no: u64,
        primary_term: u64,
    ) {
        engine
            .apply_replica_operation(crate::engine::SequencedOperation {
                seq_no,
                primary_term,
                document_version: seq_no + 1,
                client_operation: None,
                mutation: crate::engine::DocumentMutation::Index {
                    doc_id: doc_id.to_string(),
                    source,
                },
            })
            .unwrap();
    }

    async fn wait_for_leader(raft: &crate::consensus::types::RaftInstance) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if raft.is_leader() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Raft leader was not elected"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn serve(
        service: crate::transport::proto::internal_transport_server::InternalTransportServer<
            crate::transport::server::TransportService,
        >,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
        });
        (address, handle)
    }

    async fn connect(
        address: std::net::SocketAddr,
    ) -> InternalTransportClient<tonic::transport::Channel> {
        InternalTransportClient::connect(format!("http://{address}"))
            .await
            .unwrap()
    }

    #[test]
    fn recovery_file_names_reject_traversal_and_separators() {
        for invalid in ["", ".", "..", "../meta.json", "dir/meta.json", "/meta.json"] {
            assert!(validate_file_name(invalid).is_err(), "{invalid}");
        }
        for valid in ["meta.json", ".managed.json", "segment.fast"] {
            validate_file_name(valid).unwrap();
        }
    }

    #[test]
    fn corrupted_recovery_file_checksum_is_rejected() {
        let expected: String = Sha256::digest(b"expected")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let corrupted: String = Sha256::digest(b"corrupted")
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let error = verify_recovery_sha256("segment.store", &corrupted, &expected).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn recovery_operation_validation_allows_gaps_and_reordering() {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn SearchEngine> = Arc::new(
            crate::engine::CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap(),
        );
        let (next, applied) = apply_recovery_operations(
            engine.clone(),
            5,
            9,
            true,
            vec![
                RecoveryOperation {
                    seq_no: 5,
                    primary_term: 2,
                    op: "index".into(),
                    doc_id: "a".into(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                    document_version: Some(1),
                    client_operation: None,
                },
                RecoveryOperation {
                    seq_no: 8,
                    primary_term: 2,
                    op: "delete".into(),
                    doc_id: "b".into(),
                    payload_json: Vec::new(),
                    document_version: Some(1),
                    client_operation: None,
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(next, 9);
        assert_eq!(applied, 2);

        let (next, applied) = apply_recovery_operations(
            engine,
            9,
            12,
            true,
            vec![
                RecoveryOperation {
                    seq_no: 11,
                    primary_term: 2,
                    op: "delete".into(),
                    doc_id: "a".into(),
                    payload_json: Vec::new(),
                    document_version: Some(2),
                    client_operation: None,
                },
                RecoveryOperation {
                    seq_no: 10,
                    primary_term: 2,
                    op: "delete".into(),
                    doc_id: "b".into(),
                    payload_json: Vec::new(),
                    document_version: Some(2),
                    client_operation: None,
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(next, 12);
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn stalled_retry_catch_up_batch_returns_to_finalize() {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn SearchEngine> = Arc::new(
            crate::engine::CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap(),
        );
        let cursor = crate::wal::WalCursor {
            generation_id: 4,
            byte_offset: 128,
        };

        let (next_cursor, applied) = apply_recovery_operations_cursor(
            engine,
            None,
            cursor,
            cursor,
            Some(7),
            Some(8),
            false,
            Vec::new(),
        )
        .await
        .unwrap();

        assert_eq!(next_cursor, cursor);
        assert_eq!(applied, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn retry_catch_up_stall_returns_to_finalize_in_real_session() {
        let (raft, state_handle) =
            consensus::create_raft_instance_mem(1, "retry-catch-up-it".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        let source_dir = tempfile::tempdir().unwrap();
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let resources = RemoteStoreTransportResources {
            storage_manager: Arc::new(
                crate::storage::StorageManager::new_in_path(source_dir.path()).unwrap(),
            ),
            remote_store_reader_cache: Arc::new(
                crate::engine::remote_store::RemoteSplitReaderCache::default(),
            ),
        };
        let (source_service, source_handle) = create_transport_service_with_raft_and_storage_handle(
            source_manager.clone(),
            source_shards.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(TaskManager::new()),
            resources,
            "primary-node".into(),
        );
        let (source_address, source_server) = serve(source_service).await;

        for node in [
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Master, NodeRole::Data],
                raft_node_id: 1,
            },
            NodeInfo {
                id: "replica-node".into(),
                name: "replica-node".into(),
                host: "127.0.0.1".into(),
                transport_port: 1,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        ] {
            assert_eq!(
                raft.client_write(ClusterCommand::AddNode { node })
                    .await
                    .unwrap()
                    .data,
                ClusterResponse::Ok
            );
        }
        assert_eq!(
            raft.client_write(ClusterCommand::SetMaster {
                node_id: "primary-node".into(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex {
                metadata: IndexMetadata {
                    name: "docs".into(),
                    uuid: IndexUuid::new("docs-uuid"),
                    number_of_shards: 1,
                    number_of_replicas: 1,
                    shard_routing: HashMap::from([(
                        0,
                        ShardRoutingEntry {
                            primary: "primary-node".into(),
                            primary_term: 1,
                            replicas: vec!["replica-node".into()],
                            in_sync_replicas: Vec::new(),
                            unassigned_replicas: 0,
                        },
                    )]),
                    mappings: HashMap::new(),
                    dynamic: Default::default(),
                    settings: IndexSettings::default(),
                },
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        let mut source_client = connect(source_address).await;
        let write = source_client
            .index_doc(forwarding_request(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "snapshot-doc".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(write.success, "{}", write.error);
        let source_engine = source_shards.get_shard("docs", 0).unwrap();
        source_engine.flush().unwrap();

        source_handle.inject_retry_catch_up_stall_once();
        let target_dir = tempfile::tempdir().unwrap();
        let target_shards = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let candidate = RecoveryCandidate {
            index_name: "docs".into(),
            metadata: state_handle.read().unwrap().indices["docs"].clone(),
            shard_id: 0,
            allocation_id: state_handle
                .read()
                .unwrap()
                .shard_allocation_id("docs", 0, "replica-node")
                .unwrap(),
            primary: state_handle.read().unwrap().nodes["primary-node"].clone(),
        };

        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            run_peer_recovery(
                &candidate,
                "replica-node",
                source_manager,
                target_shards.clone(),
                TransportClient::new(),
                Arc::new(AtomicBool::new(false)),
            ),
        )
        .await
        .expect("peer recovery did not return to finalize")
        .unwrap();
        let stats = match outcome {
            RecoveryRunOutcome::Admitted(stats) => stats,
            RecoveryRunOutcome::AwaitingMembership(_) => {
                panic!("the real recovery session did not observe committed admission")
            }
        };

        assert!(!stats.session_id.is_empty());
        assert_eq!(stats.operations, 0);
        assert_eq!(source_handle.retry_catch_up_stall_counts(), (1, 1, 1));
        let target_engine = target_shards.get_shard("docs", 0).unwrap();
        target_engine.refresh().unwrap();
        assert_eq!(
            target_engine.get_document("snapshot-doc").unwrap().unwrap()["value"],
            serde_json::json!(1)
        );
        assert!(
            state_handle.read().unwrap().indices["docs"].shard_routing[&0]
                .in_sync_replicas
                .contains(&"replica-node".to_string())
        );
        assert!(!target_shards.is_peer_recovery_target("docs", 0));

        source_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dynamic_mapping_write_aborts_source_session_before_reopen() {
        let (raft, state_handle) =
            consensus::create_raft_instance_mem(1, "peer-recovery-mapping".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        let source_dir = tempfile::tempdir().unwrap();
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let source_service = create_transport_service_with_raft(
            source_manager,
            source_shards.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(TaskManager::new()),
            "primary-node".into(),
        );
        let (source_address, source_server) = serve(source_service).await;

        for node in [
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Master, NodeRole::Data],
                raft_node_id: 1,
            },
            NodeInfo {
                id: "replica-node".into(),
                name: "replica-node".into(),
                host: "127.0.0.1".into(),
                transport_port: 1,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        ] {
            assert_eq!(
                raft.client_write(ClusterCommand::AddNode { node })
                    .await
                    .unwrap()
                    .data,
                ClusterResponse::Ok
            );
        }
        assert_eq!(
            raft.client_write(ClusterCommand::SetMaster {
                node_id: "primary-node".into(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex {
                metadata: IndexMetadata {
                    name: "dynamic-docs".into(),
                    uuid: IndexUuid::new("dynamic-docs-uuid"),
                    number_of_shards: 1,
                    number_of_replicas: 1,
                    shard_routing: HashMap::from([(
                        0,
                        ShardRoutingEntry {
                            primary: "primary-node".into(),
                            primary_term: 1,
                            replicas: vec!["replica-node".into()],
                            in_sync_replicas: Vec::new(),
                            unassigned_replicas: 0,
                        },
                    )]),
                    mappings: HashMap::new(),
                    dynamic: DynamicMapping::True,
                    settings: IndexSettings::default(),
                },
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        let mut client = connect(source_address).await;
        let target_allocation_id = state_handle
            .read()
            .unwrap()
            .shard_allocation_id("dynamic-docs", 0, "replica-node")
            .unwrap();
        let start_request = StartPeerRecoveryRequest {
            index_name: "dynamic-docs".into(),
            index_uuid: "dynamic-docs-uuid".into(),
            shard_id: 0,
            target_node_id: "replica-node".into(),
            target_allocation_id: Some(target_allocation_id),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let session = loop {
            let response = client
                .start_peer_recovery(tonic::Request::new(start_request.clone()))
                .await
                .unwrap()
                .into_inner();
            if !response.preparing {
                break response;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dynamic-mapping source session did not become ready"
            );
            tokio::task::yield_now().await;
        };
        assert!(session.error.is_empty(), "{}", session.error);

        for (doc_id, value) in [("first", 1), ("second", 2)] {
            let response = client
                .index_doc(forwarding_request(ShardDocRequest {
                    index_name: "dynamic-docs".into(),
                    shard_id: 0,
                    doc_id: doc_id.into(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"new_field": value}))
                        .unwrap(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.success, "{}", response.error);
        }
        assert!(
            state_handle.read().unwrap().indices["dynamic-docs"]
                .mappings
                .contains_key("new_field")
        );
        assert!(source_shards.get_shard("dynamic-docs", 0).is_some());

        let stale_fetch = client
            .fetch_recovery_ops(tonic::Request::new(FetchRecoveryOpsRequest {
                session_id: session.session_id,
                cursor: session.snapshot_cursor,
                max_ops: 1,
                snapshot_processed_checkpoint: session.snapshot_processed_checkpoint,
            }))
            .await
            .unwrap_err();
        assert_eq!(stale_fetch.code(), tonic::Code::NotFound);
        source_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lagging_target_view_after_admission_keeps_copy_open() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_address = listener.local_addr().unwrap();
        let mut source_state = ClusterState::new("lagged".into());
        source_state.nodes.insert(
            "primary-node".into(),
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        );
        source_state.nodes.insert(
            "replica-node".into(),
            NodeInfo {
                id: "replica-node".into(),
                name: "replica-node".into(),
                host: "127.0.0.1".into(),
                transport_port: 1,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        );
        let admitted_metadata = IndexMetadata {
            name: "lagged".into(),
            uuid: IndexUuid::new("lagged-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 1,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: vec!["replica-node".into()],
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        };
        source_state.add_index(admitted_metadata.clone());
        let source_manager = Arc::new(ClusterManager::new("lagged".into()));
        source_manager.update_state(source_state);
        let source_dir = tempfile::tempdir().unwrap();
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_service = create_transport_service_for_test(
            source_manager,
            source_shards,
            TransportClient::new(),
            Arc::new(TaskManager::new()),
            "primary-node".into(),
        );
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let source_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(source_service)
                .serve_with_incoming(incoming)
                .await
        });

        let mut stale_state = ClusterState::new("lagged".into());
        stale_state.nodes.insert(
            "primary-node".into(),
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        );
        let mut stale_metadata = admitted_metadata;
        stale_metadata
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .in_sync_replicas = Vec::new();
        stale_state.add_index(stale_metadata);
        let target_manager = Arc::new(ClusterManager::new("lagged".into()));
        target_manager.update_state(stale_state.clone());
        let target_dir = tempfile::tempdir().unwrap();
        let target_shards = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let target_engine = target_shards
            .open_shard_with_settings(
                "lagged",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "lagged-uuid",
            )
            .unwrap();
        apply_index(
            &target_engine,
            "acked",
            serde_json::json!({"value": 1}),
            0,
            1,
        );
        target_engine.refresh().unwrap();

        let driver = PeerRecoveryDriver::new(2);
        driver.reconcile(
            &stale_state,
            "replica-node",
            target_manager,
            target_shards.clone(),
            TransportClient::new(),
        );
        let key = ShardKey::new("lagged", 0);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let finished = !driver
                .active
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .contains(&key)
                && driver
                    .retries
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains_key(&key);
            if finished {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "lagged recovery attempt did not finish"
            );
            tokio::task::yield_now().await;
        }
        let shard_dir = target_shards.shard_data_dir("lagged", 0).unwrap();
        assert!(target_shards.get_shard("lagged", 0).is_some());
        assert!(!target_shards.is_peer_recovery_target("lagged", 0));
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        );
        assert_eq!(
            target_engine.get_document("acked").unwrap().unwrap()["value"],
            serde_json::json!(1)
        );
        source_server.abort();
    }

    #[test]
    fn older_local_term_remains_unknown_for_pending_target() {
        let mut state = ClusterState::new("older".into());
        state.add_index(IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 1,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        let pending = PeerRecoveryAwaitingMembership {
            index_uuid: "docs-uuid".into(),
            allocation_id: 1,
            primary_node_id: "primary-node".into(),
            primary_term: 2,
        };
        assert_eq!(
            observe_target_membership(&state, &ShardKey::new("docs", 0), "replica-node", &pending,),
            TargetMembershipObservation::Unknown
        );
    }

    #[test]
    fn pending_target_rejects_missing_or_different_allocation_identity() {
        let mut state = ClusterState::new("allocation-observation".into());
        state.add_index(IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 2,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        let pending = PeerRecoveryAwaitingMembership {
            index_uuid: "docs-uuid".into(),
            allocation_id: 1,
            primary_node_id: "primary-node".into(),
            primary_term: 1,
        };

        state
            .shard_allocations
            .get_mut("docs")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .replicas
            .insert("replica-node".into(), 2);
        assert_eq!(
            observe_target_membership(&state, &ShardKey::new("docs", 0), "replica-node", &pending,),
            TargetMembershipObservation::Rejected
        );

        state
            .shard_allocations
            .get_mut("docs")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .replicas
            .remove("replica-node");
        assert_eq!(
            observe_target_membership(&state, &ShardKey::new("docs", 0), "replica-node", &pending,),
            TargetMembershipObservation::Rejected
        );
    }

    #[test]
    fn pending_target_rejects_newer_term_or_different_primary() {
        let pending = PeerRecoveryAwaitingMembership {
            index_uuid: "docs-uuid".into(),
            allocation_id: 1,
            primary_node_id: "primary-node".into(),
            primary_term: 7,
        };
        let mut bumped = ClusterState::new("probe".into());
        bumped.add_index(IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 8,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        bumped
            .shard_allocations
            .get_mut("docs")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_initialized = true;
        let mut changed = bumped.clone();
        changed
            .indices
            .get_mut("docs")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .primary = "other-primary".into();

        assert_eq!(
            observe_target_membership(&bumped, &ShardKey::new("docs", 0), "replica-node", &pending,),
            TargetMembershipObservation::Rejected
        );
        assert_eq!(
            observe_target_membership(
                &changed,
                &ShardKey::new("docs", 0),
                "replica-node",
                &pending,
            ),
            TargetMembershipObservation::Rejected
        );
    }

    #[tokio::test]
    async fn restarted_pending_target_is_restored_before_recovery_scheduling() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ClusterState::new("probe".into());
        state.add_index(IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 7,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        state
            .shard_allocations
            .get_mut("docs")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_initialized = true;
        let allocation_id = state
            .shard_allocation_id("docs", 0, "replica-node")
            .unwrap();
        {
            let first = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
            let engine = first
                .open_assigned_shard_with_settings(
                    "docs",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "docs-uuid",
                    crate::shard::AssignedShardOpen {
                        allocation_id,
                        primary_term: 7,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
            apply_index(&engine, "doc", serde_json::json!({"value": 1}), 0, 1);
            engine.refresh().unwrap();
            engine.flush().unwrap();
            assert!(first.begin_peer_recovery_target("docs", 0));
            first
                .mark_peer_recovery_awaiting_membership_blocking(
                    "docs".into(),
                    0,
                    PeerRecoveryAwaitingMembership {
                        index_uuid: "docs-uuid".into(),
                        allocation_id,
                        primary_node_id: "primary-node".into(),
                        primary_term: 7,
                    },
                )
                .await
                .unwrap();
        }

        let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
        let failures = crate::node::reconciliation::open_local_assigned_shards(
            &state,
            "replica-node",
            &restarted,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        );
        assert!(failures.is_empty());
        assert!(
            restarted.is_peer_recovery_target("docs", 0),
            "durable pending marker must be restored before recovery scheduling"
        );
        assert!(
            restarted.get_shard("docs", 0).is_some(),
            "the finalized pending copy must reopen intact"
        );
        assert_eq!(
            restarted
                .get_shard("docs", 0)
                .unwrap()
                .get_document("doc")
                .unwrap()
                .unwrap()["value"],
            serde_json::json!(1)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restarted_pending_target_is_not_recovered_again_over_transport() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let mut state = ClusterState::new("pending-restart-transport".into());
        state.add_node(NodeInfo {
            id: "primary-node".into(),
            name: "primary-node".into(),
            host: "127.0.0.1".into(),
            transport_port: 0,
            http_port: 0,
            roles: vec![NodeRole::Data],
            raft_node_id: 1,
        });
        state.add_node(NodeInfo {
            id: "replica-node".into(),
            name: "replica-node".into(),
            host: "127.0.0.1".into(),
            transport_port: 0,
            http_port: 0,
            roles: vec![NodeRole::Data],
            raft_node_id: 2,
        });
        state.add_index(IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 7,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        state
            .shard_allocations
            .get_mut("docs")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_initialized = true;
        let source_allocation = state
            .shard_allocation_id("docs", 0, "primary-node")
            .unwrap();
        let target_allocation = state
            .shard_allocation_id("docs", 0, "replica-node")
            .unwrap();

        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_engine = source_shards
            .open_assigned_shard_with_settings(
                "docs",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "docs-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id: source_allocation,
                    primary_term: 7,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        apply_index(
            &source_engine,
            "source-copy",
            serde_json::json!({"value": "source"}),
            0,
            7,
        );
        source_engine.refresh().unwrap();
        source_engine.flush().unwrap();
        let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
        source_manager.update_state(state.clone());
        let source_service = create_transport_service_for_test(
            source_manager.clone(),
            source_shards,
            TransportClient::new(),
            Arc::new(TaskManager::new()),
            "primary-node".into(),
        );
        let (source_address, source_server) = serve(source_service).await;
        state.nodes.get_mut("primary-node").unwrap().transport_port = source_address.port();
        source_manager.update_state(state.clone());

        {
            let first_target = Arc::new(ShardManager::new(
                target_dir.path(),
                Duration::from_secs(60),
            ));
            let target_engine = first_target
                .open_assigned_shard_with_settings(
                    "docs",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "docs-uuid",
                    crate::shard::AssignedShardOpen {
                        allocation_id: target_allocation,
                        primary_term: 7,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
            apply_index(
                &target_engine,
                "pending-copy",
                serde_json::json!({"value": "pending"}),
                0,
                7,
            );
            target_engine.refresh().unwrap();
            target_engine.flush().unwrap();
            assert!(first_target.begin_peer_recovery_target("docs", 0));
            first_target
                .mark_peer_recovery_awaiting_membership_blocking(
                    "docs".into(),
                    0,
                    PeerRecoveryAwaitingMembership {
                        index_uuid: "docs-uuid".into(),
                        allocation_id: target_allocation,
                        primary_node_id: "primary-node".into(),
                        primary_term: 7,
                    },
                )
                .await
                .unwrap();
        }

        let restarted = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let failures = crate::node::reconciliation::open_local_assigned_shards(
            &state,
            "replica-node",
            restarted.as_ref(),
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        );
        assert!(failures.is_empty());
        let target_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
        target_manager.update_state(state.clone());
        PeerRecoveryDriver::new(1).reconcile(
            &state,
            "replica-node",
            target_manager,
            restarted.clone(),
            TransportClient::new(),
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(engine) = restarted.get_shard("docs", 0)
                && engine.get_document("pending-copy").unwrap().is_some()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the restarted pending target was wiped or never restored"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let shard_dir = restarted.shard_data_dir("docs", 0).unwrap();
        assert!(
            shard_dir
                .join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)
                .exists()
        );
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        );
        source_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completion_timeout_keeps_target_open_until_committed_admission_is_observed() {
        let (raft, source_state_handle) =
            consensus::create_raft_instance_mem(1, "pending-admission".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        let metadata = IndexMetadata {
            name: "pending".into(),
            uuid: IndexUuid::new("pending-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 1,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        };
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
        let primary_allocation_id = source_state_handle
            .read()
            .unwrap()
            .primary_allocation_id("pending", 0)
            .unwrap();
        assert_eq!(
            raft.client_write(ClusterCommand::ActivatePrimary {
                index_name: "pending".into(),
                index_uuid: "pending-uuid".into(),
                shard_id: 0,
                primary: "primary-node".into(),
                allocation_id: primary_allocation_id,
                expected_term: 1,
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
        let promoting = source_state_handle.read().unwrap().clone();
        let (authority, history) =
            crate::node::tests::finish_empty_primary(&raft, &promoting, "pending", 0).await;
        let stale_target_state = source_state_handle.read().unwrap().clone();
        let allocation_id = stale_target_state
            .shard_allocation_id("pending", 0, "replica-node")
            .unwrap();
        assert_eq!(
            raft.client_write(ClusterCommand::MarkReplicaInSync {
                index_name: "pending".into(),
                index_uuid: "pending-uuid".into(),
                shard_id: 0,
                replica: "replica-node".into(),
                allocation_id,
                primary: "primary-node".into(),
                primary_term: 2,
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        let target_manager = Arc::new(ClusterManager::new("pending-admission".into()));
        target_manager.update_state(stale_target_state);
        let target_dir = tempfile::tempdir().unwrap();
        let target_shards = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let target_engine = target_shards
            .open_assigned_shard_with_settings(
                "pending",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "pending-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        target_shards
            .persist_copy_canonical_history("pending", allocation_id, &authority, &history)
            .unwrap();
        apply_index(
            &target_engine,
            "base",
            serde_json::json!({"value": 0}),
            0,
            2,
        );
        target_engine.refresh().unwrap();
        assert!(target_shards.begin_peer_recovery_target("pending", 0));
        let pending = PeerRecoveryAwaitingMembership {
            index_uuid: "pending-uuid".into(),
            allocation_id,
            primary_node_id: "primary-node".into(),
            primary_term: 2,
        };
        target_shards
            .mark_peer_recovery_awaiting_membership_blocking("pending".into(), 0, pending.clone())
            .await
            .unwrap();

        let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_port = unused.local_addr().unwrap().port();
        drop(unused);
        let candidate = RecoveryCandidate {
            index_name: "pending".into(),
            metadata,
            shard_id: 0,
            allocation_id,
            primary: NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: unavailable_port,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 1,
            },
        };
        let transport_client = TransportClient::new();
        let observation = complete_with_observed_settlement(CompletionSettlementContext {
            candidate: &candidate,
            local_node_id: "replica-node",
            cluster_manager: target_manager.clone(),
            transport_client: &transport_client,
            session_id: "lost-response",
            pending: &pending,
            applied_cursor: crate::wal::WalCursor {
                generation_id: 0,
                byte_offset: 1,
            },
            processed_checkpoint: Some(0),
            retry_timeout: Duration::from_millis(50),
        })
        .await;
        assert_eq!(observation, TargetMembershipObservation::Unknown);
        assert!(target_shards.get_shard("pending", 0).is_some());
        assert!(!target_shards.rejects_live_replication("pending", 0));
        let shard_dir = target_shards.shard_data_dir("pending", 0).unwrap();
        assert!(
            shard_dir
                .join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)
                .exists()
        );
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        );

        let target_service = create_transport_service_for_test(
            target_manager.clone(),
            target_shards.clone(),
            TransportClient::new(),
            Arc::new(TaskManager::new()),
            "replica-node".into(),
        );
        let (target_address, target_server) = serve(target_service).await;
        let mut target_client = connect(target_address).await;
        let live_apply = target_client
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                authority: Some(
                    crate::replication::capture_replica_authority(
                        &target_manager.get_state(),
                        "pending",
                        0,
                    )
                    .unwrap(),
                ),
                index_name: "pending".into(),
                shard_id: 0,
                doc_id: "live".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                op: "index".into(),
                seq_no: 1,
                index_uuid: "pending-uuid".into(),
                primary_term: Some(2),
                target_allocation_id: Some(allocation_id),
                document_version: Some(1),
                client_operation: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(live_apply.success, "{}", live_apply.error);

        target_manager.update_state(source_state_handle.read().unwrap().clone());
        let driver = PeerRecoveryDriver::new(2);
        let caught_up = target_manager.get_state();
        driver.reconcile(
            &caught_up,
            "replica-node",
            target_manager,
            target_shards.clone(),
            TransportClient::new(),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while target_shards.is_peer_recovery_target("pending", 0) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "admitted pending target was not cleared"
            );
            tokio::task::yield_now().await;
        }
        assert!(target_shards.get_shard("pending", 0).is_some());
        target_engine.refresh().unwrap();
        assert_eq!(
            target_engine.get_document("live").unwrap().unwrap()["value"],
            serde_json::json!(1)
        );
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)
                .exists()
        );
        target_server.abort();
    }

    #[tokio::test]
    async fn restarted_pending_target_observed_as_promoted_is_admitted() {
        let dir = tempfile::tempdir().unwrap();
        let first_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = first_manager
            .open_shard_with_settings(
                "promoted",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "promoted-uuid",
            )
            .unwrap();
        apply_index(&engine, "doc", serde_json::json!({"value": 1}), 0, 1);
        engine.refresh().unwrap();
        assert!(first_manager.begin_peer_recovery_target("promoted", 0));
        first_manager
            .mark_peer_recovery_awaiting_membership_blocking(
                "promoted".into(),
                0,
                PeerRecoveryAwaitingMembership {
                    index_uuid: "promoted-uuid".into(),
                    allocation_id: 1,
                    primary_node_id: "old-primary".into(),
                    primary_term: 4,
                },
            )
            .await
            .unwrap();
        drop(engine);
        drop(first_manager);

        let restarted = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let reopened = restarted
            .open_shard_with_settings(
                "promoted",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "promoted-uuid",
            )
            .unwrap();
        assert!(restarted.is_peer_recovery_target("promoted", 0));
        assert!(!restarted.rejects_live_replication("promoted", 0));

        let cluster_manager = Arc::new(ClusterManager::new("promoted".into()));
        let mut state = ClusterState::new("promoted".into());
        state.add_index(IndexMetadata {
            name: "promoted".into(),
            uuid: IndexUuid::new("promoted-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "replica-node".into(),
                    primary_term: 5,
                    replicas: vec!["other".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 1,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        cluster_manager.update_state(state.clone());
        let driver = PeerRecoveryDriver::new(2);
        driver.reconcile(
            &state,
            "replica-node",
            cluster_manager,
            restarted.clone(),
            TransportClient::new(),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while restarted.is_peer_recovery_target("promoted", 0) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "promoted pending target was not admitted"
            );
            tokio::task::yield_now().await;
        }
        assert!(restarted.get_shard("promoted", 0).is_some());
        assert_eq!(
            reopened.get_document("doc").unwrap().unwrap()["value"],
            serde_json::json!(1)
        );
        let shard_dir = restarted.shard_data_dir("promoted", 0).unwrap();
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)
                .exists()
        );
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        );
    }

    #[tokio::test]
    async fn definitive_term_bump_rejects_and_marks_pending_target() {
        let dir = tempfile::tempdir().unwrap();
        let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        shards
            .open_shard_with_settings(
                "rejected",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "rejected-uuid",
            )
            .unwrap();
        assert!(shards.begin_peer_recovery_target("rejected", 0));
        shards
            .mark_peer_recovery_awaiting_membership_blocking(
                "rejected".into(),
                0,
                PeerRecoveryAwaitingMembership {
                    index_uuid: "rejected-uuid".into(),
                    allocation_id: 1,
                    primary_node_id: "primary-node".into(),
                    primary_term: 7,
                },
            )
            .await
            .unwrap();

        let cluster_manager = Arc::new(ClusterManager::new("rejected".into()));
        let mut state = ClusterState::new("rejected".into());
        state.add_index(IndexMetadata {
            name: "rejected".into(),
            uuid: IndexUuid::new("rejected-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 8,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        state
            .shard_allocations
            .get_mut("rejected")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_initialized = true;
        cluster_manager.update_state(state.clone());
        let driver = PeerRecoveryDriver::new(2);
        driver.reconcile(
            &state,
            "replica-node",
            cluster_manager,
            shards.clone(),
            TransportClient::new(),
        );
        let shard_dir = shards.shard_data_dir("rejected", 0).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while shards.get_shard("rejected", 0).is_some()
            || !shard_dir
                .join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER)
                .exists()
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "rejected pending target was not aborted"
            );
            tokio::task::yield_now().await;
        }
        assert!(!shards.is_peer_recovery_target("rejected", 0));
        assert!(
            !shard_dir
                .join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER)
                .exists()
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn file_recovery_copies_flushed_state_catches_up_and_admits_target() {
        use std::os::unix::fs::PermissionsExt;

        let (raft, state_handle) =
            consensus::create_raft_instance_mem(1, "peer-recovery-it".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        let target_dir = tempfile::tempdir().unwrap();
        let target_shards = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let target_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let target_service = create_transport_service_for_test(
            target_manager,
            target_shards.clone(),
            TransportClient::new(),
            Arc::new(TaskManager::new()),
            "replica-node".into(),
        );
        let (target_address, target_server) = serve(target_service).await;

        let source_dir = tempfile::tempdir().unwrap();
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let source_service = create_transport_service_with_raft(
            source_manager.clone(),
            source_shards.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(TaskManager::new()),
            "primary-node".into(),
        );
        let (source_address, source_server) = serve(source_service).await;

        for node in [
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Master, NodeRole::Data],
                raft_node_id: 1,
            },
            NodeInfo {
                id: "replica-node".into(),
                name: "replica-node".into(),
                host: "127.0.0.1".into(),
                transport_port: target_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        ] {
            assert_eq!(
                raft.client_write(ClusterCommand::AddNode { node })
                    .await
                    .unwrap()
                    .data,
                ClusterResponse::Ok
            );
        }
        assert_eq!(
            raft.client_write(ClusterCommand::SetMaster {
                node_id: "primary-node".into(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
        let metadata = IndexMetadata {
            name: "docs".into(),
            uuid: IndexUuid::new("docs-uuid"),
            number_of_shards: 1,
            number_of_replicas: 1,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "primary-node".into(),
                    primary_term: 1,
                    replicas: vec!["replica-node".into()],
                    in_sync_replicas: Vec::new(),
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        };
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        let mut source_client = connect(source_address).await;
        let large_value = "x".repeat(8 * 1024);
        for doc in 0..20 {
            let response = source_client
                .index_doc(forwarding_request(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: format!("doc-{doc}"),
                    payload_json: serde_json::to_vec(&serde_json::json!({
                        "value": doc,
                        "padding": large_value
                    }))
                    .unwrap(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.success, "{}", response.error);
        }
        let source_engine = source_shards.get_shard("docs", 0).unwrap();
        source_engine.flush().unwrap();
        for doc in 20..25 {
            let response = source_client
                .index_doc(forwarding_request(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: format!("doc-{doc}"),
                    payload_json: serde_json::to_vec(&serde_json::json!({
                        "value": doc
                    }))
                    .unwrap(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.success, "{}", response.error);
        }
        let delete_seed = source_client
            .index_doc(forwarding_request(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "delete-me".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": "delete"})).unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(delete_seed.success, "{}", delete_seed.error);

        let index_dir = source_dir.path().join("docs-uuid/shard_0/index");
        let idle_before_recovery = source_client
            .index_doc(forwarding_request(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "idle-before-recovery".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({
                    "value": "idle-before-recovery"
                }))
                .unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            idle_before_recovery.success,
            "{}",
            idle_before_recovery.error
        );
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let failed_snapshot_commit = source_engine.refresh();
        std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(failed_snapshot_commit.is_err());
        assert!(source_engine.writer_is_failed_for_test());

        let current_metadata = state_handle.read().unwrap().indices["docs"].clone();
        let candidate = RecoveryCandidate {
            index_name: "docs".into(),
            metadata: current_metadata,
            shard_id: 0,
            allocation_id: state_handle
                .read()
                .unwrap()
                .shard_allocation_id("docs", 0, "replica-node")
                .unwrap(),
            primary: state_handle.read().unwrap().nodes["primary-node"].clone(),
        };
        let candidate_for_recovery = candidate.clone();
        let target_shards_for_recovery = target_shards.clone();
        let recovery = tokio::spawn(async move {
            run_peer_recovery(
                &candidate_for_recovery,
                "replica-node",
                source_manager,
                target_shards_for_recovery,
                TransportClient::new(),
                Arc::new(AtomicBool::new(false)),
            )
            .await
        });
        let active_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !target_shards.is_peer_recovery_target("docs", 0) {
            assert!(
                tokio::time::Instant::now() < active_deadline,
                "target recovery did not start"
            );
            tokio::task::yield_now().await;
        }

        let delete_response = source_client
            .delete_doc(forwarding_request(ShardDeleteRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "delete-me".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(delete_response.success, "{}", delete_response.error);
        assert!(
            !recovery.is_finished(),
            "a write completed while peer recovery was observably active"
        );

        let mut concurrent_ids = Vec::new();
        while !recovery.is_finished() && concurrent_ids.len() < 50 {
            let doc_id = format!("during-{}", concurrent_ids.len());
            let response = source_client
                .index_doc(forwarding_request(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: doc_id.clone(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"value": doc_id}))
                        .unwrap(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.success, "{}", response.error);
            concurrent_ids.push(doc_id);
        }
        let stats = match recovery.await.unwrap().unwrap() {
            RecoveryRunOutcome::Admitted(stats) => stats,
            RecoveryRunOutcome::AwaitingMembership(_) => {
                panic!("normal recovery should observe committed admission")
            }
        };
        assert!(stats.bytes > 0);

        let post = source_client
            .index_doc(forwarding_request(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "post-finalize".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": "post-finalize"}))
                    .unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(post.success, "{}", post.error);

        source_engine.refresh().unwrap();
        let target_engine = target_shards.get_shard("docs", 0).unwrap();
        target_engine.refresh().unwrap();
        for doc in 0..25 {
            let id = format!("doc-{doc}");
            assert_eq!(
                source_engine.get_document(&id).unwrap(),
                target_engine.get_document(&id).unwrap(),
                "{id}"
            );
        }
        for id in concurrent_ids
            .iter()
            .map(String::as_str)
            .chain(["idle-before-recovery", "post-finalize"])
        {
            assert_eq!(
                source_engine.get_document(id).unwrap(),
                target_engine.get_document(id).unwrap(),
                "{id}"
            );
        }
        assert!(source_engine.get_document("delete-me").unwrap().is_none());
        assert!(target_engine.get_document("delete-me").unwrap().is_none());
        assert!(
            state_handle.read().unwrap().indices["docs"].shard_routing[&0]
                .in_sync_replicas
                .contains(&"replica-node".to_string())
        );
        let stale_session = source_client
            .fetch_recovery_ops(tonic::Request::new(FetchRecoveryOpsRequest {
                session_id: stats.session_id,
                cursor: Some(crate::transport::proto::RecoveryWalCursor {
                    generation_id: 0,
                    byte_offset: 0,
                }),
                max_ops: 1,
                snapshot_processed_checkpoint: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(stale_session.code(), tonic::Code::NotFound);

        source_server.abort();
        target_server.abort();
    }

    #[tokio::test]
    async fn fs022_flushed_shard_snapshot_persists_manifest_vectors_and_complete_marker_across_restart()
     {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let mappings = HashMap::from([
            (
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            ),
            (
                "embedding".to_string(),
                FieldMapping {
                    field_type: FieldType::KnnVector,
                    dimension: Some(3),
                },
            ),
        ]);
        let shard_dir = dir.path().join("uuid-fs022/shard_0");
        {
            let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
            let engine = manager
                .open_assigned_shard_with_settings(
                    "fs022",
                    0,
                    &mappings,
                    &IndexSettings::default(),
                    "uuid-fs022",
                    crate::shard::AssignedShardOpen {
                        allocation_id: 7,
                        primary_term: 2,
                        allow_empty_creation: true,
                    },
                )
                .unwrap();
            engine
                .add_document_with_receipt_at_term(
                    "v1",
                    serde_json::json!({"title": "alpha", "embedding": [1.0, 0.0, 0.0]}),
                    2,
                )
                .unwrap();
            engine
                .add_document_with_receipt_at_term(
                    "v2",
                    serde_json::json!({"title": "beta", "embedding": [0.0, 1.0, 0.0]}),
                    2,
                )
                .unwrap();
            engine
                .delete_document_with_receipt_at_term("v2", 2)
                .unwrap();
            engine.flush().unwrap();

            let latest_dir = shard_dir.join(crate::engine::PERSISTED_SHARD_SNAPSHOT_DIR);
            assert!(
                latest_dir
                    .join(crate::engine::SHARD_SNAPSHOT_MANIFEST_FILE)
                    .is_file()
            );
            assert!(
                latest_dir
                    .join(crate::engine::SHARD_SNAPSHOT_COMPLETE_FILE)
                    .is_file()
            );
            let manifest = engine.latest_persisted_shard_snapshot().unwrap().unwrap();
            assert_eq!(
                manifest.format_version,
                crate::engine::SHARD_SNAPSHOT_FORMAT_VERSION
            );
            assert_eq!(manifest.index_uuid.as_deref(), Some("uuid-fs022"));
            assert_eq!(manifest.shard_id, Some(0));
            assert_eq!(manifest.allocation_id, Some(7));
            assert_eq!(manifest.primary_term, 2);
            assert_eq!(manifest.committed_boundary.processed_checkpoint, Some(2));
            assert!(manifest.vector_state.present);
            assert_eq!(manifest.vector_state.dimensions, Some(3));
            assert_eq!(manifest.vector_state.vector_count, 1);
        }

        // Simulate process restart and verify persisted snapshot survives and validates.
        let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = restarted
            .open_assigned_shard_with_settings(
                "fs022",
                0,
                &mappings,
                &IndexSettings::default(),
                "uuid-fs022",
                crate::shard::AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 2,
                    allow_empty_creation: false,
                },
            )
            .unwrap();
        let manifest = engine.latest_persisted_shard_snapshot().unwrap().unwrap();
        assert_eq!(manifest.committed_boundary.processed_checkpoint, Some(2));
        assert!(manifest.vector_state.present);

        let latest_dir = shard_dir.join(crate::engine::PERSISTED_SHARD_SNAPSHOT_DIR);
        let complete_path = latest_dir.join(crate::engine::SHARD_SNAPSHOT_COMPLETE_FILE);
        let complete_bytes = std::fs::read(&complete_path).unwrap();
        std::fs::remove_file(&complete_path).unwrap();
        let missing_complete =
            crate::engine::ShardSnapshotManifest::load_and_verify(&latest_dir).unwrap_err();
        assert!(
            missing_complete.to_string().contains("incomplete"),
            "{missing_complete:#}"
        );
        std::fs::write(&complete_path, &complete_bytes).unwrap();

        let target_file = latest_dir.join(&manifest.files[0].name);
        let original_bytes = std::fs::read(&target_file).unwrap();
        let mut corrupted = original_bytes.clone();
        if let Some(first) = corrupted.first_mut() {
            *first ^= 0xFF;
        } else {
            corrupted.push(0xFF);
        }
        std::fs::write(&target_file, &corrupted).unwrap();
        let corrupt_err =
            crate::engine::ShardSnapshotManifest::load_and_verify(&latest_dir).unwrap_err();
        assert!(
            corrupt_err.to_string().contains("mismatch"),
            "{corrupt_err:#}"
        );
        std::fs::write(&target_file, &original_bytes).unwrap();

        let manifest_path = latest_dir.join(crate::engine::SHARD_SNAPSHOT_MANIFEST_FILE);
        let mut bad_version: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        bad_version["format_version"] = serde_json::json!(99);
        std::fs::write(&manifest_path, serde_json::to_vec(&bad_version).unwrap()).unwrap();
        let version_err =
            crate::engine::ShardSnapshotManifest::load_and_verify(&latest_dir).unwrap_err();
        assert!(
            version_err.is::<crate::common::UnsupportedIndexFormatError>(),
            "{version_err:#}"
        );
    }

    #[tokio::test]
    async fn fs023_staged_install_preserves_previous_valid_shard_across_enospc_checksum_interruption_and_restart()
     {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mappings = HashMap::from([
            (
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            ),
            (
                "embedding".to_string(),
                FieldMapping {
                    field_type: FieldType::KnnVector,
                    dimension: Some(3),
                },
            ),
        ]);

        // Prepare a valid source snapshot with text + vectors.
        let source_dir = tempfile::tempdir().unwrap();
        let source_manager = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_engine = source_manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &mappings,
                &IndexSettings::default(),
                "uuid-fs023",
                crate::shard::AssignedShardOpen {
                    allocation_id: 1,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        source_engine
            .add_document_with_receipt_at_term(
                "new-doc-1",
                serde_json::json!({"title": "snapshot-one", "embedding": [1.0, 0.0, 0.0]}),
                2,
            )
            .unwrap();
        source_engine
            .add_document_with_receipt_at_term(
                "new-doc-2",
                serde_json::json!({"title": "snapshot-two", "embedding": [0.0, 1.0, 0.0]}),
                2,
            )
            .unwrap();
        let snapshot_dir = source_dir.path().join("snapshot");
        let snapshot = source_engine
            .create_peer_recovery_snapshot(&snapshot_dir)
            .unwrap();
        assert!(snapshot.manifest.vector_state.present);

        // Target starts with a valid previous shard at allocation 7 containing "prev-valid".
        let target_dir = tempfile::tempdir().unwrap();
        let target_manager = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        let prev_engine = target_manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &mappings,
                &IndexSettings::default(),
                "uuid-fs023",
                crate::shard::AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        prev_engine
            .add_document_with_receipt_at_term(
                "prev-valid",
                serde_json::json!({"title": "preserved-before-recovery", "embedding": [0.0, 0.0, 1.0]}),
                1,
            )
            .unwrap();
        prev_engine.refresh().unwrap();
        prev_engine.flush().unwrap();
        drop(prev_engine);

        // 1. Simulated disk exhaustion (ENOSPC = 28) during staged finalization leaves previous valid shard intact.
        assert!(
            target_manager
                .begin_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
                .await
                .unwrap()
        );
        let staging_dir = target_manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
            .await
            .unwrap();
        assert!(staging_dir.ends_with("shard_0.staging"));
        for file in &snapshot.files {
            std::fs::copy(
                snapshot_dir.join(&file.name),
                staging_dir.join("index").join(&file.name),
            )
            .unwrap();
        }
        target_manager.inject_recovery_install_io_failures(28, 1);
        let enospc_err = match target_manager
            .finalize_peer_recovery_target_blocking(PeerRecoveryTargetInstall {
                index: "idx".into(),
                shard_id: 0,
                mappings: mappings.clone(),
                settings: IndexSettings::default(),
                index_uuid: "uuid-fs023".into(),
                allocation_id: 7,
                primary_term: 2,
                shard_dir: staging_dir.clone(),
                committed_boundary: snapshot.committed_boundary.clone(),
                expected_files: snapshot.files.iter().map(|f| f.name.clone()).collect(),
            })
            .await
        {
            Ok(_) => panic!("ENOSPC must fail staged snapshot installation"),
            Err(error) => error,
        };
        assert!(
            enospc_err.chain().any(|c| c
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.raw_os_error() == Some(28))),
            "{enospc_err:#}"
        );
        target_manager
            .reset_peer_recovery_target_for_retry_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
            .await
            .unwrap();
        assert!(!staging_dir.exists());

        let reopened_prev = target_manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &mappings,
                &IndexSettings::default(),
                "uuid-fs023",
                crate::shard::AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 1,
                    allow_empty_creation: false,
                },
            )
            .unwrap();
        assert_eq!(
            reopened_prev.get_document("prev-valid").unwrap().unwrap()["title"],
            serde_json::json!("preserved-before-recovery")
        );
        assert!(reopened_prev.get_document("new-doc-1").unwrap().is_none());
        drop(reopened_prev);

        // 2. Simulated process crash mid-staging (before reset or finalize) rolls back cleanly on restart.
        assert!(
            target_manager
                .begin_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
                .await
                .unwrap()
        );
        let interrupted_staging = target_manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
            .await
            .unwrap();
        std::fs::write(interrupted_staging.join("index/partial.tmp"), b"partial").unwrap();
        drop(target_manager);

        let restarted_manager = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        assert!(
            !restarted_manager
                .failed_peer_recovery_install_matches("idx", 0, "uuid-fs023", 7)
                .unwrap()
        );
        let after_crash = restarted_manager
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &mappings,
                &IndexSettings::default(),
                "uuid-fs023",
                crate::shard::AssignedShardOpen {
                    allocation_id: 7,
                    primary_term: 1,
                    allow_empty_creation: false,
                },
            )
            .unwrap();
        assert_eq!(
            after_crash.get_document("prev-valid").unwrap().unwrap()["title"],
            serde_json::json!("preserved-before-recovery")
        );
        drop(after_crash);

        // 3. Successful retry atomically swaps the verified snapshot and restores vectors.
        assert!(
            restarted_manager
                .begin_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
                .await
                .unwrap()
        );
        let staging_dir = restarted_manager
            .prepare_peer_recovery_target_blocking("idx".into(), 0, "uuid-fs023".into(), 7)
            .await
            .unwrap();
        for file in &snapshot.files {
            std::fs::copy(
                snapshot_dir.join(&file.name),
                staging_dir.join("index").join(&file.name),
            )
            .unwrap();
        }
        let installed = restarted_manager
            .finalize_peer_recovery_target_blocking(PeerRecoveryTargetInstall {
                index: "idx".into(),
                shard_id: 0,
                mappings: mappings.clone(),
                settings: IndexSettings::default(),
                index_uuid: "uuid-fs023".into(),
                allocation_id: 7,
                primary_term: 2,
                shard_dir: staging_dir.clone(),
                committed_boundary: snapshot.committed_boundary.clone(),
                expected_files: snapshot.files.iter().map(|f| f.name.clone()).collect(),
            })
            .await
            .unwrap();
        assert!(!staging_dir.exists());
        assert!(installed.get_document("prev-valid").unwrap().is_none());
        assert_eq!(
            installed.get_document("new-doc-1").unwrap().unwrap()["title"],
            serde_json::json!("snapshot-one")
        );
        let knn_hits = installed
            .search_knn("embedding", &[1.0, 0.0, 0.0], 1)
            .unwrap();
        assert_eq!(knn_hits.len(), 1);
        assert_eq!(knn_hits[0]["_id"], "new-doc-1");
        source_engine
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
    }

    #[tokio::test]
    async fn fs024_ordered_wal_suffix_streaming_is_idempotent_rejects_gaps_and_reordering_and_bounds_memory()
     {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn SearchEngine> = Arc::new(
            crate::engine::CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap(),
        );
        let op = |seq_no: u64, doc_id: &str, value: i64| RecoveryOperation {
            seq_no,
            primary_term: 1,
            op: "index".into(),
            doc_id: doc_id.into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"value": value})).unwrap(),
            document_version: Some(1),
            client_operation: None,
        };

        // Out-of-order chunk is rejected.
        let out_of_order = apply_ordered_recovery_suffix_chunk(
            engine.clone(),
            0,
            None,
            Some(5),
            vec![op(1, "d1", 1), op(0, "d0", 0)],
        )
        .await
        .unwrap_err();
        assert!(
            out_of_order.to_string().contains("out of sequence order"),
            "{out_of_order:#}"
        );

        // Unexpected sequence gap is rejected.
        let gapped = apply_ordered_recovery_suffix_chunk(
            engine.clone(),
            0,
            None,
            Some(5),
            vec![op(0, "d0", 0), op(2, "d2", 2)],
        )
        .await
        .unwrap_err();
        assert!(
            gapped.to_string().contains("unexpected sequence gap"),
            "{gapped:#}"
        );

        // Valid contiguous chunk applies and duplicate chunk delivery is idempotent.
        let chunk = vec![op(0, "d0", 10), op(1, "d1", 20), op(1, "d1", 20)];
        let (next_seq, applied) = apply_ordered_recovery_suffix_chunk(
            engine.clone(),
            0,
            None,
            Some(2_499),
            chunk.clone(),
        )
        .await
        .unwrap();
        assert_eq!(next_seq, 2);
        assert_eq!(applied, 2);

        let (next_seq_dup, applied_dup) =
            apply_ordered_recovery_suffix_chunk(engine.clone(), 2, None, Some(2_499), chunk)
                .await
                .unwrap();
        assert_eq!(next_seq_dup, 2);
        assert_eq!(applied_dup, 0);

        // Stream 2,500 operations in bounded chunks of 500 ops (< MAX_RECOVERY_OPS) and verify contiguous final checkpoint.
        let mut cursor_seq = 2u64;
        while cursor_seq < 2_500 {
            let end_seq = (cursor_seq + 500).min(2_500);
            let batch: Vec<RecoveryOperation> = (cursor_seq..end_seq)
                .map(|s| op(s, &format!("doc-{s}"), s as i64))
                .collect();
            assert!(batch.len() <= crate::transport::server::MAX_RECOVERY_OPS);
            let (next, newly_applied) = apply_ordered_recovery_suffix_chunk(
                engine.clone(),
                cursor_seq,
                None,
                Some(2_499),
                batch,
            )
            .await
            .unwrap();
            assert_eq!(newly_applied, end_seq - cursor_seq);
            cursor_seq = next;
        }
        assert_eq!(cursor_seq, 2_500);
        let stats = engine.sequence_stats();
        assert_eq!(stats.processed_checkpoint, Some(2_499));
        assert_eq!(stats.persisted_checkpoint, Some(2_499));
        assert_eq!(stats.max_seq_no, Some(2_499));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fs024_cancellation_propagates_to_stop_both_target_and_source_session() {
        let (raft, state_handle) =
            consensus::create_raft_instance_mem(1, "cancel-recovery-it".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        let source_dir = tempfile::tempdir().unwrap();
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let resources = RemoteStoreTransportResources {
            storage_manager: Arc::new(
                crate::storage::StorageManager::new_in_path(source_dir.path()).unwrap(),
            ),
            remote_store_reader_cache: Arc::new(
                crate::engine::remote_store::RemoteSplitReaderCache::default(),
            ),
        };
        let (source_service, source_handle) = create_transport_service_with_raft_and_storage_handle(
            source_manager.clone(),
            source_shards.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(TaskManager::new()),
            resources,
            "primary-node".into(),
        );
        let (source_address, source_server) = serve(source_service).await;

        for node in [
            NodeInfo {
                id: "primary-node".into(),
                name: "primary-node".into(),
                host: "127.0.0.1".into(),
                transport_port: source_address.port(),
                http_port: 0,
                roles: vec![NodeRole::Master, NodeRole::Data],
                raft_node_id: 1,
            },
            NodeInfo {
                id: "replica-node".into(),
                name: "replica-node".into(),
                host: "127.0.0.1".into(),
                transport_port: 1,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 0,
            },
        ] {
            raft.client_write(ClusterCommand::AddNode { node })
                .await
                .unwrap();
        }
        raft.client_write(ClusterCommand::SetMaster {
            node_id: "primary-node".into(),
        })
        .await
        .unwrap();
        raft.client_write(ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "cancel-idx".into(),
                uuid: IndexUuid::new("cancel-uuid"),
                number_of_shards: 1,
                number_of_replicas: 1,
                shard_routing: HashMap::from([(
                    0,
                    ShardRoutingEntry {
                        primary: "primary-node".into(),
                        primary_term: 1,
                        replicas: vec!["replica-node".into()],
                        in_sync_replicas: Vec::new(),
                        unassigned_replicas: 0,
                    },
                )]),
                mappings: HashMap::new(),
                dynamic: Default::default(),
                settings: IndexSettings::default(),
            },
        })
        .await
        .unwrap();

        let mut source_client = connect(source_address).await;
        source_client
            .index_doc(forwarding_request(ShardDocRequest {
                index_name: "cancel-idx".into(),
                shard_id: 0,
                doc_id: "d1".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"v": 1})).unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap();

        let allocation_id = state_handle
            .read()
            .unwrap()
            .shard_allocation_id("cancel-idx", 0, "replica-node")
            .unwrap();
        let primary_info = state_handle.read().unwrap().nodes["primary-node"].clone();
        let transport_client = TransportClient::new();
        let start = loop {
            let resp = transport_client
                .start_peer_recovery(
                    &primary_info,
                    StartPeerRecoveryRequest {
                        index_name: "cancel-idx".into(),
                        index_uuid: "cancel-uuid".into(),
                        shard_id: 0,
                        target_node_id: "replica-node".into(),
                        target_allocation_id: Some(allocation_id),
                    },
                )
                .await
                .unwrap();
            if !resp.preparing {
                break resp;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(
            source_handle
                .has_active_source_recovery_session(&start.session_id)
                .await
        );

        let target_dir = tempfile::tempdir().unwrap();
        let target_shards = Arc::new(ShardManager::new(
            target_dir.path(),
            Duration::from_secs(60),
        ));
        assert!(
            target_shards
                .begin_peer_recovery_target_blocking(
                    "cancel-idx".into(),
                    0,
                    "cancel-uuid".into(),
                    allocation_id,
                )
                .await
                .unwrap()
        );
        let staging_dir = target_shards
            .prepare_peer_recovery_target_blocking(
                "cancel-idx".into(),
                0,
                "cancel-uuid".into(),
                allocation_id,
            )
            .await
            .unwrap();
        assert!(staging_dir.exists());

        // Drop the target session guard to simulate target-side cancellation.
        {
            let _guard = TargetRecoverySessionGuard {
                active: true,
                reset_target_on_drop: true,
                session_id: start.session_id.clone(),
                primary: primary_info.clone(),
                transport_client: transport_client.clone(),
                shard_manager: target_shards.clone(),
                index_name: "cancel-idx".into(),
                shard_id: 0,
                index_uuid: "cancel-uuid".into(),
                allocation_id,
                local_node_id: "replica-node".into(),
                primary_term: start.primary_term,
            };
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while source_handle
            .has_active_source_recovery_session(&start.session_id)
            .await
            || target_shards.is_peer_recovery_target("cancel-idx", 0)
            || staging_dir.exists()
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cancellation did not clean up both source and target recovery state"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let snap_state = target_shards
            .replica_states
            .snapshot(
                &ShardKey::new("cancel-idx", 0),
                "replica-node",
                Instant::now(),
            )
            .unwrap();
        assert_eq!(snap_state.state, ReplicaCopyState::Initializing);
        source_server.abort();
    }

    #[test]
    fn fs025_replica_state_machine_drives_transitions_eligibility_backoff_and_metrics() {
        let registry = crate::shard::ReplicaStateRegistry::new();
        let key = ShardKey::new("orders", 0);

        // Legal lifecycle transitions: Unassigned -> Initializing -> Snapshotting -> CatchingUp -> InSync -> Stale -> InSync -> Failed -> Unassigned.
        assert_eq!(
            registry
                .transition(
                    &key,
                    "uuid-1",
                    "node-2",
                    Some(10),
                    1,
                    ReplicaCopyState::Initializing,
                    false,
                    None
                )
                .unwrap(),
            ReplicaCopyState::Unassigned
        );
        assert!(ReplicaCopyState::Initializing.can_schedule_recovery());
        assert!(!ReplicaCopyState::Initializing.is_acknowledgement_eligible());
        assert!(!ReplicaCopyState::Initializing.is_promotion_eligible());
        assert!(!ReplicaCopyState::Initializing.accepts_live_replication(false));

        registry
            .transition(
                &key,
                "uuid-1",
                "node-2",
                Some(10),
                1,
                ReplicaCopyState::Snapshotting,
                false,
                None,
            )
            .unwrap();
        assert!(!ReplicaCopyState::Snapshotting.can_schedule_recovery());
        assert!(!ReplicaCopyState::Snapshotting.accepts_live_replication(true));

        // Illegal jump directly from Snapshotting to InSync is rejected.
        assert!(
            registry
                .transition(
                    &key,
                    "uuid-1",
                    "node-2",
                    Some(10),
                    1,
                    ReplicaCopyState::InSync,
                    false,
                    None
                )
                .is_err()
        );

        // Retry backoff returns Snapshotting -> Initializing with exponential delay.
        let delay1 = registry.record_retry_backoff(
            &key,
            "uuid-1",
            "node-2",
            10,
            1,
            Duration::from_millis(100),
            Duration::from_millis(500),
            "network reset".into(),
        );
        assert_eq!(delay1, Duration::from_millis(100));
        let snap1 = registry.snapshot(&key, "node-2", Instant::now()).unwrap();
        assert_eq!(snap1.state, ReplicaCopyState::Initializing);
        assert_eq!(snap1.attempts, 1);
        assert!(!snap1.retry_ready);

        // Advance through Snapshotting -> CatchingUp -> InSync -> Stale -> InSync -> Failed.
        registry
            .transition(
                &key,
                "uuid-1",
                "node-2",
                Some(10),
                1,
                ReplicaCopyState::Snapshotting,
                false,
                None,
            )
            .unwrap();
        registry
            .transition(
                &key,
                "uuid-1",
                "node-2",
                Some(10),
                1,
                ReplicaCopyState::CatchingUp,
                true,
                None,
            )
            .unwrap();
        assert!(ReplicaCopyState::CatchingUp.accepts_live_replication(true));
        assert!(!ReplicaCopyState::CatchingUp.accepts_live_replication(false));

        registry
            .transition(
                &key,
                "uuid-1",
                "node-2",
                Some(10),
                1,
                ReplicaCopyState::InSync,
                false,
                None,
            )
            .unwrap();
        assert!(ReplicaCopyState::InSync.is_acknowledgement_eligible());
        assert!(ReplicaCopyState::InSync.is_promotion_eligible());
        assert!(ReplicaCopyState::InSync.accepts_live_replication(false));

        registry.update_lag_observation(&key, "uuid-1", "node-2", 10, 1, Some(50), true);
        let stale_snap = registry.snapshot(&key, "node-2", Instant::now()).unwrap();
        assert_eq!(stale_snap.state, ReplicaCopyState::Stale);
        assert_eq!(stale_snap.lag_ops, Some(50));
        assert!(!stale_snap.state.is_promotion_eligible());

        registry.update_lag_observation(&key, "uuid-1", "node-2", 10, 1, Some(0), false);
        let recovered_snap = registry.snapshot(&key, "node-2", Instant::now()).unwrap();
        assert_eq!(recovered_snap.state, ReplicaCopyState::InSync);

        registry
            .transition(
                &key,
                "uuid-1",
                "node-2",
                Some(10),
                1,
                ReplicaCopyState::Failed,
                false,
                Some("disk failure".into()),
            )
            .unwrap();
        let metrics_text = crate::metrics::gather();
        assert!(metrics_text.contains("ferrissearch_replica_state_transitions_total"));
        assert!(metrics_text.contains("ferrissearch_replica_copy_state_copies"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fs026_multi_node_committed_settings_react_on_every_open_follower_engine_and_lagging_replica_rejoins()
     {
        let mut metadata = IndexMetadata::from_create_request_body(
            "reactive-idx",
            &serde_json::json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "minimum_durable_copies": 2,
                    "replica_policy": {
                        "max_lag_ops": 5,
                        "lag_timeout_ms": 100,
                        "recovery_session_timeout_ms": 10000,
                        "recovery_finalize_timeout_ms": 2000,
                        "recovery_retry_initial_backoff_ms": 100,
                        "recovery_retry_max_backoff_ms": 500,
                        "write_ack_mode": "all_configured_copies"
                    }
                }
            }),
            &["node-1".into(), "node-2".into()],
        )
        .unwrap();

        // Validate that invalid combinations and unsafe ack modes fail closed.
        for invalid in [
            serde_json::json!({"index": {"replica_max_lag_ops": 0}}),
            serde_json::json!({"index": {"replica_lag_timeout_ms": 10}}),
            serde_json::json!({"index": {"recovery_finalize_timeout_ms": 20000, "recovery_session_timeout_ms": 1000}}),
            serde_json::json!({"index": {"recovery_retry_initial_backoff_ms": 1000, "recovery_retry_max_backoff_ms": 100}}),
            serde_json::json!({"index": {"write_ack_mode": "fire_and_forget"}}),
            serde_json::json!({"index": {"minimum_durable_copies": 1, "write_ack_mode": "all_configured_copies"}}),
        ] {
            let mut candidate = metadata.clone();
            assert!(
                candidate.apply_settings_update(&invalid).is_err(),
                "invalid replica policy update should fail: {invalid}"
            );
        }

        // Switch to in_sync_replicas with minimum_durable_copies=1 for dynamic lag/rejoin test.
        assert!(
            metadata
                .apply_settings_update(&serde_json::json!({
                    "index": {
                        "write_ack_mode": "in_sync_replicas",
                        "minimum_durable_copies": 1
                    }
                }))
                .unwrap()
        );

        let (raft, state_handle) =
            consensus::create_raft_instance_mem(1, "fs026-reactive-it".into())
                .await
                .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_leader(&raft).await;

        // Follower node-2 has an already-open local shard engine and bound applied authority,
        // and never receives direct HTTP settings ingress.
        let follower_dir = tempfile::tempdir().unwrap();
        let follower_cluster = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let follower_shards = Arc::new(ShardManager::new(
            follower_dir.path(),
            Duration::from_secs(60),
        ));
        follower_shards.bind_applied_shard_authority(follower_cluster.clone(), "node-2".into());
        follower_shards.start_applied_index_reconciler();

        for node_id in ["node-1", "node-2"] {
            raft.client_write(ClusterCommand::AddNode {
                node: NodeInfo {
                    id: node_id.into(),
                    name: node_id.into(),
                    host: "127.0.0.1".into(),
                    transport_port: 1,
                    http_port: 0,
                    roles: vec![NodeRole::Master, NodeRole::Data],
                    raft_node_id: if node_id == "node-1" { 1 } else { 0 },
                },
            })
            .await
            .unwrap();
        }
        raft.client_write(ClusterCommand::SetMaster {
            node_id: "node-1".into(),
        })
        .await
        .unwrap();
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex {
                metadata: metadata.clone(),
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        let follower_alloc = state_handle
            .read()
            .unwrap()
            .shard_allocation_id("reactive-idx", 0, "node-2")
            .unwrap();
        follower_shards
            .initialize_copy_identity_for_test(
                "reactive-idx",
                0,
                metadata.uuid.as_str(),
                follower_alloc,
                1,
            )
            .unwrap();
        {
            let shard_dir = follower_dir
                .path()
                .join(metadata.uuid.as_str())
                .join("shard_0");
            crate::engine::CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
        }
        let _follower_engine = follower_shards
            .open_assigned_shard_with_settings(
                "reactive-idx",
                0,
                &HashMap::new(),
                &metadata.settings,
                metadata.uuid.as_str(),
                crate::shard::AssignedShardOpen {
                    allocation_id: follower_alloc,
                    primary_term: 1,
                    allow_empty_creation: false,
                },
            )
            .unwrap();

        let settings_mgr = follower_shards
            .get_settings_manager("reactive-idx")
            .unwrap();
        let mut refresh_rx = settings_mgr.watch_refresh_interval();
        let mut flush_rx = settings_mgr.watch_flush_threshold();
        let mut policy_rx = settings_mgr.watch_replica_policy();
        let _ = refresh_rx.borrow_and_update();
        let _ = flush_rx.borrow_and_update();
        let _ = policy_rx.borrow_and_update();

        // Commit a settings update through Raft WITHOUT calling follower_shards.apply_settings() directly.
        let mut updated_meta = state_handle.read().unwrap().indices["reactive-idx"].clone();
        assert!(
            updated_meta
                .apply_settings_update(&serde_json::json!({
                    "index": {
                        "refresh_interval_ms": 2500,
                        "flush_threshold_bytes": 1048576,
                        "replica_max_lag_ops": 2,
                        "replica_lag_timeout_ms": 80,
                        "recovery_retry_initial_backoff_ms": 75,
                        "recovery_retry_max_backoff_ms": 300
                    }
                }))
                .unwrap()
        );
        assert_eq!(
            raft.client_write(ClusterCommand::UpdateIndex {
                metadata: updated_meta,
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );

        // Follower's already-open SettingsManager and IsrTracker react automatically from committed Raft state!
        tokio::time::timeout(Duration::from_secs(3), refresh_rx.changed())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), flush_rx.changed())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), policy_rx.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*refresh_rx.borrow(), Duration::from_millis(2500));
        assert_eq!(*flush_rx.borrow(), 1_048_576);
        let observed_policy = *policy_rx.borrow();
        assert_eq!(observed_policy.max_lag_ops, 2);
        assert_eq!(observed_policy.lag_timeout_ms, 80);
        assert_eq!(observed_policy.recovery_retry_initial_backoff_ms, 75);
        assert_eq!(observed_policy.recovery_retry_max_backoff_ms, 300);
        tokio::time::timeout(Duration::from_secs(3), async {
            while follower_shards
                .isr_tracker
                .effective_max_lag_ops("reactive-idx")
                != 2
                || follower_shards
                    .isr_tracker
                    .effective_lag_timeout("reactive-idx", Duration::from_secs(60))
                    != Duration::from_millis(80)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            follower_shards
                .isr_tracker
                .effective_max_lag_ops("reactive-idx"),
            2
        );
        assert_eq!(
            follower_shards
                .isr_tracker
                .effective_lag_timeout("reactive-idx", Duration::from_secs(60)),
            Duration::from_millis(80)
        );
    }
}
