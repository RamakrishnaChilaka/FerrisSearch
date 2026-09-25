use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{ClusterState, IndexMetadata, NodeInfo};
use crate::engine::SearchEngine;
use crate::shard::{
    PeerRecoveryAwaitingMembership, PeerRecoveryTargetInstall, PeerRecoveryTargetState, ShardKey,
    ShardManager,
};
use crate::transport::TransportClient;
use crate::transport::proto::{
    CompleteFinalizeRecoveryRequest, FetchRecoveryFileChunkRequest, FetchRecoveryOpsRequest,
    PrepareFinalizeRecoveryRequest, RecoverReplicaOp, RecoveryFileMetadata,
    StartPeerRecoveryRequest,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

const MAX_RECOVERY_FILES: usize = 10_000;
const MAX_RECOVERY_TOTAL_BYTES: u64 = 1 << 40;
const RECOVERY_RETRY_MIN: Duration = Duration::from_secs(5);
const RECOVERY_RETRY_MAX: Duration = Duration::from_secs(60);
const COMPLETE_RETRY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct RecoveryCandidate {
    index_name: String,
    metadata: IndexMetadata,
    shard_id: u32,
    primary: NodeInfo,
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
        let pending_targets =
            self.reconcile_pending_targets(state, local_node_id, shard_manager.clone());

        if !self.enabled {
            return;
        }
        let now = Instant::now();
        for candidate in recovery_candidates(state, local_node_id) {
            let key = ShardKey::new(&candidate.index_name, candidate.shard_id);
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
            tokio::spawn(async move {
                let started = Instant::now();
                let result = run_peer_recovery(
                    &candidate,
                    &local_node_id,
                    cluster_manager.clone(),
                    shard_manager.clone(),
                    transport_client,
                )
                .await;
                drop(permit);

                match result {
                    Ok(RecoveryRunOutcome::Admitted(stats)) => {
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
                        let _ = shard_manager
                            .abort_peer_recovery_target_blocking(
                                candidate.index_name.clone(),
                                candidate.shard_id,
                                candidate.metadata.uuid.to_string(),
                            )
                            .await;
                        let mut retries = driver
                            .retries
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let delay = retries
                            .get(&key)
                            .map(|retry| (retry.delay * 2).min(RECOVERY_RETRY_MAX))
                            .unwrap_or(RECOVERY_RETRY_MIN);
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
            let PeerRecoveryTargetState::FinalizedAwaitingMembership(pending) = target_state else {
                continue;
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
            tokio::spawn(async move {
                let result = match observation {
                    TargetMembershipObservation::Admitted => {
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
        if !metadata.settings.engine.uses_local_shards() {
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
            candidates.push(RecoveryCandidate {
                index_name: index_name.clone(),
                metadata: metadata.clone(),
                shard_id: *shard_id,
                primary,
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
    if routing.primary == local_node_id
        || routing
            .in_sync_replicas
            .iter()
            .any(|node| node == local_node_id)
    {
        return TargetMembershipObservation::Admitted;
    }
    if !routing.replicas.iter().any(|node| node == local_node_id)
        || routing.primary != pending.primary_node_id
        || routing.primary_term != pending.primary_term
    {
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
    applied_next_seq_no: u64,
    retry_timeout: Duration,
}

async fn run_peer_recovery(
    candidate: &RecoveryCandidate,
    local_node_id: &str,
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: TransportClient,
) -> Result<RecoveryRunOutcome> {
    if !shard_manager.begin_peer_recovery_target(&candidate.index_name, candidate.shard_id) {
        anyhow::bail!("peer recovery target is already active");
    }

    let start = transport_client
        .start_peer_recovery(
            &candidate.primary,
            StartPeerRecoveryRequest {
                index_name: candidate.index_name.clone(),
                index_uuid: candidate.metadata.uuid.to_string(),
                shard_id: candidate.shard_id,
                target_node_id: local_node_id.to_string(),
            },
        )
        .await
        .context("start peer recovery")?;
    if start.session_id.is_empty() || start.primary_term == 0 {
        anyhow::bail!("peer recovery source returned an invalid session identity");
    }
    validate_file_manifest(&start.files)?;

    let shard_dir = shard_manager
        .prepare_peer_recovery_target_blocking(
            candidate.index_name.clone(),
            candidate.shard_id,
            candidate.metadata.uuid.to_string(),
        )
        .await?;
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
        .map_err(|error| anyhow::anyhow!("index directory sync task failed: {error}"))??;

    let engine = shard_manager
        .finalize_peer_recovery_target_blocking(PeerRecoveryTargetInstall {
            index: candidate.index_name.clone(),
            shard_id: candidate.shard_id,
            mappings: candidate.metadata.mappings.clone(),
            settings: candidate.metadata.settings.clone(),
            index_uuid: candidate.metadata.uuid.to_string(),
            shard_dir,
            snapshot_next_seq_no: start.snapshot_next_seq_no,
            expected_files: start.files.iter().map(|file| file.name.clone()).collect(),
        })
        .await?;
    set_checkpoint_from_next(&engine, start.snapshot_next_seq_no);

    let mut next_seq_no = start.snapshot_next_seq_no;
    let mut operations = 0u64;
    loop {
        let response = transport_client
            .fetch_recovery_ops(
                &candidate.primary,
                FetchRecoveryOpsRequest {
                    session_id: start.session_id.clone(),
                    from_seq_no: next_seq_no,
                    max_ops: crate::transport::server::MAX_RECOVERY_OPS as u32,
                },
            )
            .await
            .context("fetch peer recovery operations")?;
        let (new_next, applied) = apply_recovery_operations(
            engine.clone(),
            next_seq_no,
            response.primary_next_seq_no,
            response.complete,
            response.operations,
        )
        .await?;
        operations += applied;
        next_seq_no = new_next;
        if response.complete && next_seq_no == response.primary_next_seq_no {
            break;
        }
    }

    loop {
        let prepared = transport_client
            .prepare_finalize_recovery(
                &candidate.primary,
                PrepareFinalizeRecoveryRequest {
                    session_id: start.session_id.clone(),
                    applied_next_seq_no: next_seq_no,
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
                        from_seq_no: next_seq_no,
                        max_ops: crate::transport::server::MAX_RECOVERY_OPS as u32,
                    },
                )
                .await?;
            let (new_next, applied) = apply_recovery_operations(
                engine.clone(),
                next_seq_no,
                response.primary_next_seq_no,
                response.complete,
                response.operations,
            )
            .await?;
            next_seq_no = new_next;
            operations += applied;
            continue;
        }
        if !prepared.complete {
            anyhow::bail!("peer recovery finalize response was incomplete");
        }
        let (new_next, applied) = apply_recovery_operations(
            engine.clone(),
            next_seq_no,
            prepared.barrier_next_seq_no,
            true,
            prepared.operations,
        )
        .await?;
        next_seq_no = new_next;
        operations += applied;
        if next_seq_no != prepared.barrier_next_seq_no {
            anyhow::bail!("peer recovery target did not reach the barrier head");
        }
        break;
    }

    let engine_for_refresh = engine.clone();
    tokio::task::spawn_blocking(move || engine_for_refresh.refresh())
        .await
        .map_err(|error| anyhow::anyhow!("peer recovery refresh task failed: {error}"))??;

    let pending = PeerRecoveryAwaitingMembership {
        index_uuid: candidate.metadata.uuid.to_string(),
        primary_node_id: candidate.primary.id.clone(),
        primary_term: start.primary_term,
    };
    shard_manager
        .mark_peer_recovery_awaiting_membership_blocking(
            candidate.index_name.clone(),
            candidate.shard_id,
            pending.clone(),
        )
        .await?;
    let observation = complete_with_observed_settlement(CompletionSettlementContext {
        candidate,
        local_node_id,
        cluster_manager,
        transport_client: &transport_client,
        session_id: &start.session_id,
        pending: &pending,
        applied_next_seq_no: next_seq_no,
        retry_timeout: COMPLETE_RETRY_TIMEOUT,
    })
    .await;

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
        .await?;
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
        file.write_all(&response.data).await?;
        offset += response.data.len() as u64;
        if response.eof != (offset == metadata.length) {
            anyhow::bail!("peer recovery file EOF flag does not match the declared length");
        }
    }
    file.sync_all().await?;
    let actual_hash: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    verify_recovery_sha256(&metadata.name, &actual_hash, &metadata.sha256)?;
    Ok(offset)
}

enum TargetOperation {
    Index {
        seq_no: u64,
        doc_id: String,
        payload: serde_json::Value,
    },
    Delete {
        seq_no: u64,
        doc_id: String,
    },
}

async fn apply_recovery_operations(
    engine: Arc<dyn SearchEngine>,
    from_seq_no: u64,
    primary_next_seq_no: u64,
    complete: bool,
    operations: Vec<RecoverReplicaOp>,
) -> Result<(u64, u64)> {
    let mut decoded = Vec::with_capacity(operations.len());
    let mut previous = None;
    for operation in operations {
        if operation.seq_no < from_seq_no || operation.seq_no >= primary_next_seq_no {
            anyhow::bail!("peer recovery operation is outside the requested range");
        }
        if previous.is_some_and(|previous| operation.seq_no <= previous) {
            anyhow::bail!("peer recovery operations are not strictly increasing");
        }
        previous = Some(operation.seq_no);
        match operation.op.as_str() {
            "index" => decoded.push(TargetOperation::Index {
                seq_no: operation.seq_no,
                doc_id: operation.doc_id,
                payload: serde_json::from_slice(&operation.payload_json)
                    .context("decode peer recovery index payload")?,
            }),
            "delete" => decoded.push(TargetOperation::Delete {
                seq_no: operation.seq_no,
                doc_id: operation.doc_id,
            }),
            other => anyhow::bail!("unknown peer recovery operation '{other}'"),
        }
    }
    if !complete && decoded.is_empty() {
        anyhow::bail!("bounded peer recovery response made no progress");
    }
    let applied = decoded.len() as u64;
    let last_next = previous
        .and_then(|seq_no| seq_no.checked_add(1))
        .unwrap_or(from_seq_no);
    let new_next = if complete {
        primary_next_seq_no
    } else {
        last_next
    };
    if new_next < from_seq_no {
        anyhow::bail!("peer recovery cursor regressed");
    }

    let engine_for_apply = engine.clone();
    tokio::task::spawn_blocking(move || {
        for operation in decoded {
            match operation {
                TargetOperation::Index {
                    seq_no,
                    doc_id,
                    payload,
                } => {
                    engine_for_apply.add_document_with_seq(&doc_id, payload, seq_no)?;
                }
                TargetOperation::Delete { seq_no, doc_id } => {
                    engine_for_apply.delete_document_with_seq(&doc_id, seq_no)?;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|error| anyhow::anyhow!("peer recovery apply task failed: {error}"))??;
    set_checkpoint_from_next(&engine, new_next);
    Ok((new_next, applied))
}

fn set_checkpoint_from_next(engine: &Arc<dyn SearchEngine>, next_seq_no: u64) {
    if let Some(last_seq_no) = next_seq_no.checked_sub(1) {
        engine.update_local_checkpoint(last_seq_no);
    }
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
                    applied_next_seq_no: context.applied_next_seq_no,
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
        create_transport_service_for_test, create_transport_service_with_raft,
    };
    use std::collections::HashMap;

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
    async fn recovery_operation_validation_allows_gaps_but_rejects_reordering() {
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
                RecoverReplicaOp {
                    seq_no: 5,
                    op: "index".into(),
                    doc_id: "a".into(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                },
                RecoverReplicaOp {
                    seq_no: 8,
                    op: "delete".into(),
                    doc_id: "b".into(),
                    payload_json: Vec::new(),
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(next, 9);
        assert_eq!(applied, 2);

        let error = apply_recovery_operations(
            engine,
            9,
            12,
            true,
            vec![
                RecoverReplicaOp {
                    seq_no: 11,
                    op: "delete".into(),
                    doc_id: "a".into(),
                    payload_json: Vec::new(),
                },
                RecoverReplicaOp {
                    seq_no: 10,
                    op: "delete".into(),
                    doc_id: "b".into(),
                    payload_json: Vec::new(),
                },
            ],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("strictly increasing"));
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
        let session = client
            .start_peer_recovery(tonic::Request::new(StartPeerRecoveryRequest {
                index_name: "dynamic-docs".into(),
                index_uuid: "dynamic-docs-uuid".into(),
                shard_id: 0,
                target_node_id: "replica-node".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(session.error.is_empty(), "{}", session.error);

        for (doc_id, value) in [("first", 1), ("second", 2)] {
            let response = client
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "dynamic-docs".into(),
                    shard_id: 0,
                    doc_id: doc_id.into(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"new_field": value}))
                        .unwrap(),
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
                from_seq_no: session.snapshot_next_seq_no,
                max_ops: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(stale_fetch.code(), tonic::Code::NotFound);
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
        let stale_target_state = source_state_handle.read().unwrap().clone();
        assert_eq!(
            raft.client_write(ClusterCommand::MarkReplicaInSync {
                index_name: "pending".into(),
                index_uuid: "pending-uuid".into(),
                shard_id: 0,
                replica: "replica-node".into(),
                primary: "primary-node".into(),
                primary_term: 1,
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
            .open_shard_with_settings(
                "pending",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "pending-uuid",
            )
            .unwrap();
        target_engine
            .add_document_with_seq("base", serde_json::json!({"value": 0}), 0)
            .unwrap();
        target_engine.refresh().unwrap();
        assert!(target_shards.begin_peer_recovery_target("pending", 0));
        let pending = PeerRecoveryAwaitingMembership {
            index_uuid: "pending-uuid".into(),
            primary_node_id: "primary-node".into(),
            primary_term: 1,
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
            applied_next_seq_no: 1,
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
                index_name: "pending".into(),
                shard_id: 0,
                doc_id: "live".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                op: "index".into(),
                seq_no: 1,
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
        engine
            .add_document_with_seq("doc", serde_json::json!({"value": 1}), 0)
            .unwrap();
        engine.refresh().unwrap();
        assert!(first_manager.begin_peer_recovery_target("promoted", 0));
        first_manager
            .mark_peer_recovery_awaiting_membership_blocking(
                "promoted".into(),
                0,
                PeerRecoveryAwaitingMembership {
                    index_uuid: "promoted-uuid".into(),
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
        state.indices.insert(
            "promoted".into(),
            IndexMetadata {
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
            },
        );
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
                    primary_node_id: "primary-node".into(),
                    primary_term: 7,
                },
            )
            .await
            .unwrap();

        let cluster_manager = Arc::new(ClusterManager::new("rejected".into()));
        let mut state = ClusterState::new("rejected".into());
        state.indices.insert(
            "rejected".into(),
            IndexMetadata {
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
            },
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn file_recovery_copies_flushed_state_catches_up_and_admits_target() {
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
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: format!("doc-{doc}"),
                    payload_json: serde_json::to_vec(&serde_json::json!({
                        "value": doc,
                        "padding": large_value
                    }))
                    .unwrap(),
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
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: format!("doc-{doc}"),
                    payload_json: serde_json::to_vec(&serde_json::json!({
                        "value": doc
                    }))
                    .unwrap(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(response.success, "{}", response.error);
        }
        let delete_seed = source_client
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "delete-me".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": "delete"})).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(delete_seed.success, "{}", delete_seed.error);

        let current_metadata = state_handle.read().unwrap().indices["docs"].clone();
        let candidate = RecoveryCandidate {
            index_name: "docs".into(),
            metadata: current_metadata,
            shard_id: 0,
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
            .delete_doc(tonic::Request::new(ShardDeleteRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "delete-me".into(),
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
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "docs".into(),
                    shard_id: 0,
                    doc_id: doc_id.clone(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"value": doc_id}))
                        .unwrap(),
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
            .index_doc(tonic::Request::new(ShardDocRequest {
                index_name: "docs".into(),
                shard_id: 0,
                doc_id: "post-finalize".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": "post-finalize"}))
                    .unwrap(),
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
            .chain(std::iter::once("post-finalize"))
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
                from_seq_no: 0,
                max_ops: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(stale_session.code(), tonic::Code::NotFound);

        source_server.abort();
        target_server.abort();
    }
}
