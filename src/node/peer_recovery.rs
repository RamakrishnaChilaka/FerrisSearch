use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{ClusterState, IndexMetadata, NodeInfo};
use crate::engine::SearchEngine;
use crate::shard::{PeerRecoveryTargetInstall, ShardKey, ShardManager};
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
        if !self.enabled {
            return;
        }

        let now = Instant::now();
        for candidate in recovery_candidates(state, local_node_id) {
            let key = ShardKey::new(&candidate.index_name, candidate.shard_id);
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
                    Ok(stats) => {
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
                    Err(error) => {
                        let _ = shard_manager
                            .abort_peer_recovery_target_blocking(
                                candidate.index_name.clone(),
                                candidate.shard_id,
                                candidate.metadata.uuid.to_string(),
                            )
                            .await;
                        shard_manager
                            .end_peer_recovery_target(&candidate.index_name, candidate.shard_id);
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

struct RecoveryStats {
    session_id: String,
    bytes: u64,
    operations: u64,
}

async fn run_peer_recovery(
    candidate: &RecoveryCandidate,
    local_node_id: &str,
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: TransportClient,
) -> Result<RecoveryStats> {
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

    complete_with_observed_settlement(
        candidate,
        local_node_id,
        cluster_manager,
        &transport_client,
        &start.session_id,
        start.primary_term,
        next_seq_no,
    )
    .await?;

    shard_manager.end_peer_recovery_target(&candidate.index_name, candidate.shard_id);
    Ok(RecoveryStats {
        session_id: start.session_id,
        bytes: transferred_bytes,
        operations,
    })
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
    candidate: &RecoveryCandidate,
    local_node_id: &str,
    cluster_manager: Arc<ClusterManager>,
    transport_client: &TransportClient,
    session_id: &str,
    primary_term: u64,
    applied_next_seq_no: u64,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + COMPLETE_RETRY_TIMEOUT;
    loop {
        match transport_client
            .complete_finalize_recovery(
                &candidate.primary,
                CompleteFinalizeRecoveryRequest {
                    session_id: session_id.to_string(),
                    applied_next_seq_no,
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => {
                let state = cluster_manager.get_state();
                let Some(metadata) = state.indices.get(&candidate.index_name) else {
                    return Err(error);
                };
                let Some(routing) = metadata.shard_routing.get(&candidate.shard_id) else {
                    return Err(error);
                };
                if routing
                    .in_sync_replicas
                    .iter()
                    .any(|node| node == local_node_id)
                {
                    return Ok(());
                }
                if metadata.uuid != candidate.metadata.uuid
                    || routing.primary != candidate.primary.id
                    || routing.primary_term > primary_term
                {
                    return Err(error);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(error).context(
                        "peer recovery membership remained unsettled after completion retries",
                    );
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
        IndexMetadata, IndexSettings, IndexUuid, NodeRole, ShardRoutingEntry,
    };
    use crate::consensus;
    use crate::consensus::types::{ClusterCommand, ClusterResponse};
    use crate::tasks::TaskManager;
    use crate::transport::proto::internal_transport_client::InternalTransportClient;
    use crate::transport::proto::{FetchRecoveryOpsRequest, ShardDeleteRequest, ShardDocRequest};
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
        let stats = recovery.await.unwrap().unwrap();
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
