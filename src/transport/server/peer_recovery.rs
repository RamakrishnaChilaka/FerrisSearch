use super::TransportService;
use crate::consensus::types::ClusterCommand;
use crate::engine::{PeerRecoveryFileMetadata, PeerRecoveryOpsBatch, SearchEngine};
use crate::shard::SourceRecoverySessionCleanup;
use crate::transport::proto::{
    CompleteFinalizeRecoveryRequest, CompleteFinalizeRecoveryResponse,
    FetchRecoveryFileChunkRequest, FetchRecoveryFileChunkResponse, FetchRecoveryOpsRequest,
    FetchRecoveryOpsResponse, MarkReplicaInSyncRequest, PrepareFinalizeRecoveryRequest,
    PrepareFinalizeRecoveryResponse, RecoverReplicaOp, RecoveryFileMetadata,
    StartPeerRecoveryRequest, StartPeerRecoveryResponse,
};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, oneshot};
use tonic::Status;

pub(crate) const MAX_RECOVERY_FILE_CHUNK_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_RECOVERY_OPS: usize = 1024;
pub(super) const MAX_RECOVERY_OP_BYTES: usize = 32 * 1024 * 1024;
const MAX_SOURCE_RECOVERY_SESSIONS: usize = 8;
const RECOVERY_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const RECOVERY_SESSION_REAP_INTERVAL: Duration = Duration::from_secs(30);
const FINALIZE_BARRIER_TIMEOUT: Duration = Duration::from_secs(20);
type ShardIdentity = (String, u32);
type ShardWriteBarrier = Arc<RwLock<()>>;
type ShardWriteBarrierMap = HashMap<ShardIdentity, ShardWriteBarrier>;

#[derive(Default)]
struct SourceRegistry {
    sessions: HashMap<String, Arc<Mutex<SourceSession>>>,
    active_shards: HashMap<ShardIdentity, Option<String>>,
}

pub(crate) struct PeerRecoveryTransportState {
    registry: Mutex<SourceRegistry>,
    write_barriers: Mutex<ShardWriteBarrierMap>,
}

impl PeerRecoveryTransportState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: Mutex::new(SourceRegistry::default()),
            write_barriers: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn start_reaper(state: &Arc<Self>) {
        let weak = Arc::downgrade(state);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RECOVERY_SESSION_REAP_INTERVAL).await;
                let Some(state) = Weak::upgrade(&weak) else {
                    break;
                };
                state.reap_expired_sessions().await;
            }
        });
    }

    async fn barrier(&self, key: (String, u32)) -> Arc<RwLock<()>> {
        let mut barriers = self.write_barriers.lock().await;
        barriers
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    async fn reap_expired_sessions(&self) {
        let sessions = {
            let registry = self.registry.lock().await;
            registry
                .sessions
                .iter()
                .map(|(id, session)| (id.clone(), session.clone()))
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        let mut expired = Vec::new();
        for (session_id, session) in sessions {
            let session = session.lock().await;
            let idle = now.duration_since(session.last_activity) >= RECOVERY_SESSION_IDLE_TIMEOUT;
            let finalize_expired = session
                .finalize_deadline
                .is_some_and(|deadline| now >= deadline)
                && !session.settlement_running;
            if idle || finalize_expired {
                expired.push(session_id);
            }
        }

        for session_id in expired {
            if let Some(session) = self.remove_session(&session_id).await {
                cleanup_session(session).await;
            }
        }
    }

    async fn remove_session(&self, session_id: &str) -> Option<Arc<Mutex<SourceSession>>> {
        let session = self.registry.lock().await.sessions.remove(session_id);
        if let Some(session) = session.as_ref() {
            let key = {
                let session = session.lock().await;
                (session.index_uuid.clone(), session.shard_id)
            };
            self.registry.lock().await.active_shards.remove(&key);
        }
        session
    }

    async fn abort_shard_session(&self, index_uuid: &str, shard_id: u32) -> anyhow::Result<bool> {
        let key = (index_uuid.to_string(), shard_id);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let active_session = self.registry.lock().await.active_shards.get(&key).cloned();
            let Some(active_session) = active_session else {
                return Ok(false);
            };
            let Some(session_id) = active_session else {
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "timed out waiting for peer recovery snapshot setup before engine replacement"
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            let session = {
                self.registry
                    .lock()
                    .await
                    .sessions
                    .get(&session_id)
                    .cloned()
            };
            let Some(session) = session else {
                self.registry.lock().await.active_shards.remove(&key);
                continue;
            };
            {
                let session = session.lock().await;
                if session.finalize_preparing
                    || session.barrier_guard.is_some()
                    || session.settlement_running
                {
                    anyhow::bail!(
                        "cannot replace a primary engine while peer recovery admission is active"
                    );
                }
            }
            if let Some(session) = self.remove_session(&session_id).await {
                cleanup_session(session).await;
            }
            return Ok(true);
        }
    }

    async fn abort_index_sessions(&self, index_uuid: &str) -> anyhow::Result<usize> {
        let keys = self
            .registry
            .lock()
            .await
            .active_shards
            .keys()
            .filter(|(uuid, _)| uuid == index_uuid)
            .cloned()
            .collect::<Vec<_>>();
        let mut aborted = 0usize;
        for (_, shard_id) in keys {
            aborted += usize::from(self.abort_shard_session(index_uuid, shard_id).await?);
        }
        Ok(aborted)
    }
}

impl SourceRecoverySessionCleanup for PeerRecoveryTransportState {
    fn abort_shard<'a>(
        &'a self,
        index_uuid: &'a str,
        shard_id: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>
    {
        Box::pin(self.abort_shard_session(index_uuid, shard_id))
    }

    fn abort_index<'a>(
        &'a self,
        index_uuid: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>>
    {
        Box::pin(self.abort_index_sessions(index_uuid))
    }
}

pub(super) fn new_peer_recovery_transport_state() -> Arc<PeerRecoveryTransportState> {
    let state = PeerRecoveryTransportState::new();
    if tokio::runtime::Handle::try_current().is_ok() {
        PeerRecoveryTransportState::start_reaper(&state);
    }
    state
}

struct SourceSession {
    index_name: String,
    index_uuid: String,
    shard_id: u32,
    target_node_id: String,
    primary_node_id: String,
    primary_term: u64,
    snapshot_next_seq_no: u64,
    snapshot_dir: PathBuf,
    files: HashMap<String, PeerRecoveryFileMetadata>,
    engine: Arc<dyn SearchEngine>,
    retention_pin_id: Option<u64>,
    last_activity: Instant,
    barrier_next_seq_no: Option<u64>,
    barrier_guard: Option<OwnedRwLockWriteGuard<()>>,
    finalize_deadline: Option<Instant>,
    finalize_preparing: bool,
    settlement_running: bool,
}

struct SessionCleanup {
    engine: Arc<dyn SearchEngine>,
    retention_pin_id: Option<u64>,
    snapshot_dir: PathBuf,
}

async fn cleanup_session(session: Arc<Mutex<SourceSession>>) {
    let cleanup = {
        let mut session = session.lock().await;
        session.barrier_guard.take();
        SessionCleanup {
            engine: session.engine.clone(),
            retention_pin_id: session.retention_pin_id.take(),
            snapshot_dir: session.snapshot_dir.clone(),
        }
    };
    let result = tokio::task::spawn_blocking(move || {
        if let Some(pin_id) = cleanup.retention_pin_id {
            cleanup.engine.release_peer_recovery_pin(pin_id)?;
        }
        if cleanup.snapshot_dir.exists() {
            std::fs::remove_dir_all(&cleanup.snapshot_dir)?;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!("Failed to clean peer recovery source session: {error}");
        }
        Err(error) => {
            tracing::warn!("Peer recovery source cleanup task failed: {error}");
        }
    }
}

fn recovery_op(entry: crate::wal::TranslogEntry) -> Result<RecoverReplicaOp, Status> {
    let doc_id = entry
        .payload
        .get("_doc_id")
        .or_else(|| entry.payload.get("_id"))
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            Status::internal(format!(
                "translog operation {} has no document id",
                entry.seq_no
            ))
        })?
        .to_string();
    let payload = match entry.op {
        crate::wal::WalOperation::Index => {
            entry.payload.get("_source").cloned().ok_or_else(|| {
                Status::internal(format!(
                    "index translog operation {} has no _source",
                    entry.seq_no
                ))
            })?
        }
        crate::wal::WalOperation::Delete => serde_json::json!({}),
    };
    Ok(RecoverReplicaOp {
        seq_no: entry.seq_no,
        op: entry.op.as_str().to_string(),
        doc_id,
        payload_json: serde_json::to_vec(&payload)
            .map_err(|error| Status::internal(format!("serialize recovery operation: {error}")))?,
    })
}

fn recovery_ops(batch: PeerRecoveryOpsBatch) -> Result<Vec<RecoverReplicaOp>, Status> {
    batch.operations.into_iter().map(recovery_op).collect()
}

enum MembershipObservation {
    InSync,
    Pending,
    Impossible,
}

impl TransportService {
    pub(super) async fn peer_recovery_write_guard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<OwnedRwLockReadGuard<()>, String> {
        let state = self.cluster_manager.get_state();
        let metadata = state.indices.get(index_name).ok_or_else(|| {
            format!("index [{index_name}] is absent while acquiring the shard write barrier")
        })?;
        let barrier = self
            .peer_recovery_state
            .barrier((metadata.uuid.to_string(), shard_id))
            .await;
        Ok(barrier.read_owned().await)
    }

    async fn source_session(&self, session_id: &str) -> Result<Arc<Mutex<SourceSession>>, Status> {
        self.peer_recovery_state
            .registry
            .lock()
            .await
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| Status::not_found("peer recovery session is unknown or expired"))
    }

    async fn abort_source_session(&self, session_id: &str) {
        if let Some(session) = self.peer_recovery_state.remove_session(session_id).await {
            cleanup_session(session).await;
        }
    }

    fn source_authority_valid(&self, session: &SourceSession) -> bool {
        let state = self.cluster_manager.get_state();
        let Some(metadata) = state.indices.get(&session.index_name) else {
            return false;
        };
        if metadata.uuid.as_str() != session.index_uuid {
            return false;
        }
        let Some(routing) = metadata.shard_routing.get(&session.shard_id) else {
            return false;
        };
        routing.primary == session.primary_node_id
            && routing.primary_term == session.primary_term
            && routing.replicas.contains(&session.target_node_id)
    }

    fn observe_membership(&self, session: &SourceSession) -> MembershipObservation {
        let state = self.cluster_manager.get_state();
        let Some(metadata) = state.indices.get(&session.index_name) else {
            return MembershipObservation::Impossible;
        };
        if metadata.uuid.as_str() != session.index_uuid {
            return MembershipObservation::Impossible;
        }
        let Some(routing) = metadata.shard_routing.get(&session.shard_id) else {
            return MembershipObservation::Impossible;
        };
        if routing.in_sync_replicas.contains(&session.target_node_id) {
            return MembershipObservation::InSync;
        }
        if routing.primary != session.primary_node_id || routing.primary_term > session.primary_term
        {
            MembershipObservation::Impossible
        } else {
            MembershipObservation::Pending
        }
    }

    async fn submit_mark_replica_in_sync(&self, session: &SourceSession) -> Result<(), String> {
        let Some(raft) = self.raft.as_ref() else {
            return Err("Raft is not initialized".to_string());
        };
        if raft.is_leader() {
            let response = raft
                .client_write(ClusterCommand::MarkReplicaInSync {
                    index_name: session.index_name.clone(),
                    index_uuid: session.index_uuid.clone(),
                    shard_id: session.shard_id,
                    replica: session.target_node_id.clone(),
                    primary: session.primary_node_id.clone(),
                    primary_term: session.primary_term,
                })
                .await
                .map_err(|error| error.to_string())?;
            response.data.into_result()
        } else {
            let state = self.cluster_manager.get_state();
            let master_id = state
                .master_node
                .as_ref()
                .ok_or_else(|| "no Raft leader is known".to_string())?;
            let master = state
                .nodes
                .get(master_id)
                .ok_or_else(|| format!("Raft leader '{master_id}' is absent"))?;
            self.transport_client
                .forward_mark_replica_in_sync(
                    master,
                    MarkReplicaInSyncRequest {
                        index_name: session.index_name.clone(),
                        index_uuid: session.index_uuid.clone(),
                        shard_id: session.shard_id,
                        replica_node_id: session.target_node_id.clone(),
                        primary_node_id: session.primary_node_id.clone(),
                        primary_term: session.primary_term,
                    },
                )
                .await
                .map_err(|error| error.to_string())
        }
    }

    async fn submit_settlement_term_bump(&self, session: &SourceSession) -> Result<(), String> {
        let Some(raft) = self.raft.as_ref() else {
            return Err("Raft is not initialized".to_string());
        };
        if raft.is_leader() {
            let response = raft
                .client_write(ClusterCommand::ActivatePrimary {
                    index_name: session.index_name.clone(),
                    index_uuid: session.index_uuid.clone(),
                    shard_id: session.shard_id,
                    primary: session.primary_node_id.clone(),
                    expected_term: session.primary_term,
                })
                .await
                .map_err(|error| error.to_string())?;
            response.data.into_result()
        } else {
            let state = self.cluster_manager.get_state();
            let master_id = state
                .master_node
                .as_ref()
                .ok_or_else(|| "no Raft leader is known".to_string())?;
            let master = state
                .nodes
                .get(master_id)
                .ok_or_else(|| format!("Raft leader '{master_id}' is absent"))?;
            self.transport_client
                .forward_activate_primary(
                    master,
                    &session.index_name,
                    &session.index_uuid,
                    session.shard_id,
                    &session.primary_node_id,
                    session.primary_term,
                )
                .await
                .map_err(|error| error.to_string())
        }
    }

    pub(super) async fn start_peer_recovery_inner(
        &self,
        request: StartPeerRecoveryRequest,
    ) -> Result<StartPeerRecoveryResponse, Status> {
        if request.index_uuid.is_empty() || request.target_node_id.is_empty() {
            return Err(Status::invalid_argument(
                "peer recovery requires index UUID and target node",
            ));
        }
        self.ensure_primary_activated(&request.index_name, request.shard_id)
            .await
            .map_err(Status::failed_precondition)?;

        let state = self.cluster_manager.get_state();
        let metadata = state
            .indices
            .get(&request.index_name)
            .ok_or_else(|| Status::not_found("peer recovery index does not exist"))?;
        if metadata.uuid.as_str() != request.index_uuid {
            return Err(Status::failed_precondition(
                "peer recovery index UUID no longer matches",
            ));
        }
        let routing = metadata
            .shard_routing
            .get(&request.shard_id)
            .ok_or_else(|| Status::not_found("peer recovery shard does not exist"))?;
        if routing.primary != self.local_node_id {
            return Err(Status::failed_precondition(
                "this node is not the peer recovery source primary",
            ));
        }
        if !routing.replicas.contains(&request.target_node_id) {
            return Err(Status::failed_precondition(
                "peer recovery target is not an assigned replica",
            ));
        }
        if routing.in_sync_replicas.contains(&request.target_node_id) {
            return Err(Status::failed_precondition(
                "peer recovery target is already in sync",
            ));
        }
        let primary_term = routing.primary_term;
        drop(state);

        let key = (request.index_uuid.clone(), request.shard_id);
        let existing_session_id = {
            self.peer_recovery_state
                .registry
                .lock()
                .await
                .active_shards
                .get(&key)
                .cloned()
                .flatten()
        };
        if let Some(existing_session_id) = existing_session_id {
            let existing = self.source_session(&existing_session_id).await?;
            let replaceable = {
                let existing = existing.lock().await;
                !existing.finalize_preparing
                    && existing.barrier_guard.is_none()
                    && !existing.settlement_running
            };
            if !replaceable {
                return Err(Status::already_exists(
                    "a peer recovery admission is already active for this shard",
                ));
            }
            if let Some(existing) = self
                .peer_recovery_state
                .remove_session(&existing_session_id)
                .await
            {
                cleanup_session(existing).await;
            }
        }
        {
            let mut registry = self.peer_recovery_state.registry.lock().await;
            if registry.active_shards.len() >= MAX_SOURCE_RECOVERY_SESSIONS {
                return Err(Status::resource_exhausted(
                    "peer recovery source session limit reached",
                ));
            }
            if registry.active_shards.contains_key(&key) {
                return Err(Status::already_exists(
                    "a peer recovery source session already exists for this shard",
                ));
            }
            registry.active_shards.insert(key.clone(), None);
        }

        let session_id = uuid::Uuid::new_v4().to_string();
        let engine = match self
            .get_or_open_shard(&request.index_name, request.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(error) => {
                self.peer_recovery_state
                    .registry
                    .lock()
                    .await
                    .active_shards
                    .remove(&key);
                return Err(error);
            }
        };
        let Some(shard_dir) = self
            .shard_manager
            .shard_data_dir(&request.index_name, request.shard_id)
        else {
            self.peer_recovery_state
                .registry
                .lock()
                .await
                .active_shards
                .remove(&key);
            return Err(Status::internal(
                "peer recovery source shard path is unknown",
            ));
        };
        let snapshot_dir = shard_dir.join("peer-recovery").join(&session_id);
        let snapshot_engine = engine.clone();
        let snapshot_dir_for_task = snapshot_dir.clone();
        let snapshot = match tokio::task::spawn_blocking(move || {
            snapshot_engine.create_peer_recovery_snapshot(&snapshot_dir_for_task)
        })
        .await
        {
            Ok(result) => {
                result.map_err(|error| Status::internal(format!("peer snapshot failed: {error}")))
            }
            Err(error) => Err(Status::internal(format!(
                "peer snapshot task failed: {error}"
            ))),
        };
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.peer_recovery_state
                    .registry
                    .lock()
                    .await
                    .active_shards
                    .remove(&key);
                return Err(error);
            }
        };

        let files = snapshot
            .files
            .iter()
            .cloned()
            .map(|file| (file.name.clone(), file))
            .collect::<HashMap<_, _>>();
        let response_files = snapshot
            .files
            .iter()
            .map(|file| RecoveryFileMetadata {
                name: file.name.clone(),
                length: file.length,
                sha256: file.sha256.clone(),
            })
            .collect();
        let session = Arc::new(Mutex::new(SourceSession {
            index_name: request.index_name.clone(),
            index_uuid: request.index_uuid,
            shard_id: request.shard_id,
            target_node_id: request.target_node_id.clone(),
            primary_node_id: self.local_node_id.clone(),
            primary_term,
            snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
            snapshot_dir,
            files,
            engine,
            retention_pin_id: Some(snapshot.retention_pin_id),
            last_activity: Instant::now(),
            barrier_next_seq_no: None,
            barrier_guard: None,
            finalize_deadline: None,
            finalize_preparing: false,
            settlement_running: false,
        }));
        {
            let mut registry = self.peer_recovery_state.registry.lock().await;
            registry.sessions.insert(session_id.clone(), session);
            registry.active_shards.insert(key, Some(session_id.clone()));
        }

        tracing::info!(
            session_id,
            index = request.index_name,
            shard_id = request.shard_id,
            target = request.target_node_id,
            snapshot_next_seq_no = snapshot.snapshot_next_seq_no,
            "Started peer recovery source session"
        );
        Ok(StartPeerRecoveryResponse {
            session_id,
            primary_term,
            snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
            files: response_files,
            error: String::new(),
        })
    }

    pub(super) async fn fetch_recovery_file_chunk_inner(
        &self,
        request: FetchRecoveryFileChunkRequest,
    ) -> Result<FetchRecoveryFileChunkResponse, Status> {
        let max_len = request.max_len as usize;
        if max_len == 0 || max_len > MAX_RECOVERY_FILE_CHUNK_BYTES {
            return Err(Status::invalid_argument(
                "peer recovery file chunks must be between 1 byte and 1 MiB",
            ));
        }
        let session = self.source_session(&request.session_id).await?;
        let (path, file_length) = {
            let mut session = session.lock().await;
            if !self.source_authority_valid(&session) {
                drop(session);
                self.abort_source_session(&request.session_id).await;
                return Err(Status::failed_precondition(
                    "peer recovery source authority is stale",
                ));
            }
            let file = session
                .files
                .get(&request.file_name)
                .cloned()
                .ok_or_else(|| Status::invalid_argument("unknown peer recovery file"))?;
            if request.offset > file.length {
                return Err(Status::out_of_range(
                    "peer recovery file offset exceeds file length",
                ));
            }
            session.last_activity = Instant::now();
            (session.snapshot_dir.join(&file.name), file.length)
        };

        let offset = request.offset;
        let data = tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            let remaining = file_length.saturating_sub(offset) as usize;
            let mut data = vec![0u8; max_len.min(remaining)];
            file.read_exact(&mut data)?;
            Ok::<Vec<u8>, anyhow::Error>(data)
        })
        .await
        .map_err(|error| Status::internal(format!("file chunk task failed: {error}")))?
        .map_err(|error| Status::internal(format!("read recovery file chunk: {error}")))?;
        Ok(FetchRecoveryFileChunkResponse {
            eof: offset + data.len() as u64 == file_length,
            data,
            error: String::new(),
        })
    }

    pub(super) async fn fetch_recovery_ops_inner(
        &self,
        request: FetchRecoveryOpsRequest,
    ) -> Result<FetchRecoveryOpsResponse, Status> {
        let max_ops = request.max_ops as usize;
        if max_ops == 0 || max_ops > MAX_RECOVERY_OPS {
            return Err(Status::invalid_argument(
                "peer recovery operation batch exceeds the configured limit",
            ));
        }
        let session = self.source_session(&request.session_id).await?;
        let engine = {
            let mut session = session.lock().await;
            if !self.source_authority_valid(&session) {
                drop(session);
                self.abort_source_session(&request.session_id).await;
                return Err(Status::failed_precondition(
                    "peer recovery source authority is stale",
                ));
            }
            if request.from_seq_no < session.snapshot_next_seq_no {
                return Err(Status::invalid_argument(
                    "peer recovery operation cursor precedes the snapshot boundary",
                ));
            }
            session.last_activity = Instant::now();
            session.engine.clone()
        };

        let from_seq_no = request.from_seq_no;
        let batch = tokio::task::spawn_blocking(move || {
            engine.peer_recovery_ops(from_seq_no, max_ops, MAX_RECOVERY_OP_BYTES)
        })
        .await
        .map_err(|error| Status::internal(format!("recovery ops task failed: {error}")))?
        .map_err(|error| Status::internal(format!("read recovery operations: {error}")))?;
        let primary_next_seq_no = batch.primary_next_seq_no;
        let complete = batch.complete;
        Ok(FetchRecoveryOpsResponse {
            operations: recovery_ops(batch)?,
            primary_next_seq_no,
            complete,
            error: String::new(),
        })
    }

    pub(super) async fn prepare_finalize_recovery_inner(
        &self,
        request: PrepareFinalizeRecoveryRequest,
    ) -> Result<PrepareFinalizeRecoveryResponse, Status> {
        let session = self.source_session(&request.session_id).await?;
        let (key, engine) = {
            let mut session = session.lock().await;
            if !self.source_authority_valid(&session) {
                drop(session);
                self.abort_source_session(&request.session_id).await;
                return Err(Status::failed_precondition(
                    "peer recovery source authority is stale",
                ));
            }
            if session.settlement_running {
                return Err(Status::failed_precondition(
                    "peer recovery settlement is already running",
                ));
            }
            if session.finalize_preparing || session.barrier_guard.is_some() {
                return Err(Status::already_exists(
                    "peer recovery finalization is already being prepared",
                ));
            }
            session.finalize_preparing = true;
            session.last_activity = Instant::now();
            (
                (session.index_uuid.clone(), session.shard_id),
                session.engine.clone(),
            )
        };

        let barrier = self.peer_recovery_state.barrier(key).await;
        let guard =
            match tokio::time::timeout(FINALIZE_BARRIER_TIMEOUT, barrier.write_owned()).await {
                Ok(guard) => guard,
                Err(_) => {
                    session.lock().await.finalize_preparing = false;
                    return Err(Status::deadline_exceeded(
                        "timed out acquiring peer recovery barrier",
                    ));
                }
            };

        {
            let session_guard = session.lock().await;
            if !self.source_authority_valid(&session_guard) {
                drop(session_guard);
                self.abort_source_session(&request.session_id).await;
                return Err(Status::failed_precondition(
                    "peer recovery source authority changed at the finalize barrier",
                ));
            }
        }

        let applied_next_seq_no = request.applied_next_seq_no;
        let batch = tokio::task::spawn_blocking(move || {
            engine.peer_recovery_ops(applied_next_seq_no, MAX_RECOVERY_OPS, MAX_RECOVERY_OP_BYTES)
        })
        .await;
        let batch = match batch {
            Ok(Ok(batch)) => batch,
            Ok(Err(error)) => {
                session.lock().await.finalize_preparing = false;
                return Err(Status::internal(format!(
                    "read finalize operations: {error}"
                )));
            }
            Err(error) => {
                session.lock().await.finalize_preparing = false;
                return Err(Status::internal(format!(
                    "finalize ops task failed: {error}"
                )));
            }
        };
        let barrier_next_seq_no = batch.primary_next_seq_no;
        if !batch.complete {
            drop(guard);
            session.lock().await.finalize_preparing = false;
            return Ok(PrepareFinalizeRecoveryResponse {
                operations: Vec::new(),
                barrier_next_seq_no,
                retry_catch_up: true,
                complete: false,
                error: String::new(),
            });
        }
        let operations = match recovery_ops(batch) {
            Ok(operations) => operations,
            Err(error) => {
                session.lock().await.finalize_preparing = false;
                return Err(error);
            }
        };
        {
            let mut session = session.lock().await;
            session.finalize_preparing = false;
            session.barrier_next_seq_no = Some(barrier_next_seq_no);
            session.barrier_guard = Some(guard);
            session.finalize_deadline = Some(Instant::now() + FINALIZE_BARRIER_TIMEOUT);
            session.last_activity = Instant::now();
        }
        Ok(PrepareFinalizeRecoveryResponse {
            operations,
            barrier_next_seq_no,
            retry_catch_up: false,
            complete: true,
            error: String::new(),
        })
    }

    pub(super) async fn complete_finalize_recovery_inner(
        &self,
        request: CompleteFinalizeRecoveryRequest,
    ) -> Result<CompleteFinalizeRecoveryResponse, Status> {
        let session = self.source_session(&request.session_id).await?;
        {
            let mut session = session.lock().await;
            if session.barrier_next_seq_no != Some(request.applied_next_seq_no)
                || session.barrier_guard.is_none()
            {
                return Err(Status::failed_precondition(
                    "peer recovery target has not applied the finalize barrier head",
                ));
            }
            if session.settlement_running {
                return Err(Status::already_exists(
                    "peer recovery settlement is already running",
                ));
            }
            if session
                .finalize_deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(Status::deadline_exceeded(
                    "peer recovery completion missed the finalize deadline",
                ));
            }
            session.settlement_running = true;
            session.last_activity = Instant::now();
        }

        let service = self.clone();
        let session_id = request.session_id;
        let session_for_task = session.clone();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = service
                .settle_peer_recovery(session_id, session_for_task)
                .await;
            let _ = result_tx.send(result);
        });
        result_rx
            .await
            .map_err(|_| Status::internal("peer recovery settlement task stopped"))?
    }

    async fn settle_peer_recovery(
        &self,
        session_id: String,
        session: Arc<Mutex<SourceSession>>,
    ) -> Result<CompleteFinalizeRecoveryResponse, Status> {
        let deadline = {
            let session = session.lock().await;
            session
                .finalize_deadline
                .unwrap_or_else(|| Instant::now() + FINALIZE_BARRIER_TIMEOUT)
        };
        let mut deadline_elapsed = false;

        loop {
            let observation = {
                let session = session.lock().await;
                self.observe_membership(&session)
            };
            match observation {
                MembershipObservation::InSync => {
                    let removed = self.peer_recovery_state.remove_session(&session_id).await;
                    if let Some(removed) = removed {
                        cleanup_session(removed).await;
                    }
                    return Ok(CompleteFinalizeRecoveryResponse {
                        success: true,
                        error: String::new(),
                    });
                }
                MembershipObservation::Impossible => {
                    let removed = self.peer_recovery_state.remove_session(&session_id).await;
                    if let Some(removed) = removed {
                        cleanup_session(removed).await;
                    }
                    return Ok(CompleteFinalizeRecoveryResponse {
                        success: false,
                        error: "peer recovery membership became impossible before admission"
                            .to_string(),
                    });
                }
                MembershipObservation::Pending => {}
            }

            if !deadline_elapsed {
                let mark_result = {
                    let session = session.lock().await;
                    self.submit_mark_replica_in_sync(&session).await
                };
                if let Err(error) = mark_result {
                    tracing::warn!(
                        session_id,
                        error,
                        "Peer recovery admission command did not settle yet"
                    );
                }
                if Instant::now() >= deadline {
                    deadline_elapsed = true;
                }
            } else {
                let bump_result = {
                    let session = session.lock().await;
                    self.submit_settlement_term_bump(&session).await
                };
                if let Err(error) = bump_result {
                    tracing::error!(
                        session_id,
                        error,
                        "Peer recovery settlement term bump has not committed; writes remain blocked"
                    );
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::manager::ClusterManager;
    use crate::cluster::state::{
        ClusterState, IndexMetadata, IndexSettings, IndexUuid, ShardRoutingEntry,
    };
    use crate::engine::CompositeEngine;
    use crate::shard::ShardManager;
    use crate::tasks::TaskManager;
    use crate::transport::TransportClient;
    use crate::transport::proto::ReplicateDocRequest;
    use crate::transport::proto::internal_transport_server::InternalTransport;
    use crate::worker::WorkerPools;
    use std::collections::HashMap;

    #[tokio::test]
    async fn expired_source_session_releases_pin_and_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn SearchEngine> =
            Arc::new(CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
        engine
            .add_document("base", serde_json::json!({"value": 0}))
            .unwrap();
        let snapshot_dir = dir.path().join("peer-recovery/session");
        let snapshot = engine.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        engine
            .add_document("suffix", serde_json::json!({"value": 1}))
            .unwrap();

        let state = PeerRecoveryTransportState::new();
        let key = ("uuid-1".to_string(), 0);
        state
            .registry
            .lock()
            .await
            .active_shards
            .insert(key, Some("session".into()));
        state.registry.lock().await.sessions.insert(
            "session".into(),
            Arc::new(Mutex::new(SourceSession {
                index_name: "idx".into(),
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                target_node_id: "replica".into(),
                primary_node_id: "primary".into(),
                primary_term: 1,
                snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
                snapshot_dir: snapshot_dir.clone(),
                files: snapshot
                    .files
                    .into_iter()
                    .map(|file| (file.name.clone(), file))
                    .collect(),
                engine: engine.clone(),
                retention_pin_id: Some(snapshot.retention_pin_id),
                last_activity: Instant::now()
                    - RECOVERY_SESSION_IDLE_TIMEOUT
                    - Duration::from_secs(1),
                barrier_next_seq_no: None,
                barrier_guard: None,
                finalize_deadline: None,
                finalize_preparing: false,
                settlement_running: false,
            })),
        );

        state.reap_expired_sessions().await;
        assert!(state.registry.lock().await.sessions.is_empty());
        assert!(state.registry.lock().await.active_shards.is_empty());
        assert!(!snapshot_dir.exists());

        engine.flush().unwrap();
        assert!(
            engine
                .peer_recovery_ops(snapshot.snapshot_next_seq_no, 16, 1024 * 1024)
                .unwrap()
                .operations
                .is_empty(),
            "expired session must release the WAL retention pin"
        );
    }

    #[tokio::test]
    async fn stale_primary_term_rejects_recovery_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = shard_manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
            )
            .unwrap();
        engine
            .add_document("base", serde_json::json!({"value": 0}))
            .unwrap();
        let snapshot_dir = dir.path().join("uuid-1/shard_0/peer-recovery/session");
        let snapshot = engine.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        let first_file = snapshot.files[0].clone();

        let mut cluster_state = ClusterState::new("test".into());
        cluster_state.indices.insert(
            "idx".into(),
            IndexMetadata {
                name: "idx".into(),
                uuid: IndexUuid::new("uuid-1"),
                number_of_shards: 1,
                number_of_replicas: 1,
                shard_routing: HashMap::from([(
                    0,
                    ShardRoutingEntry {
                        primary: "primary".into(),
                        primary_term: 1,
                        replicas: vec!["replica".into()],
                        in_sync_replicas: Vec::new(),
                        unassigned_replicas: 0,
                    },
                )]),
                mappings: HashMap::new(),
                dynamic: Default::default(),
                settings: IndexSettings::default(),
            },
        );
        let cluster_manager = Arc::new(ClusterManager::new("test".into()));
        cluster_manager.update_state(cluster_state);
        let peer_recovery_state = PeerRecoveryTransportState::new();
        peer_recovery_state
            .registry
            .lock()
            .await
            .active_shards
            .insert(("uuid-1".into(), 0), Some("session".into()));
        peer_recovery_state.registry.lock().await.sessions.insert(
            "session".into(),
            Arc::new(Mutex::new(SourceSession {
                index_name: "idx".into(),
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                target_node_id: "replica".into(),
                primary_node_id: "primary".into(),
                primary_term: 1,
                snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
                snapshot_dir,
                files: snapshot
                    .files
                    .into_iter()
                    .map(|file| (file.name.clone(), file))
                    .collect(),
                engine,
                retention_pin_id: Some(snapshot.retention_pin_id),
                last_activity: Instant::now(),
                barrier_next_seq_no: None,
                barrier_guard: None,
                finalize_deadline: None,
                finalize_preparing: false,
                settlement_running: false,
            })),
        );
        let service = TransportService {
            cluster_manager: cluster_manager.clone(),
            shard_manager,
            transport_client: TransportClient::new(),
            storage_manager: Arc::new(
                crate::storage::StorageManager::new_in_path(dir.path()).unwrap(),
            ),
            remote_store_reader_cache: Arc::new(
                crate::engine::remote_store::RemoteSplitReaderCache::default(),
            ),
            raft: None,
            local_node_id: "primary".into(),
            worker_pools: WorkerPools::new(2, 2),
            task_manager: Arc::new(TaskManager::new()),
            primary_activation_state: super::super::new_primary_activation_state(),
            peer_recovery_state,
            join_lock: super::super::new_join_lock(),
        };

        assert!(service.shard_manager.begin_peer_recovery_target("idx", 0));
        let replicate = service
            .replicate_doc(tonic::Request::new(ReplicateDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "blocked".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                op: "index".into(),
                seq_no: snapshot.snapshot_next_seq_no,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!replicate.success);
        assert!(
            replicate
                .error
                .contains("installing a peer recovery snapshot")
        );
        service.shard_manager.end_peer_recovery_target("idx", 0);

        let unsafe_name = service
            .fetch_recovery_file_chunk_inner(FetchRecoveryFileChunkRequest {
                session_id: "session".into(),
                file_name: "../meta.json".into(),
                offset: 0,
                max_len: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(unsafe_name.code(), tonic::Code::InvalidArgument);
        let bad_offset = service
            .fetch_recovery_file_chunk_inner(FetchRecoveryFileChunkRequest {
                session_id: "session".into(),
                file_name: first_file.name,
                offset: first_file.length + 1,
                max_len: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(bad_offset.code(), tonic::Code::OutOfRange);

        let mut updated = cluster_manager.get_state();
        updated
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .primary_term = 2;
        cluster_manager.update_state(updated);
        let error = service
            .fetch_recovery_ops_inner(FetchRecoveryOpsRequest {
                session_id: "session".into(),
                from_seq_no: snapshot.snapshot_next_seq_no,
                max_ops: 1,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);

        if let Some(session) = service.peer_recovery_state.remove_session("session").await {
            cleanup_session(session).await;
        }
    }
}
