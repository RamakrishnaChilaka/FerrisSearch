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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, oneshot};
use tonic::Status;

pub(crate) const MAX_RECOVERY_FILE_CHUNK_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_RECOVERY_OPS: usize = 1024;
pub(super) const MAX_RECOVERY_OP_BYTES: usize = 32 * 1024 * 1024;
const MAX_SOURCE_RECOVERY_SESSIONS: usize = 8;
const RECOVERY_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const RECOVERY_SESSION_REAP_INTERVAL: Duration = Duration::from_secs(2);
const FINALIZE_BARRIER_TIMEOUT: Duration = Duration::from_secs(20);
type ShardIdentity = (String, u32);
type ShardWriteBarrier = Arc<RwLock<()>>;
type ShardWriteBarrierMap = HashMap<ShardIdentity, ShardWriteBarrier>;

#[derive(Default)]
struct SourceRegistry {
    sessions: HashMap<String, Arc<Mutex<SourceSession>>>,
    setups: HashMap<String, SourceSetup>,
    active_shards: HashMap<ShardIdentity, String>,
}

struct SourceSetup {
    target_node_id: String,
    primary_term: u64,
    last_activity: Instant,
    abort_handle: tokio::task::AbortHandle,
    lifetime: Arc<SetupLifetime>,
}

#[derive(Default)]
struct SetupLifetime {
    monitor_finished: AtomicBool,
    blocking_started: AtomicBool,
    blocking_finished: AtomicBool,
    notify: Notify,
}

impl SetupLifetime {
    fn mark_blocking_started(&self) {
        self.blocking_started.store(true, Ordering::Release);
    }

    fn finish_monitor(&self) {
        self.monitor_finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn finish_blocking(&self) {
        self.blocking_finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait_finished(&self) {
        loop {
            let monitor_finished = self.monitor_finished.load(Ordering::Acquire);
            let blocking_started = self.blocking_started.load(Ordering::Acquire);
            let blocking_finished = self.blocking_finished.load(Ordering::Acquire);
            if monitor_finished && (!blocking_started || blocking_finished) {
                return;
            }
            self.notify.notified().await;
        }
    }
}

struct SetupMonitorGuard(Arc<SetupLifetime>);

impl Drop for SetupMonitorGuard {
    fn drop(&mut self) {
        self.0.finish_monitor();
    }
}

struct SetupBlockingGuard(Arc<SetupLifetime>);

impl Drop for SetupBlockingGuard {
    fn drop(&mut self) {
        self.0.finish_blocking();
    }
}

struct FinalizePreparingGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for FinalizePreparingGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

pub(crate) struct PeerRecoveryTransportState {
    registry: Mutex<SourceRegistry>,
    write_barriers: Mutex<ShardWriteBarrierMap>,
    #[cfg(test)]
    start_response_gate: Mutex<Option<oneshot::Receiver<()>>>,
    #[cfg(test)]
    write_guard_waiting_sender: Mutex<Option<oneshot::Sender<()>>>,
    #[cfg(test)]
    start_lifecycle_waiting_sender: Mutex<Option<oneshot::Sender<()>>>,
}

impl PeerRecoveryTransportState {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: Mutex::new(SourceRegistry::default()),
            write_barriers: Mutex::new(HashMap::new()),
            #[cfg(test)]
            start_response_gate: Mutex::new(None),
            #[cfg(test)]
            write_guard_waiting_sender: Mutex::new(None),
            #[cfg(test)]
            start_lifecycle_waiting_sender: Mutex::new(None),
        })
    }

    async fn barrier(&self, key: (String, u32)) -> Arc<RwLock<()>> {
        let mut barriers = self.write_barriers.lock().await;
        barriers
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    async fn reap_expired_sessions(&self) {
        let setup_ids = {
            let registry = self.registry.lock().await;
            let now = Instant::now();
            registry
                .setups
                .iter()
                .filter_map(|(id, setup)| {
                    (now.duration_since(setup.last_activity) >= RECOVERY_SESSION_IDLE_TIMEOUT)
                        .then_some(id.clone())
                })
                .collect::<Vec<_>>()
        };
        for session_id in setup_ids {
            self.cancel_setup(&session_id).await;
        }

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
            let idle = now.duration_since(session.last_activity) >= RECOVERY_SESSION_IDLE_TIMEOUT
                && !session.finalize_preparing.load(Ordering::Acquire)
                && session.barrier_guard.is_none()
                && !session.settlement_running;
            if idle {
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
            let mut registry = self.registry.lock().await;
            if registry
                .active_shards
                .get(&key)
                .is_some_and(|active| active == session_id)
            {
                registry.active_shards.remove(&key);
            }
        }
        session
    }

    async fn cancel_setup(&self, session_id: &str) -> bool {
        let setup = {
            let mut registry = self.registry.lock().await;
            let setup = registry.setups.remove(session_id);
            if setup.is_some() {
                registry
                    .active_shards
                    .retain(|_, active| active != session_id);
            }
            setup
        };
        let Some(setup) = setup else {
            return false;
        };
        setup.abort_handle.abort();
        setup.lifetime.wait_finished().await;
        true
    }

    async fn abort_shard_session(&self, index_uuid: &str, shard_id: u32) -> anyhow::Result<bool> {
        let key = (index_uuid.to_string(), shard_id);
        loop {
            let active_session = self.registry.lock().await.active_shards.get(&key).cloned();
            let Some(active_session) = active_session else {
                return Ok(false);
            };
            let session_id = active_session;
            if self.cancel_setup(&session_id).await {
                return Ok(true);
            }
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
                if session.finalize_preparing.load(Ordering::Acquire)
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
    PeerRecoveryTransportState::new()
}

pub(super) fn start_peer_recovery_reaper(service: TransportService) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(RECOVERY_SESSION_REAP_INTERVAL).await;
            service.reap_peer_recovery_sessions().await;
        }
    });
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
    finalize_preparing: Arc<AtomicBool>,
    mark_submitted: bool,
    settlement_running: bool,
}

#[derive(Clone)]
struct SourceSessionAuthority {
    index_name: String,
    index_uuid: String,
    shard_id: u32,
    target_node_id: String,
    primary_node_id: String,
    primary_term: u64,
}

impl From<&SourceSession> for SourceSessionAuthority {
    fn from(session: &SourceSession) -> Self {
        Self {
            index_name: session.index_name.clone(),
            index_uuid: session.index_uuid.clone(),
            shard_id: session.shard_id,
            target_node_id: session.target_node_id.clone(),
            primary_node_id: session.primary_node_id.clone(),
            primary_term: session.primary_term,
        }
    }
}

struct SessionCleanup {
    engine: Arc<dyn SearchEngine>,
    retention_pin_id: Option<u64>,
    snapshot_dir: PathBuf,
}

struct PreparedSourceSnapshot {
    engine: Option<Arc<dyn SearchEngine>>,
    snapshot: Option<crate::engine::PeerRecoverySnapshot>,
    snapshot_dir: PathBuf,
    retained: bool,
}

impl PreparedSourceSnapshot {
    fn into_parts(
        mut self,
    ) -> (
        Arc<dyn SearchEngine>,
        crate::engine::PeerRecoverySnapshot,
        PathBuf,
    ) {
        self.retained = true;
        (
            self.engine.take().expect("prepared snapshot has engine"),
            self.snapshot
                .take()
                .expect("prepared snapshot has snapshot metadata"),
            self.snapshot_dir.clone(),
        )
    }
}

impl Drop for PreparedSourceSnapshot {
    fn drop(&mut self) {
        if let (Some(engine), Some(snapshot)) = (self.engine.take(), self.snapshot.take())
            && let Err(error) = engine.release_peer_recovery_pin(snapshot.retention_pin_id)
        {
            tracing::warn!(
                error = %error,
                "Failed to release cancelled peer recovery setup pin"
            );
        }
        if !self.retained
            && self.snapshot_dir.exists()
            && let Err(error) = std::fs::remove_dir_all(&self.snapshot_dir)
        {
            tracing::warn!(
                path = ?self.snapshot_dir,
                error = %error,
                "Failed to remove cancelled peer recovery setup directory"
            );
        }
    }
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

fn source_session_start_response(
    session_id: &str,
    session: &SourceSession,
) -> StartPeerRecoveryResponse {
    StartPeerRecoveryResponse {
        session_id: session_id.to_string(),
        primary_term: session.primary_term,
        snapshot_next_seq_no: session.snapshot_next_seq_no,
        files: session
            .files
            .values()
            .map(|file| RecoveryFileMetadata {
                name: file.name.clone(),
                length: file.length,
                sha256: file.sha256.clone(),
            })
            .collect(),
        error: String::new(),
        preparing: false,
    }
}

enum MembershipObservation {
    InSync,
    Pending,
    Impossible,
}

impl TransportService {
    async fn reap_peer_recovery_sessions(&self) {
        self.peer_recovery_state.reap_expired_sessions().await;
        let sessions = {
            let registry = self.peer_recovery_state.registry.lock().await;
            registry
                .sessions
                .iter()
                .map(|(id, session)| (id.clone(), session.clone()))
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        for (session_id, session) in sessions {
            let should_settle = {
                let mut session = session.lock().await;
                let expired = session
                    .finalize_deadline
                    .is_some_and(|deadline| now >= deadline)
                    && session.barrier_guard.is_some()
                    && !session.settlement_running;
                if expired {
                    session.settlement_running = true;
                }
                expired
            };
            if should_settle {
                let service = self.clone();
                tokio::spawn(async move {
                    service.settle_abandoned_finalize(session_id, session).await;
                });
            }
        }
    }

    async fn settle_abandoned_finalize(
        &self,
        session_id: String,
        session: Arc<Mutex<SourceSession>>,
    ) {
        let (authority, mark_submitted, released_guard) = {
            let mut session = session.lock().await;
            let authority = SourceSessionAuthority::from(&*session);
            let mark_submitted = session.mark_submitted;
            let released_guard = if mark_submitted {
                None
            } else {
                session.barrier_guard.take()
            };
            (authority, mark_submitted, released_guard)
        };
        drop(released_guard);

        if !mark_submitted
            && let Some(removed) = self.peer_recovery_state.remove_session(&session_id).await
        {
            cleanup_session(removed).await;
        }

        loop {
            match self.observe_membership(&authority) {
                MembershipObservation::InSync | MembershipObservation::Impossible => break,
                MembershipObservation::Pending => {}
            }
            if let Err(error) = self.submit_settlement_term_bump(&authority).await {
                tracing::error!(
                    session_id,
                    error,
                    "Unsettled peer recovery term bump has not committed"
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if mark_submitted
            && let Some(removed) = self.peer_recovery_state.remove_session(&session_id).await
        {
            cleanup_session(removed).await;
        }
    }

    async fn source_start_status(
        &self,
        key: &ShardIdentity,
        target_node_id: &str,
    ) -> Result<Option<StartPeerRecoveryResponse>, Status> {
        let active_id = self
            .peer_recovery_state
            .registry
            .lock()
            .await
            .active_shards
            .get(key)
            .cloned();
        let Some(active_id) = active_id else {
            return Ok(None);
        };

        if let Some(session) = self
            .peer_recovery_state
            .registry
            .lock()
            .await
            .sessions
            .get(&active_id)
            .cloned()
        {
            let mut session = session.lock().await;
            if session.target_node_id != target_node_id {
                return Err(Status::already_exists(
                    "a peer recovery source session already exists for a different target",
                ));
            }
            session.last_activity = Instant::now();
            return Ok(Some(source_session_start_response(&active_id, &session)));
        }

        let mut registry = self.peer_recovery_state.registry.lock().await;
        let Some(setup) = registry.setups.get_mut(&active_id) else {
            registry.active_shards.remove(key);
            return Ok(None);
        };
        if setup.target_node_id != target_node_id {
            return Err(Status::already_exists(
                "a peer recovery snapshot is being prepared for a different target",
            ));
        }
        setup.last_activity = Instant::now();
        Ok(Some(StartPeerRecoveryResponse {
            session_id: active_id,
            primary_term: setup.primary_term,
            snapshot_next_seq_no: 0,
            files: Vec::new(),
            error: String::new(),
            preparing: true,
        }))
    }

    async fn launch_source_setup(
        &self,
        session_id: String,
        key: ShardIdentity,
        request: StartPeerRecoveryRequest,
        primary_term: u64,
        engine: Arc<dyn SearchEngine>,
        snapshot_dir: PathBuf,
    ) -> Result<(), Status> {
        let lifetime = Arc::new(SetupLifetime::default());
        let (start_tx, start_rx) = oneshot::channel();
        let state = self.peer_recovery_state.clone();
        let local_node_id = self.local_node_id.clone();
        let setup_target_node_id = request.target_node_id.clone();
        let task_session_id = session_id.clone();
        let task_key = key.clone();
        let task_lifetime = lifetime.clone();
        let task = tokio::spawn(async move {
            let _monitor_guard = SetupMonitorGuard(task_lifetime.clone());
            if start_rx.await.is_err() {
                return;
            }
            task_lifetime.mark_blocking_started();
            let blocking_lifetime = task_lifetime.clone();
            let snapshot_engine = engine.clone();
            let snapshot_dir_for_task = snapshot_dir.clone();
            let prepared = tokio::task::spawn_blocking(move || {
                let _blocking_guard = SetupBlockingGuard(blocking_lifetime);
                let snapshot =
                    snapshot_engine.create_peer_recovery_snapshot(&snapshot_dir_for_task)?;
                Ok::<PreparedSourceSnapshot, anyhow::Error>(PreparedSourceSnapshot {
                    engine: Some(snapshot_engine),
                    snapshot: Some(snapshot),
                    snapshot_dir: snapshot_dir_for_task,
                    retained: false,
                })
            })
            .await;

            let prepared = match prepared {
                Ok(Ok(prepared)) => prepared,
                Ok(Err(error)) => {
                    tracing::warn!(
                        session_id = task_session_id,
                        error = %error,
                        "Peer recovery snapshot preparation failed"
                    );
                    let mut registry = state.registry.lock().await;
                    registry.setups.remove(&task_session_id);
                    if registry
                        .active_shards
                        .get(&task_key)
                        .is_some_and(|active| active == &task_session_id)
                    {
                        registry.active_shards.remove(&task_key);
                    }
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        session_id = task_session_id,
                        error = %error,
                        "Peer recovery snapshot preparation task failed"
                    );
                    let mut registry = state.registry.lock().await;
                    registry.setups.remove(&task_session_id);
                    if registry
                        .active_shards
                        .get(&task_key)
                        .is_some_and(|active| active == &task_session_id)
                    {
                        registry.active_shards.remove(&task_key);
                    }
                    return;
                }
            };

            let mut registry = state.registry.lock().await;
            let still_active = registry
                .active_shards
                .get(&task_key)
                .is_some_and(|active| active == &task_session_id)
                && registry.setups.contains_key(&task_session_id);
            if !still_active {
                drop(registry);
                drop(prepared);
                return;
            }

            let (engine, snapshot, snapshot_dir) = prepared.into_parts();
            let files = snapshot
                .files
                .iter()
                .cloned()
                .map(|file| (file.name.clone(), file))
                .collect::<HashMap<_, _>>();
            let session = Arc::new(Mutex::new(SourceSession {
                index_name: request.index_name,
                index_uuid: request.index_uuid,
                shard_id: request.shard_id,
                target_node_id: request.target_node_id,
                primary_node_id: local_node_id,
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
                finalize_preparing: Arc::new(AtomicBool::new(false)),
                mark_submitted: false,
                settlement_running: false,
            }));
            registry.setups.remove(&task_session_id);
            registry.sessions.insert(task_session_id.clone(), session);
            tracing::info!(
                session_id = task_session_id,
                snapshot_next_seq_no = snapshot.snapshot_next_seq_no,
                "Peer recovery snapshot is ready"
            );
        });
        let abort_handle = task.abort_handle();

        {
            let mut registry = self.peer_recovery_state.registry.lock().await;
            if registry.active_shards.len() >= MAX_SOURCE_RECOVERY_SESSIONS {
                task.abort();
                return Err(Status::resource_exhausted(
                    "peer recovery source session limit reached",
                ));
            }
            if registry.active_shards.contains_key(&key) {
                task.abort();
                return Err(Status::already_exists(
                    "a peer recovery source session already exists for this shard",
                ));
            }
            registry.active_shards.insert(key, session_id.clone());
            registry.setups.insert(
                session_id.clone(),
                SourceSetup {
                    target_node_id: setup_target_node_id,
                    primary_term,
                    last_activity: Instant::now(),
                    abort_handle,
                    lifetime,
                },
            );
        }
        let _ = start_tx.send(());
        Ok(())
    }

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
        #[cfg(test)]
        if let Some(sender) = self
            .peer_recovery_state
            .write_guard_waiting_sender
            .lock()
            .await
            .take()
        {
            let _ = sender.send(());
        }
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

    fn observe_membership(&self, session: &SourceSessionAuthority) -> MembershipObservation {
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

    async fn submit_mark_replica_in_sync(
        &self,
        session: &SourceSessionAuthority,
    ) -> Result<(), String> {
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

    async fn submit_settlement_term_bump(
        &self,
        session: &SourceSessionAuthority,
    ) -> Result<(), String> {
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
        let source_recovery_lock = self
            .shard_manager
            .source_recovery_lifecycle_lock(&request.index_uuid, request.shard_id);
        #[cfg(test)]
        if let Some(sender) = self
            .peer_recovery_state
            .start_lifecycle_waiting_sender
            .lock()
            .await
            .take()
        {
            let _ = sender.send(());
        }
        let _source_recovery_guard = source_recovery_lock.lock_owned().await;
        if let Some(response) = self
            .source_start_status(&key, &request.target_node_id)
            .await?
        {
            return Ok(response);
        }

        let engine = self
            .get_or_open_shard(&request.index_name, request.shard_id)
            .await?;
        let Some(shard_dir) = self
            .shard_manager
            .shard_data_dir(&request.index_name, request.shard_id)
        else {
            return Err(Status::internal(
                "peer recovery source shard path is unknown",
            ));
        };
        let session_id = uuid::Uuid::new_v4().to_string();
        let snapshot_dir = shard_dir.join("peer-recovery").join(&session_id);
        self.launch_source_setup(
            session_id.clone(),
            key,
            request,
            primary_term,
            engine,
            snapshot_dir,
        )
        .await?;

        #[cfg(test)]
        if let Some(gate) = self
            .peer_recovery_state
            .start_response_gate
            .lock()
            .await
            .take()
        {
            let _ = gate.await;
        }

        tracing::info!(
            session_id,
            "Started asynchronous peer recovery snapshot preparation"
        );
        Ok(StartPeerRecoveryResponse {
            session_id,
            primary_term,
            snapshot_next_seq_no: 0,
            files: Vec::new(),
            error: String::new(),
            preparing: true,
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
        let (key, engine, _preparing_guard) = {
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
            if session.finalize_preparing.swap(true, Ordering::AcqRel)
                || session.barrier_guard.is_some()
            {
                return Err(Status::already_exists(
                    "peer recovery finalization is already being prepared",
                ));
            }
            let preparing = FinalizePreparingGuard {
                flag: session.finalize_preparing.clone(),
            };
            session.last_activity = Instant::now();
            (
                (session.index_uuid.clone(), session.shard_id),
                session.engine.clone(),
                preparing,
            )
        };

        let barrier = self.peer_recovery_state.barrier(key).await;
        let guard =
            match tokio::time::timeout(FINALIZE_BARRIER_TIMEOUT, barrier.write_owned()).await {
                Ok(guard) => guard,
                Err(_) => {
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
                return Err(Status::internal(format!(
                    "read finalize operations: {error}"
                )));
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "finalize ops task failed: {error}"
                )));
            }
        };
        let barrier_next_seq_no = batch.primary_next_seq_no;
        if !batch.complete {
            drop(guard);
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
            Err(error) => return Err(error),
        };
        {
            let mut session = session.lock().await;
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
        let (deadline, authority) = {
            let session = session.lock().await;
            (
                session
                    .finalize_deadline
                    .unwrap_or_else(|| Instant::now() + FINALIZE_BARRIER_TIMEOUT),
                SourceSessionAuthority::from(&*session),
            )
        };
        let mut deadline_elapsed = false;

        loop {
            let observation = self.observe_membership(&authority);
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
                {
                    let mut session = session.lock().await;
                    session.mark_submitted = true;
                    session.last_activity = Instant::now();
                }
                let mark_result = self.submit_mark_replica_in_sync(&authority).await;
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
                session.lock().await.last_activity = Instant::now();
                let bump_result = self.submit_settlement_term_bump(&authority).await;
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
        ClusterState, IndexMetadata, IndexSettings, IndexUuid, NodeInfo, NodeRole,
        ShardRoutingEntry,
    };
    use crate::engine::CompositeEngine;
    use crate::shard::ShardManager;
    use crate::tasks::TaskManager;
    use crate::transport::TransportClient;
    use crate::transport::proto::internal_transport_server::InternalTransport;
    use crate::transport::proto::{
        ReplicateDocRequest, ShardBulkRequest, ShardDeleteRequest, ShardDocRequest,
    };
    use crate::worker::WorkerPools;
    use crate::{
        consensus,
        consensus::types::{ClusterCommand, ClusterResponse},
    };
    use std::collections::HashMap;

    async fn wait_for_test_leader(raft: &crate::consensus::types::RaftInstance) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !raft.is_leader() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "test Raft leader was not elected"
            );
            tokio::task::yield_now().await;
        }
    }

    fn review_state(primary: &str, term: u64) -> ClusterState {
        let mut state = ClusterState::new("review".into());
        for id in ["primary", "replica"] {
            state.nodes.insert(
                id.into(),
                NodeInfo {
                    id: id.into(),
                    name: id.into(),
                    host: "127.0.0.1".into(),
                    transport_port: 1,
                    http_port: 0,
                    roles: vec![NodeRole::Data],
                    raft_node_id: 0,
                },
            );
        }
        state.indices.insert(
            "idx".into(),
            IndexMetadata {
                name: "idx".into(),
                uuid: IndexUuid::new("uuid-1"),
                number_of_shards: 1,
                number_of_replicas: 1,
                shard_routing: HashMap::from([(
                    0,
                    ShardRoutingEntry {
                        primary: primary.into(),
                        primary_term: term,
                        replicas: vec!["replica".into()],
                        in_sync_replicas: Vec::new(),
                        unassigned_replicas: 0,
                    },
                )]),
                mappings: HashMap::from([(
                    "value".to_string(),
                    crate::cluster::state::FieldMapping {
                        field_type: crate::cluster::state::FieldType::Integer,
                        dimension: None,
                    },
                )]),
                dynamic: Default::default(),
                settings: IndexSettings::default(),
            },
        );
        state
    }

    fn review_service(
        dir: &std::path::Path,
    ) -> (TransportService, Arc<ShardManager>, Arc<ClusterManager>) {
        let shard_manager = Arc::new(ShardManager::new(dir, Duration::from_secs(60)));
        let cluster_manager = Arc::new(ClusterManager::new("review".into()));
        cluster_manager.update_state(review_state("primary", 1));
        let peer_recovery_state = PeerRecoveryTransportState::new();
        shard_manager.register_source_recovery_cleanup(peer_recovery_state.clone());
        let service = TransportService {
            cluster_manager: cluster_manager.clone(),
            shard_manager: shard_manager.clone(),
            transport_client: TransportClient::new(),
            storage_manager: Arc::new(crate::storage::StorageManager::new_in_path(dir).unwrap()),
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
        (service, shard_manager, cluster_manager)
    }

    fn review_start_request() -> StartPeerRecoveryRequest {
        StartPeerRecoveryRequest {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            target_node_id: "replica".into(),
        }
    }

    async fn wait_for_ready_source_session(service: &TransportService) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let ready = {
                let registry = service.peer_recovery_state.registry.lock().await;
                registry.sessions.keys().next().cloned()
            };
            if let Some(session_id) = ready {
                return session_id;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "peer recovery source session did not become ready"
            );
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_start_becomes_pollable_and_reopen_cleans_it() {
        let dir = tempfile::tempdir().unwrap();
        let (service, shard_manager, _cluster) = review_service(dir.path());
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

        let (gate_tx, gate_rx) = oneshot::channel();
        *service.peer_recovery_state.start_response_gate.lock().await = Some(gate_rx);
        let cancelled = tokio::time::timeout(
            Duration::from_millis(50),
            service.start_peer_recovery_inner(review_start_request()),
        )
        .await;
        assert!(cancelled.is_err(), "StartPeerRecovery should be cancelled");
        drop(gate_tx);

        let session_id = wait_for_ready_source_session(&service).await;
        let retry = service
            .start_peer_recovery_inner(review_start_request())
            .await
            .unwrap();
        assert_eq!(retry.session_id, session_id);
        assert!(!retry.preparing);
        assert!(!retry.files.is_empty());

        drop(engine);
        let reopened = shard_manager
            .reopen_shard(
                "idx".into(),
                0,
                HashMap::new(),
                IndexSettings::default(),
                "uuid-1".into(),
            )
            .await
            .unwrap();
        assert!(
            service
                .peer_recovery_state
                .registry
                .lock()
                .await
                .active_shards
                .is_empty()
        );
        reopened
            .add_document("suffix", serde_json::json!({"value": 1}))
            .unwrap();
        reopened.flush().unwrap();
        assert!(
            reopened
                .peer_recovery_ops(1, 16, 1024 * 1024)
                .unwrap()
                .operations
                .is_empty(),
            "cancelled setup/session cleanup must release the WAL pin"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn start_waits_for_reopen_engine_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let (service, shard_manager, _cluster) = review_service(dir.path());
        let engine = shard_manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
            )
            .unwrap();
        drop(engine);

        let (reopen_entered_tx, reopen_entered_rx) = oneshot::channel();
        let (reopen_release_tx, reopen_release_rx) = oneshot::channel();
        shard_manager.set_reopen_after_cleanup_gate(reopen_entered_tx, reopen_release_rx);
        let reopen_manager = shard_manager.clone();
        let reopen = tokio::spawn(async move {
            reopen_manager
                .reopen_shard(
                    "idx".into(),
                    0,
                    HashMap::new(),
                    IndexSettings::default(),
                    "uuid-1".into(),
                )
                .await
        });
        reopen_entered_rx.await.unwrap();

        let (start_waiting_tx, start_waiting_rx) = oneshot::channel();
        *service
            .peer_recovery_state
            .start_lifecycle_waiting_sender
            .lock()
            .await = Some(start_waiting_tx);
        let start_service = service.clone();
        let start = tokio::spawn(async move {
            start_service
                .start_peer_recovery_inner(review_start_request())
                .await
        });
        start_waiting_rx.await.unwrap();
        assert!(
            service
                .peer_recovery_state
                .registry
                .lock()
                .await
                .active_shards
                .is_empty(),
            "Start must not reserve a session while reopen owns the lifecycle lock"
        );

        reopen_release_tx.send(()).unwrap();
        assert!(reopen.await.unwrap().is_ok());
        let start_response = start.await.unwrap().unwrap();
        assert!(start_response.preparing);
        wait_for_ready_source_session(&service).await;
        assert!(shard_manager.get_shard("idx", 0).is_some());
    }

    #[tokio::test]
    async fn idle_reaper_keeps_barrier_during_settlement() {
        let dir = tempfile::tempdir().unwrap();
        let engine: Arc<dyn SearchEngine> =
            Arc::new(CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
        engine
            .add_document("base", serde_json::json!({"value": 0}))
            .unwrap();
        let snapshot_dir = dir.path().join("peer-recovery/session");
        let snapshot = engine.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        let state = PeerRecoveryTransportState::new();
        let key = ("uuid-1".to_string(), 0);
        let barrier = state.barrier(key.clone()).await;
        let guard = barrier.clone().write_owned().await;
        state
            .registry
            .lock()
            .await
            .active_shards
            .insert(key, "session".into());
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
                snapshot_dir,
                files: HashMap::new(),
                engine,
                retention_pin_id: Some(snapshot.retention_pin_id),
                last_activity: Instant::now()
                    - RECOVERY_SESSION_IDLE_TIMEOUT
                    - Duration::from_secs(1),
                barrier_next_seq_no: Some(snapshot.snapshot_next_seq_no),
                barrier_guard: Some(guard),
                finalize_deadline: Some(Instant::now() - Duration::from_secs(60)),
                finalize_preparing: Arc::new(AtomicBool::new(false)),
                mark_submitted: true,
                settlement_running: true,
            })),
        );

        state.reap_expired_sessions().await;
        assert!(
            barrier.try_read().is_err(),
            "only settlement may release an unresolved admission barrier"
        );
        assert!(state.registry.lock().await.sessions.contains_key("session"));
    }

    fn move_primary(cluster: &ClusterManager) {
        let mut moved = review_state("replica", 2);
        let routing = moved
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap();
        routing.replicas.clear();
        cluster.update_state(moved);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_writes_reject_primary_change_inside_barrier() {
        let dir = tempfile::tempdir().unwrap();
        let (service, shard_manager, cluster) = review_service(dir.path());
        let engine = shard_manager
            .open_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "uuid-1",
            )
            .unwrap();

        let barrier = service
            .peer_recovery_state
            .barrier(("uuid-1".to_string(), 0))
            .await;
        let guard = barrier.clone().write_owned().await;
        let (waiting_tx, waiting_rx) = oneshot::channel();
        *service
            .peer_recovery_state
            .write_guard_waiting_sender
            .lock()
            .await = Some(waiting_tx);
        let writer = service.clone();
        let index = tokio::spawn(async move {
            writer
                .index_doc(tonic::Request::new(ShardDocRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    doc_id: "queued-index".into(),
                    payload_json: serde_json::to_vec(&serde_json::json!({"value": 1})).unwrap(),
                }))
                .await
        });
        waiting_rx.await.unwrap();
        move_primary(&cluster);
        drop(guard);
        assert!(!index.await.unwrap().unwrap().into_inner().success);
        assert!(engine.get_document("queued-index").unwrap().is_none());

        cluster.update_state(review_state("primary", 1));
        let guard = barrier.clone().write_owned().await;
        let (waiting_tx, waiting_rx) = oneshot::channel();
        *service
            .peer_recovery_state
            .write_guard_waiting_sender
            .lock()
            .await = Some(waiting_tx);
        let writer = service.clone();
        let bulk = tokio::spawn(async move {
            writer
                .bulk_index(tonic::Request::new(ShardBulkRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    documents_json: vec![
                        serde_json::to_vec(&serde_json::json!({
                            "_doc_id": "queued-bulk",
                            "_source": {"value": 2}
                        }))
                        .unwrap(),
                    ],
                }))
                .await
        });
        waiting_rx.await.unwrap();
        move_primary(&cluster);
        drop(guard);
        assert!(!bulk.await.unwrap().unwrap().into_inner().success);
        assert!(engine.get_document("queued-bulk").unwrap().is_none());

        cluster.update_state(review_state("primary", 1));
        engine
            .add_document("delete-me", serde_json::json!({"value": 3}))
            .unwrap();
        engine.refresh().unwrap();
        let guard = barrier.write_owned().await;
        let (waiting_tx, waiting_rx) = oneshot::channel();
        *service
            .peer_recovery_state
            .write_guard_waiting_sender
            .lock()
            .await = Some(waiting_tx);
        let writer = service.clone();
        let delete = tokio::spawn(async move {
            writer
                .delete_doc(tonic::Request::new(ShardDeleteRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    doc_id: "delete-me".into(),
                }))
                .await
        });
        waiting_rx.await.unwrap();
        move_primary(&cluster);
        drop(guard);
        assert!(!delete.await.unwrap().unwrap().into_inner().success);
        assert!(engine.get_document("delete-me").unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_prepare_finalize_clears_preparing_flag() {
        let dir = tempfile::tempdir().unwrap();
        let (service, shard_manager, _cluster) = review_service(dir.path());
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
        let session = Arc::new(Mutex::new(SourceSession {
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
            finalize_preparing: Arc::new(AtomicBool::new(false)),
            mark_submitted: false,
            settlement_running: false,
        }));
        {
            let mut registry = service.peer_recovery_state.registry.lock().await;
            registry
                .active_shards
                .insert(("uuid-1".into(), 0), "session".into());
            registry.sessions.insert("session".into(), session.clone());
        }
        let barrier = service
            .peer_recovery_state
            .barrier(("uuid-1".into(), 0))
            .await;
        let read_guard = barrier.read_owned().await;

        let prepare_service = service.clone();
        let prepare = tokio::spawn(async move {
            prepare_service
                .prepare_finalize_recovery_inner(PrepareFinalizeRecoveryRequest {
                    session_id: "session".into(),
                    applied_next_seq_no: 1,
                })
                .await
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !session
            .lock()
            .await
            .finalize_preparing
            .load(Ordering::Acquire)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "PrepareFinalize did not reach the barrier wait"
            );
            tokio::task::yield_now().await;
        }
        prepare.abort();
        let _ = prepare.await;
        drop(read_guard);
        assert!(
            !session
                .lock()
                .await
                .finalize_preparing
                .load(Ordering::Acquire),
            "cancelled PrepareFinalize must clear the preparing state"
        );
        if let Some(removed) = service.peer_recovery_state.remove_session("session").await {
            cleanup_session(removed).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn expired_finalize_without_mark_releases_barrier_and_bumps_term() {
        let (raft, state_handle) = consensus::create_raft_instance_mem(1, "expiry-bump".into())
            .await
            .unwrap();
        consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
            .await
            .unwrap();
        wait_for_test_leader(&raft).await;
        let metadata = IndexMetadata {
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
        };
        assert_eq!(
            raft.client_write(ClusterCommand::CreateIndex { metadata })
                .await
                .unwrap()
                .data,
            ClusterResponse::Ok
        );

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
        let snapshot_dir = dir.path().join("uuid-1/shard_0/peer-recovery/session");
        let snapshot = engine.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        let cluster_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
        let peer_recovery_state = PeerRecoveryTransportState::new();
        shard_manager.register_source_recovery_cleanup(peer_recovery_state.clone());
        let service = TransportService {
            cluster_manager,
            shard_manager,
            transport_client: TransportClient::new(),
            storage_manager: Arc::new(
                crate::storage::StorageManager::new_in_path(dir.path()).unwrap(),
            ),
            remote_store_reader_cache: Arc::new(
                crate::engine::remote_store::RemoteSplitReaderCache::default(),
            ),
            raft: Some(raft),
            local_node_id: "primary".into(),
            worker_pools: WorkerPools::new(2, 2),
            task_manager: Arc::new(TaskManager::new()),
            primary_activation_state: super::super::new_primary_activation_state(),
            peer_recovery_state,
            join_lock: super::super::new_join_lock(),
        };
        let barrier = service
            .peer_recovery_state
            .barrier(("uuid-1".into(), 0))
            .await;
        let guard = barrier.clone().write_owned().await;
        let session = Arc::new(Mutex::new(SourceSession {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            target_node_id: "replica".into(),
            primary_node_id: "primary".into(),
            primary_term: 1,
            snapshot_next_seq_no: snapshot.snapshot_next_seq_no,
            snapshot_dir,
            files: HashMap::new(),
            engine,
            retention_pin_id: Some(snapshot.retention_pin_id),
            last_activity: Instant::now(),
            barrier_next_seq_no: Some(snapshot.snapshot_next_seq_no),
            barrier_guard: Some(guard),
            finalize_deadline: Some(Instant::now() - Duration::from_secs(1)),
            finalize_preparing: Arc::new(AtomicBool::new(false)),
            mark_submitted: false,
            settlement_running: false,
        }));
        {
            let mut registry = service.peer_recovery_state.registry.lock().await;
            registry
                .active_shards
                .insert(("uuid-1".into(), 0), "session".into());
            registry.sessions.insert("session".into(), session);
        }

        service.reap_peer_recovery_sessions().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let term = state_handle.read().unwrap().indices["idx"].shard_routing[&0].primary_term;
            if term > 1 && barrier.try_read().is_ok() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "expired finalize did not settle with a newer term"
            );
            tokio::task::yield_now().await;
        }
        assert!(
            service
                .peer_recovery_state
                .registry
                .lock()
                .await
                .sessions
                .is_empty()
        );
    }

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
            .insert(key, "session".into());
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
                finalize_preparing: Arc::new(AtomicBool::new(false)),
                mark_submitted: false,
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
            .insert(("uuid-1".into(), 0), "session".into());
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
                finalize_preparing: Arc::new(AtomicBool::new(false)),
                mark_submitted: false,
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
