//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::proto::internal_transport_server::{
    InternalTransport, InternalTransportServer,
};
use crate::transport::proto::*;
use futures::{FutureExt, Stream, stream};
use openraft::type_config::async_runtime::WatchReceiver;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info, trace};

mod bulk_writes;

fn primary_write_condition(
    if_seq_no: Option<u64>,
    if_primary_term: Option<u64>,
    create_only: bool,
) -> Result<crate::engine::WriteCondition, Status> {
    let condition = crate::engine::WriteCondition::from_optional_values(if_seq_no, if_primary_term)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if create_only {
        if condition != crate::engine::WriteCondition::Unconditional {
            return Err(Status::invalid_argument(
                "create operations cannot use if_seq_no or if_primary_term",
            ));
        }
        Ok(crate::engine::WriteCondition::Create)
    } else {
        Ok(condition)
    }
}

/// Shared state for the gRPC transport service.
#[derive(Clone)]
pub struct TransportService {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: crate::transport::TransportClient,
    pub storage_manager: Arc<crate::storage::StorageManager>,
    pub remote_store_reader_cache: Arc<crate::engine::remote_store::RemoteSplitReaderCache>,
    /// Optional Raft consensus instance. When present, Raft RPCs are forwarded here.
    pub raft: Option<Arc<RaftInstance>>,
    /// This node's identifier — used to filter locally-assigned shards.
    pub local_node_id: crate::cluster::state::NodeId,
    /// Dedicated thread pools for search and write workloads.
    pub worker_pools: crate::worker::WorkerPools,
    /// Tracks asynchronous background tasks running on this node.
    pub task_manager: Arc<crate::tasks::TaskManager>,
    primary_activation_state: Arc<PrimaryActivationState>,
    peer_recovery_state: Arc<peer_recovery::PeerRecoveryTransportState>,
    /// Serializes leader-side JoinCluster handling so concurrent joins cannot
    /// race identity validation or submit stale full voter sets.
    join_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
pub struct RemoteStoreTransportResources {
    pub storage_manager: Arc<crate::storage::StorageManager>,
    pub remote_store_reader_cache: Arc<crate::engine::remote_store::RemoteSplitReaderCache>,
}

fn new_join_lock() -> Arc<Mutex<()>> {
    Arc::new(Mutex::new(()))
}

type PrimaryActivationKey = (String, u32, u64);

#[cfg(test)]
type CheckpointRecordingHook = Box<dyn FnOnce() + Send>;

#[derive(Clone)]
struct PendingPromotionNoOps {
    primary_term: u64,
    operations: Vec<crate::engine::SequencedOperation>,
}

#[derive(Default)]
struct PrimaryCopyActivationLocks {
    activation: Mutex<()>,
    noop_replication: Mutex<()>,
}

#[derive(Default)]
struct PrimaryActivationState {
    activated_terms: RwLock<HashMap<PrimaryActivationKey, u64>>,
    pending_noops: RwLock<HashMap<PrimaryActivationKey, PendingPromotionNoOps>>,
    copy_locks: std::sync::Mutex<HashMap<PrimaryActivationKey, Arc<PrimaryCopyActivationLocks>>>,
    failed_copy_reports: Mutex<HashMap<(String, u32, u64), std::time::Instant>>,
    available_primary_reports: Mutex<HashMap<(String, u32, u64, u64), std::time::Instant>>,
    #[cfg(test)]
    available_report_tasks_spawned: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    promotion_noop_bulk_requests_received: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    checkpoint_recording_hook: std::sync::Mutex<Option<CheckpointRecordingHook>>,
}

impl PrimaryActivationState {
    fn copy_locks(&self, key: &PrimaryActivationKey) -> Arc<PrimaryCopyActivationLocks> {
        self.copy_locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(key.clone())
            .or_default()
            .clone()
    }
}

fn new_primary_activation_state() -> Arc<PrimaryActivationState> {
    Arc::new(PrimaryActivationState::default())
}

#[derive(Clone, Copy)]
pub(crate) enum MaintenanceDispatchOp {
    Refresh,
    Flush,
    ForceMerge(usize),
}

impl MaintenanceDispatchOp {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Refresh => "refresh",
            Self::Flush => "flush",
            Self::ForceMerge(_) => "forcemerge",
        }
    }
}

#[derive(Clone)]
struct DynamicShardOpenOverride {
    mappings: std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
    settings: crate::cluster::state::IndexSettings,
    index_uuid: String,
}

#[derive(Clone)]
struct ActivatedPrimary {
    index_uuid: String,
    allocation_id: u64,
    primary_term: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("no such index [{index_name}] for UUID [{index_uuid}]")]
struct IndexIncarnationMismatchError {
    index_name: String,
    index_uuid: String,
}

fn require_index_uuid(
    cluster_manager: &ClusterManager,
    index_name: &str,
    expected_uuid: Option<&str>,
) -> anyhow::Result<()> {
    let Some(expected_uuid) = expected_uuid else {
        return Ok(());
    };
    if cluster_manager
        .get_state()
        .indices
        .get(index_name)
        .is_some_and(|metadata| metadata.uuid.as_str() == expected_uuid)
    {
        return Ok(());
    }
    Err(IndexIncarnationMismatchError {
        index_name: index_name.to_owned(),
        index_uuid: expected_uuid.to_owned(),
    }
    .into())
}

#[derive(Clone)]
struct AssignedLocalShard {
    index_uuid: String,
    mappings: std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
    settings: crate::cluster::state::IndexSettings,
    allocation_id: u64,
    primary_term: u64,
    is_primary: bool,
    allow_empty_creation: bool,
    authoritative: bool,
    primary_unavailable: bool,
}

pub(crate) fn enqueue_force_merge_task_on_assigned_shards(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
    index_name: String,
    max_num_segments: usize,
) -> String {
    let task_id =
        task_manager.create_local_force_merge(&local_node_id, &index_name, max_num_segments);
    let task_id_for_job = task_id.clone();
    tokio::spawn(async move {
        task_manager.mark_running(&task_id_for_job);
        let result = std::panic::AssertUnwindSafe(run_maintenance_on_assigned_shards_async(
            cluster_manager,
            shard_manager,
            local_node_id,
            index_name.clone(),
            MaintenanceDispatchOp::ForceMerge(max_num_segments),
        ))
        .catch_unwind()
        .await;

        match result {
            Ok((successful, failed)) => {
                task_manager.finish_local_force_merge(&task_id_for_job, successful, failed, None);

                if failed > 0 {
                    tracing::error!(
                        "Background maintenance forcemerge finished for {} with {} successful shards and {} failures",
                        index_name,
                        successful,
                        failed
                    );
                } else {
                    tracing::info!(
                        "Background maintenance forcemerge finished for {} with {} successful shards",
                        index_name,
                        successful
                    );
                }
            }
            Err(_) => {
                task_manager.fail_local_force_merge(
                    &task_id_for_job,
                    "background forcemerge task panicked",
                );
                tracing::error!(
                    "Background maintenance forcemerge panicked for {}",
                    index_name
                );
            }
        }
    });

    task_id
}

#[allow(clippy::result_large_err)]
async fn get_or_open_read_shard(
    cluster_manager: &ClusterManager,
    shard_manager: &Arc<ShardManager>,
    local_node_id: &str,
    index_name: &str,
    shard_id: u32,
) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
    let cs = cluster_manager.get_state();
    let Some(metadata) = cs.indices.get(index_name) else {
        return Err(Status::not_found(format!(
            "Shard [{index_name}][{shard_id}] not found on this node"
        )));
    };
    let Some(routing) = metadata.shard_routing.get(&shard_id) else {
        return Err(Status::not_found(format!(
            "Shard [{index_name}][{shard_id}] not found on this node"
        )));
    };
    if routing.primary != local_node_id && !routing.is_replica_in_sync(local_node_id) {
        return Err(Status::failed_precondition(format!(
            "node [{local_node_id}] is not an authoritative copy for shard [{index_name}][{shard_id}]"
        )));
    }
    let allocation_id = cs
        .shard_allocation_id(index_name, shard_id, local_node_id)
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "Shard [{index_name}][{shard_id}] has no local allocation identity"
            ))
        })?;
    if let Some(engine) = shard_manager.get_shard(index_name, shard_id) {
        shard_manager
            .validate_open_copy_identity(
                index_name,
                shard_id,
                metadata.uuid.as_str(),
                allocation_id,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(engine);
    }

    let shard_dir = shard_manager
        .data_dir()
        .join(&metadata.uuid)
        .join(format!("shard_{shard_id}"));
    if !shard_dir.exists() {
        return Err(Status::failed_precondition(format!(
            "Shard [{index_name}][{shard_id}] is assigned here but {shard_dir:?} is missing; refusing to create a fresh shard on a read path"
        )));
    }

    let assignment = crate::shard::AssignedShardOpen {
        allocation_id,
        primary_term: routing.primary_term,
        allow_empty_creation: false,
    };
    let result = if routing.primary == local_node_id {
        Arc::clone(shard_manager)
            .open_primary_assigned_shard_with_settings_blocking(
                index_name.to_string(),
                shard_id,
                metadata.mappings.clone(),
                metadata.settings.clone(),
                metadata.uuid.clone(),
                assignment,
            )
            .await
    } else {
        Arc::clone(shard_manager)
            .open_assigned_shard_with_settings_blocking(
                index_name.to_string(),
                shard_id,
                metadata.mappings.clone(),
                metadata.settings.clone(),
                metadata.uuid.clone(),
                assignment,
            )
            .await
    };
    result.map_err(|e| Status::internal(format!("Failed to open shard: {e}")))
}

pub(crate) async fn run_maintenance_on_assigned_shards_async(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    local_node_id: String,
    index_name: String,
    op: MaintenanceDispatchOp,
) -> (u32, u32) {
    let mut successful = 0u32;
    let mut failed = 0u32;

    let cs = cluster_manager.get_state();
    let Some(metadata) = cs.indices.get(&index_name).cloned() else {
        return (successful, failed);
    };

    for (shard_id, routing) in &metadata.shard_routing {
        let assigned_here = routing.primary == local_node_id
            || routing
                .replicas
                .iter()
                .any(|node_id| node_id == &local_node_id);
        if !assigned_here {
            continue;
        }

        let engine = match get_or_open_read_shard(
            cluster_manager.as_ref(),
            &shard_manager,
            &local_node_id,
            &index_name,
            *shard_id,
        )
        .await
        {
            Ok(engine) => engine,
            Err(status) => {
                tracing::error!(
                    "Maintenance {} skipped {}/{}: {}",
                    op.label(),
                    index_name,
                    shard_id,
                    status.message()
                );
                if matches!(op, MaintenanceDispatchOp::ForceMerge(_)) {
                    failed += 1;
                }
                continue;
            }
        };

        let result = crate::worker::spawn_engine_maintenance(op.label(), move || match op {
            MaintenanceDispatchOp::Refresh => engine.refresh(),
            MaintenanceDispatchOp::Flush => engine.flush_with_global_checkpoint(),
            MaintenanceDispatchOp::ForceMerge(max_segments) => engine.force_merge(max_segments),
        })
        .await;

        match result {
            Ok(()) => successful += 1,
            Err(e) => {
                tracing::error!(
                    "Maintenance {} failed on {}/{}: {}",
                    op.label(),
                    index_name,
                    shard_id,
                    e
                );
                failed += 1;
            }
        }
    }

    (successful, failed)
}

pub mod conversions;
mod peer_recovery;
pub(crate) use peer_recovery::{MAX_RECOVERY_FILE_CHUNK_BYTES, MAX_RECOVERY_OPS};

pub use conversions::cluster_state_to_proto;
pub use conversions::proto_to_cluster_state;
use conversions::{node_info_to_proto, proto_to_node_info, validate_join_identity};

// ─── gRPC Service Implementation ───────────────────────────────────────────
// ─── gRPC Service Implementation ───────────────────────────────────────────

fn sql_batch_success_response(
    batch: datafusion::arrow::record_batch::RecordBatch,
    total_hits: usize,
    collected_rows: usize,
    streaming_used: bool,
) -> anyhow::Result<SqlRecordBatchResponse> {
    let ipc_bytes = crate::hybrid::arrow_bridge::record_batch_to_ipc(&batch)
        .map_err(|e| anyhow::anyhow!("Arrow IPC encode error: {e}"))?;
    Ok(SqlRecordBatchResponse {
        success: true,
        arrow_ipc: ipc_bytes,
        total_hits: total_hits as u64,
        error: String::new(),
        collected_rows: collected_rows as u64,
        streaming_used,
    })
}

fn sql_batch_error_response(error: impl Into<String>) -> SqlRecordBatchResponse {
    SqlRecordBatchResponse {
        success: false,
        arrow_ipc: vec![],
        total_hits: 0,
        error: error.into(),
        collected_rows: 0,
        streaming_used: false,
    }
}

fn create_index_error_status(error: crate::cluster::state::CreateIndexMetadataError) -> Status {
    match error {
        crate::cluster::state::CreateIndexMetadataError::NoDataNodes => {
            Status::internal("No data nodes available to assign shards")
        }
        crate::cluster::state::CreateIndexMetadataError::InvalidArgument(message) => {
            Status::invalid_argument(message)
        }
        crate::cluster::state::CreateIndexMetadataError::MapperParsing(message) => {
            Status::invalid_argument(message)
        }
        crate::cluster::state::CreateIndexMetadataError::UnimplementedEngine(engine) => {
            Status::unimplemented(format!(
                "index engine [{engine}] is recognized but not implemented yet"
            ))
        }
    }
}

#[cfg(test)]
static TRACKED_REPLICA_INDEX_PAYLOAD: std::sync::Mutex<Option<Vec<u8>>> =
    std::sync::Mutex::new(None);
#[cfg(test)]
static TRACKED_REPLICA_INDEX_PARSE_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static TRACK_REPLICA_INDEX_PAYLOAD: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[cfg(test)]
fn start_tracking_replica_index_payload(payload: &[u8]) {
    *TRACKED_REPLICA_INDEX_PAYLOAD
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some(payload.to_vec());
    TRACKED_REPLICA_INDEX_PARSE_COUNT.store(0, std::sync::atomic::Ordering::Release);
    TRACK_REPLICA_INDEX_PAYLOAD.store(true, std::sync::atomic::Ordering::Release);
}

#[cfg(test)]
fn stop_tracking_replica_index_payload() -> usize {
    TRACK_REPLICA_INDEX_PAYLOAD.store(false, std::sync::atomic::Ordering::Release);
    *TRACKED_REPLICA_INDEX_PAYLOAD
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = None;
    TRACKED_REPLICA_INDEX_PARSE_COUNT.swap(0, std::sync::atomic::Ordering::AcqRel)
}

fn parse_replica_index_source(
    payload_json: &[u8],
    invalid_json_context: &str,
) -> Result<serde_json::Value, Status> {
    #[cfg(test)]
    if TRACK_REPLICA_INDEX_PAYLOAD.load(std::sync::atomic::Ordering::Acquire)
        && TRACKED_REPLICA_INDEX_PAYLOAD
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_deref()
            == Some(payload_json)
    {
        TRACKED_REPLICA_INDEX_PARSE_COUNT.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    let source = serde_json::from_slice(payload_json)
        .map_err(|error| Status::invalid_argument(format!("{invalid_json_context}: {error}")))?;
    crate::common::validate_document_source(&source)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(source)
}

#[tonic::async_trait]
impl InternalTransport for TransportService {
    type SqlRecordBatchStreamStream =
        Pin<Box<dyn Stream<Item = Result<SqlRecordBatchResponse, Status>> + Send + 'static>>;

    async fn join_cluster(
        &self,
        request: Request<JoinRequest>,
    ) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let node_info = req
            .node_info
            .ok_or_else(|| Status::invalid_argument("missing node_info"))?;
        let mut ni = proto_to_node_info(&node_info)?;
        let joining_raft_id = req.raft_node_id;
        debug!(
            "gRPC: join request from node {} (raft_id={})",
            ni.id, joining_raft_id
        );

        // If Raft is active, only the leader can process joins.
        // Followers must forward to the leader — never mutate cluster state locally.
        if let Some(ref raft) = self.raft {
            if !raft.is_leader() {
                // Forward join to the Raft leader
                let cs = self.cluster_manager.get_state();
                let master_id = cs.master_node.as_ref().ok_or_else(|| {
                    Status::unavailable("No master node available to forward join request")
                })?;
                let master_node = cs.nodes.get(master_id).ok_or_else(|| {
                    Status::unavailable("Master node info not found in cluster state")
                })?;
                let proto_node_fwd = node_info_to_proto(&ni);
                let mut client = self
                    .transport_client
                    .connect(&master_node.host, master_node.transport_port)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to connect to master for join: {e}"))
                    })?;
                let fwd_request = tonic::Request::new(JoinRequest {
                    node_info: Some(proto_node_fwd),
                    raft_node_id: joining_raft_id,
                });
                let fwd_response = client.join_cluster(fwd_request).await.map_err(|e| {
                    Status::internal(format!("Failed to forward join to master: {e}"))
                })?;
                return Ok(fwd_response);
            }

            if joining_raft_id > 0 {
                let _join_guard = self.join_lock.lock().await;
                let state = self.cluster_manager.get_state();
                validate_join_identity(&state, &ni.id, joining_raft_id)?;
                ni.raft_node_id = joining_raft_id;

                let already_voter = raft.voter_ids().any(|id| id == joining_raft_id);

                if !already_voter {
                    // Register the transport address as a learner (non-blocking).
                    // Using blocking=false so unreachable nodes don't stall the
                    // leader's gRPC handler; change_membership below handles
                    // the catch-up semantics.
                    let addr = format!("{}:{}", ni.host, ni.transport_port);
                    raft.add_learner(joining_raft_id, openraft::BasicNode { addr }, false)
                        .await
                        .map_err(|e| Status::internal(format!("Raft add_learner failed: {e}")))?;
                }

                let cmd = crate::consensus::types::ClusterCommand::AddNode { node: ni.clone() };
                if let Err(e) = raft.client_write(cmd).await {
                    return Err(Status::internal(format!("Raft AddNode failed: {e}")));
                }

                if !already_voter {
                    let mut target_voters: std::collections::BTreeSet<u64> =
                        raft.voter_ids().collect();
                    target_voters.insert(joining_raft_id);
                    if let Err(e) = raft.change_membership(target_voters, false).await {
                        // NOTE: The learner registered by add_learner above is NOT
                        // removed here because openraft has no remove_learner API.
                        // The orphan learner is harmless — the leader will stop
                        // replicating to it once a future membership change cleans
                        // it up, or if the node restarts and re-joins successfully.
                        let rollback = raft
                            .client_write(crate::consensus::types::ClusterCommand::RemoveNode {
                                node_id: ni.id.clone(),
                            })
                            .await;
                        if let Err(rollback_error) = rollback {
                            tracing::error!(
                                "Join for node {} failed during membership promotion and cluster-state rollback also failed: {}",
                                ni.id,
                                rollback_error
                            );
                            return Err(Status::internal(format!(
                                "Raft change_membership failed after AddNode: {e}; rollback failed: {rollback_error}"
                            )));
                        }
                        return Err(Status::internal(format!(
                            "Raft change_membership failed: {e}"
                        )));
                    }
                }

                if already_voter {
                    tracing::info!(
                        "Join request for existing voter {} — refreshed cluster-state node registration only",
                        ni.id
                    );
                }

                let state = self.cluster_manager.get_state();
                return Ok(Response::new(JoinResponse {
                    state: Some(cluster_state_to_proto(&state)),
                }));
            }
        }

        Err(Status::unavailable(
            "Raft is required for JoinCluster on every production node",
        ))
    }

    async fn publish_state(
        &self,
        _request: Request<PublishStateRequest>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented(
            "PublishState is not supported; cluster state is managed via Raft consensus",
        ))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        if !self.cluster_manager.contains_node(&req.source_node_id) {
            return Err(Status::not_found(
                "source node is not registered in cluster state",
            ));
        }
        self.cluster_manager.ping_node(&req.source_node_id);
        Ok(Response::new(Empty {}))
    }

    async fn index_doc(
        &self,
        request: Request<ShardDocRequest>,
    ) -> Result<Response<ShardDocResponse>, Status> {
        let req = request.into_inner();
        let condition =
            primary_write_condition(req.if_seq_no, req.if_primary_term, req.create_only)?;
        require_index_uuid(
            &self.cluster_manager,
            &req.index_name,
            req.index_uuid.as_deref(),
        )
        .map_err(|error| Status::not_found(error.to_string()))?;

        let activated_primary = match self
            .ensure_primary_activated(&req.index_name, req.shard_id)
            .await
        {
            Ok(term) => term,
            Err(error) => {
                require_index_uuid(
                    &self.cluster_manager,
                    &req.index_name,
                    req.index_uuid.as_deref(),
                )
                .map_err(|error| Status::not_found(error.to_string()))?;
                return Ok(Response::new(ShardDocResponse {
                    success: false,
                    doc_id: req.doc_id,
                    error,
                    seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        require_index_uuid(
            &self.cluster_manager,
            &req.index_name,
            req.index_uuid.as_deref(),
        )
        .map_err(|error| Status::not_found(error.to_string()))?;
        let _write_guard = match self
            .peer_recovery_write_guard(&req.index_name, req.shard_id)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                require_index_uuid(
                    &self.cluster_manager,
                    &req.index_name,
                    req.index_uuid.as_deref(),
                )
                .map_err(|error| Status::not_found(error.to_string()))?;
                return Ok(Response::new(ShardDocResponse {
                    success: false,
                    doc_id: req.doc_id,
                    error,
                    seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        require_index_uuid(
            &self.cluster_manager,
            &req.index_name,
            req.index_uuid.as_deref(),
        )
        .map_err(|error| Status::not_found(error.to_string()))?;
        let _pre_mapping_write_state = match self.validated_primary_write_state(
            &req.index_name,
            req.shard_id,
            &activated_primary,
        ) {
            Ok(state) => state,
            Err(error) => {
                return Ok(Response::new(ShardDocResponse {
                    success: false,
                    doc_id: req.doc_id,
                    error,
                    seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };

        let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {e}")))?;
        crate::common::validate_document_source(&payload)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let doc_id = if req.doc_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.doc_id
        };

        // Dynamic mapping: detect/register new fields before opening the live
        // engine so a successful AddMappings commit can safely reopen the shard.
        let dynamic_override = self
            .ensure_dynamic_mappings(&req.index_name, req.shard_id, &payload)
            .await?;
        require_index_uuid(
            &self.cluster_manager,
            &req.index_name,
            req.index_uuid.as_deref(),
        )
        .map_err(|error| Status::not_found(error.to_string()))?;
        let write_state = match self.validated_primary_write_state(
            &req.index_name,
            req.shard_id,
            &activated_primary,
        ) {
            Ok(state) => state,
            Err(error) => {
                return Ok(Response::new(ShardDocResponse {
                    success: false,
                    doc_id,
                    error,
                    seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        let engine = self
            .get_or_open_shard_with_override(&req.index_name, req.shard_id, dynamic_override)
            .await?;
        require_index_uuid(
            &self.cluster_manager,
            &req.index_name,
            req.index_uuid.as_deref(),
        )
        .map_err(|error| Status::not_found(error.to_string()))?;
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::TraceCopy {
            node: self.local_node_id.clone(),
            index_uuid: activated_primary.index_uuid.clone(),
            shard: req.shard_id,
            allocation: activated_primary.allocation_id,
        };
        #[cfg(feature = "protocol-trace")]
        let trace_request = crate::protocol_trace::route_client_write(
            &self.local_node_id,
            &activated_primary.index_uuid,
            req.shard_id,
            &self.local_node_id,
            &doc_id,
            &crate::engine::DocumentMutation::Index {
                doc_id: doc_id.clone(),
                source: payload.clone(),
            },
        );

        info!(
            "gRPC: index doc '{}' into {}/shard_{}",
            doc_id, req.index_name, req.shard_id
        );

        let write_result = match self.shard_manager.ensure_local_apply_allowed(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
        ) {
            Ok(()) => {
                let engine = engine.clone();
                let doc_id = doc_id.clone();
                let payload = payload.clone();
                let primary_term = activated_primary.primary_term;
                let cluster_manager = self.cluster_manager.clone();
                let index_name = req.index_name.clone();
                let expected_uuid = req.index_uuid.clone();
                #[cfg(feature = "protocol-trace")]
                let trace_copy = trace_copy.clone();
                #[cfg(feature = "protocol-trace")]
                let trace_request = trace_request.clone();
                self.worker_pools
                    .spawn_write(move || {
                        require_index_uuid(
                            &cluster_manager,
                            &index_name,
                            expected_uuid.as_deref(),
                        )?;
                        #[cfg(feature = "protocol-trace")]
                        {
                            crate::protocol_trace::with_open_copy(trace_copy, || {
                                crate::protocol_trace::with_request_tokens(
                                    trace_request.into_iter().collect(),
                                    || {
                                        crate::protocol_trace::with_apply_scope(
                                            crate::protocol_trace::ApplyOrigin::Primary,
                                            Vec::new(),
                                            || {
                                                engine.add_document_with_condition_at_term(
                                                    &doc_id,
                                                    payload,
                                                    primary_term,
                                                    condition,
                                                )
                                            },
                                        )
                                    },
                                )
                            })
                        }
                        #[cfg(not(feature = "protocol-trace"))]
                        engine.add_document_with_condition_at_term(
                            &doc_id,
                            payload,
                            primary_term,
                            condition,
                        )
                    })
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &write_result
            && error.is::<IndexIncarnationMismatchError>()
        {
            #[cfg(feature = "protocol-trace")]
            if let Some(trace_request) = trace_request.as_ref() {
                crate::protocol_trace::record_client_result(
                    trace_request,
                    &self.local_node_id,
                    &activated_primary.index_uuid,
                    req.shard_id,
                    "failed",
                    Some("index_not_found"),
                );
            }
            return Err(Status::not_found(error.to_string()));
        }
        let write_result = self.shard_manager.record_local_apply_result(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
            write_result,
        );

        match write_result {
            Ok(receipt) => {
                let created = receipt.created;
                let id = receipt.doc_id;
                let seq_no = receipt.seq_no;
                let primary_term = receipt.primary_term;
                let primary_sequence = engine.sequence_stats();
                self.spawn_primary_available_report_after_write(
                    &req.index_name,
                    req.shard_id,
                    &activated_primary,
                );

                // Replicate to replica shards with seq_no
                match crate::replication::replicate_write_with_durability(
                    &self.transport_client,
                    &write_state,
                    &req.index_name,
                    req.shard_id,
                    &id,
                    &payload,
                    "index",
                    seq_no,
                    primary_term,
                    self.shard_manager.durability(),
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        self.record_replica_checkpoints(
                            &engine,
                            &req.index_name,
                            req.shard_id,
                            &activated_primary,
                            primary_sequence,
                            &replica_checkpoints,
                        );
                        #[cfg(feature = "protocol-trace")]
                        if let Some(trace_request) = trace_request.as_ref() {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "acknowledged",
                                None,
                            );
                        }
                    }
                    Err(errors) => {
                        self.report_definitive_replica_failures(
                            &write_state,
                            &req.index_name,
                            req.shard_id,
                            primary_term,
                            &errors,
                        )
                        .await;
                        tracing::warn!(
                            "Replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        #[cfg(feature = "protocol-trace")]
                        if let Some(trace_request) = trace_request.as_ref() {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "failed",
                                Some("replication"),
                            );
                        }
                        return Ok(Response::new(ShardDocResponse {
                            success: false,
                            doc_id: id,
                            error: format!(
                                "Replication failed: {}",
                                Self::replication_failure_message(&errors)
                            ),
                            seq_no: Some(seq_no),
                            primary_term: Some(primary_term),
                            ..Default::default()
                        }));
                    }
                }
                crate::metrics::DOCS_INDEXED_TOTAL.inc();
                Ok(Response::new(ShardDocResponse {
                    success: true,
                    doc_id: id,
                    error: String::new(),
                    seq_no: Some(seq_no),
                    primary_term: Some(primary_term),
                    created,
                }))
            }
            Err(e) if e.is::<crate::engine::VersionConflictError>() => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("version_conflict"),
                    );
                }
                Err(Status::already_exists(e.to_string()))
            }
            Err(e) if crate::engine::is_write_validation_error(&e) => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::invalid_argument(e.to_string()))
            }
            Err(e) if e.is::<crate::engine::version_map::VersionMapCapacityError>() => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::resource_exhausted(format!(
                    "{}{e}",
                    crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX
                )))
            }
            Err(e) => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                self.report_local_copy_failure(
                    &req.index_name,
                    &activated_primary.index_uuid,
                    req.shard_id,
                    activated_primary.allocation_id,
                    activated_primary.primary_term,
                    &e,
                )
                .await;
                Ok(Response::new(ShardDocResponse {
                    success: false,
                    doc_id: String::new(),
                    error: e.to_string(),
                    seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }))
            }
        }
    }

    async fn bulk_index(
        &self,
        request: Request<ShardBulkRequest>,
    ) -> Result<Response<ShardBulkResponse>, Status> {
        let req = request.into_inner();
        if !req.operations.is_empty() {
            if req.operations.len() != req.documents_json.len() {
                return Err(Status::invalid_argument(
                    "bulk operation count does not match document count",
                ));
            }
            for operation in &req.operations {
                let kind = ShardBulkOpKind::try_from(operation.kind)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                primary_write_condition(
                    operation.if_seq_no,
                    operation.if_primary_term,
                    kind == ShardBulkOpKind::Create,
                )?;
            }
            if req.operations.iter().any(|operation| {
                operation.kind != ShardBulkOpKind::Index as i32 || operation.if_seq_no.is_some()
            }) {
                return bulk_writes::execute_ordered_bulk(self, req).await;
            }
        }

        let activated_primary = match self
            .ensure_primary_activated(&req.index_name, req.shard_id)
            .await
        {
            Ok(term) => term,
            Err(error) => {
                return Ok(Response::new(ShardBulkResponse {
                    success: false,
                    doc_ids: Vec::new(),
                    error,
                    start_seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        let _write_guard = match self
            .peer_recovery_write_guard(&req.index_name, req.shard_id)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                return Ok(Response::new(ShardBulkResponse {
                    success: false,
                    doc_ids: Vec::new(),
                    error,
                    start_seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        let _pre_mapping_write_state = match self.validated_primary_write_state(
            &req.index_name,
            req.shard_id,
            &activated_primary,
        ) {
            Ok(state) => state,
            Err(error) => {
                return Ok(Response::new(ShardBulkResponse {
                    success: false,
                    doc_ids: Vec::new(),
                    error,
                    start_seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };

        let mut docs: Vec<(String, serde_json::Value)> =
            Vec::with_capacity(req.documents_json.len());
        for b in &req.documents_json {
            let mut val: serde_json::Value = serde_json::from_slice(b)
                .map_err(|e| Status::invalid_argument(format!("invalid JSON in bulk: {e}")))?;
            let (doc_id, payload) = if val.get("_doc_id").is_some_and(serde_json::Value::is_string)
                && val.get("_source").is_some()
            {
                let serde_json::Value::String(doc_id) = val["_doc_id"].take() else {
                    unreachable!("bulk envelope document ID was checked as a string");
                };
                (doc_id, val["_source"].take())
            } else {
                (uuid::Uuid::new_v4().to_string(), val)
            };
            crate::common::validate_document_source(&payload)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            docs.push((doc_id, payload));
        }

        // Dynamic mapping (batch): detect/register before opening the live
        // engine so a successful AddMappings commit can reopen safely.
        let dynamic_override = self
            .ensure_dynamic_mappings_batch(&req.index_name, req.shard_id, &docs)
            .await?;
        let write_state = match self.validated_primary_write_state(
            &req.index_name,
            req.shard_id,
            &activated_primary,
        ) {
            Ok(state) => state,
            Err(error) => {
                return Ok(Response::new(ShardBulkResponse {
                    success: false,
                    doc_ids: Vec::new(),
                    error,
                    start_seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }));
            }
        };
        let engine = self
            .get_or_open_shard_with_override(&req.index_name, req.shard_id, dynamic_override)
            .await?;
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::TraceCopy {
            node: self.local_node_id.clone(),
            index_uuid: activated_primary.index_uuid.clone(),
            shard: req.shard_id,
            allocation: activated_primary.allocation_id,
        };
        #[cfg(feature = "protocol-trace")]
        let trace_requests = docs
            .iter()
            .filter_map(|(doc_id, payload)| {
                crate::protocol_trace::route_client_write(
                    &self.local_node_id,
                    &activated_primary.index_uuid,
                    req.shard_id,
                    &self.local_node_id,
                    doc_id,
                    &crate::engine::DocumentMutation::Index {
                        doc_id: doc_id.clone(),
                        source: payload.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();

        trace!(
            "gRPC: bulk {} docs into {}/shard_{}",
            docs.len(),
            req.index_name,
            req.shard_id
        );

        let write_result = match self.shard_manager.ensure_local_apply_allowed(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
        ) {
            Ok(()) => {
                let engine = engine.clone();
                let docs_for_write = docs.clone();
                let primary_term = activated_primary.primary_term;
                #[cfg(feature = "protocol-trace")]
                let trace_copy = trace_copy.clone();
                #[cfg(feature = "protocol-trace")]
                let trace_requests = trace_requests.clone();
                self.worker_pools
                    .spawn_write(move || {
                        #[cfg(feature = "protocol-trace")]
                        {
                            crate::protocol_trace::with_open_copy(trace_copy, || {
                                crate::protocol_trace::with_request_tokens(trace_requests, || {
                                    crate::protocol_trace::with_apply_scope(
                                        crate::protocol_trace::ApplyOrigin::Primary,
                                        Vec::new(),
                                        || {
                                            engine.bulk_add_documents_with_receipt_at_term(
                                                docs_for_write,
                                                primary_term,
                                            )
                                        },
                                    )
                                })
                            })
                        }
                        #[cfg(not(feature = "protocol-trace"))]
                        engine.bulk_add_documents_with_receipt_at_term(docs_for_write, primary_term)
                    })
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            Err(error) => Err(error),
        };
        let write_result = self.shard_manager.record_local_apply_result(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
            write_result,
        );

        match write_result {
            Ok(receipt) => {
                let last_seq_no = receipt
                    .last_seq_no()
                    .map_err(|e| Status::internal(e.to_string()))?;
                let results = bulk_writes::index_batch_results(&receipt)
                    .map_err(|error| Status::internal(error.to_string()))?;
                let ids = receipt.doc_ids;
                let primary_term = receipt.primary_term;
                let primary_sequence = engine.sequence_stats();
                let Some(start_seq_no) = receipt.start_seq_no else {
                    return Ok(Response::new(ShardBulkResponse {
                        success: true,
                        doc_ids: ids,
                        error: String::new(),
                        start_seq_no: None,
                        primary_term: Some(primary_term),
                        results,
                    }));
                };
                last_seq_no.ok_or_else(|| {
                    Status::internal("non-empty bulk receipt has no last sequence")
                })?;
                self.spawn_primary_available_report_after_write(
                    &req.index_name,
                    req.shard_id,
                    &activated_primary,
                );
                // Replicate to replica shards
                match crate::replication::replicate_bulk_with_durability(
                    &self.transport_client,
                    &write_state,
                    &req.index_name,
                    req.shard_id,
                    &docs,
                    start_seq_no,
                    primary_term,
                    self.shard_manager.durability(),
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        self.record_replica_checkpoints(
                            &engine,
                            &req.index_name,
                            req.shard_id,
                            &activated_primary,
                            primary_sequence,
                            &replica_checkpoints,
                        );
                        #[cfg(feature = "protocol-trace")]
                        for trace_request in &trace_requests {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "acknowledged",
                                None,
                            );
                        }
                    }
                    Err(errors) => {
                        self.report_definitive_replica_failures(
                            &write_state,
                            &req.index_name,
                            req.shard_id,
                            primary_term,
                            &errors,
                        )
                        .await;
                        tracing::warn!(
                            "Bulk replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        #[cfg(feature = "protocol-trace")]
                        for trace_request in &trace_requests {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "failed",
                                Some("replication"),
                            );
                        }
                        return Ok(Response::new(ShardBulkResponse {
                            success: false,
                            doc_ids: ids,
                            error: format!(
                                "Replication failed: {}",
                                Self::replication_failure_message(&errors)
                            ),
                            start_seq_no: Some(start_seq_no),
                            primary_term: Some(primary_term),
                            ..Default::default()
                        }));
                    }
                }
                let doc_count = ids.len() as u64;
                crate::metrics::BULK_DOCS_TOTAL.inc_by(doc_count);
                Ok(Response::new(ShardBulkResponse {
                    success: true,
                    doc_ids: ids,
                    error: String::new(),
                    start_seq_no: Some(start_seq_no),
                    primary_term: Some(primary_term),
                    results,
                }))
            }
            Err(e) if crate::engine::is_write_validation_error(&e) => {
                #[cfg(feature = "protocol-trace")]
                for trace_request in &trace_requests {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::invalid_argument(e.to_string()))
            }
            Err(e) if e.is::<crate::engine::version_map::VersionMapCapacityError>() => {
                #[cfg(feature = "protocol-trace")]
                for trace_request in &trace_requests {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::resource_exhausted(format!(
                    "{}{e}",
                    crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX
                )))
            }
            Err(e) => {
                #[cfg(feature = "protocol-trace")]
                for trace_request in &trace_requests {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                self.report_local_copy_failure(
                    &req.index_name,
                    &activated_primary.index_uuid,
                    req.shard_id,
                    activated_primary.allocation_id,
                    activated_primary.primary_term,
                    &e,
                )
                .await;
                Ok(Response::new(ShardBulkResponse {
                    success: false,
                    doc_ids: vec![],
                    error: e.to_string(),
                    start_seq_no: None,
                    primary_term: None,
                    ..Default::default()
                }))
            }
        }
    }

    async fn delete_doc(
        &self,
        request: Request<ShardDeleteRequest>,
    ) -> Result<Response<ShardDeleteResponse>, Status> {
        let req = request.into_inner();
        let condition = primary_write_condition(req.if_seq_no, req.if_primary_term, false)?;
        let activated_primary = match self
            .ensure_primary_activated(&req.index_name, req.shard_id)
            .await
        {
            Ok(term) => term,
            Err(error) => {
                return Ok(Response::new(ShardDeleteResponse {
                    success: false,
                    deleted: 0,
                    error,
                    seq_no: None,
                    primary_term: None,
                }));
            }
        };
        let _write_guard = match self
            .peer_recovery_write_guard(&req.index_name, req.shard_id)
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                return Ok(Response::new(ShardDeleteResponse {
                    success: false,
                    deleted: 0,
                    error,
                    seq_no: None,
                    primary_term: None,
                }));
            }
        };
        let write_state = match self.validated_primary_write_state(
            &req.index_name,
            req.shard_id,
            &activated_primary,
        ) {
            Ok(state) => state,
            Err(error) => {
                return Ok(Response::new(ShardDeleteResponse {
                    success: false,
                    deleted: 0,
                    error,
                    seq_no: None,
                    primary_term: None,
                }));
            }
        };
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::TraceCopy {
            node: self.local_node_id.clone(),
            index_uuid: activated_primary.index_uuid.clone(),
            shard: req.shard_id,
            allocation: activated_primary.allocation_id,
        };
        #[cfg(feature = "protocol-trace")]
        let trace_request = crate::protocol_trace::route_client_write(
            &self.local_node_id,
            &activated_primary.index_uuid,
            req.shard_id,
            &self.local_node_id,
            &req.doc_id,
            &crate::engine::DocumentMutation::Delete {
                doc_id: req.doc_id.clone(),
            },
        );
        info!(
            "gRPC: delete doc '{}' from {}/shard_{}",
            req.doc_id, req.index_name, req.shard_id
        );

        let delete_result = match self.shard_manager.ensure_local_apply_allowed(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
        ) {
            Ok(()) => {
                let engine = engine.clone();
                let doc_id = req.doc_id.clone();
                let primary_term = activated_primary.primary_term;
                #[cfg(feature = "protocol-trace")]
                let trace_copy = trace_copy.clone();
                #[cfg(feature = "protocol-trace")]
                let trace_request = trace_request.clone();
                self.worker_pools
                    .spawn_write(move || {
                        #[cfg(feature = "protocol-trace")]
                        {
                            crate::protocol_trace::with_open_copy(trace_copy, || {
                                crate::protocol_trace::with_request_tokens(
                                    trace_request.into_iter().collect(),
                                    || {
                                        crate::protocol_trace::with_apply_scope(
                                            crate::protocol_trace::ApplyOrigin::Primary,
                                            Vec::new(),
                                            || {
                                                engine.delete_document_with_condition_at_term(
                                                    &doc_id,
                                                    primary_term,
                                                    condition,
                                                )
                                            },
                                        )
                                    },
                                )
                            })
                        }
                        #[cfg(not(feature = "protocol-trace"))]
                        engine.delete_document_with_condition_at_term(
                            &doc_id,
                            primary_term,
                            condition,
                        )
                    })
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            Err(error) => Err(error),
        };
        let delete_result = self.shard_manager.record_local_apply_result(
            &activated_primary.index_uuid,
            req.shard_id,
            activated_primary.allocation_id,
            delete_result,
        );

        match delete_result {
            Ok(receipt) => {
                let deleted = receipt.deleted;
                let seq_no = receipt.seq_no;
                let primary_term = receipt.primary_term;
                let primary_sequence = engine.sequence_stats();
                self.spawn_primary_available_report_after_write(
                    &req.index_name,
                    req.shard_id,
                    &activated_primary,
                );
                // Replicate delete to replica shards
                match crate::replication::replicate_write_with_durability(
                    &self.transport_client,
                    &write_state,
                    &req.index_name,
                    req.shard_id,
                    &req.doc_id,
                    &serde_json::json!({}),
                    "delete",
                    seq_no,
                    primary_term,
                    self.shard_manager.durability(),
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        self.record_replica_checkpoints(
                            &engine,
                            &req.index_name,
                            req.shard_id,
                            &activated_primary,
                            primary_sequence,
                            &replica_checkpoints,
                        );
                        #[cfg(feature = "protocol-trace")]
                        if let Some(trace_request) = trace_request.as_ref() {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "acknowledged",
                                None,
                            );
                        }
                    }
                    Err(errors) => {
                        self.report_definitive_replica_failures(
                            &write_state,
                            &req.index_name,
                            req.shard_id,
                            primary_term,
                            &errors,
                        )
                        .await;
                        tracing::warn!(
                            "Delete replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        #[cfg(feature = "protocol-trace")]
                        if let Some(trace_request) = trace_request.as_ref() {
                            crate::protocol_trace::record_client_result(
                                trace_request,
                                &self.local_node_id,
                                &activated_primary.index_uuid,
                                req.shard_id,
                                "failed",
                                Some("replication"),
                            );
                        }
                        return Ok(Response::new(ShardDeleteResponse {
                            success: false,
                            deleted,
                            error: format!(
                                "Replication failed: {}",
                                Self::replication_failure_message(&errors)
                            ),
                            seq_no: Some(seq_no),
                            primary_term: Some(primary_term),
                        }));
                    }
                }
                Ok(Response::new(ShardDeleteResponse {
                    success: true,
                    deleted,
                    error: String::new(),
                    seq_no: Some(seq_no),
                    primary_term: Some(primary_term),
                }))
            }
            Err(e) if e.is::<crate::engine::VersionConflictError>() => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("version_conflict"),
                    );
                }
                Err(Status::already_exists(e.to_string()))
            }
            Err(e) if crate::engine::is_write_validation_error(&e) => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::invalid_argument(e.to_string()))
            }
            Err(e) if e.is::<crate::engine::version_map::VersionMapCapacityError>() => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                Err(Status::resource_exhausted(format!(
                    "{}{e}",
                    crate::engine::version_map::VERSION_MAP_CAPACITY_STATUS_PREFIX
                )))
            }
            Err(e) => {
                #[cfg(feature = "protocol-trace")]
                if let Some(trace_request) = trace_request.as_ref() {
                    crate::protocol_trace::record_client_result(
                        trace_request,
                        &self.local_node_id,
                        &activated_primary.index_uuid,
                        req.shard_id,
                        "failed",
                        Some("primary_apply"),
                    );
                }
                self.report_local_copy_failure(
                    &req.index_name,
                    &activated_primary.index_uuid,
                    req.shard_id,
                    activated_primary.allocation_id,
                    activated_primary.primary_term,
                    &e,
                )
                .await;
                Ok(Response::new(ShardDeleteResponse {
                    success: false,
                    deleted: 0,
                    error: e.to_string(),
                    seq_no: None,
                    primary_term: None,
                }))
            }
        }
    }

    async fn get_doc(
        &self,
        request: Request<ShardGetRequest>,
    ) -> Result<Response<ShardGetResponse>, Status> {
        let req = request.into_inner();
        let index_uuid = self
            .cluster_manager
            .get_state()
            .indices
            .get(&req.index_name)
            .map(|metadata| metadata.uuid.to_string())
            .ok_or_else(|| Status::not_found(format!("no such index [{}]", req.index_name)))?;
        let engine = match self
            .get_or_open_search_shard(&req.index_name, req.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(status) => {
                return Ok(Response::new(ShardGetResponse {
                    found: false,
                    source_json: vec![],
                    error: status.message().to_string(),
                    ..Default::default()
                }));
            }
        };
        let doc_result = {
            let engine = engine.clone();
            let doc_id = req.doc_id.clone();
            self.worker_pools
                .spawn_search(move || {
                    engine.get_document_with_metadata(&doc_id, req.realtime.unwrap_or(true))
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };
        require_index_uuid(&self.cluster_manager, &req.index_name, Some(&index_uuid))
            .map_err(|error| Status::not_found(error.to_string()))?;

        match doc_result {
            Ok(Some(document)) => {
                let source_json = serde_json::to_vec(&document.source)
                    .map_err(|e| Status::internal(format!("serialize get_doc response: {e}")))?;
                Ok(Response::new(ShardGetResponse {
                    found: true,
                    source_json,
                    error: String::new(),
                    seq_no: Some(document.seq_no),
                    primary_term: Some(document.primary_term),
                    index_uuid,
                }))
            }
            Ok(None) => Ok(Response::new(ShardGetResponse {
                found: false,
                source_json: vec![],
                error: String::new(),
                index_uuid,
                ..Default::default()
            })),
            Err(e) => Ok(Response::new(ShardGetResponse {
                found: false,
                source_json: vec![],
                error: e.to_string(),
                ..Default::default()
            })),
        }
    }

    async fn search_shard(
        &self,
        request: Request<ShardSearchRequest>,
    ) -> Result<Response<ShardSearchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self
            .get_or_open_search_shard(&req.index_name, req.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(status) => {
                return Ok(Response::new(ShardSearchResponse {
                    success: false,
                    hits: vec![],
                    error: status.message().to_string(),
                    total_hits: 0,
                    partial_aggs_json: vec![],
                }));
            }
        };

        let search_result = {
            let engine = engine.clone();
            let query = req.query.clone();
            self.worker_pools
                .spawn_search(move || engine.search(&query))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match search_result {
            Ok(hits) => {
                let hits = hits
                    .into_iter()
                    .map(|v| {
                        Ok::<SearchHit, serde_json::Error>(SearchHit {
                            source_json: serde_json::to_vec(&v)?,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Status::internal(format!("serialize search hit: {e}")))?;

                Ok(Response::new(ShardSearchResponse {
                    success: true,
                    hits,
                    error: String::new(),
                    total_hits: 0,
                    partial_aggs_json: vec![],
                }))
            }
            Err(e) => Ok(Response::new(ShardSearchResponse {
                success: false,
                hits: vec![],
                error: e.to_string(),
                total_hits: 0,
                partial_aggs_json: vec![],
            })),
        }
    }

    async fn search_shard_dsl(
        &self,
        request: Request<ShardSearchDslRequest>,
    ) -> Result<Response<ShardSearchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self
            .get_or_open_search_shard(&req.index_name, req.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(status) => {
                return Ok(Response::new(ShardSearchResponse {
                    success: false,
                    hits: vec![],
                    error: status.message().to_string(),
                    total_hits: 0,
                    partial_aggs_json: vec![],
                }));
            }
        };

        let search_req: crate::search::SearchRequest =
            serde_json::from_slice(&req.search_request_json).map_err(|e| {
                Status::invalid_argument(format!("invalid SearchRequest JSON: {e}"))
            })?;

        let mut all_hits = Vec::new();
        let mut total_hits: usize = 0;

        // Run all blocking engine work on the search pool
        let search_result = {
            let engine = engine.clone();
            let search_req = search_req.clone();
            self.worker_pools
                .spawn_search(move || -> crate::common::Result<(Vec<serde_json::Value>, usize, std::collections::HashMap<String, crate::search::PartialAggResult>, Vec<serde_json::Value>)> {
                    let (hits, total, partial_aggs) = engine.search_query(&search_req)?;
                    let mut knn_hits = Vec::new();
                    if let Some(ref knn) = search_req.knn
                        && let Some((field_name, params)) = knn.fields.iter().next()
                    {
                        match engine.search_knn_filtered(
                            field_name,
                            &params.vector,
                            params.k,
                            params.filter.as_ref(),
                        ) {
                            Ok(h) => knn_hits = h,
                            Err(e) => tracing::error!("Vector search on remote shard failed: {}", e),
                        }
                    }
                    Ok((hits, total, partial_aggs, knn_hits))
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match search_result {
            Ok((hits, total, partial_aggs, knn_hits)) => {
                total_hits += total;
                all_hits.extend(hits);
                all_hits.extend(knn_hits);

                let aggs_json = if partial_aggs.is_empty() {
                    vec![]
                } else {
                    crate::search::encode_partial_aggs(&partial_aggs)
                        .map_err(|e| Status::internal(format!("encode partial aggs: {e}")))?
                };

                let hits = all_hits
                    .into_iter()
                    .map(|v| {
                        Ok::<SearchHit, serde_json::Error>(SearchHit {
                            source_json: serde_json::to_vec(&v)?,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Status::internal(format!("serialize search hit: {e}")))?;

                Ok(Response::new(ShardSearchResponse {
                    success: true,
                    hits,
                    error: String::new(),
                    total_hits: total_hits as u64,
                    partial_aggs_json: aggs_json,
                }))
            }
            Err(e) => Ok(Response::new(ShardSearchResponse {
                success: false,
                hits: vec![],
                error: e.to_string(),
                total_hits: 0,
                partial_aggs_json: vec![],
            })),
        }
    }

    async fn get_remote_store_leaf_status(
        &self,
        request: Request<RemoteStoreLeafStatusRequest>,
    ) -> Result<Response<RemoteStoreLeafStatusResponse>, Status> {
        let req = request.into_inner();
        let split_plans: Vec<_> = req
            .splits
            .into_iter()
            .map(|split| crate::engine::remote_store::AssignedRemoteSplit {
                split_id: split.split_id,
                bundle_path: split.bundle_path,
                checksum: split.checksum,
                size_bytes: split.size_bytes,
            })
            .collect();
        let cluster_state = self.cluster_manager.get_state();
        let status = crate::engine::remote_store::local_leaf_status_snapshot(
            &cluster_state,
            &self.local_node_id,
            self.storage_manager.as_ref(),
            self.remote_store_reader_cache.as_ref(),
            &req.index_uuid,
            &split_plans,
        );

        Ok(Response::new(RemoteStoreLeafStatusResponse {
            root_capable: status.root_capable,
            leaf_capable: status.leaf_capable,
            inflight_bytes: status.inflight_bytes,
            queue_depth: status.queue_depth as u32,
            split_statuses: status
                .split_statuses
                .into_iter()
                .map(|(split_id, warmth)| RemoteStoreSplitCacheStatus {
                    split_id,
                    artifact_cached: warmth.artifact_cached,
                    reader_cached: warmth.reader_cached,
                })
                .collect(),
        }))
    }

    async fn search_remote_store_splits(
        &self,
        request: Request<RemoteStoreSearchRequest>,
    ) -> Result<Response<RemoteStoreSearchResponse>, Status> {
        let req = request.into_inner();
        let metadata = self
            .cluster_manager
            .get_state()
            .indices
            .get(&req.index_name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("index [{}] not found", req.index_name)))?;
        if metadata.uuid.as_str() != req.index_uuid {
            return Err(Status::invalid_argument(format!(
                "remote_store UUID mismatch for [{}]: request={} cluster={}",
                req.index_name, req.index_uuid, metadata.uuid
            )));
        }
        if !matches!(
            metadata.settings.engine,
            crate::cluster::state::IndexEngine::RemoteStore
        ) {
            return Err(Status::invalid_argument(format!(
                "index [{}] uses engine [{}], not remote_store",
                req.index_name, metadata.settings.engine
            )));
        }

        let search_req: crate::search::SearchRequest =
            serde_json::from_slice(&req.search_request_json).map_err(|e| {
                Status::invalid_argument(format!("invalid SearchRequest JSON: {e}"))
            })?;
        let split_plans: Vec<_> = req
            .splits
            .into_iter()
            .map(|split| crate::engine::remote_store::AssignedRemoteSplit {
                split_id: split.split_id,
                bundle_path: split.bundle_path,
                checksum: split.checksum,
                size_bytes: split.size_bytes,
            })
            .collect();
        let live_split_ids: HashSet<String> = req.live_split_ids.into_iter().collect();
        let context = crate::engine::remote_store::LeafExecutionContext {
            worker_pools: self.worker_pools.clone(),
            storage_manager: self.storage_manager.clone(),
            reader_cache: self.remote_store_reader_cache.clone(),
        };

        let outcomes = crate::engine::remote_store::execute_leaf_search_batch(
            &context,
            &metadata,
            &search_req,
            &split_plans,
            &live_split_ids,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let mut results = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            let mut hits = Vec::with_capacity(outcome.hits.len());
            for value in outcome.hits {
                hits.push(SearchHit {
                    source_json: serde_json::to_vec(&value).map_err(|e| {
                        Status::internal(format!("serialize remote_store hit: {e}"))
                    })?,
                });
            }
            let partial_aggs_json = if outcome.partial_aggs.is_empty() {
                Vec::new()
            } else {
                vec![
                    crate::search::encode_partial_aggs(&outcome.partial_aggs)
                        .map_err(|e| Status::internal(format!("encode partial aggs: {e}")))?,
                ]
            };
            results.push(RemoteStoreSplitSearchResult {
                split_id: outcome.split_id,
                success: outcome.error.is_none(),
                hits,
                total_hits: outcome.total_hits as u64,
                partial_aggs_json,
                error: outcome.error.unwrap_or_default(),
            });
        }

        Ok(Response::new(RemoteStoreSearchResponse { results }))
    }

    async fn sql_record_batch(
        &self,
        request: Request<SqlRecordBatchRequest>,
    ) -> Result<Response<SqlRecordBatchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self
            .get_or_open_search_shard(&req.index_name, req.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(status) => {
                return Ok(Response::new(SqlRecordBatchResponse {
                    success: false,
                    arrow_ipc: vec![],
                    total_hits: 0,
                    error: status.message().to_string(),
                    collected_rows: 0,
                    streaming_used: false,
                }));
            }
        };

        let search_req: crate::search::SearchRequest =
            serde_json::from_slice(&req.search_request_json).map_err(|e| {
                Status::invalid_argument(format!("invalid SearchRequest JSON: {e}"))
            })?;

        let columns: Vec<String> = req.columns;

        let batch_result = {
            let engine = engine.clone();
            let search_req = search_req.clone();
            let columns = columns.clone();
            let needs_id = req.needs_id;
            let needs_score = req.needs_score;
            self.worker_pools
                .spawn_search(move || {
                    engine.sql_record_batch(&search_req, &columns, needs_id, needs_score)
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match batch_result {
            Ok(Some(batch_result)) => {
                let collected_rows = batch_result.batch.num_rows();
                Ok(Response::new(
                    sql_batch_success_response(
                        batch_result.batch,
                        batch_result.total_hits,
                        collected_rows,
                        false,
                    )
                    .map_err(|error| Status::internal(error.to_string()))?,
                ))
            }
            Ok(None) => Ok(Response::new(sql_batch_error_response(
                "shard does not support sql_record_batch",
            ))),
            Err(e) => Ok(Response::new(sql_batch_error_response(e.to_string()))),
        }
    }

    async fn sql_record_batch_stream(
        &self,
        request: Request<SqlRecordBatchRequest>,
    ) -> Result<Response<Self::SqlRecordBatchStreamStream>, Status> {
        enum SqlStreamSource {
            Lazy(crate::engine::SqlStreamingBatchHandle),
            Buffered {
                batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
                total_hits: usize,
                collected_rows: usize,
                streaming_used: bool,
            },
        }

        let req = request.into_inner();
        let engine = match self
            .get_or_open_search_shard(&req.index_name, req.shard_id)
            .await
        {
            Ok(engine) => engine,
            Err(status) => {
                let response_stream: Self::SqlRecordBatchStreamStream = Box::pin(stream::iter(
                    vec![Ok(sql_batch_error_response(status.message().to_string()))],
                ));
                return Ok(Response::new(response_stream));
            }
        };

        let search_req: crate::search::SearchRequest =
            serde_json::from_slice(&req.search_request_json).map_err(|e| {
                Status::invalid_argument(format!("invalid SearchRequest JSON: {e}"))
            })?;

        let columns = req.columns;
        let batch_size = req.batch_size as usize;
        let stream_result = {
            let engine = engine.clone();
            let search_req = search_req.clone();
            let columns = columns.clone();
            let needs_id = req.needs_id;
            let needs_score = req.needs_score;
            self.worker_pools
                .spawn_search(move || {
                    match engine.sql_streaming_batch_handle(
                        &search_req,
                        &columns,
                        needs_id,
                        needs_score,
                        batch_size,
                    )? {
                        Some(handle) => Ok(Some(SqlStreamSource::Lazy(handle))),
                        None => engine
                            .sql_record_batch(&search_req, &columns, needs_id, needs_score)
                            .map(|opt| {
                                opt.map(|r| {
                                    let collected_rows = r.batch.num_rows();
                                    SqlStreamSource::Buffered {
                                        batches: vec![r.batch],
                                        total_hits: r.total_hits,
                                        collected_rows,
                                        streaming_used: false,
                                    }
                                })
                            }),
                    }
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        // Lazily encode each batch to IPC as it is consumed by the gRPC
        // transport. The streaming-handle path also keeps raw RecordBatches
        // lazy on the shard instead of draining them into a Vec upfront.
        let response_stream: Self::SqlRecordBatchStreamStream = match stream_result {
            Ok(Some(SqlStreamSource::Lazy(handle))) => {
                let total_hits = handle.total_hits;
                let collected_rows = handle.collected_rows;
                let worker_pools = self.worker_pools.clone();
                let handle = Arc::new(std::sync::Mutex::new(handle));
                Box::pin(stream::try_unfold(
                    (handle, worker_pools, total_hits, collected_rows),
                    |(handle, worker_pools, total_hits, collected_rows)| async move {
                        let handle_for_batch = handle.clone();
                        let next_batch = worker_pools
                            .clone()
                            .spawn_search(move || {
                                let mut handle =
                                    handle_for_batch.lock().unwrap_or_else(|e| e.into_inner());
                                handle.next_batch()
                            })
                            .await
                            .map_err(|e| Status::internal(e.to_string()))?;

                        match next_batch {
                            Ok(Some(batch)) => {
                                let item = sql_batch_success_response(
                                    batch,
                                    total_hits,
                                    collected_rows,
                                    true,
                                )
                                .map_err(|error| Status::internal(error.to_string()))?;
                                Ok(Some((
                                    item,
                                    (handle, worker_pools, total_hits, collected_rows),
                                )))
                            }
                            Ok(None) => Ok(None),
                            Err(error) => Err(Status::internal(error.to_string())),
                        }
                    },
                ))
            }
            Ok(Some(SqlStreamSource::Buffered {
                batches,
                total_hits,
                collected_rows,
                streaming_used,
            })) => Box::pin(stream::unfold(
                (
                    batches.into_iter(),
                    total_hits,
                    collected_rows,
                    streaming_used,
                ),
                |(mut batches, total_hits, collected_rows, streaming_used)| async move {
                    let batch = batches.next()?;
                    let item = sql_batch_success_response(
                        batch,
                        total_hits,
                        collected_rows,
                        streaming_used,
                    )
                    .map_err(|error| Status::internal(error.to_string()));
                    Some((item, (batches, total_hits, collected_rows, streaming_used)))
                },
            )),
            Ok(None) => Box::pin(stream::iter(vec![Ok(sql_batch_error_response(
                "shard does not support sql_record_batch_stream",
            ))])),
            Err(e) => Box::pin(stream::iter(vec![Ok(sql_batch_error_response(
                e.to_string(),
            ))])),
        };
        Ok(Response::new(response_stream))
    }

    async fn replicate_doc(
        &self,
        request: Request<ReplicateDocRequest>,
    ) -> Result<Response<ReplicateDocResponse>, Status> {
        let req = request.into_inner();
        if req.index_uuid.is_empty() {
            return Err(Status::invalid_argument(
                "replication requires an index UUID",
            ));
        }
        let primary_term = req
            .primary_term
            .filter(|term| *term > 0)
            .ok_or_else(|| Status::invalid_argument("replication requires a primary term"))?;
        let allocation_id = req
            .target_allocation_id
            .filter(|allocation_id| *allocation_id > 0)
            .ok_or_else(|| {
                Status::invalid_argument("replication requires a target allocation ID")
            })?;
        #[cfg(feature = "protocol-trace")]
        let trace_seq_nos = [req.seq_no];
        let index_source = if req.op == "index" {
            Some(parse_replica_index_source(
                &req.payload_json,
                "invalid JSON",
            )?)
        } else {
            None
        };
        let assigned = match self.replica_apply_routing(
            &req.index_name,
            req.shard_id,
            &req.index_uuid,
            allocation_id,
        ) {
            Ok(assigned) => assigned,
            Err(error) => {
                #[cfg(feature = "protocol-trace")]
                self.record_protocol_trace_replica_rejection(
                    &req.index_uuid,
                    req.shard_id,
                    allocation_id,
                    primary_term,
                    &trace_seq_nos,
                    "identity_mismatch",
                )
                .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
                return Ok(Response::new(ReplicateDocResponse {
                    success: false,
                    error,
                    processed_checkpoint: None,
                    persisted_checkpoint: None,
                    operation_processed: false,
                    operation_persisted: false,
                }));
            }
        };
        if self
            .shard_manager
            .rejects_live_replication(&req.index_name, req.shard_id)
        {
            #[cfg(feature = "protocol-trace")]
            self.record_protocol_trace_replica_rejection(
                &req.index_uuid,
                req.shard_id,
                allocation_id,
                primary_term,
                &trace_seq_nos,
                "recovery_gate",
            )
            .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
            return Ok(Response::new(ReplicateDocResponse {
                success: false,
                error: "replica is installing a peer recovery snapshot".to_string(),
                processed_checkpoint: None,
                persisted_checkpoint: None,
                operation_processed: false,
                operation_persisted: false,
            }));
        }
        let assigned_uuid = assigned.index_uuid.clone();
        let assigned_authoritative = assigned.authoritative;
        if let Err(error) = self
            .shard_manager
            .open_assigned_shard_with_settings_blocking(
                req.index_name.clone(),
                req.shard_id,
                assigned.mappings,
                assigned.settings,
                assigned_uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: assigned.primary_term,
                    allow_empty_creation: assigned.allow_empty_creation,
                },
            )
            .await
        {
            #[cfg(feature = "protocol-trace")]
            self.record_protocol_trace_replica_rejection(
                &req.index_uuid,
                req.shard_id,
                allocation_id,
                primary_term,
                &trace_seq_nos,
                Self::protocol_trace_replica_rejection_reason(&error),
            )
            .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
            if assigned_authoritative {
                self.report_local_copy_failure(
                    &req.index_name,
                    &assigned_uuid,
                    req.shard_id,
                    allocation_id,
                    primary_term,
                    &error,
                )
                .await;
            }
            if error.is::<crate::shard::CollisionQuarantinedShardCopy>() {
                return Err(Status::data_loss(error.to_string()));
            }
            return Ok(Response::new(ReplicateDocResponse {
                success: false,
                error: format!("failed to open replica copy: {error}"),
                processed_checkpoint: None,
                persisted_checkpoint: None,
                operation_processed: false,
                operation_persisted: false,
            }));
        }

        info!(
            "gRPC: replicate {} doc '{}' (seq_no={}) to {}/shard_{}",
            req.op, req.doc_id, req.seq_no, req.index_name, req.shard_id
        );

        let operation = match req.op.as_str() {
            "index" => {
                let source = index_source.ok_or_else(|| {
                    Status::internal("validated replica index source is unavailable")
                })?;
                crate::engine::DocumentMutation::Index {
                    doc_id: req.doc_id.clone(),
                    source,
                }
            }
            "delete" => crate::engine::DocumentMutation::Delete {
                doc_id: req.doc_id.clone(),
            },
            "noop" => {
                let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
                    .map_err(|e| Status::invalid_argument(format!("invalid no-op JSON: {e}")))?;
                let reason = payload
                    .get("_reason")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| Status::invalid_argument("replication no-op has no _reason"))?;
                crate::engine::DocumentMutation::NoOp {
                    reason: reason.to_string(),
                }
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unknown replication op: {other}"
                )));
            }
        };
        let service = self.clone();
        let index_name = req.index_name.clone();
        let index_uuid = req.index_uuid.clone();
        let shard_id = req.shard_id;
        let seq_no = req.seq_no;
        let sequenced_operation = crate::engine::SequencedOperation {
            seq_no,
            primary_term,
            mutation: operation,
        };
        #[cfg(feature = "protocol-trace")]
        let trace_seq_nos = trace_seq_nos.to_vec();
        let result = self
            .worker_pools
            .spawn_write(move || {
                let current = match service.replica_apply_routing(
                    &index_name,
                    shard_id,
                    &index_uuid,
                    allocation_id,
                ) {
                    Ok(current) => current,
                    Err(error) => {
                        #[cfg(feature = "protocol-trace")]
                        service.record_protocol_trace_replica_rejection(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                            primary_term,
                            &trace_seq_nos,
                            "identity_mismatch",
                        )?;
                        return Err(anyhow::Error::msg(error));
                    }
                };
                #[cfg(feature = "protocol-trace")]
                {
                    let result = crate::protocol_trace::with_apply_scope(
                        crate::protocol_trace::ApplyOrigin::LiveReplication,
                        vec![sequenced_operation.clone()],
                        || {
                            service.shard_manager.apply_replica_operation(
                                &index_name,
                                shard_id,
                                crate::shard::ReplicaApplyContext {
                                    index_uuid: &index_uuid,
                                    allocation_id,
                                    applied_view_term: current.primary_term,
                                    message_term: primary_term,
                                },
                                |engine| engine.apply_replica_operation(sequenced_operation),
                            )
                        },
                    );
                    if let Err(error) = &result {
                        service.record_protocol_trace_replica_rejection(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                            primary_term,
                            &trace_seq_nos,
                            Self::protocol_trace_replica_rejection_reason(error),
                        )?;
                    }
                    result
                }
                #[cfg(not(feature = "protocol-trace"))]
                service.shard_manager.apply_replica_operation(
                    &index_name,
                    shard_id,
                    crate::shard::ReplicaApplyContext {
                        index_uuid: &index_uuid,
                        allocation_id,
                        applied_view_term: current.primary_term,
                        message_term: primary_term,
                    },
                    |engine| engine.apply_replica_operation(sequenced_operation),
                )
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(receipt) => Ok(Response::new(ReplicateDocResponse {
                success: true,
                error: String::new(),
                processed_checkpoint: receipt.sequence.processed_checkpoint,
                persisted_checkpoint: receipt.sequence.persisted_checkpoint,
                operation_processed: receipt.operation_processed,
                operation_persisted: receipt.operation_persisted,
            })),
            Err(e) if crate::engine::is_write_validation_error(&e) => {
                Err(Status::invalid_argument(e.to_string()))
            }
            Err(e) => {
                let definitive = ShardManager::is_definitive_copy_failure(&e);
                if assigned_authoritative {
                    self.report_local_copy_failure(
                        &req.index_name,
                        &assigned_uuid,
                        req.shard_id,
                        allocation_id,
                        primary_term,
                        &e,
                    )
                    .await;
                }
                if definitive {
                    return Err(Status::data_loss(e.to_string()));
                }
                let sequence = self
                    .shard_manager
                    .get_shard(&req.index_name, req.shard_id)
                    .map(|engine| engine.sequence_stats());
                Ok(Response::new(ReplicateDocResponse {
                    success: false,
                    error: e.to_string(),
                    processed_checkpoint: sequence
                        .as_ref()
                        .and_then(|stats| stats.processed_checkpoint),
                    persisted_checkpoint: sequence
                        .as_ref()
                        .and_then(|stats| stats.persisted_checkpoint),
                    operation_processed: false,
                    operation_persisted: false,
                }))
            }
        }
    }

    async fn replicate_bulk(
        &self,
        request: Request<ReplicateBulkRequest>,
    ) -> Result<Response<ReplicateBulkResponse>, Status> {
        let req = request.into_inner();
        #[cfg(test)]
        if !req.ops.is_empty() && req.ops.iter().all(|operation| operation.op == "noop") {
            self.primary_activation_state
                .promotion_noop_bulk_requests_received
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        if req.index_uuid.is_empty() {
            return Err(Status::invalid_argument(
                "bulk replication requires an index UUID",
            ));
        }
        let primary_term = req
            .primary_term
            .filter(|term| *term > 0)
            .ok_or_else(|| Status::invalid_argument("bulk replication requires a primary term"))?;
        let allocation_id = req
            .target_allocation_id
            .filter(|allocation_id| *allocation_id > 0)
            .ok_or_else(|| {
                Status::invalid_argument("bulk replication requires a target allocation ID")
            })?;
        #[cfg(feature = "protocol-trace")]
        let trace_seq_nos = req
            .ops
            .iter()
            .map(|operation| operation.seq_no)
            .collect::<Vec<_>>();
        let index_sources = req
            .ops
            .iter()
            .map(|operation| {
                if operation.op == "index" {
                    parse_replica_index_source(
                        &operation.payload_json,
                        "invalid JSON in bulk replicate",
                    )
                    .map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let assigned = match self.replica_apply_routing(
            &req.index_name,
            req.shard_id,
            &req.index_uuid,
            allocation_id,
        ) {
            Ok(assigned) => assigned,
            Err(error) => {
                #[cfg(feature = "protocol-trace")]
                self.record_protocol_trace_replica_rejection(
                    &req.index_uuid,
                    req.shard_id,
                    allocation_id,
                    primary_term,
                    &trace_seq_nos,
                    "identity_mismatch",
                )
                .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
                return Ok(Response::new(ReplicateBulkResponse {
                    success: false,
                    error,
                    processed_checkpoint: None,
                    persisted_checkpoint: None,
                    all_operations_processed: false,
                    all_operations_persisted: false,
                }));
            }
        };
        if self
            .shard_manager
            .rejects_live_replication(&req.index_name, req.shard_id)
        {
            #[cfg(feature = "protocol-trace")]
            self.record_protocol_trace_replica_rejection(
                &req.index_uuid,
                req.shard_id,
                allocation_id,
                primary_term,
                &trace_seq_nos,
                "recovery_gate",
            )
            .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
            return Ok(Response::new(ReplicateBulkResponse {
                success: false,
                error: "replica is installing a peer recovery snapshot".to_string(),
                processed_checkpoint: None,
                persisted_checkpoint: None,
                all_operations_processed: false,
                all_operations_persisted: false,
            }));
        }
        let assigned_uuid = assigned.index_uuid.clone();
        let assigned_authoritative = assigned.authoritative;
        if let Err(error) = self
            .shard_manager
            .open_assigned_shard_with_settings_blocking(
                req.index_name.clone(),
                req.shard_id,
                assigned.mappings,
                assigned.settings,
                assigned_uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: assigned.primary_term,
                    allow_empty_creation: assigned.allow_empty_creation,
                },
            )
            .await
        {
            #[cfg(feature = "protocol-trace")]
            self.record_protocol_trace_replica_rejection(
                &req.index_uuid,
                req.shard_id,
                allocation_id,
                primary_term,
                &trace_seq_nos,
                Self::protocol_trace_replica_rejection_reason(&error),
            )
            .map_err(|trace_error| Status::internal(trace_error.to_string()))?;
            if assigned_authoritative {
                self.report_local_copy_failure(
                    &req.index_name,
                    &assigned_uuid,
                    req.shard_id,
                    allocation_id,
                    primary_term,
                    &error,
                )
                .await;
            }
            if error.is::<crate::shard::CollisionQuarantinedShardCopy>() {
                return Err(Status::data_loss(error.to_string()));
            }
            return Ok(Response::new(ReplicateBulkResponse {
                success: false,
                error: format!("failed to open replica copy: {error}"),
                processed_checkpoint: None,
                persisted_checkpoint: None,
                all_operations_processed: false,
                all_operations_persisted: false,
            }));
        }

        info!(
            "gRPC: replicate bulk {} ops to {}/shard_{}",
            req.ops.len(),
            req.index_name,
            req.shard_id
        );

        let batch_op = req.ops.first().map(|operation| operation.op.as_str());
        let start_seq_no = req.ops.first().map(|operation| operation.seq_no);
        let mut previous_seq_no = None;
        let mut operations = Vec::with_capacity(req.ops.len());
        for (offset, (op, index_source)) in req.ops.iter().zip(index_sources).enumerate() {
            if op.index_name != req.index_name
                || op.shard_id != req.shard_id
                || op.index_uuid != req.index_uuid
                || op.primary_term != Some(primary_term)
                || op.target_allocation_id != Some(allocation_id)
            {
                return Err(Status::invalid_argument(
                    "bulk replication operation identity does not match the envelope",
                ));
            }
            if Some(op.op.as_str()) != batch_op {
                return Err(Status::invalid_argument(
                    "bulk replication operations must use one operation kind",
                ));
            }
            let mutation = match batch_op {
                Some("index") => {
                    let expected_seq = start_seq_no
                        .and_then(|start| start.checked_add(offset as u64))
                        .ok_or_else(|| {
                            Status::invalid_argument("bulk replication sequence range overflows")
                        })?;
                    if op.seq_no != expected_seq {
                        return Err(Status::invalid_argument(
                            "bulk index replication sequences must be contiguous and ordered",
                        ));
                    }
                    let payload = index_source.ok_or_else(|| {
                        Status::internal("validated bulk replica index source is unavailable")
                    })?;
                    crate::engine::DocumentMutation::Index {
                        doc_id: op.doc_id.clone(),
                        source: payload,
                    }
                }
                Some("noop") => {
                    if previous_seq_no.is_some_and(|previous| op.seq_no <= previous) {
                        return Err(Status::invalid_argument(
                            "bulk NoOp replication sequences must be strictly increasing",
                        ));
                    }
                    let payload: serde_json::Value = serde_json::from_slice(&op.payload_json)
                        .map_err(|e| {
                            Status::invalid_argument(format!(
                                "invalid no-op JSON in bulk replicate: {e}"
                            ))
                        })?;
                    let reason = payload
                        .get("_reason")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            Status::invalid_argument("bulk replication no-op has no _reason")
                        })?;
                    crate::engine::DocumentMutation::NoOp {
                        reason: reason.to_string(),
                    }
                }
                Some(other) => {
                    return Err(Status::invalid_argument(format!(
                        "bulk replication does not support operation '{other}'"
                    )));
                }
                None => unreachable!("the operation loop is empty when no batch kind exists"),
            };
            previous_seq_no = Some(op.seq_no);
            operations.push(crate::engine::SequencedOperation {
                seq_no: op.seq_no,
                primary_term,
                mutation,
            });
        }

        let service = self.clone();
        let index_name = req.index_name.clone();
        let index_uuid = req.index_uuid.clone();
        let shard_id = req.shard_id;
        #[cfg(feature = "protocol-trace")]
        let trace_operations = operations.clone();
        let write_result = self
            .worker_pools
            .spawn_write(move || {
                let current = match service.replica_apply_routing(
                    &index_name,
                    shard_id,
                    &index_uuid,
                    allocation_id,
                ) {
                    Ok(current) => current,
                    Err(error) => {
                        #[cfg(feature = "protocol-trace")]
                        service.record_protocol_trace_replica_rejection(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                            primary_term,
                            &trace_seq_nos,
                            "identity_mismatch",
                        )?;
                        return Err(anyhow::Error::msg(error));
                    }
                };
                #[cfg(feature = "protocol-trace")]
                {
                    let result = crate::protocol_trace::with_apply_scope(
                        crate::protocol_trace::ApplyOrigin::LiveReplication,
                        trace_operations,
                        || {
                            service.shard_manager.apply_replica_operation(
                                &index_name,
                                shard_id,
                                crate::shard::ReplicaApplyContext {
                                    index_uuid: &index_uuid,
                                    allocation_id,
                                    applied_view_term: current.primary_term,
                                    message_term: primary_term,
                                },
                                |engine| engine.apply_replica_batch(operations),
                            )
                        },
                    );
                    if let Err(error) = &result {
                        service.record_protocol_trace_replica_rejection(
                            &index_uuid,
                            shard_id,
                            allocation_id,
                            primary_term,
                            &trace_seq_nos,
                            Self::protocol_trace_replica_rejection_reason(error),
                        )?;
                    }
                    result
                }
                #[cfg(not(feature = "protocol-trace"))]
                service.shard_manager.apply_replica_operation(
                    &index_name,
                    shard_id,
                    crate::shard::ReplicaApplyContext {
                        index_uuid: &index_uuid,
                        allocation_id,
                        applied_view_term: current.primary_term,
                        message_term: primary_term,
                    },
                    |engine| engine.apply_replica_batch(operations),
                )
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match write_result {
            Ok(receipt) => Ok(Response::new(ReplicateBulkResponse {
                success: true,
                error: String::new(),
                processed_checkpoint: receipt.sequence.processed_checkpoint,
                persisted_checkpoint: receipt.sequence.persisted_checkpoint,
                all_operations_processed: receipt.all_operations_processed,
                all_operations_persisted: receipt.all_operations_persisted,
            })),
            Err(e) if crate::engine::is_write_validation_error(&e) => {
                Err(Status::invalid_argument(e.to_string()))
            }
            Err(e) => {
                let definitive = ShardManager::is_definitive_copy_failure(&e);
                if assigned_authoritative {
                    self.report_local_copy_failure(
                        &req.index_name,
                        &assigned_uuid,
                        req.shard_id,
                        allocation_id,
                        primary_term,
                        &e,
                    )
                    .await;
                }
                if definitive {
                    return Err(Status::data_loss(e.to_string()));
                }
                let sequence = self
                    .shard_manager
                    .get_shard(&req.index_name, req.shard_id)
                    .map(|engine| engine.sequence_stats());
                Ok(Response::new(ReplicateBulkResponse {
                    success: false,
                    error: e.to_string(),
                    processed_checkpoint: sequence
                        .as_ref()
                        .and_then(|stats| stats.processed_checkpoint),
                    persisted_checkpoint: sequence
                        .as_ref()
                        .and_then(|stats| stats.persisted_checkpoint),
                    all_operations_processed: false,
                    all_operations_persisted: false,
                }))
            }
        }
    }

    async fn get_shard_sequence_state(
        &self,
        request: Request<GetShardSequenceStateRequest>,
    ) -> Result<Response<GetShardSequenceStateResponse>, Status> {
        let req = request.into_inner();
        if req.index_name.is_empty() || req.index_uuid.is_empty() {
            return Err(Status::invalid_argument(
                "sequence-state probe requires index name and UUID",
            ));
        }
        let allocation_id = req
            .allocation_id
            .filter(|allocation_id| *allocation_id > 0)
            .ok_or_else(|| {
                Status::invalid_argument("sequence-state probe requires an allocation ID")
            })?;
        if req.expected_primary_term == 0 {
            return Err(Status::invalid_argument(
                "sequence-state probe requires an expected primary term",
            ));
        }

        let state = self.cluster_manager.get_state();
        let metadata = state
            .indices
            .get(&req.index_name)
            .ok_or_else(|| Status::not_found("sequence-state probe index is unavailable"))?;
        if metadata.uuid.as_str() != req.index_uuid {
            return Err(Status::failed_precondition(
                "sequence-state probe index UUID mismatch",
            ));
        }
        let routing = metadata
            .shard_routing
            .get(&req.shard_id)
            .ok_or_else(|| Status::not_found("sequence-state probe shard is unavailable"))?;
        if state.shard_allocation_id(&req.index_name, req.shard_id, &self.local_node_id)
            != Some(allocation_id)
        {
            return Err(Status::failed_precondition(
                "sequence-state probe allocation mismatch",
            ));
        }
        let identity = self
            .shard_manager
            .copy_identity(&req.index_name, req.shard_id)
            .ok_or_else(|| Status::unavailable("sequence-state probe copy is not open"))?;
        if identity.index_uuid != req.index_uuid || identity.allocation_id != allocation_id {
            return Err(Status::failed_precondition(
                "sequence-state probe durable identity mismatch",
            ));
        }
        if req.expected_primary_term < identity.replica_fence {
            return Err(Status::failed_precondition(format!(
                "sequence-state probe term {} is below durable fence {}",
                req.expected_primary_term, identity.replica_fence
            )));
        }
        let engine = self
            .shard_manager
            .get_shard(&req.index_name, req.shard_id)
            .ok_or_else(|| Status::unavailable("sequence-state probe copy is not open"))?;
        let sequence = engine.sequence_stats();
        let active_primary = routing.primary == self.local_node_id
            && routing.primary_term == req.expected_primary_term
            && identity.replica_fence == routing.primary_term
            && self
                .primary_activation_state
                .activated_terms
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(&(req.index_uuid.clone(), req.shard_id, allocation_id))
                .is_some_and(|term| *term == routing.primary_term);

        Ok(Response::new(GetShardSequenceStateResponse {
            processed_checkpoint: sequence.processed_checkpoint,
            persisted_checkpoint: sequence.persisted_checkpoint,
            max_seq_no: sequence.max_seq_no,
            sequence_format_version: crate::engine::SEQUENCE_FORMAT_VERSION,
            active_primary,
        }))
    }

    async fn start_peer_recovery(
        &self,
        request: Request<StartPeerRecoveryRequest>,
    ) -> Result<Response<StartPeerRecoveryResponse>, Status> {
        Ok(Response::new(
            self.start_peer_recovery_inner(request.into_inner()).await?,
        ))
    }

    async fn fetch_recovery_file_chunk(
        &self,
        request: Request<FetchRecoveryFileChunkRequest>,
    ) -> Result<Response<FetchRecoveryFileChunkResponse>, Status> {
        Ok(Response::new(
            self.fetch_recovery_file_chunk_inner(request.into_inner())
                .await?,
        ))
    }

    async fn fetch_recovery_ops(
        &self,
        request: Request<FetchRecoveryOpsRequest>,
    ) -> Result<Response<FetchRecoveryOpsResponse>, Status> {
        Ok(Response::new(
            self.fetch_recovery_ops_inner(request.into_inner()).await?,
        ))
    }

    async fn prepare_finalize_recovery(
        &self,
        request: Request<PrepareFinalizeRecoveryRequest>,
    ) -> Result<Response<PrepareFinalizeRecoveryResponse>, Status> {
        Ok(Response::new(
            self.prepare_finalize_recovery_inner(request.into_inner())
                .await?,
        ))
    }

    async fn complete_finalize_recovery(
        &self,
        request: Request<CompleteFinalizeRecoveryRequest>,
    ) -> Result<Response<CompleteFinalizeRecoveryResponse>, Status> {
        Ok(Response::new(
            self.complete_finalize_recovery_inner(request.into_inner())
                .await?,
        ))
    }

    // ─── Dynamic Settings ─────────────────────────────────────────────────────

    async fn update_settings(
        &self,
        request: Request<UpdateSettingsRequest>,
    ) -> Result<Response<UpdateSettingsResponse>, Status> {
        let req = request.into_inner();
        let index_name = &req.index_name;

        let body: serde_json::Value = serde_json::from_slice(&req.settings_json)
            .map_err(|e| Status::invalid_argument(format!("bad settings JSON: {e}")))?;

        // Look up current metadata
        let cluster_state = self.cluster_manager.get_state();
        let mut metadata = cluster_state
            .indices
            .get(index_name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("no such index [{index_name}]")))?;

        let mut changed = false;

        // Apply number_of_replicas
        if let Some(new_replicas) = body
            .pointer("/index/number_of_replicas")
            .and_then(|v| v.as_u64())
        {
            let new_replicas = new_replicas as u32;
            if new_replicas != metadata.number_of_replicas {
                metadata.update_number_of_replicas(new_replicas);
                changed = true;
            }
        }

        // Apply refresh_interval_ms
        if let Some(val) = body.pointer("/index/refresh_interval_ms") {
            if val.is_null() {
                if metadata.settings.refresh_interval_ms.is_some() {
                    metadata.settings.refresh_interval_ms = None;
                    changed = true;
                }
            } else if let Some(ms) = val.as_u64()
                && metadata.settings.refresh_interval_ms != Some(ms)
            {
                metadata.settings.refresh_interval_ms = Some(ms);
                changed = true;
            }
        }

        if let Some(val) = body.pointer("/index/flush_threshold_bytes") {
            if val.is_null() {
                if metadata.settings.flush_threshold_bytes.is_some() {
                    metadata.settings.flush_threshold_bytes = None;
                    changed = true;
                }
            } else if let Some(bytes) = val.as_u64()
                && metadata.settings.flush_threshold_bytes != Some(bytes)
            {
                metadata.settings.flush_threshold_bytes = Some(bytes);
                changed = true;
            }
        }

        if !changed {
            return Ok(Response::new(UpdateSettingsResponse {
                acknowledged: true,
                error: String::new(),
            }));
        }

        // This RPC should only be handled by the leader
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader",
            ));
        }

        let cmd = crate::consensus::types::ClusterCommand::UpdateIndex {
            metadata: metadata.clone(),
        };
        crate::consensus::client_write_checked(raft, cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;

        // Apply settings to local engines
        self.shard_manager
            .apply_settings(index_name, &metadata.settings);

        tracing::info!("gRPC: updated settings for index '{}'", index_name);
        Ok(Response::new(UpdateSettingsResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    async fn mark_replica_in_sync(
        &self,
        request: Request<MarkReplicaInSyncRequest>,
    ) -> Result<Response<MarkReplicaInSyncResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }
        let allocation_id = req.allocation_id.ok_or_else(|| {
            Status::invalid_argument("MarkReplicaInSync requires an allocation ID")
        })?;
        if allocation_id == 0 {
            return Err(Status::invalid_argument(
                "MarkReplicaInSync allocation ID must be greater than zero",
            ));
        }

        let command = crate::consensus::types::ClusterCommand::MarkReplicaInSync {
            index_name: req.index_name,
            index_uuid: req.index_uuid,
            shard_id: req.shard_id,
            replica: req.replica_node_id,
            allocation_id,
            primary: req.primary_node_id,
            primary_term: req.primary_term,
        };
        let response = raft
            .client_write(command)
            .await
            .map_err(|error| Status::internal(format!("Raft MarkReplicaInSync failed: {error}")))?;
        match response.data {
            crate::consensus::types::ClusterResponse::Ok => {
                Ok(Response::new(MarkReplicaInSyncResponse {
                    acknowledged: true,
                    error: String::new(),
                }))
            }
            crate::consensus::types::ClusterResponse::Error(error) => {
                Ok(Response::new(MarkReplicaInSyncResponse {
                    acknowledged: false,
                    error,
                }))
            }
        }
    }

    async fn activate_primary(
        &self,
        request: Request<ActivatePrimaryRequest>,
    ) -> Result<Response<ActivatePrimaryResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }
        let allocation_id = req
            .allocation_id
            .ok_or_else(|| Status::invalid_argument("ActivatePrimary requires an allocation ID"))?;
        if allocation_id == 0 {
            return Err(Status::invalid_argument(
                "ActivatePrimary allocation ID must be greater than zero",
            ));
        }

        let command = crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: req.index_name,
            index_uuid: req.index_uuid,
            shard_id: req.shard_id,
            primary: req.primary_node_id,
            allocation_id,
            expected_term: req.expected_term,
        };
        let response = raft
            .client_write(command)
            .await
            .map_err(|error| Status::internal(format!("Raft ActivatePrimary failed: {error}")))?;
        match response.data {
            crate::consensus::types::ClusterResponse::Ok => {
                Ok(Response::new(ActivatePrimaryResponse {
                    acknowledged: true,
                    error: String::new(),
                }))
            }
            crate::consensus::types::ClusterResponse::Error(error) => {
                Ok(Response::new(ActivatePrimaryResponse {
                    acknowledged: false,
                    error,
                }))
            }
        }
    }

    async fn mark_primary_unavailable(
        &self,
        request: Request<MarkPrimaryUnavailableRequest>,
    ) -> Result<Response<MarkPrimaryUnavailableResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }
        let allocation_id = req.allocation_id.ok_or_else(|| {
            Status::invalid_argument("MarkPrimaryUnavailable requires an allocation ID")
        })?;
        if allocation_id == 0 {
            return Err(Status::invalid_argument(
                "MarkPrimaryUnavailable allocation ID must be greater than zero",
            ));
        }
        let state = self.cluster_manager.get_state();
        if state.primary_unavailable(&req.index_name, req.shard_id)
            && state.indices.get(&req.index_name).is_some_and(|metadata| {
                metadata.uuid.as_str() == req.index_uuid
                    && metadata
                        .shard_routing
                        .get(&req.shard_id)
                        .is_some_and(|routing| {
                            routing.primary == req.primary_node_id
                                && state.shard_allocation_id(
                                    &req.index_name,
                                    req.shard_id,
                                    &req.primary_node_id,
                                ) == Some(allocation_id)
                        })
            })
        {
            return Ok(Response::new(MarkPrimaryUnavailableResponse {
                acknowledged: true,
                error: String::new(),
            }));
        }
        let response = raft
            .client_write(
                crate::consensus::types::ClusterCommand::MarkPrimaryUnavailable {
                    index_name: req.index_name,
                    index_uuid: req.index_uuid,
                    shard_id: req.shard_id,
                    primary: req.primary_node_id,
                    allocation_id,
                },
            )
            .await
            .map_err(|error| {
                Status::internal(format!("Raft MarkPrimaryUnavailable failed: {error}"))
            })?;
        match response.data {
            crate::consensus::types::ClusterResponse::Ok => {
                Ok(Response::new(MarkPrimaryUnavailableResponse {
                    acknowledged: true,
                    error: String::new(),
                }))
            }
            crate::consensus::types::ClusterResponse::Error(error) => {
                Ok(Response::new(MarkPrimaryUnavailableResponse {
                    acknowledged: false,
                    error,
                }))
            }
        }
    }

    async fn mark_primary_available(
        &self,
        request: Request<MarkPrimaryAvailableRequest>,
    ) -> Result<Response<MarkPrimaryAvailableResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }
        let allocation_id = req.allocation_id.ok_or_else(|| {
            Status::invalid_argument("MarkPrimaryAvailable requires an allocation ID")
        })?;
        if allocation_id == 0 {
            return Err(Status::invalid_argument(
                "MarkPrimaryAvailable allocation ID must be greater than zero",
            ));
        }
        if req.primary_term == 0 {
            return Err(Status::invalid_argument(
                "MarkPrimaryAvailable primary term must be greater than zero",
            ));
        }
        let response = raft
            .client_write(
                crate::consensus::types::ClusterCommand::MarkPrimaryAvailable {
                    index_name: req.index_name,
                    index_uuid: req.index_uuid,
                    shard_id: req.shard_id,
                    primary: req.primary_node_id,
                    allocation_id,
                    primary_term: req.primary_term,
                },
            )
            .await
            .map_err(|error| {
                Status::internal(format!("Raft MarkPrimaryAvailable failed: {error}"))
            })?;
        match response.data {
            crate::consensus::types::ClusterResponse::Ok => {
                Ok(Response::new(MarkPrimaryAvailableResponse {
                    acknowledged: true,
                    error: String::new(),
                }))
            }
            crate::consensus::types::ClusterResponse::Error(error) => {
                Ok(Response::new(MarkPrimaryAvailableResponse {
                    acknowledged: false,
                    error,
                }))
            }
        }
    }

    async fn fail_shard_copy(
        &self,
        request: Request<FailShardCopyRequest>,
    ) -> Result<Response<FailShardCopyResponse>, Status> {
        let req = request.into_inner();
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }
        if req.index_name.is_empty() || req.index_uuid.is_empty() || req.node_id.is_empty() {
            return Err(Status::invalid_argument(
                "FailShardCopy requires index, UUID, and node identity",
            ));
        }
        let allocation_id = req
            .allocation_id
            .ok_or_else(|| Status::invalid_argument("FailShardCopy requires an allocation ID"))?;
        if allocation_id == 0 {
            return Err(Status::invalid_argument(
                "FailShardCopy allocation ID must be greater than zero",
            ));
        }
        if req.expected_primary_term == 0 {
            return Err(Status::invalid_argument(
                "FailShardCopy requires an expected primary term",
            ));
        }

        let promotion_candidate = if req.promote_only {
            let state = self.cluster_manager.get_state();
            if state
                .indices
                .get(&req.index_name)
                .and_then(|metadata| metadata.shard_routing.get(&req.shard_id))
                .is_none_or(|routing| routing.primary_term != req.expected_primary_term)
            {
                return Ok(Response::new(FailShardCopyResponse {
                    acknowledged: false,
                    error: "FailShardCopy primary term does not match current routing".to_string(),
                }));
            }
            let promotion_candidate =
                self.select_live_promotion_candidate(&state, &req.index_name, req.shard_id);
            if promotion_candidate.is_none()
                && state.primary_unavailable(&req.index_name, req.shard_id)
                && state.indices.get(&req.index_name).is_some_and(|metadata| {
                    metadata.uuid.as_str() == req.index_uuid
                        && metadata
                            .shard_routing
                            .get(&req.shard_id)
                            .is_some_and(|routing| {
                                routing.primary == req.node_id
                                    && state.shard_allocation_id(
                                        &req.index_name,
                                        req.shard_id,
                                        &req.node_id,
                                    ) == Some(allocation_id)
                            })
                })
            {
                return Ok(Response::new(FailShardCopyResponse {
                    acknowledged: true,
                    error: String::new(),
                }));
            }
            promotion_candidate
        } else {
            None
        };
        let command = if req.promote_only && promotion_candidate.is_none() {
            crate::consensus::types::ClusterCommand::MarkPrimaryUnavailable {
                index_name: req.index_name,
                index_uuid: req.index_uuid,
                shard_id: req.shard_id,
                primary: req.node_id,
                allocation_id,
            }
        } else {
            crate::consensus::types::ClusterCommand::FailShardCopy {
                index_name: req.index_name,
                index_uuid: req.index_uuid,
                shard_id: req.shard_id,
                node: req.node_id,
                allocation_id,
                expected_primary_term: req.expected_primary_term,
                promote_only: req.promote_only,
                promotion_candidate,
            }
        };
        let response = raft
            .client_write(command)
            .await
            .map_err(|error| Status::internal(format!("Raft FailShardCopy failed: {error}")))?;
        match response.data {
            crate::consensus::types::ClusterResponse::Ok => {
                Ok(Response::new(FailShardCopyResponse {
                    acknowledged: true,
                    error: String::new(),
                }))
            }
            crate::consensus::types::ClusterResponse::Error(error) => {
                Ok(Response::new(FailShardCopyResponse {
                    acknowledged: false,
                    error,
                }))
            }
        }
    }

    // ─── Index Management RPCs ──────────────────────────────────────────────

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        let req = request.into_inner();
        let index_name = &req.index_name;

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader",
            ));
        }

        let body: serde_json::Value =
            serde_json::from_slice(&req.body_json).unwrap_or(serde_json::json!({}));

        let cluster_state = self.cluster_manager.get_state();

        if cluster_state.indices.contains_key(index_name) {
            return Ok(Response::new(CreateIndexResponse {
                acknowledged: false,
                error: format!("index [{index_name}] already exists"),
                response_json: Vec::new(),
            }));
        }

        let data_nodes: Vec<String> = cluster_state
            .nodes
            .values()
            .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
            .map(|n| n.id.clone())
            .collect();

        let metadata = crate::cluster::state::IndexMetadata::from_create_request_body(
            index_name,
            &body,
            &data_nodes,
        )
        .map_err(create_index_error_status)?;

        let engine_name = metadata.settings.engine.to_string();
        let num_shards = metadata.number_of_shards;
        let num_replicas = metadata.number_of_replicas;

        let cmd = crate::consensus::types::ClusterCommand::CreateIndex { metadata };
        crate::consensus::client_write_checked(raft, cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;

        let resp_json = serde_json::to_vec(&serde_json::json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index_name
        }))
        .map_err(|e| Status::internal(format!("serialize create index response: {e}")))?;

        tracing::info!(
            "gRPC: created index '{}' with engine {}, {} shards, {} replicas",
            index_name,
            engine_name,
            num_shards,
            num_replicas
        );

        Ok(Response::new(CreateIndexResponse {
            acknowledged: true,
            error: String::new(),
            response_json: resp_json,
        }))
    }

    async fn delete_index(
        &self,
        request: Request<DeleteIndexRequest>,
    ) -> Result<Response<DeleteIndexResponse>, Status> {
        let req = request.into_inner();
        let index_name = &req.index_name;

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader",
            ));
        }

        let cluster_state = self.cluster_manager.get_state();
        let Some(index_metadata) = cluster_state.indices.get(index_name) else {
            return Err(Status::not_found(format!("no such index [{index_name}]")));
        };
        self.shard_manager
            .abort_source_recoveries_for_index(&index_metadata.uuid)
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "failed to stop peer recovery before deleting index '{index_name}': {error}"
                ))
            })?;

        let cmd = crate::consensus::types::ClusterCommand::DeleteIndex {
            index_name: index_name.clone(),
        };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {e}")))?;

        // Close local shard engines and delete data on this (leader) node
        if let Err(e) = self
            .shard_manager
            .close_index_shards_blocking_with_reason(
                index_name.clone(),
                crate::shard::SHARD_DATA_REMOVE_REASON_TRANSPORT_DELETE_INDEX,
            )
            .await
        {
            tracing::error!("Failed to close shards for index '{}': {}", index_name, e);
        }

        tracing::info!("gRPC: deleted index '{}'", index_name);
        Ok(Response::new(DeleteIndexResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    async fn transfer_master(
        &self,
        request: Request<TransferMasterRequest>,
    ) -> Result<Response<TransferMasterResponse>, Status> {
        let req = request.into_inner();
        let target_node_id = &req.target_node_id;

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader",
            ));
        }

        let cs = self.cluster_manager.get_state();
        let target_info = cs
            .nodes
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Node '{target_node_id}' not found")))?;

        if target_info.raft_node_id == 0 {
            return Err(Status::invalid_argument(format!(
                "Node '{target_node_id}' has no Raft ID assigned"
            )));
        }

        let vote = {
            let m = raft.metrics();
            m.borrow_watched().vote
        };
        let last_log_id = {
            let m = raft.metrics();
            m.borrow_watched().last_applied
        };

        let transfer_req =
            openraft::raft::TransferLeaderRequest::new(vote, target_info.raft_node_id, last_log_id);
        raft.handle_transfer_leader(transfer_req)
            .await
            .map_err(|e| Status::internal(format!("Transfer leader failed: {e}")))?;

        tracing::info!(
            "gRPC: leadership transfer initiated to node '{}'",
            target_node_id
        );
        Ok(Response::new(TransferMasterResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    // ─── Dynamic Mapping ──────────────────────────────────────────────────────

    async fn add_mappings(
        &self,
        request: Request<AddMappingsRequest>,
    ) -> Result<Response<AddMappingsResponse>, Status> {
        let req = request.into_inner();

        crate::common::validate_mapping_field_names(
            req.new_fields.iter().map(|entry| entry.name.as_str()),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        for entry in &req.new_fields {
            crate::common::validate_builtin_body_mapping_entry(
                &entry.name,
                &entry.field_type,
                entry.dimension.is_some(),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;

        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }

        let mut new_fields = std::collections::HashMap::new();
        for entry in &req.new_fields {
            let field_type = conversions::proto_to_field_type(&entry.field_type).map_err(|_| {
                Status::invalid_argument(format!(
                    "unknown field type '{}' for field '{}'",
                    entry.field_type, entry.name
                ))
            })?;
            new_fields.insert(
                entry.name.clone(),
                crate::cluster::state::FieldMapping {
                    field_type,
                    dimension: entry.dimension.map(|d| d as usize),
                },
            );
        }

        let dynamic = conversions::proto_to_dynamic_mapping(&req.dynamic)?;

        let cmd = crate::consensus::types::ClusterCommand::AddMappings {
            index_name: req.index_name.clone(),
            new_fields,
            dynamic,
        };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft AddMappings failed: {e}")))?;

        tracing::info!(
            "gRPC: added {} dynamic mappings for index '{}'",
            req.new_fields.len(),
            req.index_name
        );
        Ok(Response::new(AddMappingsResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    // ─── Dynamic Security Control Plane ───────────────────────────────────────

    async fn put_api_key(
        &self,
        request: Request<PutApiKeyRequest>,
    ) -> Result<Response<PutApiKeyResponse>, Status> {
        let req = request.into_inner();

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;

        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }

        let record: crate::cluster::state::SecurityApiKeyRecord =
            serde_json::from_str(&req.record_json)
                .map_err(|e| Status::invalid_argument(format!("invalid api key record: {e}")))?;

        // Validate at the transport trust boundary before committing to Raft.
        // The HTTP handler always builds a well-formed record, but a record can
        // also arrive directly over this RPC, so re-check the security-critical
        // invariants here (mirrors add_mappings validating its proto payload).
        if record.id.trim().is_empty() {
            return Err(Status::invalid_argument("api key record has empty id"));
        }
        if record.name.trim().is_empty() {
            return Err(Status::invalid_argument("api key record has empty name"));
        }
        if record.hash_sha256.len() != 64
            || !record.hash_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Status::invalid_argument(
                "api key record hash_sha256 must be a 64-character hex digest",
            ));
        }
        let key_id = record.id.clone();

        let cmd = crate::consensus::types::ClusterCommand::PutApiKey { record };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft PutApiKey failed: {e}")))?;

        info!("gRPC: stored dynamic api key '{key_id}'");
        Ok(Response::new(PutApiKeyResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    async fn delete_api_key(
        &self,
        request: Request<DeleteApiKeyRequest>,
    ) -> Result<Response<DeleteApiKeyResponse>, Status> {
        let req = request.into_inner();

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;

        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }

        let cmd = crate::consensus::types::ClusterCommand::DeleteApiKey {
            key_id: req.key_id.clone(),
        };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft DeleteApiKey failed: {e}")))?;

        info!("gRPC: deleted dynamic api key '{}'", req.key_id);
        Ok(Response::new(DeleteApiKeyResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    async fn put_role(
        &self,
        request: Request<PutRoleRequest>,
    ) -> Result<Response<PutRoleResponse>, Status> {
        let req = request.into_inner();

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;

        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }

        let role: crate::cluster::state::SecurityRoleDefinition =
            serde_json::from_str(&req.role_json)
                .map_err(|e| Status::invalid_argument(format!("invalid role definition: {e}")))?;
        if role.name.trim().is_empty() {
            return Err(Status::invalid_argument("role definition has empty name"));
        }
        let role_name = role.name.clone();

        let cmd = crate::consensus::types::ClusterCommand::PutRole { role };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft PutRole failed: {e}")))?;

        info!("gRPC: stored custom role '{role_name}'");
        Ok(Response::new(PutRoleResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    async fn delete_role(
        &self,
        request: Request<DeleteRoleRequest>,
    ) -> Result<Response<DeleteRoleResponse>, Status> {
        let req = request.into_inner();

        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;

        if !raft.is_leader() {
            return Err(Status::failed_precondition(
                "This node is not the Raft leader — caller should forward",
            ));
        }

        let cmd = crate::consensus::types::ClusterCommand::DeleteRole {
            name: req.name.clone(),
        };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft DeleteRole failed: {e}")))?;

        info!("gRPC: deleted custom role '{}'", req.name);
        Ok(Response::new(DeleteRoleResponse {
            acknowledged: true,
            error: String::new(),
        }))
    }

    // ─── Shard Stats ──────────────────────────────────────────────────────────

    async fn get_shard_stats(
        &self,
        _request: Request<ShardStatsRequest>,
    ) -> Result<Response<ShardStatsResponse>, Status> {
        let all = self.shard_manager.all_shards();
        let shards = all
            .iter()
            .map(|(key, engine)| ShardStat {
                index_name: key.index.clone(),
                shard_id: key.shard_id,
                doc_count: engine.doc_count(),
            })
            .collect();
        Ok(Response::new(ShardStatsResponse { shards }))
    }

    async fn get_segment_stats(
        &self,
        _request: Request<SegmentStatsRequest>,
    ) -> Result<Response<SegmentStatsResponse>, Status> {
        let all = self.shard_manager.all_shards();
        let mut segments = Vec::new();
        for (key, engine) in &all {
            for segment in engine.segment_infos() {
                segments.push(SegmentStat {
                    index_name: key.index.clone(),
                    shard_id: key.shard_id,
                    segment_id: segment.segment_id,
                    num_docs: segment.num_docs as u64,
                    deleted_docs: segment.deleted_docs as u64,
                });
            }
        }
        Ok(Response::new(SegmentStatsResponse { segments }))
    }

    // ─── Index Maintenance RPCs ───────────────────────────────────────────────

    async fn refresh_index(
        &self,
        request: Request<IndexMaintenanceRequest>,
    ) -> Result<Response<IndexMaintenanceResponse>, Status> {
        let index_name = request.into_inner().index_name;
        let (successful, failed) = run_maintenance_on_assigned_shards_async(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
            self.local_node_id.clone(),
            index_name,
            MaintenanceDispatchOp::Refresh,
        )
        .await;
        Ok(Response::new(IndexMaintenanceResponse {
            successful_shards: successful,
            failed_shards: failed,
        }))
    }

    async fn flush_index(
        &self,
        request: Request<IndexMaintenanceRequest>,
    ) -> Result<Response<IndexMaintenanceResponse>, Status> {
        let index_name = request.into_inner().index_name;
        let (successful, failed) = run_maintenance_on_assigned_shards_async(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
            self.local_node_id.clone(),
            index_name,
            MaintenanceDispatchOp::Flush,
        )
        .await;
        Ok(Response::new(IndexMaintenanceResponse {
            successful_shards: successful,
            failed_shards: failed,
        }))
    }

    async fn force_merge_index(
        &self,
        request: Request<ForceMergeRequest>,
    ) -> Result<Response<ForceMergeResponse>, Status> {
        let inner = request.into_inner();
        if inner.max_num_segments == 0 {
            return Err(Status::invalid_argument(
                "max_num_segments must be at least 1",
            ));
        }
        let max_segments = inner.max_num_segments as usize;
        let task_id = enqueue_force_merge_task_on_assigned_shards(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
            self.task_manager.clone(),
            self.local_node_id.clone(),
            inner.index_name,
            max_segments,
        );
        Ok(Response::new(ForceMergeResponse { task_id }))
    }

    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> Result<Response<GetTaskStatusResponse>, Status> {
        let task_id = request.into_inner().task_id;
        let Some(task) = self.task_manager.get_local_force_merge(&task_id) else {
            return Ok(Response::new(GetTaskStatusResponse {
                found: false,
                task_id,
                action: String::new(),
                status: String::new(),
                node_id: String::new(),
                index_name: String::new(),
                max_num_segments: 0,
                successful_shards: 0,
                failed_shards: 0,
                created_at_epoch_ms: 0,
                started_at_epoch_ms: 0,
                completed_at_epoch_ms: 0,
                error: String::new(),
            }));
        };

        Ok(Response::new(GetTaskStatusResponse {
            found: true,
            task_id: task.task_id,
            action: task.action,
            status: task.status.as_str().to_string(),
            node_id: task.node_id,
            index_name: task.index_name,
            max_num_segments: task.max_num_segments as u32,
            successful_shards: task.successful_shards,
            failed_shards: task.failed_shards,
            created_at_epoch_ms: task.created_at_epoch_ms,
            started_at_epoch_ms: task.started_at_epoch_ms.unwrap_or(0),
            completed_at_epoch_ms: task.completed_at_epoch_ms.unwrap_or(0),
            error: task.error.unwrap_or_default(),
        }))
    }

    // ─── Raft RPCs ────────────────────────────────────────────────────────────

    async fn raft_vote(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let rpc: openraft::raft::VoteRequest<crate::consensus::TypeConfig> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("bad vote request: {e}")))?;
        let resp = raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(format!("raft vote error: {e}")))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise vote response: {e}")))?;
        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }

    async fn raft_append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let rpc: openraft::raft::AppendEntriesRequest<crate::consensus::TypeConfig> =
            serde_json::from_slice(&request.into_inner().data).map_err(|e| {
                Status::invalid_argument(format!("bad append_entries request: {e}"))
            })?;
        let resp = raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(format!("raft append_entries error: {e}")))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise append_entries response: {e}")))?;
        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }

    async fn raft_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let payload: serde_json::Value = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("bad snapshot request: {e}")))?;
        let vote_value = payload
            .get("vote")
            .cloned()
            .ok_or_else(|| Status::invalid_argument("bad snapshot request: missing vote"))?;
        let meta_value = payload
            .get("meta")
            .cloned()
            .ok_or_else(|| Status::invalid_argument("bad snapshot request: missing meta"))?;
        let data_value = payload
            .get("data")
            .cloned()
            .ok_or_else(|| Status::invalid_argument("bad snapshot request: missing data"))?;
        let vote: crate::consensus::types::Vote = serde_json::from_value(vote_value)
            .map_err(|e| Status::invalid_argument(format!("bad vote in snapshot: {e}")))?;
        let meta: crate::consensus::types::SnapshotMeta = serde_json::from_value(meta_value)
            .map_err(|e| Status::invalid_argument(format!("bad meta in snapshot: {e}")))?;
        let data: Vec<u8> = serde_json::from_value(data_value)
            .map_err(|e| Status::invalid_argument(format!("bad data in snapshot: {e}")))?;
        let snapshot = crate::consensus::types::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        };
        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("raft snapshot error: {e}")))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise snapshot response: {e}")))?;
        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }
}

impl TransportService {
    #[cfg(feature = "protocol-trace")]
    fn record_protocol_trace_replica_rejection(
        &self,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: u64,
        primary_term: u64,
        seq_nos: &[u64],
        reason: &str,
    ) -> anyhow::Result<()> {
        let copy = crate::protocol_trace::TraceCopy {
            node: self.local_node_id.clone(),
            index_uuid: index_uuid.to_string(),
            shard: shard_id,
            allocation: allocation_id,
        };
        let operations = seq_nos
            .iter()
            .map(|seq_no| {
                crate::protocol_trace::OperationKey::new(
                    index_uuid,
                    shard_id,
                    primary_term,
                    *seq_no,
                )
            })
            .collect::<Vec<_>>();
        crate::protocol_trace::record_replica_rejected(&copy, &operations, reason)
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_replica_rejection_reason(error: &anyhow::Error) -> &'static str {
        let message = format!("{error:#}");
        if error.is::<crate::shard::CollisionQuarantinedShardCopy>()
            || message.contains("collision quarantine")
        {
            "quarantined"
        } else if message.contains("below local fence")
            || message.contains("below the current primary term")
        {
            "term_fence"
        } else if message.contains("installing a peer recovery snapshot")
            || message.contains("peer recovery")
        {
            "recovery_gate"
        } else if message.contains("UUID mismatch")
            || message.contains("allocation mismatch")
            || message.contains("no current allocation")
            || message.contains("not an authoritative")
        {
            "identity_mismatch"
        } else if ShardManager::is_sequence_collision_failure(error) {
            "batch_rejected"
        } else if message.contains("engine is not open")
            || message.contains("failed to open replica copy")
        {
            "copy_unavailable"
        } else {
            "apply_failure"
        }
    }

    fn select_live_promotion_candidate(
        &self,
        state: &crate::cluster::state::ClusterState,
        index_name: &str,
        shard_id: u32,
    ) -> Option<String> {
        let metadata = state.indices.get(index_name)?;
        let live_nodes = state.nodes.keys().cloned().collect();
        let checkpoints = self
            .shard_manager
            .isr_tracker
            .replica_checkpoints(index_name, shard_id);
        metadata.select_live_promotion_candidate(shard_id, &checkpoints, &live_nodes)
    }

    fn replica_apply_routing(
        &self,
        index_name: &str,
        shard_id: u32,
        index_uuid: &str,
        allocation_id: u64,
    ) -> Result<AssignedLocalShard, String> {
        let cluster_state = self.cluster_manager.get_state();
        let metadata = cluster_state
            .indices
            .get(index_name)
            .ok_or_else(|| format!("replication index [{index_name}] is not present"))?;
        if metadata.uuid.as_str() != index_uuid {
            return Err(format!(
                "replication index UUID mismatch for [{index_name}]: expected {}, got {index_uuid}",
                metadata.uuid
            ));
        }
        let routing = metadata.shard_routing.get(&shard_id).ok_or_else(|| {
            format!("replication shard [{index_name}][{shard_id}] is not present")
        })?;
        let current_allocation = cluster_state
            .shard_allocation_id(index_name, shard_id, &self.local_node_id)
            .ok_or_else(|| {
                format!(
                    "node [{}] has no current allocation for shard [{index_name}][{shard_id}]",
                    self.local_node_id
                )
            })?;
        if current_allocation != allocation_id {
            return Err(format!(
                "replication allocation mismatch for shard [{index_name}][{shard_id}]: expected {current_allocation}, got {allocation_id}"
            ));
        }
        let ordinary_authoritative = routing.primary == self.local_node_id
            || routing.is_replica_in_sync(&self.local_node_id);
        if !ordinary_authoritative
            && !self
                .shard_manager
                .accepts_live_replication_while_pending(index_name, shard_id)
        {
            return Err(format!(
                "node [{}] is not an authoritative or finalized pending copy for shard [{index_name}][{shard_id}]",
                self.local_node_id
            ));
        }
        Ok(AssignedLocalShard {
            index_uuid: metadata.uuid.to_string(),
            mappings: metadata.mappings.clone(),
            settings: metadata.settings.clone(),
            allocation_id,
            primary_term: routing.primary_term,
            is_primary: routing.primary == self.local_node_id,
            allow_empty_creation: ordinary_authoritative
                && cluster_state.may_create_initial_empty_copy(
                    index_name,
                    shard_id,
                    &self.local_node_id,
                ),
            authoritative: ordinary_authoritative,
            primary_unavailable: cluster_state.primary_unavailable(index_name, shard_id),
        })
    }

    fn assigned_local_shard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<AssignedLocalShard, String> {
        let cluster_state = self.cluster_manager.get_state();
        let metadata = cluster_state
            .indices
            .get(index_name)
            .ok_or_else(|| format!("index [{index_name}] is not present in local cluster state"))?;
        let routing = metadata.shard_routing.get(&shard_id).ok_or_else(|| {
            format!("shard [{index_name}][{shard_id}] is not present in local cluster state")
        })?;
        let authoritative = routing.primary == self.local_node_id
            || routing.is_replica_in_sync(&self.local_node_id);
        if !authoritative {
            return Err(format!(
                "node [{}] is not an authoritative copy for shard [{index_name}][{shard_id}]",
                self.local_node_id
            ));
        }
        let allocation_id = cluster_state
            .shard_allocation_id(index_name, shard_id, &self.local_node_id)
            .ok_or_else(|| {
                format!(
                    "node [{}] has no allocation ID for shard [{index_name}][{shard_id}]",
                    self.local_node_id
                )
            })?;
        Ok(AssignedLocalShard {
            index_uuid: metadata.uuid.to_string(),
            mappings: metadata.mappings.clone(),
            settings: metadata.settings.clone(),
            allocation_id,
            primary_term: routing.primary_term,
            is_primary: routing.primary == self.local_node_id,
            allow_empty_creation: cluster_state.may_create_initial_empty_copy(
                index_name,
                shard_id,
                &self.local_node_id,
            ),
            authoritative: true,
            primary_unavailable: cluster_state.primary_unavailable(index_name, shard_id),
        })
    }

    pub(crate) async fn reconcile_replica_gaps(&self) {
        self.reconcile_replica_gaps_with_probe_timeout(std::time::Duration::from_secs(2))
            .await;
    }

    async fn reconcile_replica_gaps_with_probe_timeout(&self, probe_timeout: std::time::Duration) {
        const GAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let observations = self
            .shard_manager
            .isr_tracker
            .expired_gap_observations(GAP_TIMEOUT);
        futures::future::join_all(observations.into_iter().map(|(key, observation)| {
            let service = self.clone();
            async move {
                service
                    .reconcile_replica_gap(key, observation, probe_timeout)
                    .await;
            }
        }))
        .await;
    }

    async fn reconcile_replica_gap(
        &self,
        key: crate::shard::ShardKey,
        observation: crate::shard::ReplicaGapObservation,
        probe_timeout: std::time::Duration,
    ) {
        let state = self.cluster_manager.get_state();
        let current = state
            .indices
            .get(&key.index)
            .and_then(|metadata| {
                metadata
                    .shard_routing
                    .get(&key.shard_id)
                    .map(|routing| (metadata, routing))
            })
            .filter(|(metadata, routing)| {
                metadata.uuid.as_str() == observation.index_uuid
                    && routing.primary == self.local_node_id
                    && routing.primary_term == observation.primary_term
                    && state.shard_allocation_id(
                        &key.index,
                        key.shard_id,
                        &observation.replica_node_id,
                    ) == Some(observation.allocation_id)
            });
        if current.is_none() {
            self.shard_manager.isr_tracker.remove_gap_observation(
                &key.index,
                key.shard_id,
                &observation,
            );
            return;
        }

        let Some(replica) = state.nodes.get(&observation.replica_node_id) else {
            tracing::warn!(
                index = key.index,
                shard_id = key.shard_id,
                replica = observation.replica_node_id,
                allocation_id = observation.allocation_id,
                "Replica gap probe could not start because the assigned node is absent"
            );
            return;
        };
        let probe = self.transport_client.get_shard_sequence_state(
            replica,
            GetShardSequenceStateRequest {
                index_name: key.index.clone(),
                index_uuid: observation.index_uuid.clone(),
                shard_id: key.shard_id,
                allocation_id: Some(observation.allocation_id),
                expected_primary_term: observation.primary_term,
            },
        );
        let should_fail = match tokio::time::timeout(probe_timeout, probe).await {
            Ok(Ok(response)) => {
                if response.sequence_format_version != crate::engine::SEQUENCE_FORMAT_VERSION {
                    tracing::warn!(
                        index = key.index,
                        shard_id = key.shard_id,
                        replica = observation.replica_node_id,
                        allocation_id = observation.allocation_id,
                        format_version = response.sequence_format_version,
                        "Replica gap probe returned an unsupported sequence format"
                    );
                    false
                } else if self.shard_manager.isr_tracker.record_gap_probe_checkpoint(
                    &key.index,
                    key.shard_id,
                    &observation,
                    response.processed_checkpoint,
                ) {
                    false
                } else {
                    self.shard_manager.isr_tracker.has_gap_observation(
                        &key.index,
                        key.shard_id,
                        &observation,
                    )
                }
            }
            Ok(Err(error)) => {
                let definitive = error.downcast_ref::<tonic::Status>().is_some_and(|status| {
                    matches!(
                        status.code(),
                        tonic::Code::FailedPrecondition | tonic::Code::DataLoss
                    )
                });
                if !definitive {
                    tracing::warn!(
                        index = key.index,
                        shard_id = key.shard_id,
                        replica = observation.replica_node_id,
                        allocation_id = observation.allocation_id,
                        error = %error,
                        "Replica gap probe failed transiently; retaining the fixed target"
                    );
                }
                definitive
                    && self.shard_manager.isr_tracker.has_gap_observation(
                        &key.index,
                        key.shard_id,
                        &observation,
                    )
            }
            Err(_) => {
                tracing::warn!(
                    index = key.index,
                    shard_id = key.shard_id,
                    replica = observation.replica_node_id,
                    allocation_id = observation.allocation_id,
                    timeout_ms = probe_timeout.as_millis(),
                    "Replica gap probe timed out; retaining the fixed target"
                );
                false
            }
        };
        if !should_fail
            || !self.shard_manager.isr_tracker.has_gap_observation(
                &key.index,
                key.shard_id,
                &observation,
            )
        {
            return;
        }

        let state = self.cluster_manager.get_state();
        let still_current = state
            .indices
            .get(&key.index)
            .and_then(|metadata| {
                metadata
                    .shard_routing
                    .get(&key.shard_id)
                    .map(|routing| (metadata, routing))
            })
            .is_some_and(|(metadata, routing)| {
                metadata.uuid.as_str() == observation.index_uuid
                    && routing.primary == self.local_node_id
                    && routing.primary_term == observation.primary_term
                    && state.shard_allocation_id(
                        &key.index,
                        key.shard_id,
                        &observation.replica_node_id,
                    ) == Some(observation.allocation_id)
            });
        if !still_current {
            self.shard_manager.isr_tracker.remove_gap_observation(
                &key.index,
                key.shard_id,
                &observation,
            );
            return;
        }

        let command = crate::consensus::types::ClusterCommand::FailShardCopy {
            index_name: key.index.clone(),
            index_uuid: observation.index_uuid.clone(),
            shard_id: key.shard_id,
            node: observation.replica_node_id.clone(),
            allocation_id: observation.allocation_id,
            expected_primary_term: observation.primary_term,
            promote_only: false,
            promotion_candidate: None,
        };
        let result = if self.raft.as_ref().is_some_and(|raft| raft.is_leader()) {
            crate::consensus::client_write_checked(
                self.raft
                    .as_ref()
                    .expect("Raft presence checked for leader gap report"),
                command,
            )
            .await
            .map_err(anyhow::Error::msg)
        } else {
            let Some(master_id) = state.master_node.as_ref() else {
                tracing::warn!(
                    index = key.index,
                    shard_id = key.shard_id,
                    "Cannot report replica gap because no Raft leader is known"
                );
                return;
            };
            let Some(master) = state.nodes.get(master_id) else {
                tracing::warn!(
                    index = key.index,
                    shard_id = key.shard_id,
                    master = master_id,
                    "Cannot report replica gap because the Raft leader is absent"
                );
                return;
            };
            self.transport_client
                .forward_fail_shard_copy(
                    master,
                    FailShardCopyRequest {
                        index_name: key.index.clone(),
                        index_uuid: observation.index_uuid.clone(),
                        shard_id: key.shard_id,
                        node_id: observation.replica_node_id.clone(),
                        allocation_id: Some(observation.allocation_id),
                        promote_only: false,
                        expected_primary_term: observation.primary_term,
                    },
                )
                .await
        };
        match result {
            Ok(()) => self.shard_manager.isr_tracker.remove_gap_observation(
                &key.index,
                key.shard_id,
                &observation,
            ),
            Err(error) => tracing::warn!(
                index = key.index,
                shard_id = key.shard_id,
                replica = observation.replica_node_id,
                allocation_id = observation.allocation_id,
                error = %error,
                "Failed to remove replica after the fixed gap target remained unmet"
            ),
        }
    }

    async fn report_local_copy_failure(
        &self,
        index_name: &str,
        index_uuid: &str,
        shard_id: u32,
        allocation_id: u64,
        expected_primary_term: u64,
        error: &anyhow::Error,
    ) {
        if !ShardManager::should_report_copy_failure(error) {
            tracing::warn!(
                index = index_name,
                shard_id,
                allocation_id,
                error = %error,
                "Local shard-copy error is retryable and was not reported to routing"
            );
            return;
        }
        let collision_failure = ShardManager::is_sequence_collision_failure(error);
        if collision_failure
            && let Err(quarantine_error) = self
                .shard_manager
                .quarantine_sequence_collision_blocking(
                    index_name.to_string(),
                    shard_id,
                    index_uuid.to_string(),
                    allocation_id,
                )
                .await
        {
            tracing::warn!(
                index = index_name,
                shard_id,
                error = %quarantine_error,
                "Failed to quarantine sequence-colliding local shard copy"
            );
        }
        let current = self.cluster_manager.get_state();
        let Some(metadata) = current.indices.get(index_name) else {
            return;
        };
        let Some(routing) = metadata.shard_routing.get(&shard_id) else {
            return;
        };
        if metadata.uuid.as_str() != index_uuid
            || !current.primary_initialized(index_name, shard_id)
            || routing.primary_term != expected_primary_term
            || current.shard_allocation_id(index_name, shard_id, &self.local_node_id)
                != Some(allocation_id)
        {
            tracing::debug!(
                index = index_name,
                shard_id,
                allocation_id,
                "Skipping stale or uninitialized local shard-copy failure report"
            );
            return;
        }
        let promote_only = routing.primary == self.local_node_id;
        const REPORT_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let report_key = (index_uuid.to_string(), shard_id, allocation_id);
        let now = std::time::Instant::now();
        {
            let mut reports = self
                .primary_activation_state
                .failed_copy_reports
                .lock()
                .await;
            if reports
                .get(&report_key)
                .is_some_and(|last| now.duration_since(*last) < REPORT_RETRY_INTERVAL)
            {
                tracing::debug!(
                    index = index_name,
                    shard_id,
                    allocation_id,
                    "Suppressing duplicate local shard-copy failure report"
                );
                return;
            }
            reports.insert(report_key, now);
        }
        let promotion_candidate = if promote_only {
            self.select_live_promotion_candidate(&current, index_name, shard_id)
        } else {
            None
        };
        let repeated_unavailable = promote_only
            && promotion_candidate.is_none()
            && current.primary_unavailable(index_name, shard_id);
        if promote_only && !current.primary_unavailable(index_name, shard_id) {
            self.primary_activation_state
                .available_primary_reports
                .lock()
                .await
                .remove(&(
                    index_uuid.to_string(),
                    shard_id,
                    allocation_id,
                    routing.primary_term,
                ));
        }
        if !collision_failure && ShardManager::should_quarantine_copy_failure(error) {
            if promote_only {
                let activation_key = (index_uuid.to_string(), shard_id, allocation_id);
                self.primary_activation_state
                    .activated_terms
                    .write()
                    .unwrap_or_else(|lock_error| lock_error.into_inner())
                    .remove(&activation_key);
                self.primary_activation_state
                    .pending_noops
                    .write()
                    .unwrap_or_else(|lock_error| lock_error.into_inner())
                    .remove(&activation_key);
            }
            if let Err(quarantine_error) = self
                .shard_manager
                .quarantine_shard_copy_blocking(index_name.to_string(), shard_id)
                .await
            {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    error = %quarantine_error,
                    "Failed to quarantine invalid local shard copy"
                );
            }
        }
        if repeated_unavailable && self.raft.as_ref().is_some_and(|raft| raft.is_leader()) {
            tracing::debug!(
                index = index_name,
                shard_id,
                allocation_id,
                "Primary is already marked unavailable for this allocation"
            );
            return;
        }
        let reason = error.to_string();
        let Some(raft) = self.raft.as_ref() else {
            return;
        };
        let result = if raft.is_leader() {
            let command = if promote_only && promotion_candidate.is_none() {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    allocation_id,
                    "Keeping failed primary authority unchanged because no live in-sync promotion candidate exists"
                );
                crate::consensus::types::ClusterCommand::MarkPrimaryUnavailable {
                    index_name: index_name.to_string(),
                    index_uuid: index_uuid.to_string(),
                    shard_id,
                    primary: self.local_node_id.clone(),
                    allocation_id,
                }
            } else {
                crate::consensus::types::ClusterCommand::FailShardCopy {
                    index_name: index_name.to_string(),
                    index_uuid: index_uuid.to_string(),
                    shard_id,
                    node: self.local_node_id.clone(),
                    allocation_id,
                    expected_primary_term,
                    promote_only,
                    promotion_candidate,
                }
            };
            crate::consensus::client_write_checked(raft, command)
                .await
                .map_err(anyhow::Error::msg)
        } else {
            let state = self.cluster_manager.get_state();
            let Some(master_id) = state.master_node.as_ref() else {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    reason = reason.as_str(),
                    "Cannot report local shard failure because no Raft leader is known"
                );
                return;
            };
            let Some(master) = state.nodes.get(master_id) else {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    reason = reason.as_str(),
                    master = master_id,
                    "Cannot report local shard failure because the Raft leader is absent"
                );
                return;
            };
            self.transport_client
                .forward_fail_shard_copy(
                    master,
                    FailShardCopyRequest {
                        index_name: index_name.to_string(),
                        index_uuid: index_uuid.to_string(),
                        shard_id,
                        node_id: self.local_node_id.clone(),
                        allocation_id: Some(allocation_id),
                        promote_only,
                        expected_primary_term,
                    },
                )
                .await
        };
        if let Err(error) = result {
            tracing::warn!(
                index = index_name,
                shard_id,
                allocation_id,
                reason = reason.as_str(),
                error = %error,
                "Local shard-copy failure report was not applied"
            );
        }
    }

    fn spawn_primary_available_report_after_write(
        &self,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) {
        if !self
            .cluster_manager
            .primary_unavailable(index_name, shard_id)
        {
            return;
        }
        #[cfg(test)]
        self.primary_activation_state
            .available_report_tasks_spawned
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let service = self.clone();
        let index_name = index_name.to_string();
        let activated_primary = activated_primary.clone();
        tokio::spawn(async move {
            service
                .report_primary_available_after_write(&index_name, shard_id, &activated_primary)
                .await;
        });
    }

    async fn report_primary_available_after_write(
        &self,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) {
        let current = self.cluster_manager.get_state();
        let Some(metadata) = current.indices.get(index_name) else {
            return;
        };
        let Some(routing) = metadata.shard_routing.get(&shard_id) else {
            return;
        };
        if metadata.uuid.as_str() != activated_primary.index_uuid
            || routing.primary != self.local_node_id
            || routing.primary_term != activated_primary.primary_term
            || current.shard_allocation_id(index_name, shard_id, &self.local_node_id)
                != Some(activated_primary.allocation_id)
            || !current.primary_unavailable(index_name, shard_id)
        {
            return;
        }

        const REPORT_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let report_key = (
            activated_primary.index_uuid.clone(),
            shard_id,
            activated_primary.allocation_id,
            activated_primary.primary_term,
        );
        let now = std::time::Instant::now();
        {
            let mut reports = self
                .primary_activation_state
                .available_primary_reports
                .lock()
                .await;
            if reports
                .get(&report_key)
                .is_some_and(|last| now.duration_since(*last) < REPORT_RETRY_INTERVAL)
            {
                return;
            }
            reports.insert(report_key, now);
        }

        let Some(raft) = self.raft.as_ref() else {
            return;
        };
        let result = if raft.is_leader() {
            crate::consensus::client_write_checked(
                raft,
                crate::consensus::types::ClusterCommand::MarkPrimaryAvailable {
                    index_name: index_name.to_string(),
                    index_uuid: activated_primary.index_uuid.clone(),
                    shard_id,
                    primary: self.local_node_id.clone(),
                    allocation_id: activated_primary.allocation_id,
                    primary_term: activated_primary.primary_term,
                },
            )
            .await
            .map_err(anyhow::Error::msg)
        } else {
            let Some(master_id) = current.master_node.as_ref() else {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    "Cannot clear primary-unavailable status because no Raft leader is known"
                );
                return;
            };
            let Some(master) = current.nodes.get(master_id) else {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    master = master_id,
                    "Cannot clear primary-unavailable status because the Raft leader is absent"
                );
                return;
            };
            self.transport_client
                .forward_mark_primary_available(
                    master,
                    MarkPrimaryAvailableRequest {
                        index_name: index_name.to_string(),
                        index_uuid: activated_primary.index_uuid.clone(),
                        shard_id,
                        primary_node_id: self.local_node_id.clone(),
                        allocation_id: Some(activated_primary.allocation_id),
                        primary_term: activated_primary.primary_term,
                    },
                )
                .await
        };

        match result {
            Ok(()) => {
                self.primary_activation_state
                    .failed_copy_reports
                    .lock()
                    .await
                    .remove(&(
                        activated_primary.index_uuid.clone(),
                        shard_id,
                        activated_primary.allocation_id,
                    ));
            }
            Err(error) => {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    allocation_id = activated_primary.allocation_id,
                    primary_term = activated_primary.primary_term,
                    error = %error,
                    "Failed to clear primary-unavailable status after a successful local write"
                );
            }
        }
    }

    fn primary_routing(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<AssignedLocalShard, String> {
        let cluster_state = self.cluster_manager.get_state();
        let metadata = cluster_state
            .indices
            .get(index_name)
            .ok_or_else(|| format!("index [{index_name}] is not present in local cluster state"))?;
        let routing = metadata.shard_routing.get(&shard_id).ok_or_else(|| {
            format!("shard [{index_name}][{shard_id}] is not present in local cluster state")
        })?;
        if routing.primary != self.local_node_id {
            return Err(format!(
                "node [{}] is not the primary for shard [{index_name}][{shard_id}] at term {}; retry after refreshing shard routing",
                self.local_node_id, routing.primary_term
            ));
        }
        let allocation_id = cluster_state
            .shard_allocation_id(index_name, shard_id, &self.local_node_id)
            .ok_or_else(|| {
                format!(
                    "node [{}] has no allocation ID for shard [{index_name}][{shard_id}]",
                    self.local_node_id
                )
            })?;
        Ok(AssignedLocalShard {
            index_uuid: metadata.uuid.to_string(),
            mappings: metadata.mappings.clone(),
            settings: metadata.settings.clone(),
            allocation_id,
            primary_term: routing.primary_term,
            is_primary: true,
            allow_empty_creation: cluster_state.may_create_initial_empty_copy(
                index_name,
                shard_id,
                &self.local_node_id,
            ),
            authoritative: true,
            primary_unavailable: cluster_state.primary_unavailable(index_name, shard_id),
        })
    }

    pub(crate) async fn activate_primary_for_lifecycle(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<(), String> {
        self.ensure_primary_activated(index_name, shard_id)
            .await
            .map(|_| ())
    }

    #[cfg(feature = "protocol-trace")]
    pub async fn protocol_trace_activate_primary_for_test(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<(), String> {
        self.activate_primary_for_lifecycle(index_name, shard_id)
            .await
    }

    async fn ensure_primary_activated(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<ActivatedPrimary, String> {
        let current = self.primary_routing(index_name, shard_id)?;
        let initial_key = (current.index_uuid.clone(), shard_id, current.allocation_id);
        if current.primary_unavailable
            && self.shard_manager.get_shard(index_name, shard_id).is_none()
        {
            self.primary_activation_state
                .activated_terms
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&initial_key);
            self.primary_activation_state
                .pending_noops
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&initial_key);
        }
        if let Err(error) = self
            .shard_manager
            .open_primary_assigned_shard_with_settings_blocking(
                index_name.to_string(),
                shard_id,
                current.mappings.clone(),
                current.settings.clone(),
                current.index_uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id: current.allocation_id,
                    primary_term: current.primary_term,
                    allow_empty_creation: current.allow_empty_creation,
                },
            )
            .await
        {
            self.report_local_copy_failure(
                index_name,
                &current.index_uuid,
                shard_id,
                current.allocation_id,
                current.primary_term,
                &error,
            )
            .await;
            return Err(format!("failed to open primary shard copy: {error}"));
        }
        if let Err(error) = self
            .shard_manager
            .raise_copy_fence_blocking(
                index_name.to_string(),
                shard_id,
                current.index_uuid.clone(),
                current.allocation_id,
                current.primary_term,
            )
            .await
        {
            self.report_local_copy_failure(
                index_name,
                &current.index_uuid,
                shard_id,
                current.allocation_id,
                current.primary_term,
                &error,
            )
            .await;
            return Err(format!("failed to persist primary fence: {error}"));
        }
        if self
            .primary_activation_state
            .activated_terms
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&initial_key)
            .is_some_and(|term| *term == current.primary_term)
        {
            let activated = ActivatedPrimary {
                index_uuid: current.index_uuid,
                allocation_id: current.allocation_id,
                primary_term: current.primary_term,
            };
            self.retry_pending_promotion_noops(index_name, shard_id, &activated)
                .await?;
            return Ok(activated);
        }
        if self.raft.is_none() {
            let activated = ActivatedPrimary {
                index_uuid: current.index_uuid,
                allocation_id: current.allocation_id,
                primary_term: current.primary_term,
            };
            self.prepare_local_primary_activation(index_name, shard_id, &activated)
                .await?;
            return Ok(activated);
        }

        let key = initial_key.clone();
        if self
            .primary_activation_state
            .activated_terms
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)
            .is_some_and(|term| *term == current.primary_term)
        {
            let activated = ActivatedPrimary {
                index_uuid: current.index_uuid,
                allocation_id: current.allocation_id,
                primary_term: current.primary_term,
            };
            self.retry_pending_promotion_noops(index_name, shard_id, &activated)
                .await?;
            return Ok(activated);
        }

        let activation_locks = self.primary_activation_state.copy_locks(&initial_key);
        let _activation_guard = activation_locks.activation.lock().await;
        let current = self.primary_routing(index_name, shard_id)?;
        let current_key = (current.index_uuid.clone(), shard_id, current.allocation_id);
        if current_key != initial_key {
            return Err(format!(
                "primary allocation changed for shard [{index_name}][{shard_id}] while activation was waiting; retry the write"
            ));
        }
        if let Err(error) = self
            .shard_manager
            .open_primary_assigned_shard_with_settings_blocking(
                index_name.to_string(),
                shard_id,
                current.mappings.clone(),
                current.settings.clone(),
                current.index_uuid.clone(),
                crate::shard::AssignedShardOpen {
                    allocation_id: current.allocation_id,
                    primary_term: current.primary_term,
                    allow_empty_creation: current.allow_empty_creation,
                },
            )
            .await
        {
            self.report_local_copy_failure(
                index_name,
                &current.index_uuid,
                shard_id,
                current.allocation_id,
                current.primary_term,
                &error,
            )
            .await;
            return Err(format!("failed to open primary shard copy: {error}"));
        }
        if let Err(error) = self
            .shard_manager
            .raise_copy_fence_blocking(
                index_name.to_string(),
                shard_id,
                current.index_uuid.clone(),
                current.allocation_id,
                current.primary_term,
            )
            .await
        {
            self.report_local_copy_failure(
                index_name,
                &current.index_uuid,
                shard_id,
                current.allocation_id,
                current.primary_term,
                &error,
            )
            .await;
            return Err(format!("failed to persist primary fence: {error}"));
        }
        let expected_term = current.primary_term;
        let index_uuid = current.index_uuid.clone();
        let allocation_id = current.allocation_id;
        let key = (index_uuid.clone(), shard_id, allocation_id);
        if self
            .primary_activation_state
            .activated_terms
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&key)
            .is_some_and(|term| *term == expected_term)
        {
            let activated = ActivatedPrimary {
                index_uuid,
                allocation_id,
                primary_term: expected_term,
            };
            self.retry_pending_promotion_noops(index_name, shard_id, &activated)
                .await?;
            return Ok(activated);
        }

        let raft = self
            .raft
            .as_ref()
            .expect("Raft presence checked before primary activation");
        if raft.is_leader() {
            let response = raft
                .client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
                    index_name: index_name.to_string(),
                    index_uuid: index_uuid.clone(),
                    shard_id,
                    primary: self.local_node_id.clone(),
                    allocation_id,
                    expected_term,
                })
                .await
                .map_err(|error| format!("primary activation Raft write failed: {error}"))?;
            response
                .data
                .into_result()
                .map_err(|error| format!("primary activation was rejected: {error}"))?;
        } else {
            let cluster_state = self.cluster_manager.get_state();
            let master_id = cluster_state
                .master_node
                .as_ref()
                .ok_or_else(|| "no Raft leader is known for primary activation".to_string())?;
            let master = cluster_state.nodes.get(master_id).ok_or_else(|| {
                format!("Raft leader '{master_id}' is absent from local cluster state")
            })?;
            self.transport_client
                .forward_activate_primary(
                    master,
                    ActivatePrimaryRequest {
                        index_name: index_name.to_string(),
                        index_uuid: index_uuid.clone(),
                        shard_id,
                        primary_node_id: self.local_node_id.clone(),
                        expected_term,
                        allocation_id: Some(allocation_id),
                    },
                )
                .await
                .map_err(|error| format!("primary activation forward failed: {error}"))?;
        }

        let activated_term = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            async {
                loop {
                    let cluster_state = self.cluster_manager.get_state();
                    let metadata = cluster_state.indices.get(index_name).ok_or_else(|| {
                        format!("index [{index_name}] disappeared during primary activation")
                    })?;
                    if metadata.uuid.as_str() != index_uuid {
                        return Err(format!(
                            "index [{index_name}] was replaced during primary activation"
                        ));
                    }
                    let routing = metadata.shard_routing.get(&shard_id).ok_or_else(|| {
                        format!(
                            "shard [{index_name}][{shard_id}] disappeared during primary activation"
                        )
                    })?;
                    if routing.primary != self.local_node_id {
                        return Err(format!(
                            "node [{}] lost primary assignment for shard [{index_name}][{shard_id}] during activation",
                            self.local_node_id
                        ));
                    }
                    if cluster_state.shard_allocation_id(
                        index_name,
                        shard_id,
                        &self.local_node_id,
                    ) != Some(allocation_id)
                    {
                        return Err(format!(
                            "node [{}] lost allocation {} for shard [{index_name}][{shard_id}] during activation",
                            self.local_node_id, allocation_id
                        ));
                    }
                    if routing.primary_term > expected_term {
                        return Ok(routing.primary_term);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            },
        )
        .await
        .map_err(|_| {
            format!(
                "timed out waiting for primary activation of shard [{index_name}][{shard_id}]"
            )
        })??;

        if let Err(error) = self
            .shard_manager
            .raise_copy_fence_blocking(
                index_name.to_string(),
                shard_id,
                index_uuid.clone(),
                allocation_id,
                activated_term,
            )
            .await
        {
            self.report_local_copy_failure(
                index_name,
                &index_uuid,
                shard_id,
                allocation_id,
                activated_term,
                &error,
            )
            .await;
            return Err(format!(
                "failed to persist activated primary fence: {error}"
            ));
        }
        let activated = ActivatedPrimary {
            index_uuid,
            allocation_id,
            primary_term: activated_term,
        };
        self.prepare_local_primary_activation(index_name, shard_id, &activated)
            .await?;
        Ok(activated)
    }

    async fn prepare_local_primary_activation(
        &self,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) -> Result<(), String> {
        let guard = self
            .peer_recovery_exclusive_guard(&activated_primary.index_uuid, shard_id)
            .await;
        let engine = self
            .shard_manager
            .get_shard(index_name, shard_id)
            .ok_or_else(|| {
                format!("primary shard [{index_name}][{shard_id}] is not open during activation")
            })?;
        self.validated_primary_write_state(index_name, shard_id, activated_primary)?;
        let activation_engine = engine.clone();
        let primary_term = activated_primary.primary_term;
        let cluster_manager = self.cluster_manager.clone();
        let index_name_for_write = index_name.to_owned();
        let index_uuid = activated_primary.index_uuid.clone();
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::TraceCopy {
            node: self.local_node_id.clone(),
            index_uuid: activated_primary.index_uuid.clone(),
            shard: shard_id,
            allocation: activated_primary.allocation_id,
        };
        #[cfg(feature = "protocol-trace")]
        let activation_trace_copy = trace_copy.clone();
        let noops = self
            .worker_pools
            .spawn_write(move || {
                require_index_uuid(&cluster_manager, &index_name_for_write, Some(&index_uuid))?;
                #[cfg(feature = "protocol-trace")]
                {
                    crate::protocol_trace::with_open_copy(activation_trace_copy, || {
                        activation_engine.prepare_primary_activation(primary_term)
                    })
                }
                #[cfg(not(feature = "protocol-trace"))]
                activation_engine.prepare_primary_activation(primary_term)
            })
            .await
            .map_err(|error| format!("primary activation task failed: {error}"))?
            .map_err(|error| format!("primary activation replay/gap fill failed: {error}"))?;
        let activation_key = (
            activated_primary.index_uuid.clone(),
            shard_id,
            activated_primary.allocation_id,
        );
        if !noops.is_empty() {
            self.primary_activation_state
                .pending_noops
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .insert(
                    activation_key.clone(),
                    PendingPromotionNoOps {
                        primary_term: activated_primary.primary_term,
                        operations: noops,
                    },
                );
        }
        {
            let mut activated_terms = self
                .primary_activation_state
                .activated_terms
                .write()
                .unwrap_or_else(|error| error.into_inner());
            activated_terms.insert(activation_key, activated_primary.primary_term);
            #[cfg(feature = "protocol-trace")]
            crate::protocol_trace::record_primary_activated(
                &trace_copy,
                activated_primary.primary_term,
            );
        }
        drop(guard);

        self.retry_pending_promotion_noops(index_name, shard_id, activated_primary)
            .await
    }

    async fn retry_pending_promotion_noops(
        &self,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) -> Result<(), String> {
        let activation_key = (
            activated_primary.index_uuid.clone(),
            shard_id,
            activated_primary.allocation_id,
        );
        let has_pending = self
            .primary_activation_state
            .pending_noops
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&activation_key)
            .is_some_and(|pending| {
                pending.primary_term == activated_primary.primary_term
                    && !pending.operations.is_empty()
            });
        if !has_pending {
            return Ok(());
        }

        let activation_locks = self.primary_activation_state.copy_locks(&activation_key);
        let _replication_guard = activation_locks.noop_replication.lock().await;
        let operations = self
            .primary_activation_state
            .pending_noops
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&activation_key)
            .filter(|pending| pending.primary_term == activated_primary.primary_term)
            .map(|pending| pending.operations.clone())
            .unwrap_or_default();
        if operations.is_empty() {
            return Ok(());
        }

        let engine = self
            .shard_manager
            .get_shard(index_name, shard_id)
            .ok_or_else(|| {
                format!(
                    "primary shard [{index_name}][{shard_id}] is not open during promotion NoOp retry"
                )
            })?;
        let write_state =
            self.validated_primary_write_state(index_name, shard_id, activated_primary)?;
        let mut failed_operations = Vec::new();
        for batch in operations.chunks(MAX_RECOVERY_OPS) {
            let first_seq_no = batch
                .first()
                .expect("promotion NoOp batch is non-empty")
                .seq_no;
            let last_seq_no = batch
                .last()
                .expect("promotion NoOp batch is non-empty")
                .seq_no;
            match crate::replication::replicate_noop_batch_with_durability(
                &self.transport_client,
                &write_state,
                index_name,
                shard_id,
                batch,
                activated_primary.primary_term,
                self.shard_manager.durability(),
            )
            .await
            {
                Ok(replica_checkpoints) => {
                    self.record_replica_checkpoints(
                        &engine,
                        index_name,
                        shard_id,
                        activated_primary,
                        engine.sequence_stats(),
                        &replica_checkpoints,
                    );
                }
                Err(errors) => {
                    self.report_definitive_replica_failures(
                        &write_state,
                        index_name,
                        shard_id,
                        activated_primary.primary_term,
                        &errors,
                    )
                    .await;
                    let primary_sequence = engine.sequence_stats();
                    if let Some(metadata) = write_state.indices.get(index_name) {
                        for replica_node_id in metadata.in_sync_replica_nodes(shard_id) {
                            if let Some(allocation_id) = write_state.shard_allocation_id(
                                index_name,
                                shard_id,
                                replica_node_id,
                            ) {
                                self.shard_manager.isr_tracker.update_replica_checkpoint(
                                    index_name,
                                    &activated_primary.index_uuid,
                                    shard_id,
                                    activated_primary.primary_term,
                                    primary_sequence.processed_checkpoint,
                                    crate::shard::ReplicaCheckpointUpdate {
                                        node_id: replica_node_id.clone(),
                                        allocation_id,
                                        processed_checkpoint: None,
                                        persisted_checkpoint: None,
                                    },
                                );
                            }
                        }
                    }
                    tracing::warn!(
                        index = index_name,
                        shard_id,
                        first_seq_no,
                        last_seq_no,
                        operation_count = batch.len(),
                        errors = ?errors,
                        "Promotion NoOp batch replication failed; retaining replica gap observation"
                    );
                    failed_operations.extend_from_slice(batch);
                }
            }
        }

        let mut pending = self
            .primary_activation_state
            .pending_noops
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if pending
            .get(&activation_key)
            .is_some_and(|current| current.primary_term == activated_primary.primary_term)
        {
            if failed_operations.is_empty() {
                pending.remove(&activation_key);
            } else if let Some(current) = pending.get_mut(&activation_key) {
                current.operations = failed_operations;
            }
        }
        Ok(())
    }

    fn validated_primary_write_state(
        &self,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
    ) -> Result<crate::cluster::state::ClusterState, String> {
        let state = self.cluster_manager.get_state();
        let metadata = state
            .indices
            .get(index_name)
            .ok_or_else(|| format!("index [{index_name}] is not present in local cluster state"))?;
        if metadata.uuid.as_str() != activated_primary.index_uuid {
            return Err(format!(
                "index UUID changed for [{index_name}] from activated UUID [{}] to [{}]; retry the write",
                activated_primary.index_uuid, metadata.uuid
            ));
        }
        let routing = metadata.shard_routing.get(&shard_id).ok_or_else(|| {
            format!("shard [{index_name}][{shard_id}] is not present in local cluster state")
        })?;
        if routing.primary != self.local_node_id {
            return Err(format!(
                "node [{}] is no longer the primary for shard [{index_name}][{shard_id}]; retry after refreshing shard routing",
                self.local_node_id
            ));
        }
        if routing.primary_term != activated_primary.primary_term {
            return Err(format!(
                "primary term changed for shard [{index_name}][{shard_id}] from activated term {} to {}; retry the write",
                activated_primary.primary_term, routing.primary_term
            ));
        }
        if state.shard_allocation_id(index_name, shard_id, &self.local_node_id)
            != Some(activated_primary.allocation_id)
        {
            return Err(format!(
                "primary allocation changed for shard [{index_name}][{shard_id}] from {}",
                activated_primary.allocation_id
            ));
        }
        self.shard_manager
            .validate_open_copy_identity(
                index_name,
                shard_id,
                &activated_primary.index_uuid,
                activated_primary.allocation_id,
            )
            .map_err(|error| {
                format!(
                    "local primary identity is invalid for shard [{index_name}][{shard_id}]: {error}"
                )
            })?;
        Ok(state)
    }

    #[allow(clippy::result_large_err)]
    async fn get_or_open_shard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        self.get_or_open_shard_with_override(index_name, shard_id, None)
            .await
    }

    #[allow(clippy::result_large_err)]
    async fn get_or_open_shard_with_override(
        &self,
        index_name: &str,
        shard_id: u32,
        open_override: Option<DynamicShardOpenOverride>,
    ) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        let state = self.cluster_manager.get_state();
        let Some(metadata) = state.indices.get(index_name) else {
            return Err(Status::not_found(format!(
                "Shard [{index_name}][{shard_id}] not found on this node"
            )));
        };
        if !metadata.shard_routing.contains_key(&shard_id) {
            return Err(Status::not_found(format!(
                "Shard [{index_name}][{shard_id}] not found on this node"
            )));
        }
        let assigned = self
            .assigned_local_shard(index_name, shard_id)
            .map_err(Status::failed_precondition)?;
        if let Some(engine) = self.shard_manager.get_shard(index_name, shard_id) {
            if let Err(error) = self.shard_manager.validate_open_copy_identity(
                index_name,
                shard_id,
                &assigned.index_uuid,
                assigned.allocation_id,
            ) {
                self.report_local_copy_failure(
                    index_name,
                    &assigned.index_uuid,
                    shard_id,
                    assigned.allocation_id,
                    assigned.primary_term,
                    &error,
                )
                .await;
                return Err(Status::failed_precondition(error.to_string()));
            }
            return Ok(engine);
        }

        if let Some(open_override) = open_override {
            if open_override.index_uuid != assigned.index_uuid {
                return Err(Status::aborted(format!(
                    "index UUID changed for [{index_name}] before shard open"
                )));
            }
            let assignment = crate::shard::AssignedShardOpen {
                allocation_id: assigned.allocation_id,
                primary_term: assigned.primary_term,
                allow_empty_creation: assigned.allow_empty_creation,
            };
            let result = if assigned.is_primary {
                self.shard_manager
                    .open_primary_assigned_shard_with_settings_blocking(
                        index_name.to_string(),
                        shard_id,
                        open_override.mappings,
                        open_override.settings,
                        open_override.index_uuid,
                        assignment,
                    )
                    .await
            } else {
                self.shard_manager
                    .open_assigned_shard_with_settings_blocking(
                        index_name.to_string(),
                        shard_id,
                        open_override.mappings,
                        open_override.settings,
                        open_override.index_uuid,
                        assignment,
                    )
                    .await
            };
            return match result {
                Ok(engine) => Ok(engine),
                Err(error) => {
                    self.report_local_copy_failure(
                        index_name,
                        &assigned.index_uuid,
                        shard_id,
                        assigned.allocation_id,
                        assigned.primary_term,
                        &error,
                    )
                    .await;
                    Err(Status::internal(format!("Failed to open shard: {error}")))
                }
            };
        }

        let assignment = crate::shard::AssignedShardOpen {
            allocation_id: assigned.allocation_id,
            primary_term: assigned.primary_term,
            allow_empty_creation: assigned.allow_empty_creation,
        };
        let result = if assigned.is_primary {
            self.shard_manager
                .open_primary_assigned_shard_with_settings_blocking(
                    index_name.to_string(),
                    shard_id,
                    assigned.mappings,
                    assigned.settings,
                    assigned.index_uuid.clone(),
                    assignment,
                )
                .await
        } else {
            self.shard_manager
                .open_assigned_shard_with_settings_blocking(
                    index_name.to_string(),
                    shard_id,
                    assigned.mappings,
                    assigned.settings,
                    assigned.index_uuid.clone(),
                    assignment,
                )
                .await
        };
        match result {
            Ok(engine) => Ok(engine),
            Err(error) => {
                self.report_local_copy_failure(
                    index_name,
                    &assigned.index_uuid,
                    shard_id,
                    assigned.allocation_id,
                    assigned.primary_term,
                    &error,
                )
                .await;
                Err(Status::internal(format!("Failed to open shard: {error}")))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    async fn get_or_open_search_shard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        get_or_open_read_shard(
            self.cluster_manager.as_ref(),
            &self.shard_manager,
            &self.local_node_id,
            index_name,
            shard_id,
        )
        .await
    }

    /// Advance the replicated checkpoint only when every in-sync copy reports
    /// a contiguous persisted prefix.
    fn advance_global_checkpoint(
        engine: &Arc<dyn crate::engine::SearchEngine>,
        primary_checkpoint: Option<u64>,
        replica_checkpoints: &[crate::shard::ReplicaCheckpointUpdate],
    ) {
        let Some(primary_checkpoint) = primary_checkpoint else {
            return;
        };
        if replica_checkpoints.is_empty() {
            engine.update_global_checkpoint(primary_checkpoint);
            return;
        }
        let Some(min_replica) = replica_checkpoints
            .iter()
            .map(|checkpoint| checkpoint.persisted_checkpoint)
            .collect::<Option<Vec<_>>>()
            .and_then(|checkpoints| checkpoints.into_iter().min())
        else {
            return;
        };
        let global = std::cmp::min(primary_checkpoint, min_replica);
        if engine
            .global_checkpoint()
            .is_none_or(|current| global > current)
        {
            engine.update_global_checkpoint(global);
        }
    }

    fn record_replica_checkpoints(
        &self,
        engine: &Arc<dyn crate::engine::SearchEngine>,
        index_name: &str,
        shard_id: u32,
        activated_primary: &ActivatedPrimary,
        primary_sequence: crate::engine::SequenceStats,
        replica_checkpoints: &[crate::shard::ReplicaCheckpointUpdate],
    ) {
        let state =
            match self.validated_primary_write_state(index_name, shard_id, activated_primary) {
                Ok(state) => state,
                Err(reason) => {
                    tracing::debug!(
                        index = index_name,
                        shard_id,
                        reason,
                        "Skipping checkpoint reports from obsolete primary authority"
                    );
                    return;
                }
            };
        let metadata = &state.indices[index_name];
        let routing = &metadata.shard_routing[&shard_id];
        let current_reports = replica_checkpoints
            .iter()
            .filter(|checkpoint| {
                routing.is_replica_in_sync(&checkpoint.node_id)
                    && state.shard_allocation_id(index_name, shard_id, &checkpoint.node_id)
                        == Some(checkpoint.allocation_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        #[cfg(test)]
        {
            let hook = self
                .primary_activation_state
                .checkpoint_recording_hook
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        // Request durability persists before fan-out, so a post-replication sample
        // covers the primary prefix completed by a last-gap or highest-sequence round.
        // Serialized tracker maxima then converge without holding the node-wide
        // tracker lock while waiting for the engine's apply-state mutex.
        let primary_persisted_checkpoint = engine.sequence_stats().persisted_checkpoint;
        self.shard_manager
            .isr_tracker
            .with_updated_replica_checkpoints_at(
                index_name,
                shard_id,
                crate::shard::ReplicaCheckpointContext {
                    index_uuid: &activated_primary.index_uuid,
                    primary_term: activated_primary.primary_term,
                    primary_processed_checkpoint: primary_sequence.processed_checkpoint,
                },
                &current_reports,
                std::time::Instant::now(),
                |tracked| {
                    let authoritative = metadata
                        .in_sync_replica_nodes(shard_id)
                        .into_iter()
                        .map(|node_id| {
                            let allocation_id =
                                state.shard_allocation_id(index_name, shard_id, node_id)?;
                            let checkpoint = tracked.get(node_id)?;
                            checkpoint
                                .matches_copy(
                                    &activated_primary.index_uuid,
                                    allocation_id,
                                    activated_primary.primary_term,
                                )
                                .then(|| crate::shard::ReplicaCheckpointUpdate {
                                    node_id: node_id.clone(),
                                    allocation_id,
                                    processed_checkpoint: checkpoint.processed_checkpoint,
                                    persisted_checkpoint: checkpoint.persisted_checkpoint,
                                })
                        })
                        .collect::<Option<Vec<_>>>();
                    let Some(authoritative) = authoritative else {
                        tracing::debug!(
                            index = index_name,
                            shard_id,
                            "Waiting for checkpoint reports from every current in-sync copy"
                        );
                        return;
                    };
                    Self::advance_global_checkpoint(
                        engine,
                        primary_persisted_checkpoint,
                        &authoritative,
                    );
                },
            );
    }

    fn replication_failure_message(
        failures: &[crate::replication::ReplicaReplicationFailure],
    ) -> String {
        failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    }

    async fn report_definitive_replica_failures(
        &self,
        write_state: &crate::cluster::state::ClusterState,
        index_name: &str,
        shard_id: u32,
        primary_term: u64,
        failures: &[crate::replication::ReplicaReplicationFailure],
    ) {
        let Some(metadata) = write_state.indices.get(index_name) else {
            return;
        };
        let Some(routing) = metadata.shard_routing.get(&shard_id) else {
            return;
        };
        if routing.primary != self.local_node_id || routing.primary_term != primary_term {
            return;
        }
        let index_uuid = metadata.uuid.to_string();

        for failure in failures.iter().filter(|failure| failure.definitive) {
            let Some(allocation_id) = failure.allocation_id else {
                continue;
            };
            if !routing.is_replica_in_sync(&failure.node_id)
                || write_state.shard_allocation_id(index_name, shard_id, &failure.node_id)
                    != Some(allocation_id)
            {
                continue;
            }

            let command = crate::consensus::types::ClusterCommand::FailShardCopy {
                index_name: index_name.to_string(),
                index_uuid: index_uuid.clone(),
                shard_id,
                node: failure.node_id.clone(),
                allocation_id,
                expected_primary_term: primary_term,
                promote_only: false,
                promotion_candidate: None,
            };
            let result = if let Some(raft) = self.raft.as_ref().filter(|raft| raft.is_leader()) {
                crate::consensus::client_write_checked(raft, command)
                    .await
                    .map_err(anyhow::Error::msg)
            } else {
                let Some(master_id) = write_state.master_node.as_ref() else {
                    tracing::warn!(
                        index = index_name,
                        shard_id,
                        replica = failure.node_id,
                        allocation_id,
                        "Cannot report definitive replica failure because no Raft leader is known"
                    );
                    continue;
                };
                let Some(master) = write_state.nodes.get(master_id) else {
                    tracing::warn!(
                        index = index_name,
                        shard_id,
                        replica = failure.node_id,
                        allocation_id,
                        master = master_id,
                        "Cannot report definitive replica failure because the Raft leader is absent"
                    );
                    continue;
                };
                self.transport_client
                    .forward_fail_shard_copy(
                        master,
                        FailShardCopyRequest {
                            index_name: index_name.to_string(),
                            index_uuid: index_uuid.clone(),
                            shard_id,
                            node_id: failure.node_id.clone(),
                            allocation_id: Some(allocation_id),
                            promote_only: false,
                            expected_primary_term: primary_term,
                        },
                    )
                    .await
            };
            if let Err(error) = result {
                tracing::warn!(
                    index = index_name,
                    shard_id,
                    replica = failure.node_id,
                    allocation_id,
                    primary_term,
                    reason = failure.message,
                    error = %error,
                    "Primary could not remove a definitively failed replica"
                );
            }
        }
    }

    /// Run a maintenance operation (refresh or flush) only on shards assigned
    /// to this node per the routing table, skipping orphaned shards.
    #[cfg(test)]
    fn run_maintenance_on_assigned_shards<F>(&self, index_name: &str, op: F) -> (u32, u32)
    where
        F: Fn(&Arc<dyn crate::engine::SearchEngine>) -> crate::common::Result<()>,
    {
        let mut successful = 0u32;
        let mut failed = 0u32;

        let cs = self.cluster_manager.get_state();
        let metadata = match cs.indices.get(index_name) {
            Some(m) => m,
            None => return (successful, failed),
        };

        for (shard_id, routing) in &metadata.shard_routing {
            let assigned_here = routing.primary == self.local_node_id
                || routing.replicas.iter().any(|n| n == &self.local_node_id);
            if !assigned_here {
                continue;
            }
            if let Some(engine) = self.shard_manager.get_shard(index_name, *shard_id) {
                match op(&engine) {
                    Ok(_) => successful += 1,
                    Err(e) => {
                        tracing::error!("Maintenance on {}/{} failed: {}", index_name, shard_id, e);
                        failed += 1;
                    }
                }
            }
        }

        (successful, failed)
    }

    /// Detect new fields in a document payload, register them via Raft, and if
    /// needed prepare merged mappings for the immediate shard open.
    async fn ensure_dynamic_mappings(
        &self,
        index_name: &str,
        shard_id: u32,
        payload: &serde_json::Value,
    ) -> Result<Option<DynamicShardOpenOverride>, Status> {
        let cs = self.cluster_manager.get_state();
        let metadata = cs.indices.get(index_name).ok_or_else(|| {
            Status::not_found(format!("index '{index_name}' not found in cluster state"))
        })?;

        if !matches!(
            metadata.dynamic,
            crate::cluster::state::DynamicMapping::True
        ) {
            if matches!(
                metadata.dynamic,
                crate::cluster::state::DynamicMapping::Strict
            ) {
                let unknown_fields =
                    crate::common::detect_unknown_fields(payload, &metadata.mappings);
                if !unknown_fields.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "strict mapping: unknown fields {unknown_fields:?} in index '{index_name}'"
                    )));
                }
            }
            return Ok(None);
        }

        let new_fields = crate::common::detect_new_fields(payload, &metadata.mappings);
        if new_fields.is_empty() {
            return Ok(None);
        }

        tracing::info!(
            "Dynamic mapping: detected {} new field(s) in index '{}': {:?}",
            new_fields.len(),
            index_name,
            new_fields.keys().collect::<Vec<_>>()
        );

        let mut merged_mappings = metadata.mappings.clone();
        for (name, mapping) in &new_fields {
            merged_mappings
                .entry(name.clone())
                .or_insert(mapping.clone());
        }

        self.apply_dynamic_mappings(
            index_name,
            shard_id,
            &new_fields,
            &metadata.dynamic,
            metadata,
            &merged_mappings,
        )
        .await?;

        Ok(Some(DynamicShardOpenOverride {
            mappings: merged_mappings,
            settings: metadata.settings.clone(),
            index_uuid: metadata.uuid.to_string(),
        }))
    }

    /// Batch version: detect/register new fields and prepare merged mappings for
    /// the immediate shard open if needed.
    async fn ensure_dynamic_mappings_batch(
        &self,
        index_name: &str,
        shard_id: u32,
        docs: &[(String, serde_json::Value)],
    ) -> Result<Option<DynamicShardOpenOverride>, Status> {
        let cs = self.cluster_manager.get_state();
        let metadata = cs.indices.get(index_name).ok_or_else(|| {
            Status::not_found(format!("index '{index_name}' not found in cluster state"))
        })?;

        if !matches!(
            metadata.dynamic,
            crate::cluster::state::DynamicMapping::True
        ) {
            if matches!(
                metadata.dynamic,
                crate::cluster::state::DynamicMapping::Strict
            ) {
                let unknown_fields =
                    crate::common::detect_unknown_fields_batch(docs, &metadata.mappings);
                if !unknown_fields.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "strict mapping: unknown fields {unknown_fields:?} in index '{index_name}'"
                    )));
                }
            }
            return Ok(None);
        }

        let new_fields = crate::common::detect_new_fields_batch(docs, &metadata.mappings);
        if new_fields.is_empty() {
            return Ok(None);
        }

        tracing::info!(
            "Dynamic mapping (bulk): detected {} new field(s) in index '{}': {:?}",
            new_fields.len(),
            index_name,
            new_fields.keys().collect::<Vec<_>>()
        );

        let mut merged_mappings = metadata.mappings.clone();
        for (name, mapping) in &new_fields {
            merged_mappings
                .entry(name.clone())
                .or_insert(mapping.clone());
        }

        self.apply_dynamic_mappings(
            index_name,
            shard_id,
            &new_fields,
            &metadata.dynamic,
            metadata,
            &merged_mappings,
        )
        .await?;

        Ok(Some(DynamicShardOpenOverride {
            mappings: merged_mappings,
            settings: metadata.settings.clone(),
            index_uuid: metadata.uuid.to_string(),
        }))
    }

    /// Common path: commit new mappings via Raft and immediately reopen any live
    /// local shard so the current request can index against the updated schema.
    async fn apply_dynamic_mappings(
        &self,
        index_name: &str,
        shard_id: u32,
        new_fields: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
        dynamic: &crate::cluster::state::DynamicMapping,
        metadata: &crate::cluster::state::IndexMetadata,
        merged_mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> Result<(), Status> {
        crate::common::validate_mapping_field_names(new_fields.keys().map(String::as_str))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        for (name, mapping) in new_fields {
            crate::common::validate_builtin_body_field_mapping(name, mapping)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        }
        if let Some(raft) = self.raft.as_ref()
            && raft.is_leader()
        {
            let cmd = crate::consensus::types::ClusterCommand::AddMappings {
                index_name: index_name.to_string(),
                new_fields: new_fields.clone(),
                dynamic: dynamic.clone(),
            };
            raft.client_write(cmd).await.map_err(|e| {
                Status::internal(format!(
                    "Raft AddMappings failed for index '{index_name}': {e}"
                ))
            })?;
        } else {
            let cs = self.cluster_manager.get_state();
            if let Some(master_id) = cs.master_node.as_ref()
                && let Some(master_node) = cs.nodes.get(master_id)
            {
                self.transport_client
                    .forward_add_mappings(master_node, index_name, new_fields, dynamic)
                    .await
                    .map_err(|e| {
                        Status::internal(format!(
                            "forward AddMappings to leader for index '{index_name}': {e}"
                        ))
                    })?;
            } else {
                return Err(Status::internal(format!(
                    "No master node available to commit dynamic mappings for index '{index_name}'"
                )));
            }
        }

        #[cfg(test)]
        {
            if let Some(sender) = self
                .peer_recovery_state
                .dynamic_mapping_committed_sender
                .lock()
                .await
                .take()
            {
                let _ = sender.send(());
            }
            if let Some(release) = self
                .peer_recovery_state
                .dynamic_mapping_release
                .lock()
                .await
                .take()
            {
                let _ = release.await;
            }
        }

        if self.shard_manager.get_shard(index_name, shard_id).is_some() {
            let current_state = self.cluster_manager.get_state();
            let Some(current_metadata) = current_state.indices.get(index_name) else {
                return Ok(());
            };
            let Some(current_routing) = current_metadata.shard_routing.get(&shard_id) else {
                return Ok(());
            };
            let Some(previous_routing) = metadata.shard_routing.get(&shard_id) else {
                return Ok(());
            };
            if current_metadata.uuid != metadata.uuid {
                self.shard_manager
                    .close_index_shards_blocking_with_reason(
                        index_name.to_string(),
                        crate::shard::SHARD_DATA_REMOVE_REASON_STALE_UUID_REPLACEMENT,
                    )
                    .await
                    .map_err(|error| {
                        Status::internal(format!(
                            "close stale UUID shard after dynamic mapping for [{index_name}][{shard_id}]: {error}"
                        ))
                    })?;
                return Ok(());
            }
            if current_routing.primary != self.local_node_id
                || current_routing.primary != previous_routing.primary
                || current_routing.primary_term != previous_routing.primary_term
            {
                return Ok(());
            }
            let Some(allocation_id) =
                current_state.shard_allocation_id(index_name, shard_id, &self.local_node_id)
            else {
                return Ok(());
            };
            if self
                .shard_manager
                .copy_identity(index_name, shard_id)
                .is_none_or(|identity| {
                    identity.index_uuid != current_metadata.uuid.as_str()
                        || identity.allocation_id != allocation_id
                })
            {
                return Ok(());
            }
            self.shard_manager
                .reopen_shard(
                    index_name.to_string(),
                    shard_id,
                    merged_mappings.clone(),
                    metadata.settings.clone(),
                    metadata.uuid.to_string(),
                    allocation_id,
                )
                .await
                .map_err(|e| {
                    let message = format!(
                        "reopen shard after dynamic mapping for [{index_name}][{shard_id}]: {e}"
                    );
                    if e.is::<crate::shard::ShardReopenAborted>() {
                        Status::aborted(format!("{message}; retry the write"))
                    } else {
                        Status::internal(message)
                    }
                })?;
        }

        Ok(())
    }
}

/// Create a gRPC transport server **without Raft** for shard-level integration tests.
/// Production code must use [`create_transport_service_with_raft`].
fn build_transport_service_for_test(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
) -> TransportService {
    #[cfg(feature = "protocol-trace")]
    {
        cluster_manager.set_protocol_trace_node(local_node_id.clone());
        shard_manager.set_protocol_trace_node(local_node_id.clone());
    }
    let storage_manager = Arc::new(
        crate::storage::StorageManager::new_in_path(shard_manager.data_dir()).unwrap_or_else(
            |error| panic!("create default test remote_store storage manager: {error}"),
        ),
    );
    let peer_recovery_state = peer_recovery::new_peer_recovery_transport_state();
    shard_manager.register_source_recovery_cleanup(peer_recovery_state.clone());
    TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        storage_manager,
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
        raft: None,
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        task_manager,
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state,
        join_lock: new_join_lock(),
    }
}

pub fn create_transport_service_for_test(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    let service = build_transport_service_for_test(
        cluster_manager,
        shard_manager,
        transport_client,
        task_manager,
        local_node_id,
    );
    peer_recovery::start_peer_recovery_reaper(service.clone());
    InternalTransportServer::new(service)
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
}

#[cfg(feature = "protocol-trace")]
pub fn create_transport_service_for_test_with_handle(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
) -> (InternalTransportServer<TransportService>, TransportService) {
    let service = build_transport_service_for_test(
        cluster_manager,
        shard_manager,
        transport_client,
        task_manager,
        local_node_id,
    );
    peer_recovery::start_peer_recovery_reaper(service.clone());
    (
        InternalTransportServer::new(service.clone())
            .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE),
        service,
    )
}

/// Create the gRPC transport server with Raft consensus.
pub fn create_transport_service_with_raft(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    raft: Arc<RaftInstance>,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    let remote_store_resources = RemoteStoreTransportResources {
        storage_manager: Arc::new(
            crate::storage::StorageManager::new_in_path(shard_manager.data_dir()).unwrap_or_else(
                |error| panic!("create default remote_store storage manager: {error}"),
            ),
        ),
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
    };
    create_transport_service_with_raft_and_storage(
        cluster_manager,
        shard_manager,
        transport_client,
        raft,
        task_manager,
        remote_store_resources,
        local_node_id,
    )
}

pub fn create_transport_service_with_raft_and_storage(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    raft: Arc<RaftInstance>,
    task_manager: Arc<crate::tasks::TaskManager>,
    remote_store_resources: RemoteStoreTransportResources,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    create_transport_service_with_raft_and_storage_handle(
        cluster_manager,
        shard_manager,
        transport_client,
        raft,
        task_manager,
        remote_store_resources,
        local_node_id,
    )
    .0
}

pub(crate) fn create_transport_service_with_raft_and_storage_handle(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    raft: Arc<RaftInstance>,
    task_manager: Arc<crate::tasks::TaskManager>,
    remote_store_resources: RemoteStoreTransportResources,
    local_node_id: String,
) -> (InternalTransportServer<TransportService>, TransportService) {
    #[cfg(feature = "protocol-trace")]
    {
        cluster_manager.set_protocol_trace_node(local_node_id.clone());
        shard_manager.set_protocol_trace_node(local_node_id.clone());
    }
    let peer_recovery_state = peer_recovery::new_peer_recovery_transport_state();
    shard_manager.register_source_recovery_cleanup(peer_recovery_state.clone());
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        storage_manager: remote_store_resources.storage_manager,
        remote_store_reader_cache: remote_store_resources.remote_store_reader_cache,
        raft: Some(raft),
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        task_manager,
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state,
        join_lock: new_join_lock(),
    };
    peer_recovery::start_peer_recovery_reaper(service.clone());
    let handle = service.clone();
    (
        InternalTransportServer::new(service)
            .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE),
        handle,
    )
}

#[cfg(test)]
mod tests;
