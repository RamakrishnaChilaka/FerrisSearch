//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::proto::internal_transport_server::{
    InternalTransport, InternalTransportServer,
};
use crate::transport::proto::*;
use crate::wal::WriteAheadLog;
use futures::{FutureExt, Stream, stream};
use openraft::type_config::async_runtime::WatchReceiver;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info, trace};

/// Shared state for the gRPC transport service.
#[derive(Clone)]
pub struct TransportService {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: crate::transport::TransportClient,
    /// Optional Raft consensus instance. When present, Raft RPCs are forwarded here.
    pub raft: Option<Arc<RaftInstance>>,
    /// This node's identifier — used to filter locally-assigned shards.
    pub local_node_id: crate::cluster::state::NodeId,
    /// Dedicated thread pools for search and write workloads.
    pub worker_pools: crate::worker::WorkerPools,
    /// Tracks asynchronous background tasks running on this node.
    pub task_manager: Arc<crate::tasks::TaskManager>,
    /// Serializes leader-side JoinCluster handling so concurrent joins cannot
    /// race identity validation or submit stale full voter sets.
    join_lock: Arc<Mutex<()>>,
}

fn new_join_lock() -> Arc<Mutex<()>> {
    Arc::new(Mutex::new(()))
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

pub(crate) fn enqueue_force_merge_task_on_assigned_shards(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    task_manager: Arc<crate::tasks::TaskManager>,
    worker_pools: crate::worker::WorkerPools,
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
            worker_pools,
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
    index_name: &str,
    shard_id: u32,
) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
    if let Some(engine) = shard_manager.get_shard(index_name, shard_id) {
        return Ok(engine);
    }

    let cs = cluster_manager.get_state();
    let Some(metadata) = cs.indices.get(index_name) else {
        return Err(Status::not_found(format!(
            "Shard [{index_name}][{shard_id}] not found on this node"
        )));
    };
    if !metadata.shard_routing.contains_key(&shard_id) {
        return Err(Status::not_found(format!(
            "Shard [{index_name}][{shard_id}] not found on this node"
        )));
    }

    let shard_dir = shard_manager
        .data_dir()
        .join(&metadata.uuid)
        .join(format!("shard_{}", shard_id));
    if !shard_dir.exists() {
        return Err(Status::failed_precondition(format!(
            "Shard [{index_name}][{shard_id}] is assigned here but {:?} is missing; refusing to create a fresh shard on a read path",
            shard_dir
        )));
    }

    Arc::clone(shard_manager)
        .open_shard_with_settings_blocking(
            index_name.to_string(),
            shard_id,
            metadata.mappings.clone(),
            metadata.settings.clone(),
            metadata.uuid.clone(),
        )
        .await
        .map_err(|e| Status::internal(format!("Failed to open shard: {}", e)))
}

pub(crate) async fn run_maintenance_on_assigned_shards_async(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    worker_pools: crate::worker::WorkerPools,
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

        let result = worker_pools
            .spawn_write(move || match op {
                MaintenanceDispatchOp::Refresh => engine.refresh(),
                MaintenanceDispatchOp::Flush => engine.flush_with_global_checkpoint(),
                MaintenanceDispatchOp::ForceMerge(max_segments) => engine.force_merge(max_segments),
            })
            .await;

        match result {
            Ok(Ok(_)) => successful += 1,
            Ok(Err(e)) | Err(e) => {
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

mod conversions;

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
        .map_err(|e| anyhow::anyhow!("Arrow IPC encode error: {}", e))?;
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
                        Status::internal(format!("Failed to connect to master for join: {}", e))
                    })?;
                let fwd_request = tonic::Request::new(JoinRequest {
                    node_info: Some(proto_node_fwd),
                    raft_node_id: joining_raft_id,
                });
                let fwd_response = client.join_cluster(fwd_request).await.map_err(|e| {
                    Status::internal(format!("Failed to forward join to master: {}", e))
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
                        .map_err(|e| Status::internal(format!("Raft add_learner failed: {}", e)))?;
                }

                let cmd = crate::consensus::types::ClusterCommand::AddNode { node: ni.clone() };
                if let Err(e) = raft.client_write(cmd).await {
                    return Err(Status::internal(format!("Raft AddNode failed: {}", e)));
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
                                "Raft change_membership failed after AddNode: {}; rollback failed: {}",
                                e, rollback_error
                            )));
                        }
                        return Err(Status::internal(format!(
                            "Raft change_membership failed: {}",
                            e
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

        // Fallback: legacy join (no Raft configured at all)
        self.cluster_manager.add_node(ni);
        let state = self.cluster_manager.get_state();
        Ok(Response::new(JoinResponse {
            state: Some(cluster_state_to_proto(&state)),
        }))
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
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;

        let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {}", e)))?;

        let doc_id = if req.doc_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.doc_id
        };

        info!(
            "gRPC: index doc '{}' into {}/shard_{}",
            doc_id, req.index_name, req.shard_id
        );

        let write_result = {
            let engine = engine.clone();
            let doc_id = doc_id.clone();
            let payload = payload.clone();
            self.worker_pools
                .spawn_write(move || engine.add_document(&doc_id, payload))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match write_result {
            Ok(id) => {
                // Get the seq_no assigned by the WAL during add_document
                let seq_no = engine.local_checkpoint();

                // Replicate to replica shards with seq_no
                let cs = self.cluster_manager.get_state();
                match crate::replication::replicate_write(
                    &self.transport_client,
                    &cs,
                    &req.index_name,
                    req.shard_id,
                    &id,
                    &payload,
                    "index",
                    seq_no,
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        Self::advance_global_checkpoint(&engine, seq_no, &replica_checkpoints);
                        self.shard_manager.isr_tracker.update_replica_checkpoints(
                            &req.index_name,
                            req.shard_id,
                            &replica_checkpoints,
                        );
                    }
                    Err(errors) => {
                        tracing::warn!(
                            "Replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        return Ok(Response::new(ShardDocResponse {
                            success: false,
                            doc_id: id,
                            error: format!("Replication failed: {}", errors.join("; ")),
                        }));
                    }
                }
                crate::metrics::DOCS_INDEXED_TOTAL.inc();
                Ok(Response::new(ShardDocResponse {
                    success: true,
                    doc_id: id,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(ShardDocResponse {
                success: false,
                doc_id: String::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn bulk_index(
        &self,
        request: Request<ShardBulkRequest>,
    ) -> Result<Response<ShardBulkResponse>, Status> {
        let req = request.into_inner();
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;

        let mut docs: Vec<(String, serde_json::Value)> =
            Vec::with_capacity(req.documents_json.len());
        for b in &req.documents_json {
            let val: serde_json::Value = serde_json::from_slice(b)
                .map_err(|e| Status::invalid_argument(format!("invalid JSON in bulk: {}", e)))?;
            let doc_id = val
                .get("_doc_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let payload = val.get("_source").cloned().unwrap_or(val.clone());
            docs.push((doc_id, payload));
        }

        trace!(
            "gRPC: bulk {} docs into {}/shard_{}",
            docs.len(),
            req.index_name,
            req.shard_id
        );

        let write_result = {
            let engine = engine.clone();
            let docs_for_write = docs.clone();
            self.worker_pools
                .spawn_write(move || engine.bulk_add_documents(docs_for_write))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match write_result {
            Ok(ids) => {
                let seq_no = engine.local_checkpoint();
                // Replicate to replica shards
                let cs = self.cluster_manager.get_state();
                let start_seq_no = seq_no.saturating_sub(ids.len().saturating_sub(1) as u64);
                match crate::replication::replicate_bulk(
                    &self.transport_client,
                    &cs,
                    &req.index_name,
                    req.shard_id,
                    &docs,
                    start_seq_no,
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        Self::advance_global_checkpoint(&engine, seq_no, &replica_checkpoints);
                        self.shard_manager.isr_tracker.update_replica_checkpoints(
                            &req.index_name,
                            req.shard_id,
                            &replica_checkpoints,
                        );
                    }
                    Err(errors) => {
                        tracing::warn!(
                            "Bulk replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        return Ok(Response::new(ShardBulkResponse {
                            success: false,
                            doc_ids: ids,
                            error: format!("Replication failed: {}", errors.join("; ")),
                        }));
                    }
                }
                let doc_count = ids.len() as u64;
                crate::metrics::BULK_DOCS_TOTAL.inc_by(doc_count);
                Ok(Response::new(ShardBulkResponse {
                    success: true,
                    doc_ids: ids,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(ShardBulkResponse {
                success: false,
                doc_ids: vec![],
                error: e.to_string(),
            })),
        }
    }

    async fn delete_doc(
        &self,
        request: Request<ShardDeleteRequest>,
    ) -> Result<Response<ShardDeleteResponse>, Status> {
        let req = request.into_inner();
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;
        info!(
            "gRPC: delete doc '{}' from {}/shard_{}",
            req.doc_id, req.index_name, req.shard_id
        );

        let delete_result = {
            let engine = engine.clone();
            let doc_id = req.doc_id.clone();
            self.worker_pools
                .spawn_write(move || engine.delete_document(&doc_id))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match delete_result {
            Ok(deleted) => {
                let seq_no = engine.local_checkpoint();
                // Replicate delete to replica shards
                let cs = self.cluster_manager.get_state();
                match crate::replication::replicate_write(
                    &self.transport_client,
                    &cs,
                    &req.index_name,
                    req.shard_id,
                    &req.doc_id,
                    &serde_json::json!({}),
                    "delete",
                    seq_no,
                )
                .await
                {
                    Ok(replica_checkpoints) => {
                        Self::advance_global_checkpoint(&engine, seq_no, &replica_checkpoints);
                        self.shard_manager.isr_tracker.update_replica_checkpoints(
                            &req.index_name,
                            req.shard_id,
                            &replica_checkpoints,
                        );
                    }
                    Err(errors) => {
                        tracing::warn!(
                            "Delete replication errors for {}/shard_{}: {:?}",
                            req.index_name,
                            req.shard_id,
                            errors
                        );
                        return Ok(Response::new(ShardDeleteResponse {
                            success: false,
                            deleted,
                            error: format!("Replication failed: {}", errors.join("; ")),
                        }));
                    }
                }
                Ok(Response::new(ShardDeleteResponse {
                    success: true,
                    deleted,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(ShardDeleteResponse {
                success: false,
                deleted: 0,
                error: e.to_string(),
            })),
        }
    }

    async fn get_doc(
        &self,
        request: Request<ShardGetRequest>,
    ) -> Result<Response<ShardGetResponse>, Status> {
        let req = request.into_inner();
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
                }));
            }
        };
        let doc_result = {
            let engine = engine.clone();
            let doc_id = req.doc_id.clone();
            self.worker_pools
                .spawn_search(move || engine.get_document(&doc_id))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match doc_result {
            Ok(Some(source)) => {
                let source_json = serde_json::to_vec(&source)
                    .map_err(|e| Status::internal(format!("serialize get_doc response: {}", e)))?;
                Ok(Response::new(ShardGetResponse {
                    found: true,
                    source_json,
                    error: String::new(),
                }))
            }
            Ok(None) => Ok(Response::new(ShardGetResponse {
                found: false,
                source_json: vec![],
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ShardGetResponse {
                found: false,
                source_json: vec![],
                error: e.to_string(),
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
                    .map_err(|e| Status::internal(format!("serialize search hit: {}", e)))?;
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
                Status::invalid_argument(format!("invalid SearchRequest JSON: {}", e))
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
                        .map_err(|e| Status::internal(format!("encode partial aggs: {}", e)))?
                };

                let hits = all_hits
                    .into_iter()
                    .map(|v| {
                        Ok::<SearchHit, serde_json::Error>(SearchHit {
                            source_json: serde_json::to_vec(&v)?,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Status::internal(format!("serialize search hit: {}", e)))?;

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
                Status::invalid_argument(format!("invalid SearchRequest JSON: {}", e))
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
                Status::invalid_argument(format!("invalid SearchRequest JSON: {}", e))
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
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;

        info!(
            "gRPC: replicate {} doc '{}' (seq_no={}) to {}/shard_{}",
            req.op, req.doc_id, req.seq_no, req.index_name, req.shard_id
        );

        let result = match req.op.as_str() {
            "index" => {
                let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
                    .map_err(|e| Status::invalid_argument(format!("invalid JSON: {}", e)))?;
                let engine = engine.clone();
                let doc_id = req.doc_id.clone();
                let seq_no = req.seq_no;
                self.worker_pools
                    .spawn_write(move || {
                        engine
                            .add_document_with_seq(&doc_id, payload, seq_no)
                            .map(|_| ())
                    })
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            "delete" => {
                let engine = engine.clone();
                let doc_id = req.doc_id.clone();
                let seq_no = req.seq_no;
                self.worker_pools
                    .spawn_write(move || {
                        engine.delete_document_with_seq(&doc_id, seq_no).map(|_| ())
                    })
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            }
            other => Err(anyhow::anyhow!("Unknown replication op: {}", other)),
        };

        match result {
            Ok(()) => Ok(Response::new(ReplicateDocResponse {
                success: true,
                error: String::new(),
                local_checkpoint: engine.local_checkpoint(),
            })),
            Err(e) => Ok(Response::new(ReplicateDocResponse {
                success: false,
                error: e.to_string(),
                local_checkpoint: engine.local_checkpoint(),
            })),
        }
    }

    async fn replicate_bulk(
        &self,
        request: Request<ReplicateBulkRequest>,
    ) -> Result<Response<ReplicateBulkResponse>, Status> {
        let req = request.into_inner();
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;

        info!(
            "gRPC: replicate bulk {} ops to {}/shard_{}",
            req.ops.len(),
            req.index_name,
            req.shard_id
        );

        let mut docs = Vec::with_capacity(req.ops.len());
        for op in &req.ops {
            let payload: serde_json::Value =
                serde_json::from_slice(&op.payload_json).map_err(|e| {
                    Status::invalid_argument(format!("invalid JSON in bulk replicate: {}", e))
                })?;
            docs.push((op.doc_id.clone(), payload));
        }

        let start_seq_no = req.ops.first().map(|op| op.seq_no).unwrap_or(0);

        let write_result = {
            let engine = engine.clone();
            self.worker_pools
                .spawn_write(move || engine.bulk_add_documents_with_start_seq(docs, start_seq_no))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        match write_result {
            Ok(_) => Ok(Response::new(ReplicateBulkResponse {
                success: true,
                error: String::new(),
                local_checkpoint: engine.local_checkpoint(),
            })),
            Err(e) => Ok(Response::new(ReplicateBulkResponse {
                success: false,
                error: e.to_string(),
                local_checkpoint: engine.local_checkpoint(),
            })),
        }
    }

    async fn recover_replica(
        &self,
        request: Request<RecoverReplicaRequest>,
    ) -> Result<Response<RecoverReplicaResponse>, Status> {
        let req = request.into_inner();
        let engine = self
            .get_or_open_shard(&req.index_name, req.shard_id)
            .await?;

        info!(
            "gRPC: recover_replica for {}/shard_{} from checkpoint {}",
            req.index_name, req.shard_id, req.local_checkpoint
        );

        // Read translog entries above the replica's checkpoint
        // The engine's underlying HotEngine has the translog — we need to read ops from it.
        // Since we can't access the translog directly through the SearchEngine trait,
        // we replay by reading all entries and filtering.
        // For now, use the WAL's read_from capability through the shard directory.
        let shard_dir = match self
            .shard_manager
            .shard_data_dir(&req.index_name, req.shard_id)
        {
            Some(dir) => dir,
            None => {
                return Ok(Response::new(RecoverReplicaResponse {
                    success: false,
                    error: format!(
                        "No UUID mapping for index '{}' — cannot locate shard directory",
                        req.index_name
                    ),
                    ops_replayed: 0,
                    primary_checkpoint: engine.local_checkpoint(),
                    operations: vec![],
                }));
            }
        };

        let entries: Vec<crate::wal::TranslogEntry> = {
            let shard_dir = shard_dir.clone();
            let checkpoint = req.local_checkpoint;
            match self
                .worker_pools
                .spawn_search(
                    move || -> crate::common::Result<Vec<crate::wal::TranslogEntry>> {
                        let tl = crate::wal::HotTranslog::open(&shard_dir)?;
                        tl.read_from(checkpoint)
                    },
                )
                .await
            {
                Ok(Ok(entries)) => entries,
                Ok(Err(e)) => {
                    return Ok(Response::new(RecoverReplicaResponse {
                        success: false,
                        error: format!("Failed to read translog: {}", e),
                        ops_replayed: 0,
                        primary_checkpoint: engine.local_checkpoint(),
                        operations: vec![],
                    }));
                }
                Err(e) => {
                    return Ok(Response::new(RecoverReplicaResponse {
                        success: false,
                        error: format!("Translog read task failed: {}", e),
                        ops_replayed: 0,
                        primary_checkpoint: engine.local_checkpoint(),
                        operations: vec![],
                    }));
                }
            }
        };

        let ops_count = entries.len() as u64;

        // Convert translog entries to proto operations for the replica to replay
        let operations: Vec<RecoverReplicaOp> = entries
            .iter()
            .map(|e| {
                // Extract doc_id from payload (stored in _doc_id or _id field)
                let doc_id = e
                    .payload
                    .get("_doc_id")
                    .or_else(|| e.payload.get("_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                Ok::<RecoverReplicaOp, serde_json::Error>(RecoverReplicaOp {
                    seq_no: e.seq_no,
                    op: e.op.as_str().to_string(),
                    doc_id,
                    payload_json: serde_json::to_vec(&e.payload)?,
                })
            })
            .collect::<Result<_, _>>()
            .map_err(|e| Status::internal(format!("serialize recovery op: {}", e)))?;

        Ok(Response::new(RecoverReplicaResponse {
            success: true,
            error: String::new(),
            ops_replayed: ops_count,
            primary_checkpoint: engine.local_checkpoint(),
            operations,
        }))
    }

    // ─── Dynamic Settings ─────────────────────────────────────────────────────

    async fn update_settings(
        &self,
        request: Request<UpdateSettingsRequest>,
    ) -> Result<Response<UpdateSettingsResponse>, Status> {
        let req = request.into_inner();
        let index_name = &req.index_name;

        let body: serde_json::Value = serde_json::from_slice(&req.settings_json)
            .map_err(|e| Status::invalid_argument(format!("bad settings JSON: {}", e)))?;

        // Look up current metadata
        let cluster_state = self.cluster_manager.get_state();
        let mut metadata = cluster_state
            .indices
            .get(index_name)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("no such index [{}]", index_name)))?;

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
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {}", e)))?;

        // Apply settings to local engines
        self.shard_manager
            .apply_settings(index_name, &metadata.settings);

        tracing::info!("gRPC: updated settings for index '{}'", index_name);
        Ok(Response::new(UpdateSettingsResponse {
            acknowledged: true,
            error: String::new(),
        }))
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

        let num_shards = body
            .pointer("/settings/number_of_shards")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;
        let num_replicas = body
            .pointer("/settings/number_of_replicas")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32;
        let refresh_interval_ms = body
            .pointer("/settings/refresh_interval_ms")
            .and_then(|v| v.as_u64());
        let flush_threshold_bytes = body
            .pointer("/settings/flush_threshold_bytes")
            .and_then(|v| v.as_u64());

        let cluster_state = self.cluster_manager.get_state();

        if cluster_state.indices.contains_key(index_name) {
            return Ok(Response::new(CreateIndexResponse {
                acknowledged: false,
                error: format!("index [{}] already exists", index_name),
                response_json: Vec::new(),
            }));
        }

        let data_nodes: Vec<String> = cluster_state
            .nodes
            .values()
            .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
            .map(|n| n.id.clone())
            .collect();

        if data_nodes.is_empty() {
            return Err(Status::internal("No data nodes available to assign shards"));
        }

        let mut metadata = crate::cluster::state::IndexMetadata::build_shard_routing(
            index_name,
            num_shards,
            num_replicas,
            &data_nodes,
        );

        if let Some(ms) = refresh_interval_ms {
            metadata.settings.refresh_interval_ms = Some(ms);
        }
        if let Some(bytes) = flush_threshold_bytes {
            metadata.settings.flush_threshold_bytes = Some(bytes);
        }

        // Parse field mappings
        if let Some(properties) = body
            .pointer("/mappings/properties")
            .and_then(|v| v.as_object())
        {
            for (field_name, field_def) in properties {
                if let Some(type_str) = field_def.get("type").and_then(|v| v.as_str()) {
                    let field_mapping = match type_str {
                        "text" => Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Text,
                            dimension: None,
                        }),
                        "keyword" => Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Keyword,
                            dimension: None,
                        }),
                        "integer" | "long" => Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Integer,
                            dimension: None,
                        }),
                        "float" | "double" => Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Float,
                            dimension: None,
                        }),
                        "boolean" => Some(crate::cluster::state::FieldMapping {
                            field_type: crate::cluster::state::FieldType::Boolean,
                            dimension: None,
                        }),
                        "knn_vector" => {
                            let dim = field_def
                                .get("dimension")
                                .and_then(|v| v.as_u64())
                                .map(|d| d as usize);
                            Some(crate::cluster::state::FieldMapping {
                                field_type: crate::cluster::state::FieldType::KnnVector,
                                dimension: dim,
                            })
                        }
                        _ => None,
                    };
                    if let Some(fm) = field_mapping {
                        metadata.mappings.insert(field_name.clone(), fm);
                    }
                }
            }
        }

        let cmd = crate::consensus::types::ClusterCommand::CreateIndex { metadata };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {}", e)))?;

        let resp_json = serde_json::to_vec(&serde_json::json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": index_name
        }))
        .map_err(|e| Status::internal(format!("serialize create index response: {}", e)))?;

        tracing::info!(
            "gRPC: created index '{}' with {} shards, {} replicas",
            index_name,
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
        if !cluster_state.indices.contains_key(index_name) {
            return Err(Status::not_found(format!("no such index [{}]", index_name)));
        }

        let cmd = crate::consensus::types::ClusterCommand::DeleteIndex {
            index_name: index_name.clone(),
        };
        raft.client_write(cmd)
            .await
            .map_err(|e| Status::internal(format!("Raft write failed: {}", e)))?;

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
            .ok_or_else(|| Status::not_found(format!("Node '{}' not found", target_node_id)))?;

        if target_info.raft_node_id == 0 {
            return Err(Status::invalid_argument(format!(
                "Node '{}' has no Raft ID assigned",
                target_node_id
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
            .map_err(|e| Status::internal(format!("Transfer leader failed: {}", e)))?;

        tracing::info!(
            "gRPC: leadership transfer initiated to node '{}'",
            target_node_id
        );
        Ok(Response::new(TransferMasterResponse {
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
            self.worker_pools.clone(),
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
            self.worker_pools.clone(),
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
        let max_segments = (inner.max_num_segments as usize).max(1);
        let task_id = enqueue_force_merge_task_on_assigned_shards(
            self.cluster_manager.clone(),
            self.shard_manager.clone(),
            self.task_manager.clone(),
            self.worker_pools.clone(),
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
                .map_err(|e| Status::invalid_argument(format!("bad vote request: {}", e)))?;
        let resp = raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(format!("raft vote error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise vote response: {}", e)))?;
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
                Status::invalid_argument(format!("bad append_entries request: {}", e))
            })?;
        let resp = raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(format!("raft append_entries error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise append_entries response: {}", e)))?;
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
            .map_err(|e| Status::invalid_argument(format!("bad snapshot request: {}", e)))?;
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
            .map_err(|e| Status::invalid_argument(format!("bad vote in snapshot: {}", e)))?;
        let meta: crate::consensus::types::SnapshotMeta = serde_json::from_value(meta_value)
            .map_err(|e| Status::invalid_argument(format!("bad meta in snapshot: {}", e)))?;
        let data: Vec<u8> = serde_json::from_value(data_value)
            .map_err(|e| Status::invalid_argument(format!("bad data in snapshot: {}", e)))?;
        let snapshot = crate::consensus::types::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        };
        let resp = raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| Status::internal(format!("raft snapshot error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise snapshot response: {}", e)))?;
        Ok(Response::new(RaftReply {
            data,
            error: String::new(),
        }))
    }
}

impl TransportService {
    #[allow(clippy::result_large_err)]
    fn ensure_authoritative_shard_uuid(
        &self,
        _index_name: &str,
        shard_id: u32,
        metadata: &crate::cluster::state::IndexMetadata,
    ) -> Result<std::path::PathBuf, Status> {
        Ok(self
            .shard_manager
            .data_dir()
            .join(&metadata.uuid)
            .join(format!("shard_{}", shard_id)))
    }

    #[allow(clippy::result_large_err)]
    async fn get_or_open_shard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        if let Some(e) = self.shard_manager.get_shard(index_name, shard_id) {
            return Ok(e);
        }
        let cs = self.cluster_manager.get_state();
        let Some(metadata) = cs.indices.get(index_name) else {
            return Err(Status::not_found(format!(
                "Shard [{index_name}][{shard_id}] not found on this node"
            )));
        };
        if !metadata.shard_routing.contains_key(&shard_id) {
            return Err(Status::not_found(format!(
                "Shard [{index_name}][{shard_id}] not found on this node"
            )));
        }
        let _ = self.ensure_authoritative_shard_uuid(index_name, shard_id, metadata)?;
        self.shard_manager
            .open_shard_with_settings_blocking(
                index_name.to_string(),
                shard_id,
                metadata.mappings.clone(),
                metadata.settings.clone(),
                metadata.uuid.clone(),
            )
            .await
            .map_err(|e| Status::internal(format!("Failed to open shard: {}", e)))
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
            index_name,
            shard_id,
        )
        .await
    }

    /// Compute and advance the global checkpoint for a shard.
    /// The global checkpoint is min(primary_checkpoint, all_replica_checkpoints).
    /// Only advances (never goes backward).
    fn advance_global_checkpoint(
        engine: &Arc<dyn crate::engine::SearchEngine>,
        primary_checkpoint: u64,
        replica_checkpoints: &[(String, u64)],
    ) {
        if replica_checkpoints.is_empty() {
            // No replicas → global checkpoint = primary checkpoint
            engine.update_global_checkpoint(primary_checkpoint);
            return;
        }
        let min_replica = replica_checkpoints
            .iter()
            .map(|(_, cp)| *cp)
            .min()
            .unwrap_or(0);
        let global = std::cmp::min(primary_checkpoint, min_replica);
        let current = engine.global_checkpoint();
        if global > current {
            engine.update_global_checkpoint(global);
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
}

/// Create a gRPC transport server **without Raft** for shard-level integration tests.
/// Production code must use [`create_transport_service_with_raft`].
pub fn create_transport_service_for_test(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    task_manager: Arc<crate::tasks::TaskManager>,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: None,
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        task_manager,
        join_lock: new_join_lock(),
    };
    InternalTransportServer::new(service)
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
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
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: Some(raft),
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        task_manager,
        join_lock: new_join_lock(),
    };
    InternalTransportServer::new(service)
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
}

#[cfg(test)]
mod tests;
