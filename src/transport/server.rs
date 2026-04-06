//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::proto::internal_transport_server::{
    InternalTransport, InternalTransportServer,
};
use crate::transport::proto::*;
use crate::wal::WriteAheadLog;
use futures::{Stream, stream};
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
    pub local_node_id: String,
    /// Dedicated thread pools for search and write workloads.
    pub worker_pools: crate::worker::WorkerPools,
    /// Serializes leader-side JoinCluster handling so concurrent joins cannot
    /// race identity validation or submit stale full voter sets.
    join_lock: Arc<Mutex<()>>,
}

fn new_join_lock() -> Arc<Mutex<()>> {
    Arc::new(Mutex::new(()))
}

// ─── Conversion helpers: domain types ↔ proto types ─────────────────────────

fn node_info_to_proto(n: &crate::cluster::state::NodeInfo) -> NodeInfo {
    NodeInfo {
        id: n.id.clone(),
        name: n.name.clone(),
        host: n.host.clone(),
        transport_port: n.transport_port as u32,
        http_port: n.http_port as u32,
        roles: n
            .roles
            .iter()
            .map(|r| match r {
                crate::cluster::state::NodeRole::Master => "master".into(),
                crate::cluster::state::NodeRole::Data => "data".into(),
                crate::cluster::state::NodeRole::Client => "client".into(),
            })
            .collect(),
        raft_node_id: n.raft_node_id,
    }
}

fn proto_to_node_info(p: &NodeInfo) -> crate::cluster::state::NodeInfo {
    crate::cluster::state::NodeInfo {
        id: p.id.clone(),
        name: p.name.clone(),
        host: p.host.clone(),
        transport_port: p.transport_port as u16,
        http_port: p.http_port as u16,
        roles: p
            .roles
            .iter()
            .map(|r| match r.as_str() {
                "master" => crate::cluster::state::NodeRole::Master,
                "data" => crate::cluster::state::NodeRole::Data,
                "client" => crate::cluster::state::NodeRole::Client,
                _ => crate::cluster::state::NodeRole::Data,
            })
            .collect(),
        raft_node_id: p.raft_node_id,
    }
}

fn field_type_to_proto(field_type: &crate::cluster::state::FieldType) -> String {
    match field_type {
        crate::cluster::state::FieldType::Text => "text",
        crate::cluster::state::FieldType::Keyword => "keyword",
        crate::cluster::state::FieldType::Integer => "integer",
        crate::cluster::state::FieldType::Float => "float",
        crate::cluster::state::FieldType::Boolean => "boolean",
        crate::cluster::state::FieldType::KnnVector => "knn_vector",
    }
    .to_string()
}

#[allow(clippy::result_large_err)]
fn proto_to_field_type(field_type: &str) -> Result<crate::cluster::state::FieldType, Status> {
    match field_type {
        "text" => Ok(crate::cluster::state::FieldType::Text),
        "keyword" => Ok(crate::cluster::state::FieldType::Keyword),
        "integer" | "long" => Ok(crate::cluster::state::FieldType::Integer),
        "float" | "double" => Ok(crate::cluster::state::FieldType::Float),
        "boolean" => Ok(crate::cluster::state::FieldType::Boolean),
        "knn_vector" => Ok(crate::cluster::state::FieldType::KnnVector),
        other => Err(Status::invalid_argument(format!(
            "unknown field type in cluster state snapshot: {}",
            other
        ))),
    }
}

fn index_settings_to_proto(settings: &crate::cluster::state::IndexSettings) -> IndexSettings {
    IndexSettings {
        refresh_interval_ms: settings.refresh_interval_ms,
        flush_threshold_bytes: settings.flush_threshold_bytes,
    }
}

fn proto_to_index_settings(
    settings: Option<&IndexSettings>,
) -> crate::cluster::state::IndexSettings {
    let Some(settings) = settings else {
        return crate::cluster::state::IndexSettings::default();
    };

    crate::cluster::state::IndexSettings {
        refresh_interval_ms: settings.refresh_interval_ms,
        flush_threshold_bytes: settings.flush_threshold_bytes,
    }
}

#[allow(clippy::result_large_err)]
fn validate_join_identity(
    state: &crate::cluster::state::ClusterState,
    node_id: &str,
    raft_node_id: u64,
) -> Result<(), Status> {
    if let Some(existing) = state.nodes.get(node_id)
        && existing.raft_node_id > 0
        && existing.raft_node_id != raft_node_id
    {
        return Err(Status::already_exists(format!(
            "node [{}] is already registered with raft_node_id {}",
            node_id, existing.raft_node_id
        )));
    }

    if raft_node_id > 0
        && let Some(existing) = state
            .nodes
            .values()
            .find(|existing| existing.raft_node_id == raft_node_id && existing.id != node_id)
    {
        return Err(Status::already_exists(format!(
            "raft_node_id {} is already registered to node [{}]",
            raft_node_id, existing.id
        )));
    }

    Ok(())
}

pub fn cluster_state_to_proto(s: &crate::cluster::state::ClusterState) -> ClusterState {
    ClusterState {
        cluster_name: s.cluster_name.clone(),
        version: s.version,
        master_node: s.master_node.clone(),
        nodes: s.nodes.values().map(node_info_to_proto).collect(),
        indices: s
            .indices
            .values()
            .map(|idx| IndexMetadata {
                name: idx.name.clone(),
                uuid: idx.uuid.clone(),
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shards: idx
                    .shard_routing
                    .iter()
                    .map(|(sid, routing)| ShardAssignment {
                        shard_id: *sid,
                        node_id: routing.primary.clone(),
                        replica_node_ids: routing.replicas.clone(),
                        unassigned_replicas: routing.unassigned_replicas,
                    })
                    .collect(),
                mappings: idx
                    .mappings
                    .iter()
                    .map(|(name, mapping)| FieldMappingEntry {
                        name: name.clone(),
                        field_type: field_type_to_proto(&mapping.field_type),
                        dimension: mapping.dimension.map(|dimension| dimension as u32),
                    })
                    .collect(),
                settings: Some(index_settings_to_proto(&idx.settings)),
            })
            .collect(),
    }
}

#[allow(clippy::result_large_err)]
pub fn proto_to_cluster_state(
    p: &ClusterState,
) -> Result<crate::cluster::state::ClusterState, Status> {
    let mut state = crate::cluster::state::ClusterState::new(p.cluster_name.clone());
    state.version = p.version;
    state.master_node = p.master_node.clone();
    for node in &p.nodes {
        let ni = proto_to_node_info(node);
        state.nodes.insert(ni.id.clone(), ni);
    }
    for idx in &p.indices {
        let mut shard_routing = std::collections::HashMap::new();
        for sa in &idx.shards {
            shard_routing.insert(
                sa.shard_id,
                crate::cluster::state::ShardRoutingEntry {
                    primary: sa.node_id.clone(),
                    replicas: sa.replica_node_ids.clone(),
                    unassigned_replicas: sa.unassigned_replicas,
                },
            );
        }
        let mut mappings = std::collections::HashMap::new();
        for mapping in &idx.mappings {
            let field_type = proto_to_field_type(&mapping.field_type).map_err(|_| {
                Status::invalid_argument(format!(
                    "unknown field type '{}' for field '{}' in index '{}'",
                    mapping.field_type, mapping.name, idx.name
                ))
            })?;
            mappings.insert(
                mapping.name.clone(),
                crate::cluster::state::FieldMapping {
                    field_type,
                    dimension: mapping.dimension.map(|dimension| dimension as usize),
                },
            );
        }
        state.indices.insert(
            idx.name.clone(),
            crate::cluster::state::IndexMetadata {
                name: idx.name.clone(),
                uuid: idx.uuid.clone(),
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shard_routing,
                mappings,
                settings: proto_to_index_settings(idx.settings.as_ref()),
            },
        );
    }
    Ok(state)
}

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
        let mut ni = proto_to_node_info(&node_info);
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
        request: Request<PublishStateRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let proto_state = req
            .state
            .ok_or_else(|| Status::invalid_argument("missing state"))?;
        let new_state = proto_to_cluster_state(&proto_state)?;
        info!("gRPC: cluster state update, version {}", new_state.version);

        // Detect indices that were removed and close their local shards + delete data
        let old_state = self.cluster_manager.get_state();
        for old_index in old_state.indices.keys() {
            if !new_state.indices.contains_key(old_index) {
                info!(
                    "Index '{}' removed from cluster state — closing local shards",
                    old_index
                );
                if let Err(e) = self
                    .shard_manager
                    .close_index_shards_blocking(old_index.clone())
                    .await
                {
                    tracing::error!(
                        "Failed to close shards for deleted index '{}': {}",
                        old_index,
                        e
                    );
                }
            }
        }

        self.cluster_manager.update_state(new_state);
        Ok(Response::new(Empty {}))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
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
                    op: e.op.clone(),
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
            .close_index_shards_blocking(index_name.clone())
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

    // ─── Index Maintenance RPCs ───────────────────────────────────────────────

    async fn refresh_index(
        &self,
        request: Request<IndexMaintenanceRequest>,
    ) -> Result<Response<IndexMaintenanceResponse>, Status> {
        let index_name = request.into_inner().index_name;
        let shard_manager = self.shard_manager.clone();
        let cluster_manager = self.cluster_manager.clone();
        let local_node_id = self.local_node_id.clone();
        let (successful, failed) = self
            .worker_pools
            .spawn_write(move || {
                run_maintenance_sync(
                    &cluster_manager,
                    &shard_manager,
                    &local_node_id,
                    &index_name,
                    |engine| engine.refresh(),
                )
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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
        let shard_manager = self.shard_manager.clone();
        let cluster_manager = self.cluster_manager.clone();
        let local_node_id = self.local_node_id.clone();
        let (successful, failed) = self
            .worker_pools
            .spawn_write(move || {
                run_maintenance_sync(
                    &cluster_manager,
                    &shard_manager,
                    &local_node_id,
                    &index_name,
                    |engine| engine.flush_with_global_checkpoint(),
                )
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(IndexMaintenanceResponse {
            successful_shards: successful,
            failed_shards: failed,
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

/// Run a maintenance operation on all locally-assigned shards (sync, for pool dispatch).
fn run_maintenance_sync<F>(
    cluster_manager: &ClusterManager,
    shard_manager: &crate::shard::ShardManager,
    local_node_id: &str,
    index_name: &str,
    op: F,
) -> (u32, u32)
where
    F: Fn(&Arc<dyn crate::engine::SearchEngine>) -> crate::common::Result<()>,
{
    let mut successful = 0u32;
    let mut failed = 0u32;
    let cs = cluster_manager.get_state();
    let Some(metadata) = cs.indices.get(index_name) else {
        return (successful, failed);
    };
    for (shard_id, routing) in &metadata.shard_routing {
        let assigned_here =
            routing.primary == local_node_id || routing.replicas.iter().any(|n| n == local_node_id);
        if !assigned_here {
            continue;
        }
        if let Some(engine) = shard_manager.get_shard(index_name, *shard_id) {
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

impl TransportService {
    #[allow(clippy::result_large_err)]
    fn ensure_authoritative_shard_uuid(
        &self,
        index_name: &str,
        shard_id: u32,
        metadata: &crate::cluster::state::IndexMetadata,
    ) -> Result<std::path::PathBuf, Status> {
        if !metadata.has_uuid() {
            return Err(Status::failed_precondition(format!(
                "Shard [{index_name}][{shard_id}] has no authoritative UUID in cluster state"
            )));
        }

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
        if let Some(engine) = self.shard_manager.get_shard(index_name, shard_id) {
            return Ok(engine);
        }

        let cs = self.cluster_manager.get_state();
        if let Some(metadata) = cs.indices.get(index_name) {
            if !metadata.shard_routing.contains_key(&shard_id) {
                return Err(Status::not_found(format!(
                    "Shard [{index_name}][{shard_id}] not found on this node"
                )));
            }
            let shard_dir = self.ensure_authoritative_shard_uuid(index_name, shard_id, metadata)?;
            if !shard_dir.exists() {
                return Err(Status::failed_precondition(format!(
                    "Shard [{index_name}][{shard_id}] is assigned here but {:?} is missing; refusing to create a fresh shard on a read path",
                    shard_dir
                )));
            }
            return self
                .shard_manager
                .open_shard_with_settings_blocking(
                    index_name.to_string(),
                    shard_id,
                    metadata.mappings.clone(),
                    metadata.settings.clone(),
                    metadata.uuid.clone(),
                )
                .await
                .map_err(|e| Status::internal(format!("Failed to open shard: {}", e)));
        }

        Err(Status::not_found(format!(
            "Shard [{index_name}][{shard_id}] not found on this node"
        )))
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

/// Create the gRPC transport server (tonic)
pub fn create_transport_service(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: None,
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        join_lock: new_join_lock(),
    };
    InternalTransportServer::new(service)
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
}

/// Create the gRPC transport server with Raft consensus enabled.
pub fn create_transport_service_with_raft(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    raft: Arc<RaftInstance>,
    local_node_id: String,
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: Some(raft),
        local_node_id,
        worker_pools: crate::worker::WorkerPools::default_for_system(),
        join_lock: new_join_lock(),
    };
    InternalTransportServer::new(service)
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::manager::ClusterManager;
    use crate::cluster::state::{
        ClusterState as DomainClusterState, FieldMapping, FieldType,
        IndexMetadata as DomainIndexMetadata, NodeInfo as DomainNodeInfo, NodeRole,
        ShardRoutingEntry,
    };
    use crate::engine::{CompositeEngine, SearchEngine};
    use crate::shard::ShardManager;
    use serde_json::json;
    use std::collections::HashMap;
    use std::time::Duration;

    fn make_full_cluster_state() -> DomainClusterState {
        let mut cs = DomainClusterState::new("roundtrip-cluster".into());
        cs.version = 42;
        cs.master_node = Some("node-1".into());

        cs.add_node(DomainNodeInfo {
            id: "node-1".into(),
            name: "primary-node".into(),
            host: "10.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Master, NodeRole::Data],
            raft_node_id: 11,
        });
        cs.add_node(DomainNodeInfo {
            id: "node-2".into(),
            name: "replica-node".into(),
            host: "10.0.0.2".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 22,
        });

        // Reset version (add_node bumps it)
        cs.version = 42;

        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        );
        shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-2".into(),
                replicas: vec!["node-1".into()],
                unassigned_replicas: 1,
            },
        );

        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "embedding".into(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(384),
            },
        );

        cs.add_index(DomainIndexMetadata {
            name: "products".into(),
            uuid: "products-uuid".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing,
            mappings,
            settings: crate::cluster::state::IndexSettings {
                refresh_interval_ms: Some(1500),
                flush_threshold_bytes: Some(65_536),
            },
        });
        cs.version = 42; // reset again after add_index

        cs
    }

    #[test]
    fn cluster_state_roundtrip_preserves_metadata() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto).unwrap();

        assert_eq!(restored.cluster_name, "roundtrip-cluster");
        assert_eq!(restored.version, 42);
        assert_eq!(restored.master_node, Some("node-1".into()));
        assert_eq!(restored.nodes.len(), 2);
        assert_eq!(restored.indices.len(), 1);
    }

    #[test]
    fn roundtrip_preserves_node_info() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto).unwrap();

        let n1 = restored.nodes.get("node-1").unwrap();
        assert_eq!(n1.name, "primary-node");
        assert_eq!(n1.host, "10.0.0.1");
        assert_eq!(n1.transport_port, 9300);
        assert_eq!(n1.http_port, 9200);
        assert!(n1.roles.contains(&NodeRole::Master));
        assert!(n1.roles.contains(&NodeRole::Data));
        assert_eq!(n1.raft_node_id, 11);

        let n2 = restored.nodes.get("node-2").unwrap();
        assert_eq!(n2.name, "replica-node");
        assert_eq!(n2.roles, vec![NodeRole::Data]);
        assert_eq!(n2.raft_node_id, 22);
    }

    #[test]
    fn roundtrip_preserves_shard_routing() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto).unwrap();

        let idx = restored.indices.get("products").unwrap();
        assert_eq!(idx.number_of_shards, 2);
        assert_eq!(idx.number_of_replicas, 1);
        assert_eq!(idx.shard_routing.len(), 2);

        let shard0 = idx.shard_routing.get(&0).unwrap();
        assert_eq!(shard0.primary, "node-1");
        assert_eq!(shard0.replicas, vec!["node-2".to_string()]);

        let shard1 = idx.shard_routing.get(&1).unwrap();
        assert_eq!(shard1.primary, "node-2");
        assert_eq!(shard1.replicas, vec!["node-1".to_string()]);
        assert_eq!(shard1.unassigned_replicas, 1);
    }

    #[test]
    fn roundtrip_preserves_index_mappings_and_settings() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto).unwrap();

        let idx = restored.indices.get("products").unwrap();
        assert_eq!(idx.uuid, "products-uuid");
        assert_eq!(idx.settings.refresh_interval_ms, Some(1500));
        assert_eq!(idx.settings.flush_threshold_bytes, Some(65_536));
        assert_eq!(idx.mappings["title"].field_type, FieldType::Text);
        assert_eq!(idx.mappings["embedding"].field_type, FieldType::KnnVector);
        assert_eq!(idx.mappings["embedding"].dimension, Some(384));
    }

    #[test]
    fn roundtrip_empty_cluster_state() {
        let original = DomainClusterState::new("empty".into());
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto).unwrap();

        assert_eq!(restored.cluster_name, "empty");
        assert_eq!(restored.version, 0);
        assert!(restored.master_node.is_none());
        assert!(restored.nodes.is_empty());
        assert!(restored.indices.is_empty());
    }

    #[test]
    fn roundtrip_index_with_no_replicas() {
        let mut cs = DomainClusterState::new("test".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cs.add_index(DomainIndexMetadata {
            name: "logs".into(),
            uuid: String::new(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });

        let proto = cluster_state_to_proto(&cs);
        let restored = proto_to_cluster_state(&proto).unwrap();

        let idx = restored.indices.get("logs").unwrap();
        assert_eq!(idx.number_of_replicas, 0);
        assert!(idx.shard_routing[&0].replicas.is_empty());
    }

    #[test]
    fn roundtrip_client_role() {
        let mut cs = DomainClusterState::new("test".into());
        cs.add_node(DomainNodeInfo {
            id: "coord".into(),
            name: "coordinator".into(),
            host: "10.0.0.3".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Client],
            raft_node_id: 0,
        });

        let proto = cluster_state_to_proto(&cs);
        let restored = proto_to_cluster_state(&proto).unwrap();

        let node = restored.nodes.get("coord").unwrap();
        assert_eq!(node.roles, vec![NodeRole::Client]);
    }

    // ── advance_global_checkpoint tests ────────────────────────────────

    fn make_checkpoint_engine() -> (tempfile::TempDir, Arc<dyn crate::engine::SearchEngine>) {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            crate::engine::CompositeEngine::new(dir.path(), std::time::Duration::from_secs(60))
                .unwrap();
        (dir, Arc::new(engine))
    }

    #[test]
    fn advance_global_checkpoint_no_replicas_uses_primary() {
        let (_dir, engine) = make_checkpoint_engine();
        TransportService::advance_global_checkpoint(&engine, 10, &[]);
        assert_eq!(engine.global_checkpoint(), 10);
    }

    #[test]
    fn advance_global_checkpoint_min_of_primary_and_replicas() {
        let (_dir, engine) = make_checkpoint_engine();
        let replicas = vec![("r1".into(), 5u64), ("r2".into(), 8u64)];
        TransportService::advance_global_checkpoint(&engine, 10, &replicas);
        assert_eq!(engine.global_checkpoint(), 5, "should be min(10, 5, 8) = 5");
    }

    #[test]
    fn advance_global_checkpoint_primary_lower_than_replicas() {
        let (_dir, engine) = make_checkpoint_engine();
        let replicas = vec![("r1".into(), 20u64)];
        TransportService::advance_global_checkpoint(&engine, 3, &replicas);
        assert_eq!(engine.global_checkpoint(), 3, "primary is the bottleneck");
    }

    #[test]
    fn advance_global_checkpoint_never_goes_backward() {
        let (_dir, engine) = make_checkpoint_engine();
        // Set to 10 first
        TransportService::advance_global_checkpoint(&engine, 10, &[]);
        assert_eq!(engine.global_checkpoint(), 10);

        // Try to set lower — should stay at 10
        let replicas = vec![("r1".into(), 5u64)];
        TransportService::advance_global_checkpoint(&engine, 5, &replicas);
        assert_eq!(engine.global_checkpoint(), 10, "should never go backward");
    }

    #[test]
    fn advance_global_checkpoint_advances_forward() {
        let (_dir, engine) = make_checkpoint_engine();
        TransportService::advance_global_checkpoint(&engine, 5, &[]);
        assert_eq!(engine.global_checkpoint(), 5);

        TransportService::advance_global_checkpoint(&engine, 10, &[]);
        assert_eq!(engine.global_checkpoint(), 10);
    }

    #[test]
    fn advance_global_checkpoint_single_lagging_replica() {
        let (_dir, engine) = make_checkpoint_engine();
        let replicas = vec![
            ("fast".into(), 100u64),
            ("slow".into(), 2u64),
            ("medium".into(), 50u64),
        ];
        TransportService::advance_global_checkpoint(&engine, 100, &replicas);
        assert_eq!(
            engine.global_checkpoint(),
            2,
            "slowest replica determines global checkpoint"
        );
    }

    #[tokio::test]
    async fn get_or_open_search_shard_reopens_persisted_shard_via_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let test_uuid = "test-uuid-reopen";
        {
            let shard_dir = dir.path().join(test_uuid).join("shard_0");
            std::fs::create_dir_all(&shard_dir).unwrap();
            let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"title": "rust restart unit"}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let mut cluster_state = DomainClusterState::new("restart-unit".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cluster_state.add_index(DomainIndexMetadata {
            name: "restart-idx".into(),
            uuid: test_uuid.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
        let manager = ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);

        let service = TransportService {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let engine = service
            .get_or_open_search_shard("restart-idx", 0)
            .await
            .expect("persisted shard should reopen via metadata");
        let hits = engine.search("rust").unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[tokio::test]
    async fn get_doc_reopens_persisted_shard_via_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let test_uuid = "test-uuid-getdoc";
        {
            let shard_dir = dir.path().join(test_uuid).join("shard_0");
            std::fs::create_dir_all(&shard_dir).unwrap();
            let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"title": "rust restart unit"}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let mut cluster_state = DomainClusterState::new("restart-unit".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cluster_state.add_index(DomainIndexMetadata {
            name: "restart-idx".into(),
            uuid: test_uuid.into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
        let manager = ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);

        let service = TransportService {
            cluster_manager: Arc::new(manager),
            shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let response = service
            .get_doc(Request::new(ShardGetRequest {
                index_name: "restart-idx".into(),
                shard_id: 0,
                doc_id: "d1".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(response.found);
        let source: serde_json::Value = serde_json::from_slice(&response.source_json).unwrap();
        assert_eq!(source["title"], "rust restart unit");
    }

    #[tokio::test]
    async fn get_shard_stats_only_reports_open_shards() {
        let dir = tempfile::tempdir().unwrap();

        let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        sm.open_shard("open-idx", 0).unwrap();

        let service = TransportService {
            cluster_manager: Arc::new(ClusterManager::new("stats-cluster".into())),
            shard_manager: sm,
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let response = service
            .get_shard_stats(Request::new(ShardStatsRequest {}))
            .await
            .unwrap()
            .into_inner();

        // Only the shard opened above should appear — no disk scanning
        assert_eq!(response.shards.len(), 1);
        assert_eq!(response.shards[0].index_name, "open-idx");
        assert_eq!(response.shards[0].shard_id, 0);
    }

    #[tokio::test]
    async fn get_or_open_search_shard_returns_not_found_for_unknown_shard() {
        let dir = tempfile::tempdir().unwrap();
        let service = TransportService {
            cluster_manager: Arc::new(ClusterManager::new("missing-unit".into())),
            shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let err = match service.get_or_open_search_shard("missing-idx", 99).await {
            Ok(_) => panic!("unknown shard should not be opened"),
            Err(err) => err,
        };
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("not found"));
    }

    #[tokio::test]
    async fn get_or_open_shard_returns_not_found_for_unknown_shard() {
        let dir = tempfile::tempdir().unwrap();
        let service = TransportService {
            cluster_manager: Arc::new(ClusterManager::new("missing-unit".into())),
            shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let err = match service.get_or_open_shard("missing-idx", 99).await {
            Ok(_) => panic!("unknown write shard should not be opened"),
            Err(err) => err,
        };
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("not found"));
    }

    #[tokio::test]
    async fn maintenance_skips_orphaned_shards() {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

        // Open shard 0 (assigned to node-1) and shard 2 (orphan — assigned to node-3)
        sm.open_shard("maint-idx", 0).unwrap();
        sm.open_shard("maint-idx", 2).unwrap();

        let mut cluster_state = DomainClusterState::new("maint-cluster".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-2".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        shard_routing.insert(
            2,
            ShardRoutingEntry {
                primary: "node-3".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        cluster_state.add_index(DomainIndexMetadata {
            name: "maint-idx".into(),
            uuid: String::new(),
            number_of_shards: 3,
            number_of_replicas: 0,
            shard_routing,
            mappings: HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });

        let manager = ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);

        let service = TransportService {
            cluster_manager: Arc::new(manager),
            shard_manager: sm,
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let (successful, failed) =
            service.run_maintenance_on_assigned_shards("maint-idx", |e| e.refresh());
        // Only shard 0 is assigned to node-1; shard 2 is orphaned → skipped
        assert_eq!(
            successful, 1,
            "only locally-assigned shard should be refreshed"
        );
        assert_eq!(failed, 0);
    }

    #[tokio::test]
    async fn maintenance_includes_replica_shards() {
        let dir = tempfile::tempdir().unwrap();
        let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        sm.open_shard("rep-idx", 0).unwrap();
        sm.open_shard("rep-idx", 1).unwrap();

        let mut cluster_state = DomainClusterState::new("rep-cluster".into());
        let mut shard_routing = HashMap::new();
        shard_routing.insert(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                replicas: vec![],
                unassigned_replicas: 0,
            },
        );
        shard_routing.insert(
            1,
            ShardRoutingEntry {
                primary: "node-2".into(),
                replicas: vec!["node-1".into()],
                unassigned_replicas: 0,
            },
        );
        cluster_state.add_index(DomainIndexMetadata {
            name: "rep-idx".into(),
            uuid: String::new(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing,
            mappings: HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });

        let manager = ClusterManager::new(cluster_state.cluster_name.clone());
        manager.update_state(cluster_state);

        let service = TransportService {
            cluster_manager: Arc::new(manager),
            shard_manager: sm,
            transport_client: crate::transport::TransportClient::new(),
            raft: None,
            local_node_id: "node-1".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            join_lock: new_join_lock(),
        };

        let (successful, failed) =
            service.run_maintenance_on_assigned_shards("rep-idx", |e| e.refresh());
        // Shard 0 primary + shard 1 replica = 2
        assert_eq!(successful, 2, "primary + replica assigned here");
        assert_eq!(failed, 0);
    }

    #[test]
    fn validate_join_identity_allows_zero_raft_id_for_multiple_nodes() {
        // raft_node_id=0 is the legacy/non-Raft value. Multiple nodes can share
        // it without triggering the duplicate-identity rejection.
        let mut state = DomainClusterState::new("test".into());
        state.add_node(DomainNodeInfo {
            id: "node-a".into(),
            name: "a".into(),
            host: "10.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
        state.add_node(DomainNodeInfo {
            id: "node-b".into(),
            name: "b".into(),
            host: "10.0.0.2".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });

        // A third node joining with raft_node_id=0 must NOT be rejected
        assert!(validate_join_identity(&state, "node-c", 0).is_ok());
    }

    #[test]
    fn validate_join_identity_rejects_duplicate_nonzero_raft_id() {
        let mut state = DomainClusterState::new("test".into());
        state.add_node(DomainNodeInfo {
            id: "node-a".into(),
            name: "a".into(),
            host: "10.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 5,
        });

        // Different node trying to reuse raft_node_id=5 must fail
        let err = validate_join_identity(&state, "node-b", 5).unwrap_err();
        assert!(
            err.message()
                .contains("raft_node_id 5 is already registered")
        );
    }

    #[test]
    fn validate_join_identity_allows_same_node_same_raft_id() {
        let mut state = DomainClusterState::new("test".into());
        state.add_node(DomainNodeInfo {
            id: "node-a".into(),
            name: "a".into(),
            host: "10.0.0.1".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 5,
        });

        // Same node re-joining with its own raft_node_id must succeed (idempotent)
        assert!(validate_join_identity(&state, "node-a", 5).is_ok());
    }

    #[test]
    fn roundtrip_unknown_field_type_returns_error() {
        // Unknown field types must fail snapshot decoding so startup never
        // consumes a lossy authoritative JoinCluster snapshot.
        let proto = ClusterState {
            cluster_name: "test".into(),
            version: 1,
            master_node: None,
            nodes: vec![],
            indices: vec![IndexMetadata {
                name: "test-idx".into(),
                uuid: "test-uuid".into(),
                number_of_shards: 1,
                number_of_replicas: 0,
                shards: vec![],
                mappings: vec![FieldMappingEntry {
                    name: "weird_field".into(),
                    field_type: "future_type_v99".into(),
                    dimension: None,
                }],
                settings: None,
            }],
        };

        let err = proto_to_cluster_state(&proto).unwrap_err();
        assert!(
            err.message().contains(
                "unknown field type 'future_type_v99' for field 'weird_field' in index 'test-idx'"
            ),
            "unexpected error: {}",
            err.message()
        );
    }
}
