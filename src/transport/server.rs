//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::proto::internal_transport_server::{
    InternalTransport, InternalTransportServer,
};
use crate::transport::proto::*;
use crate::wal::WriteAheadLog;
use openraft::type_config::async_runtime::WatchReceiver;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::info;

/// Shared state for the gRPC transport service.
#[derive(Clone)]
pub struct TransportService {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: crate::transport::TransportClient,
    /// Optional Raft consensus instance. When present, Raft RPCs are forwarded here.
    pub raft: Option<Arc<RaftInstance>>,
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
        raft_node_id: 0,
    }
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
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shards: idx
                    .shard_routing
                    .iter()
                    .map(|(sid, routing)| ShardAssignment {
                        shard_id: *sid,
                        node_id: routing.primary.clone(),
                        replica_node_ids: routing.replicas.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

pub fn proto_to_cluster_state(p: &ClusterState) -> crate::cluster::state::ClusterState {
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
                    unassigned_replicas: 0,
                },
            );
        }
        state.indices.insert(
            idx.name.clone(),
            crate::cluster::state::IndexMetadata {
                name: idx.name.clone(),
                number_of_shards: idx.number_of_shards,
                number_of_replicas: idx.number_of_replicas,
                shard_routing,
                mappings: std::collections::HashMap::new(),
                settings: crate::cluster::state::IndexSettings::default(),
            },
        );
    }
    state
}

// ─── gRPC Service Implementation ───────────────────────────────────────────

#[tonic::async_trait]
impl InternalTransport for TransportService {
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
        info!(
            "gRPC: join request from node {} (raft_id={})",
            ni.id, joining_raft_id
        );

        // If Raft is active and we are the leader, register the node through Raft
        if let Some(ref raft) = self.raft {
            if raft.is_leader() && joining_raft_id > 0 {
                ni.raft_node_id = joining_raft_id;

                // 1. Register the node in cluster state via Raft
                let cmd = crate::consensus::types::ClusterCommand::AddNode { node: ni.clone() };
                raft.client_write(cmd)
                    .await
                    .map_err(|e| Status::internal(format!("Raft AddNode failed: {}", e)))?;

                // 2. Add as Raft learner (non-blocking — replication starts in background)
                let addr = format!("{}:{}", ni.host, ni.transport_port);
                raft.add_learner(joining_raft_id, openraft::BasicNode { addr }, false)
                    .await
                    .map_err(|e| Status::internal(format!("Raft add_learner failed: {}", e)))?;

                // 3. Promote to voter
                let voters: std::collections::BTreeSet<u64> = raft
                    .voter_ids()
                    .chain(std::iter::once(joining_raft_id))
                    .collect();
                raft.change_membership(voters, false).await.map_err(|e| {
                    Status::internal(format!("Raft change_membership failed: {}", e))
                })?;

                let state = self.cluster_manager.get_state();
                return Ok(Response::new(JoinResponse {
                    state: Some(cluster_state_to_proto(&state)),
                }));
            }
        }

        // Fallback: legacy join (no Raft or not leader)
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
        let new_state = proto_to_cluster_state(&proto_state);
        info!("gRPC: cluster state update, version {}", new_state.version);

        // Detect indices that were removed and close their local shards + delete data
        let old_state = self.cluster_manager.get_state();
        for old_index in old_state.indices.keys() {
            if !new_state.indices.contains_key(old_index) {
                info!(
                    "Index '{}' removed from cluster state — closing local shards",
                    old_index
                );
                if let Err(e) = self.shard_manager.close_index_shards(old_index) {
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
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

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
        match engine.add_document(&doc_id, payload.clone()) {
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
                    }
                }
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
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        let docs: Vec<(String, serde_json::Value)> = req
            .documents_json
            .iter()
            .map(|b| {
                let val: serde_json::Value = serde_json::from_slice(b).map_err(|e| {
                    Status::invalid_argument(format!("invalid JSON in bulk: {}", e))
                })?;
                // Extract _id from the payload or generate one
                let doc_id = val
                    .get("_doc_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let payload = val.get("_source").cloned().unwrap_or(val.clone());
                Ok((doc_id, payload))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        info!(
            "gRPC: bulk {} docs into {}/shard_{}",
            docs.len(),
            req.index_name,
            req.shard_id
        );
        match engine.bulk_add_documents(docs.clone()) {
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
                    }
                }
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
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;
        info!(
            "gRPC: delete doc '{}' from {}/shard_{}",
            req.doc_id, req.index_name, req.shard_id
        );
        match engine.delete_document(&req.doc_id) {
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
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => {
                return Ok(Response::new(ShardGetResponse {
                    found: false,
                    source_json: vec![],
                    error: "Shard not found on this node".into(),
                }));
            }
        };
        match engine.get_document(&req.doc_id) {
            Ok(Some(source)) => Ok(Response::new(ShardGetResponse {
                found: true,
                source_json: serde_json::to_vec(&source).unwrap_or_default(),
                error: String::new(),
            })),
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
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => {
                return Ok(Response::new(ShardSearchResponse {
                    success: false,
                    hits: vec![],
                    error: "Shard not found on this node".into(),
                    total_hits: 0,
                }));
            }
        };

        match engine.search(&req.query) {
            Ok(hits) => Ok(Response::new(ShardSearchResponse {
                success: true,
                hits: hits
                    .into_iter()
                    .map(|v| SearchHit {
                        source_json: serde_json::to_vec(&v).unwrap_or_default(),
                    })
                    .collect(),
                error: String::new(),
                total_hits: 0,
            })),
            Err(e) => Ok(Response::new(ShardSearchResponse {
                success: false,
                hits: vec![],
                error: e.to_string(),
                total_hits: 0,
            })),
        }
    }

    async fn search_shard_dsl(
        &self,
        request: Request<ShardSearchDslRequest>,
    ) -> Result<Response<ShardSearchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => {
                return Ok(Response::new(ShardSearchResponse {
                    success: false,
                    hits: vec![],
                    error: "Shard not found on this node".into(),
                    total_hits: 0,
                }));
            }
        };

        let search_req: crate::search::SearchRequest =
            serde_json::from_slice(&req.search_request_json).map_err(|e| {
                Status::invalid_argument(format!("invalid SearchRequest JSON: {}", e))
            })?;

        let mut all_hits = Vec::new();
        let mut total_hits: usize = 0;

        // Text / DSL search
        match engine.search_query(&search_req) {
            Ok((hits, total)) => {
                total_hits += total;
                all_hits.extend(hits);
            }
            Err(e) => {
                return Ok(Response::new(ShardSearchResponse {
                    success: false,
                    hits: vec![],
                    error: e.to_string(),
                    total_hits: 0,
                }));
            }
        }

        // k-NN vector search (if knn clause present)
        if let Some(ref knn) = search_req.knn {
            if let Some((field_name, params)) = knn.fields.iter().next() {
                match engine.search_knn_filtered(
                    field_name,
                    &params.vector,
                    params.k,
                    params.filter.as_ref(),
                ) {
                    Ok(hits) => all_hits.extend(hits),
                    Err(e) => {
                        tracing::error!("Vector search on remote shard failed: {}", e);
                        // Non-fatal: text results still returned
                    }
                }
            }
        }

        Ok(Response::new(ShardSearchResponse {
            success: true,
            hits: all_hits
                .into_iter()
                .map(|v| SearchHit {
                    source_json: serde_json::to_vec(&v).unwrap_or_default(),
                })
                .collect(),
            error: String::new(),
            total_hits: total_hits as u64,
        }))
    }

    async fn replicate_doc(
        &self,
        request: Request<ReplicateDocRequest>,
    ) -> Result<Response<ReplicateDocResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        info!(
            "gRPC: replicate {} doc '{}' (seq_no={}) to {}/shard_{}",
            req.op, req.doc_id, req.seq_no, req.index_name, req.shard_id
        );

        let result = match req.op.as_str() {
            "index" => {
                let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
                    .map_err(|e| Status::invalid_argument(format!("invalid JSON: {}", e)))?;
                engine.add_document(&req.doc_id, payload).map(|_| ())
            }
            "delete" => engine.delete_document(&req.doc_id).map(|_| ()),
            other => Err(anyhow::anyhow!("Unknown replication op: {}", other)),
        };

        match result {
            Ok(()) => {
                // Update replica's local checkpoint
                engine.update_local_checkpoint(req.seq_no);
                Ok(Response::new(ReplicateDocResponse {
                    success: true,
                    error: String::new(),
                    local_checkpoint: engine.local_checkpoint(),
                }))
            }
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
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

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

        match engine.bulk_add_documents(docs) {
            Ok(_) => {
                // Update replica's checkpoint with the highest seq_no in the batch
                if let Some(last_op) = req.ops.last() {
                    engine.update_local_checkpoint(last_op.seq_no);
                }
                Ok(Response::new(ReplicateBulkResponse {
                    success: true,
                    error: String::new(),
                    local_checkpoint: engine.local_checkpoint(),
                }))
            }
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
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        info!(
            "gRPC: recover_replica for {}/shard_{} from checkpoint {}",
            req.index_name, req.shard_id, req.local_checkpoint
        );

        // Read translog entries above the replica's checkpoint
        // The engine's underlying HotEngine has the translog — we need to read ops from it.
        // Since we can't access the translog directly through the SearchEngine trait,
        // we replay by reading all entries and filtering.
        // For now, use the WAL's read_from capability through the shard directory.
        let shard_dir = self
            .shard_manager
            .data_dir()
            .join(&req.index_name)
            .join(format!("shard_{}", req.shard_id));

        let entries: Vec<crate::wal::TranslogEntry> =
            match crate::wal::HotTranslog::open(&shard_dir) {
                Ok(tl) => match tl.read_from(req.local_checkpoint) {
                    Ok(entries) => entries,
                    Err(e) => {
                        return Ok(Response::new(RecoverReplicaResponse {
                            success: false,
                            error: format!("Failed to read translog: {}", e),
                            ops_replayed: 0,
                            primary_checkpoint: engine.local_checkpoint(),
                            operations: vec![],
                        }));
                    }
                },
                Err(e) => {
                    return Ok(Response::new(RecoverReplicaResponse {
                        success: false,
                        error: format!("Failed to open translog: {}", e),
                        ops_replayed: 0,
                        primary_checkpoint: engine.local_checkpoint(),
                        operations: vec![],
                    }));
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
                RecoverReplicaOp {
                    seq_no: e.seq_no,
                    op: e.op.clone(),
                    doc_id,
                    payload_json: serde_json::to_vec(&e.payload).unwrap_or_default(),
                }
            })
            .collect();

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
            } else if let Some(ms) = val.as_u64() {
                if metadata.settings.refresh_interval_ms != Some(ms) {
                    metadata.settings.refresh_interval_ms = Some(ms);
                    changed = true;
                }
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
        .unwrap_or_default();

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
        if let Err(e) = self.shard_manager.close_index_shards(index_name) {
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
            m.borrow_watched().vote.clone()
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
        let vote: crate::consensus::types::Vote =
            serde_json::from_value(payload.get("vote").cloned().unwrap_or_default())
                .map_err(|e| Status::invalid_argument(format!("bad vote in snapshot: {}", e)))?;
        let meta: crate::consensus::types::SnapshotMeta =
            serde_json::from_value(payload.get("meta").cloned().unwrap_or_default())
                .map_err(|e| Status::invalid_argument(format!("bad meta in snapshot: {}", e)))?;
        let data: Vec<u8> =
            serde_json::from_value(payload.get("data").cloned().unwrap_or_default())
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
    fn get_or_open_shard(
        &self,
        index_name: &str,
        shard_id: u32,
    ) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        if let Some(e) = self.shard_manager.get_shard(index_name, shard_id) {
            return Ok(e);
        }
        // Look up mappings from cluster state if available
        let cs = self.cluster_manager.get_state();
        let mappings = cs
            .indices
            .get(index_name)
            .map(|m| m.mappings.clone())
            .unwrap_or_default();
        self.shard_manager
            .open_shard_with_mappings(index_name, shard_id, &mappings)
            .map_err(|e| Status::internal(format!("Failed to open shard: {}", e)))
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
}

/// Create the gRPC transport server (tonic)
pub fn create_transport_service(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: None,
    };
    InternalTransportServer::new(service)
}

/// Create the gRPC transport server with Raft consensus enabled.
pub fn create_transport_service_with_raft(
    cluster_manager: Arc<ClusterManager>,
    shard_manager: Arc<ShardManager>,
    transport_client: crate::transport::TransportClient,
    raft: Arc<RaftInstance>,
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client,
        raft: Some(raft),
    };
    InternalTransportServer::new(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        ClusterState as DomainClusterState, IndexMetadata as DomainIndexMetadata,
        NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
    };
    use std::collections::HashMap;

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
            raft_node_id: 0,
        });
        cs.add_node(DomainNodeInfo {
            id: "node-2".into(),
            name: "replica-node".into(),
            host: "10.0.0.2".into(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
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
                unassigned_replicas: 0,
            },
        );

        cs.add_index(DomainIndexMetadata {
            name: "products".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
        cs.version = 42; // reset again after add_index

        cs
    }

    #[test]
    fn cluster_state_roundtrip_preserves_metadata() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto);

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
        let restored = proto_to_cluster_state(&proto);

        let n1 = restored.nodes.get("node-1").unwrap();
        assert_eq!(n1.name, "primary-node");
        assert_eq!(n1.host, "10.0.0.1");
        assert_eq!(n1.transport_port, 9300);
        assert_eq!(n1.http_port, 9200);
        assert!(n1.roles.contains(&NodeRole::Master));
        assert!(n1.roles.contains(&NodeRole::Data));

        let n2 = restored.nodes.get("node-2").unwrap();
        assert_eq!(n2.name, "replica-node");
        assert_eq!(n2.roles, vec![NodeRole::Data]);
    }

    #[test]
    fn roundtrip_preserves_shard_routing() {
        let original = make_full_cluster_state();
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto);

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
    }

    #[test]
    fn roundtrip_empty_cluster_state() {
        let original = DomainClusterState::new("empty".into());
        let proto = cluster_state_to_proto(&original);
        let restored = proto_to_cluster_state(&proto);

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
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
            mappings: std::collections::HashMap::new(),
            settings: crate::cluster::state::IndexSettings::default(),
        });

        let proto = cluster_state_to_proto(&cs);
        let restored = proto_to_cluster_state(&proto);

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
        let restored = proto_to_cluster_state(&proto);

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
}
