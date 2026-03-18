//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
use crate::consensus::types::RaftInstance;
use crate::shard::ShardManager;
use crate::transport::proto::internal_transport_server::{InternalTransport, InternalTransportServer};
use crate::transport::proto::*;
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
        roles: n.roles.iter().map(|r| match r {
            crate::cluster::state::NodeRole::Master => "master".into(),
            crate::cluster::state::NodeRole::Data => "data".into(),
            crate::cluster::state::NodeRole::Client => "client".into(),
        }).collect(),
    }
}

fn proto_to_node_info(p: &NodeInfo) -> crate::cluster::state::NodeInfo {
    crate::cluster::state::NodeInfo {
        id: p.id.clone(),
        name: p.name.clone(),
        host: p.host.clone(),
        transport_port: p.transport_port as u16,
        http_port: p.http_port as u16,
        roles: p.roles.iter().map(|r| match r.as_str() {
            "master" => crate::cluster::state::NodeRole::Master,
            "data" => crate::cluster::state::NodeRole::Data,
            "client" => crate::cluster::state::NodeRole::Client,
            _ => crate::cluster::state::NodeRole::Data,
        }).collect(),
        raft_node_id: 0,
    }
}

pub fn cluster_state_to_proto(s: &crate::cluster::state::ClusterState) -> ClusterState {
    ClusterState {
        cluster_name: s.cluster_name.clone(),
        version: s.version,
        master_node: s.master_node.clone(),
        nodes: s.nodes.values().map(node_info_to_proto).collect(),
        indices: s.indices.values().map(|idx| IndexMetadata {
            name: idx.name.clone(),
            number_of_shards: idx.number_of_shards,
            number_of_replicas: idx.number_of_replicas,
            shards: idx.shard_routing.iter().map(|(sid, routing)| ShardAssignment {
                shard_id: *sid,
                node_id: routing.primary.clone(),
                replica_node_ids: routing.replicas.clone(),
            }).collect(),
        }).collect(),
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
            shard_routing.insert(sa.shard_id, crate::cluster::state::ShardRoutingEntry {
                primary: sa.node_id.clone(),
                replicas: sa.replica_node_ids.clone(),
            });
        }
        state.indices.insert(idx.name.clone(), crate::cluster::state::IndexMetadata {
            name: idx.name.clone(),
            number_of_shards: idx.number_of_shards,
            number_of_replicas: idx.number_of_replicas,
            shard_routing,
        });
    }
    state
}

// ─── gRPC Service Implementation ───────────────────────────────────────────

#[tonic::async_trait]
impl InternalTransport for TransportService {
    async fn join_cluster(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let node_info = req.node_info.ok_or_else(|| Status::invalid_argument("missing node_info"))?;
        let mut ni = proto_to_node_info(&node_info);
        let joining_raft_id = req.raft_node_id;
        info!("gRPC: join request from node {} (raft_id={})", ni.id, joining_raft_id);

        // If Raft is active and we are the leader, register the node through Raft
        if let Some(ref raft) = self.raft {
            if raft.is_leader() && joining_raft_id > 0 {
                ni.raft_node_id = joining_raft_id;

                // 1. Register the node in cluster state via Raft
                let cmd = crate::consensus::types::ClusterCommand::AddNode { node: ni.clone() };
                raft.client_write(cmd).await
                    .map_err(|e| Status::internal(format!("Raft AddNode failed: {}", e)))?;

                // 2. Add as Raft learner (non-blocking — replication starts in background)
                let addr = format!("{}:{}", ni.host, ni.transport_port);
                raft.add_learner(joining_raft_id, openraft::BasicNode { addr }, false).await
                    .map_err(|e| Status::internal(format!("Raft add_learner failed: {}", e)))?;

                // 3. Promote to voter
                let voters: std::collections::BTreeSet<u64> = raft.voter_ids().chain(std::iter::once(joining_raft_id)).collect();
                raft.change_membership(voters, false).await
                    .map_err(|e| Status::internal(format!("Raft change_membership failed: {}", e)))?;

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

    async fn publish_state(&self, request: Request<PublishStateRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let proto_state = req.state.ok_or_else(|| Status::invalid_argument("missing state"))?;
        let new_state = proto_to_cluster_state(&proto_state);
        info!("gRPC: cluster state update, version {}", new_state.version);

        // Detect indices that were removed and close their local shards + delete data
        let old_state = self.cluster_manager.get_state();
        for old_index in old_state.indices.keys() {
            if !new_state.indices.contains_key(old_index) {
                info!("Index '{}' removed from cluster state — closing local shards", old_index);
                if let Err(e) = self.shard_manager.close_index_shards(old_index) {
                    tracing::error!("Failed to close shards for deleted index '{}': {}", old_index, e);
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

    async fn index_doc(&self, request: Request<ShardDocRequest>) -> Result<Response<ShardDocResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        let payload: serde_json::Value = serde_json::from_slice(&req.payload_json)
            .map_err(|e| Status::invalid_argument(format!("invalid JSON: {}", e)))?;

        let doc_id = if req.doc_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.doc_id
        };

        info!("gRPC: index doc '{}' into {}/shard_{}", doc_id, req.index_name, req.shard_id);
        match engine.add_document(&doc_id, payload.clone()) {
            Ok(id) => {
                // Replicate to replica shards
                let cs = self.cluster_manager.get_state();
                if let Err(errors) = crate::replication::replicate_write(
                    &self.transport_client, &cs, &req.index_name, req.shard_id, &id, &payload, "index",
                ).await {
                    tracing::warn!("Replication errors for {}/shard_{}: {:?}", req.index_name, req.shard_id, errors);
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

    async fn bulk_index(&self, request: Request<ShardBulkRequest>) -> Result<Response<ShardBulkResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        let docs: Vec<(String, serde_json::Value)> = req.documents_json.iter()
            .map(|b| {
                let val: serde_json::Value = serde_json::from_slice(b)
                    .map_err(|e| Status::invalid_argument(format!("invalid JSON in bulk: {}", e)))?;
                // Extract _id from the payload or generate one
                let doc_id = val.get("_doc_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let payload = val.get("_source")
                    .cloned()
                    .unwrap_or(val.clone());
                Ok((doc_id, payload))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        info!("gRPC: bulk {} docs into {}/shard_{}", docs.len(), req.index_name, req.shard_id);
        match engine.bulk_add_documents(docs.clone()) {
            Ok(ids) => {
                // Replicate to replica shards
                let cs = self.cluster_manager.get_state();
                if let Err(errors) = crate::replication::replicate_bulk(
                    &self.transport_client, &cs, &req.index_name, req.shard_id, &docs,
                ).await {
                    tracing::warn!("Bulk replication errors for {}/shard_{}: {:?}", req.index_name, req.shard_id, errors);
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

    async fn delete_doc(&self, request: Request<ShardDeleteRequest>) -> Result<Response<ShardDeleteResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;
        info!("gRPC: delete doc '{}' from {}/shard_{}", req.doc_id, req.index_name, req.shard_id);
        match engine.delete_document(&req.doc_id) {
            Ok(deleted) => {
                // Replicate delete to replica shards
                let cs = self.cluster_manager.get_state();
                if let Err(errors) = crate::replication::replicate_write(
                    &self.transport_client, &cs, &req.index_name, req.shard_id,
                    &req.doc_id, &serde_json::json!({}), "delete",
                ).await {
                    tracing::warn!("Delete replication errors for {}/shard_{}: {:?}", req.index_name, req.shard_id, errors);
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

    async fn get_doc(&self, request: Request<ShardGetRequest>) -> Result<Response<ShardGetResponse>, Status> {
        let req = request.into_inner();
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => return Ok(Response::new(ShardGetResponse {
                found: false, source_json: vec![], error: "Shard not found on this node".into(),
            })),
        };
        match engine.get_document(&req.doc_id) {
            Ok(Some(source)) => Ok(Response::new(ShardGetResponse {
                found: true,
                source_json: serde_json::to_vec(&source).unwrap_or_default(),
                error: String::new(),
            })),
            Ok(None) => Ok(Response::new(ShardGetResponse {
                found: false, source_json: vec![], error: String::new(),
            })),
            Err(e) => Ok(Response::new(ShardGetResponse {
                found: false, source_json: vec![], error: e.to_string(),
            })),
        }
    }

    async fn search_shard(&self, request: Request<ShardSearchRequest>) -> Result<Response<ShardSearchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => return Ok(Response::new(ShardSearchResponse {
                success: false, hits: vec![], error: "Shard not found on this node".into(),
            })),
        };

        match engine.search(&req.query) {
            Ok(hits) => Ok(Response::new(ShardSearchResponse {
                success: true,
                hits: hits.into_iter().map(|v| SearchHit {
                    source_json: serde_json::to_vec(&v).unwrap_or_default(),
                }).collect(),
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ShardSearchResponse {
                success: false, hits: vec![], error: e.to_string(),
            })),
        }
    }

    async fn search_shard_dsl(&self, request: Request<ShardSearchDslRequest>) -> Result<Response<ShardSearchResponse>, Status> {
        let req = request.into_inner();
        let engine = match self.shard_manager.get_shard(&req.index_name, req.shard_id) {
            Some(e) => e,
            None => return Ok(Response::new(ShardSearchResponse {
                success: false, hits: vec![], error: "Shard not found on this node".into(),
            })),
        };

        let search_req: crate::search::SearchRequest = serde_json::from_slice(&req.search_request_json)
            .map_err(|e| Status::invalid_argument(format!("invalid SearchRequest JSON: {}", e)))?;

        let mut all_hits = Vec::new();

        // Text / DSL search
        match engine.search_query(&search_req) {
            Ok(hits) => all_hits.extend(hits),
            Err(e) => return Ok(Response::new(ShardSearchResponse {
                success: false, hits: vec![], error: e.to_string(),
            })),
        }

        // k-NN vector search (if knn clause present)
        if let Some(ref knn) = search_req.knn {
            if let Some((field_name, params)) = knn.fields.iter().next() {
                match engine.search_knn_filtered(field_name, &params.vector, params.k, params.filter.as_ref()) {
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
            hits: all_hits.into_iter().map(|v| SearchHit {
                source_json: serde_json::to_vec(&v).unwrap_or_default(),
            }).collect(),
            error: String::new(),
        }))
    }

    async fn replicate_doc(&self, request: Request<ReplicateDocRequest>) -> Result<Response<ReplicateDocResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        info!("gRPC: replicate {} doc '{}' to {}/shard_{}", req.op, req.doc_id, req.index_name, req.shard_id);

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
            Ok(()) => Ok(Response::new(ReplicateDocResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ReplicateDocResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn replicate_bulk(&self, request: Request<ReplicateBulkRequest>) -> Result<Response<ReplicateBulkResponse>, Status> {
        let req = request.into_inner();
        let engine = self.get_or_open_shard(&req.index_name, req.shard_id)?;

        info!("gRPC: replicate bulk {} ops to {}/shard_{}", req.ops.len(), req.index_name, req.shard_id);

        let mut docs = Vec::with_capacity(req.ops.len());
        for op in &req.ops {
            let payload: serde_json::Value = serde_json::from_slice(&op.payload_json)
                .map_err(|e| Status::invalid_argument(format!("invalid JSON in bulk replicate: {}", e)))?;
            docs.push((op.doc_id.clone(), payload));
        }

        match engine.bulk_add_documents(docs) {
            Ok(_) => Ok(Response::new(ReplicateBulkResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ReplicateBulkResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    // ─── Raft RPCs ────────────────────────────────────────────────────────────

    async fn raft_vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let raft = self.raft.as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let rpc: openraft::raft::VoteRequest<crate::consensus::TypeConfig> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("bad vote request: {}", e)))?;
        let resp = raft.vote(rpc).await
            .map_err(|e| Status::internal(format!("raft vote error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise vote response: {}", e)))?;
        Ok(Response::new(RaftReply { data, error: String::new() }))
    }

    async fn raft_append_entries(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let raft = self.raft.as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let rpc: openraft::raft::AppendEntriesRequest<crate::consensus::TypeConfig> =
            serde_json::from_slice(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("bad append_entries request: {}", e)))?;
        let resp = raft.append_entries(rpc).await
            .map_err(|e| Status::internal(format!("raft append_entries error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise append_entries response: {}", e)))?;
        Ok(Response::new(RaftReply { data, error: String::new() }))
    }

    async fn raft_snapshot(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let raft = self.raft.as_ref()
            .ok_or_else(|| Status::unavailable("Raft not initialised on this node"))?;
        let payload: serde_json::Value = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("bad snapshot request: {}", e)))?;
        let vote: crate::consensus::types::Vote = serde_json::from_value(
            payload.get("vote").cloned().unwrap_or_default(),
        ).map_err(|e| Status::invalid_argument(format!("bad vote in snapshot: {}", e)))?;
        let meta: crate::consensus::types::SnapshotMeta = serde_json::from_value(
            payload.get("meta").cloned().unwrap_or_default(),
        ).map_err(|e| Status::invalid_argument(format!("bad meta in snapshot: {}", e)))?;
        let data: Vec<u8> = serde_json::from_value(
            payload.get("data").cloned().unwrap_or_default(),
        ).map_err(|e| Status::invalid_argument(format!("bad data in snapshot: {}", e)))?;
        let snapshot = crate::consensus::types::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(data),
        };
        let resp = raft.install_full_snapshot(vote, snapshot).await
            .map_err(|e| Status::internal(format!("raft snapshot error: {}", e)))?;
        let data = serde_json::to_vec(&resp)
            .map_err(|e| Status::internal(format!("serialise snapshot response: {}", e)))?;
        Ok(Response::new(RaftReply { data, error: String::new() }))
    }
}

impl TransportService {
    fn get_or_open_shard(&self, index_name: &str, shard_id: u32) -> Result<Arc<dyn crate::engine::SearchEngine>, Status> {
        if let Some(e) = self.shard_manager.get_shard(index_name, shard_id) {
            return Ok(e);
        }
        self.shard_manager.open_shard(index_name, shard_id)
            .map_err(|e| Status::internal(format!("Failed to open shard: {}", e)))
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
        shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec!["node-2".into()],
        });
        shard_routing.insert(1, ShardRoutingEntry {
            primary: "node-2".into(),
            replicas: vec!["node-1".into()],
        });

        cs.add_index(DomainIndexMetadata {
            name: "products".into(),
            number_of_shards: 2,
            number_of_replicas: 1,
            shard_routing,
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
        shard_routing.insert(0, ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
        });
        cs.add_index(DomainIndexMetadata {
            name: "logs".into(),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing,
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
}
