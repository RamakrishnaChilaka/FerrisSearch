//! gRPC transport server — implements the InternalTransport service.

use crate::cluster::manager::ClusterManager;
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
            shards: idx.shards.iter().map(|(sid, nid)| ShardAssignment {
                shard_id: *sid,
                node_id: nid.clone(),
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
        let mut shards = std::collections::HashMap::new();
        for sa in &idx.shards {
            shards.insert(sa.shard_id, sa.node_id.clone());
        }
        state.indices.insert(idx.name.clone(), crate::cluster::state::IndexMetadata {
            name: idx.name.clone(),
            number_of_shards: idx.number_of_shards,
            shards,
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
        let ni = proto_to_node_info(&node_info);
        info!("gRPC: join request from node {}", ni.id);
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
        match engine.add_document(&doc_id, payload) {
            Ok(id) => Ok(Response::new(ShardDocResponse {
                success: true,
                doc_id: id,
                error: String::new(),
            })),
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
        match engine.bulk_add_documents(docs) {
            Ok(ids) => Ok(Response::new(ShardBulkResponse {
                success: true,
                doc_ids: ids,
                error: String::new(),
            })),
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
            Ok(deleted) => Ok(Response::new(ShardDeleteResponse {
                success: true,
                deleted,
                error: String::new(),
            })),
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

        match engine.search_query(&search_req) {
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
) -> InternalTransportServer<TransportService> {
    let service = TransportService {
        cluster_manager,
        shard_manager,
    };
    InternalTransportServer::new(service)
}
