//! gRPC transport client — connects to remote nodes via the InternalTransport service.

use crate::cluster::state::{ClusterState, NodeInfo};
use crate::transport::proto::internal_transport_client::InternalTransportClient;
use crate::transport::proto::*;
use crate::transport::server::{cluster_state_to_proto, proto_to_cluster_state};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct TransportClient {
    timeout: Duration,
    /// Cached gRPC channels keyed by "host:port".
    /// Uses RwLock for concurrent reads (cache hits) — only blocks on writes (cache misses).
    /// Tonic channels handle HTTP/2 multiplexing and reconnection internally.
    channels: Arc<RwLock<HashMap<String, Channel>>>,
}

impl Default for TransportClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportClient {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Connect to a remote node's gRPC transport endpoint, reusing cached channels.
    async fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<InternalTransportClient<Channel>, tonic::transport::Error> {
        let key = format!("{}:{}", host, port);

        // Fast path: read lock for cache hit (concurrent, non-blocking)
        {
            let cache = self.channels.read().unwrap_or_else(|e| e.into_inner());
            if let Some(channel) = cache.get(&key) {
                return Ok(InternalTransportClient::new(channel.clone()));
            }
        }

        // Slow path: create new channel, then write lock to cache it
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}:{}", host, port))
            .expect("valid endpoint URI")
            .timeout(self.timeout)
            .connect_timeout(self.timeout);
        let channel = endpoint.connect().await?;

        {
            let mut cache = self.channels.write().unwrap_or_else(|e| e.into_inner());
            cache.insert(key, channel.clone());
        }

        Ok(InternalTransportClient::new(channel))
    }

    /// Attempts to join the cluster by contacting the seed hosts.
    /// `raft_node_id` is sent to the leader so it can add this node to Raft membership.
    pub async fn join_cluster(
        &self,
        seed_hosts: &[String],
        local_node: &NodeInfo,
        raft_node_id: u64,
    ) -> Option<ClusterState> {
        for host in seed_hosts {
            debug!("Attempting to join cluster via seed host: {}", host);

            // Parse "host:port" format
            let (h, p) = match host.rsplit_once(':') {
                Some((h, p)) => (h, p.parse::<u16>().unwrap_or(9300)),
                None => (host.as_str(), 9300u16),
            };

            match self.connect(h, p).await {
                Ok(mut client) => {
                    let proto_node = node_info_to_proto(local_node);
                    let request = tonic::Request::new(JoinRequest {
                        node_info: Some(proto_node),
                        raft_node_id,
                    });
                    match client.join_cluster(request).await {
                        Ok(response) => {
                            if let Some(state) = response.into_inner().state {
                                let cs = proto_to_cluster_state(&state);
                                info!("Successfully joined cluster via {}", host);
                                return Some(cs);
                            }
                        }
                        Err(e) => debug!("Join RPC to {} failed: {}", host, e),
                    }
                }
                Err(e) => debug!("Failed to connect to seed {}: {}", host, e),
            }
        }

        error!("Could not join cluster; no seed hosts responded affirmatively.");
        None
    }

    /// (Master Only) Broadcasts state to all active nodes
    pub async fn publish_state(&self, state: &ClusterState) {
        let proto_state = cluster_state_to_proto(state);
        for node in state.nodes.values() {
            match self.connect(&node.host, node.transport_port).await {
                Ok(mut client) => {
                    let request = tonic::Request::new(PublishStateRequest {
                        state: Some(proto_state.clone()),
                    });
                    if let Err(e) = client.publish_state(request).await {
                        error!("Failed to publish state to node {}: {}", node.id, e);
                    }
                }
                Err(e) => error!(
                    "Failed to connect to node {} for state publish: {}",
                    node.id, e
                ),
            }
        }
    }

    /// Forward a single document to a specific shard on a node
    pub async fn forward_index_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardDocRequest {
            index_name: index_name.to_string(),
            shard_id,
            payload_json: serde_json::to_vec(payload)?,
            doc_id: doc_id.to_string(),
        });
        let response = client.index_doc(request).await?.into_inner();
        if response.success {
            Ok(serde_json::json!({
                "_index": index_name,
                "_id": response.doc_id,
                "_shard": shard_id,
                "result": "created"
            }))
        } else {
            Ok(serde_json::json!({ "error": response.error }))
        }
    }

    /// Forward a bulk batch to a specific shard on a node
    pub async fn forward_bulk_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        docs: &[(String, serde_json::Value)],
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let documents_json: Vec<Vec<u8>> = docs
            .iter()
            .map(|(id, payload)| {
                serde_json::to_vec(&serde_json::json!({
                    "_doc_id": id,
                    "_source": payload
                }))
                .unwrap_or_default()
            })
            .collect();
        let request = tonic::Request::new(ShardBulkRequest {
            index_name: index_name.to_string(),
            shard_id,
            documents_json,
        });
        let response = client.bulk_index(request).await?.into_inner();
        if response.success {
            Ok(serde_json::json!({
                "took": 0,
                "errors": false,
                "items": response.doc_ids.iter().map(|id| serde_json::json!({ "index": { "_id": id, "result": "created" } })).collect::<Vec<_>>()
            }))
        } else {
            Ok(serde_json::json!({ "error": response.error }))
        }
    }

    /// Forward a delete operation to a specific shard on a node
    pub async fn forward_delete_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardDeleteRequest {
            index_name: index_name.to_string(),
            shard_id,
            doc_id: doc_id.to_string(),
        });
        let response = client.delete_doc(request).await?.into_inner();
        if response.success {
            Ok(serde_json::json!({
                "_index": index_name,
                "_id": doc_id,
                "_shard": shard_id,
                "result": "deleted"
            }))
        } else {
            Err(anyhow::anyhow!("Delete failed: {}", response.error))
        }
    }

    /// Forward a get-by-ID request to a specific shard on a node
    pub async fn forward_get_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
    ) -> Result<Option<serde_json::Value>, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardGetRequest {
            index_name: index_name.to_string(),
            shard_id,
            doc_id: doc_id.to_string(),
        });
        let response = client.get_doc(request).await?.into_inner();
        if response.found {
            let source: serde_json::Value = serde_json::from_slice(&response.source_json)?;
            Ok(Some(source))
        } else if !response.error.is_empty() {
            Err(anyhow::anyhow!("Get failed: {}", response.error))
        } else {
            Ok(None)
        }
    }

    /// Forward a query-string search to a specific shard on a remote node
    pub async fn forward_search_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        query: &str,
    ) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardSearchRequest {
            index_name: index_name.to_string(),
            shard_id,
            query: query.to_string(),
        });
        let response = client.search_shard(request).await?.into_inner();
        if response.success {
            Ok(response
                .hits
                .iter()
                .filter_map(|h| serde_json::from_slice(&h.source_json).ok())
                .collect())
        } else {
            Err(anyhow::anyhow!("Shard search failed: {}", response.error))
        }
    }

    /// Forward a DSL search to a specific shard
    pub async fn forward_search_dsl_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        req: &crate::search::SearchRequest,
    ) -> Result<Vec<serde_json::Value>, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardSearchDslRequest {
            index_name: index_name.to_string(),
            shard_id,
            search_request_json: serde_json::to_vec(req)?,
        });
        let response = client.search_shard_dsl(request).await?.into_inner();
        if response.success {
            Ok(response
                .hits
                .iter()
                .filter_map(|h| serde_json::from_slice(&h.source_json).ok())
                .collect())
        } else {
            Err(anyhow::anyhow!(
                "Shard DSL search failed: {}",
                response.error
            ))
        }
    }

    /// Sends a heartbeat ping to another node
    pub async fn send_ping(
        &self,
        target_node: &NodeInfo,
        local_node_id: &str,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&target_node.host, target_node.transport_port)
            .await?;
        let request = tonic::Request::new(PingRequest {
            source_node_id: local_node_id.to_string(),
        });
        client.ping(request).await?;
        Ok(())
    }

    /// Replicate a single document operation to a replica shard on a remote node.
    pub async fn replicate_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        payload: &serde_json::Value,
        op: &str,
        seq_no: u64,
    ) -> Result<u64, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ReplicateDocRequest {
            index_name: index_name.to_string(),
            shard_id,
            doc_id: doc_id.to_string(),
            payload_json: serde_json::to_vec(payload)?,
            op: op.to_string(),
            seq_no,
        });
        let response = client.replicate_doc(request).await?.into_inner();
        if response.success {
            Ok(response.local_checkpoint)
        } else {
            Err(anyhow::anyhow!("Replication failed: {}", response.error))
        }
    }

    /// Replicate a bulk set of document operations to a replica shard on a remote node.
    pub async fn replicate_bulk_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        docs: &[(String, serde_json::Value)],
        start_seq_no: u64,
    ) -> Result<u64, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let ops: Vec<ReplicateDocRequest> = docs
            .iter()
            .enumerate()
            .map(|(i, (id, payload))| ReplicateDocRequest {
                index_name: index_name.to_string(),
                shard_id,
                doc_id: id.clone(),
                payload_json: serde_json::to_vec(payload).unwrap_or_default(),
                op: "index".to_string(),
                seq_no: start_seq_no + i as u64,
            })
            .collect();
        let request = tonic::Request::new(ReplicateBulkRequest {
            index_name: index_name.to_string(),
            shard_id,
            ops,
        });
        let response = client.replicate_bulk(request).await?.into_inner();
        if response.success {
            Ok(response.local_checkpoint)
        } else {
            Err(anyhow::anyhow!(
                "Bulk replication failed: {}",
                response.error
            ))
        }
    }

    /// Request recovery from the primary: send our local checkpoint,
    /// primary returns translog entries for replay.
    pub async fn request_recovery(
        &self,
        primary_node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        local_checkpoint: u64,
    ) -> Result<RecoveryResult, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let request = tonic::Request::new(RecoverReplicaRequest {
            index_name: index_name.to_string(),
            shard_id,
            local_checkpoint,
        });
        let response = client.recover_replica(request).await?.into_inner();
        if response.success {
            Ok(RecoveryResult {
                ops_replayed: response.ops_replayed,
                primary_checkpoint: response.primary_checkpoint,
                operations: response.operations,
            })
        } else {
            Err(anyhow::anyhow!("Recovery failed: {}", response.error))
        }
    }
}

/// Result of a recovery request from the primary.
pub struct RecoveryResult {
    pub ops_replayed: u64,
    pub primary_checkpoint: u64,
    pub operations: Vec<RecoverReplicaOp>,
}

// ─── Helper to convert domain NodeInfo → proto NodeInfo ─────────────────────

fn node_info_to_proto(n: &NodeInfo) -> crate::transport::proto::NodeInfo {
    crate::transport::proto::NodeInfo {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_client_has_empty_cache() {
        let client = TransportClient::new();
        let cache = client.channels.read().unwrap();
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn cloned_client_shares_cache() {
        let client = TransportClient::new();
        let client2 = client.clone();

        // Insert a dummy entry via the first client (write lock)
        {
            let mut cache = client.channels.write().unwrap();
            let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
            cache.insert("127.0.0.1:1".into(), endpoint.connect_lazy());
        }

        // The clone should see it (read lock — concurrent)
        let cache2 = client2.channels.read().unwrap();
        assert!(
            cache2.contains_key("127.0.0.1:1"),
            "cloned client must share the channel cache"
        );
    }

    #[tokio::test]
    async fn connect_caches_channel_on_success() {
        // We can't test a real connection without a running server,
        // but we can verify the cache is populated after the replication
        // integration tests run (they start real gRPC servers).
        // Here we just verify the structure works.
        let client = TransportClient::new();

        // Attempting to connect to a non-existent server should fail
        let result = client.connect("127.0.0.1", 1).await;
        assert!(result.is_err(), "connecting to a closed port should fail");

        // Cache should NOT contain the failed connection
        let cache = client.channels.read().unwrap();
        assert!(
            !cache.contains_key("127.0.0.1:1"),
            "failed connections should not be cached"
        );
    }
}
