//! gRPC transport client — connects to remote nodes via the InternalTransport service.

use crate::cluster::state::{ClusterState, NodeInfo};
use crate::transport::proto::internal_transport_client::InternalTransportClient;
use crate::transport::proto::*;
use crate::transport::server::proto_to_cluster_state;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct TransportClient {
    timeout: Duration,
    /// Cached gRPC channels keyed by "host:port".
    /// Uses RwLock for concurrent reads (cache hits) — only blocks on writes (cache misses).
    /// Tonic channels handle HTTP/2 multiplexing and reconnection internally.
    channels: Arc<RwLock<HashMap<String, Channel>>>,
    /// Optional TLS endpoint configurator. When set, `connect()` uses https and
    /// applies TLS settings. Populated by `with_tls()` (transport-tls feature only).
    tls_connector: Option<Arc<dyn TlsConnector>>,
}

/// Trait to abstract TLS configuration behind the feature flag.
/// The concrete implementation lives in transport/mod.rs behind #[cfg(feature = "transport-tls")].
pub trait TlsConnector: Send + Sync {
    fn configure_endpoint(
        &self,
        endpoint: tonic::transport::Endpoint,
    ) -> Result<tonic::transport::Endpoint, tonic::transport::Error>;
}

pub struct SqlBatchStream {
    first_batch: Option<RecordBatch>,
    total_hits: usize,
    collected_rows: usize,
    streaming_used: bool,
    inner: tonic::Streaming<SqlRecordBatchResponse>,
}

impl SqlBatchStream {
    pub fn total_hits(&self) -> usize {
        self.total_hits
    }

    pub fn collected_rows(&self) -> usize {
        self.collected_rows
    }

    pub fn streaming_used(&self) -> bool {
        self.streaming_used
    }

    pub fn into_stream(
        self,
    ) -> impl futures::Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static {
        futures::stream::try_unfold(
            (
                self.first_batch,
                self.inner,
                self.total_hits,
                self.collected_rows,
                self.streaming_used,
            ),
            |(first_batch, mut inner, total_hits, collected_rows, streaming_used)| async move {
                if let Some(batch) = first_batch {
                    return Ok(Some((
                        batch,
                        (None, inner, total_hits, collected_rows, streaming_used),
                    )));
                }

                match inner.message().await? {
                    Some(response) => {
                        let decoded = decode_sql_batch_response(response)?;
                        if decoded.total_hits != total_hits {
                            return Err(anyhow::anyhow!(
                                "Shard SQL batch stream returned inconsistent hit counts: {} vs {}",
                                total_hits,
                                decoded.total_hits
                            ));
                        }
                        if decoded.collected_rows != collected_rows {
                            return Err(anyhow::anyhow!(
                                "Shard SQL batch stream returned inconsistent collected row counts: {} vs {}",
                                collected_rows,
                                decoded.collected_rows
                            ));
                        }
                        if decoded.streaming_used != streaming_used {
                            return Err(anyhow::anyhow!(
                                "Shard SQL batch stream returned inconsistent streaming flags: {} vs {}",
                                streaming_used,
                                decoded.streaming_used
                            ));
                        }
                        Ok(Some((
                            decoded.batch,
                            (None, inner, total_hits, collected_rows, streaming_used),
                        )))
                    }
                    None => Ok(None),
                }
            },
        )
    }
}

impl Default for TransportClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportClient {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            channels: Arc::new(RwLock::new(HashMap::new())),
            tls_connector: None,
        }
    }

    /// Create a transport client with a TLS connector for encrypted inter-node communication.
    pub fn with_tls_connector(connector: Arc<dyn TlsConnector>) -> Self {
        Self {
            timeout: Duration::from_secs(30),
            channels: Arc::new(RwLock::new(HashMap::new())),
            tls_connector: Some(connector),
        }
    }

    /// Connect to a remote node's gRPC transport endpoint, reusing cached channels.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<InternalTransportClient<Channel>, tonic::transport::Error> {
        let key = format!("{}:{}", host, port);

        // Fast path: read lock for cache hit (concurrent, non-blocking)
        {
            let cache = self.channels.read().unwrap_or_else(|e| e.into_inner());
            if let Some(channel) = cache.get(&key) {
                return Ok(InternalTransportClient::new(channel.clone())
                    .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE));
            }
        }

        // Slow path: create new channel, then write lock to cache it
        let scheme = if self.tls_connector.is_some() {
            "https"
        } else {
            "http"
        };
        let mut endpoint =
            tonic::transport::Endpoint::from_shared(format!("{}://{}:{}", scheme, host, port))?
                .timeout(self.timeout)
                .connect_timeout(Duration::from_secs(5));

        if let Some(ref connector) = self.tls_connector {
            endpoint = connector.configure_endpoint(endpoint)?;
        }

        let channel = endpoint.connect().await?;

        {
            let mut cache = self.channels.write().unwrap_or_else(|e| e.into_inner());
            cache.insert(key, channel.clone());
        }

        Ok(InternalTransportClient::new(channel)
            .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE))
    }

    /// Attempts to join the cluster by contacting the seed hosts.
    /// `raft_node_id` is sent to the leader so it can add this node to Raft membership.
    pub async fn join_cluster(
        &self,
        seed_hosts: &[String],
        local_node: &NodeInfo,
        raft_node_id: u64,
    ) -> Option<ClusterState> {
        // Build self address to skip sending join requests to ourselves
        let self_addr = format!("{}:{}", local_node.host, local_node.transport_port);

        for host in seed_hosts {
            // Skip self — sending a join request to ourselves is pointless
            if host == &self_addr {
                continue;
            }
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
                                match proto_to_cluster_state(&state) {
                                    Ok(cs) => {
                                        info!("Successfully joined cluster via {}", host);
                                        return Some(cs);
                                    }
                                    Err(e) => {
                                        error!(
                                            "Join response from {} contained invalid cluster snapshot: {}",
                                            host, e
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => debug!("Join RPC to {} failed: {}", host, e),
                    }
                }
                Err(e) => debug!("Failed to connect to seed {}: {}", host, e),
            }
        }

        warn!("Could not join cluster; no seed hosts responded affirmatively.");
        None
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
            Err(anyhow::anyhow!("Shard index failed: {}", response.error))
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
                // Write directly to buffer — avoids creating intermediate serde_json::Value
                let mut buf = Vec::with_capacity(128 + id.len());
                buf.extend_from_slice(b"{\"_doc_id\":");
                serde_json::to_writer(&mut buf, id)?;
                buf.extend_from_slice(b",\"_source\":");
                serde_json::to_writer(&mut buf, payload)?;
                buf.push(b'}');
                Ok::<Vec<u8>, serde_json::Error>(buf)
            })
            .collect::<Result<_, _>>()?;
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
            Err(anyhow::anyhow!(
                "Shard bulk index failed: {}",
                response.error
            ))
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
            decode_search_hits(&response.hits)
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
    ) -> Result<
        (
            Vec<serde_json::Value>,
            usize,
            std::collections::HashMap<String, crate::search::PartialAggResult>,
        ),
        anyhow::Error,
    > {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(ShardSearchDslRequest {
            index_name: index_name.to_string(),
            shard_id,
            search_request_json: serde_json::to_vec(req)?,
        });
        let response = client.search_shard_dsl(request).await?.into_inner();
        if response.success {
            let hits = decode_search_hits(&response.hits)?;
            let partial_aggs = if response.partial_aggs_json.is_empty() {
                std::collections::HashMap::new()
            } else {
                crate::search::decode_partial_aggs(&response.partial_aggs_json)?
            };
            Ok((hits, response.total_hits as usize, partial_aggs))
        } else {
            Err(anyhow::anyhow!(
                "Shard DSL search failed: {}",
                response.error
            ))
        }
    }

    /// Forward a SQL RecordBatch request to a specific shard (returns Arrow IPC)
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_sql_batch_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<(datafusion::arrow::record_batch::RecordBatch, usize), anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(SqlRecordBatchRequest {
            index_name: index_name.to_string(),
            shard_id,
            search_request_json: serde_json::to_vec(req)?,
            columns: columns.to_vec(),
            needs_id,
            needs_score,
            batch_size: 0,
        });
        let response = client.sql_record_batch(request).await?.into_inner();
        let decoded = decode_sql_batch_response(response)?;
        Ok((decoded.batch, decoded.total_hits))
    }

    /// Forward a streaming SQL RecordBatch request to a specific shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward_sql_batch_stream_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<(Vec<RecordBatch>, usize, bool), anyhow::Error> {
        let stream = self
            .open_sql_batch_stream_to_shard(
                node,
                index_name,
                shard_id,
                req,
                columns,
                needs_id,
                needs_score,
                batch_size,
            )
            .await?;
        let hits = stream.total_hits();
        let streaming_used = stream.streaming_used();
        let batches = stream.into_stream().try_collect().await?;
        Ok((batches, hits, streaming_used))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_sql_batch_stream_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<SqlBatchStream, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = tonic::Request::new(SqlRecordBatchRequest {
            index_name: index_name.to_string(),
            shard_id,
            search_request_json: serde_json::to_vec(req)?,
            columns: columns.to_vec(),
            needs_id,
            needs_score,
            batch_size: batch_size as u32,
        });
        let mut stream = client.sql_record_batch_stream(request).await?.into_inner();
        let Some(first_response) = stream.message().await? else {
            return Err(anyhow::anyhow!(
                "Shard SQL batch stream returned no batches"
            ));
        };
        let decoded = decode_sql_batch_response(first_response)?;
        Ok(SqlBatchStream {
            first_batch: Some(decoded.batch),
            total_hits: decoded.total_hits,
            collected_rows: decoded.collected_rows,
            streaming_used: decoded.streaming_used,
            inner: stream,
        })
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
    #[allow(clippy::too_many_arguments)]
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
            .map(|(i, (id, payload))| {
                Ok::<ReplicateDocRequest, serde_json::Error>(ReplicateDocRequest {
                    index_name: index_name.to_string(),
                    shard_id,
                    doc_id: id.clone(),
                    payload_json: serde_json::to_vec(payload)?,
                    op: "index".to_string(),
                    seq_no: start_seq_no + i as u64,
                })
            })
            .collect::<Result<_, _>>()?;
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

    /// Forward a settings update to the master node via gRPC.
    /// The master applies the changes via Raft.
    pub async fn forward_update_settings(
        &self,
        master: &NodeInfo,
        index_name: &str,
        settings_body: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {}", e))?;
        let settings_json = serde_json::to_vec(settings_body)?;
        let request = tonic::Request::new(UpdateSettingsRequest {
            index_name: index_name.to_string(),
            settings_json,
        });
        let resp = client
            .update_settings(request)
            .await
            .map_err(|e| anyhow::anyhow!("UpdateSettings RPC: {}", e))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward an index creation request to the master node via gRPC.
    /// The master parses the body, builds metadata, and commits via Raft.
    pub async fn forward_create_index(
        &self,
        master: &NodeInfo,
        index_name: &str,
        body: &[u8],
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {}", e))?;
        let request = tonic::Request::new(CreateIndexRequest {
            index_name: index_name.to_string(),
            body_json: body.to_vec(),
        });
        let resp = client
            .create_index(request)
            .await
            .map_err(|e| anyhow::anyhow!("CreateIndex RPC: {}", e))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        let response: serde_json::Value = serde_json::from_slice(&inner.response_json)
            .unwrap_or(serde_json::json!({"acknowledged": inner.acknowledged}));
        Ok(response)
    }

    /// Forward an index deletion request to the master node via gRPC.
    /// The master commits the deletion via Raft and cleans up local shards.
    pub async fn forward_delete_index(
        &self,
        master: &NodeInfo,
        index_name: &str,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {}", e))?;
        let request = tonic::Request::new(DeleteIndexRequest {
            index_name: index_name.to_string(),
        });
        let resp = client
            .delete_index(request)
            .await
            .map_err(|e| anyhow::anyhow!("DeleteIndex RPC: {}", e))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward a transfer-master request to the current master via gRPC.
    pub async fn forward_transfer_master(
        &self,
        master: &NodeInfo,
        target_node_id: &str,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {}", e))?;
        let request = tonic::Request::new(TransferMasterRequest {
            target_node_id: target_node_id.to_string(),
        });
        let resp = client
            .transfer_master(request)
            .await
            .map_err(|e| anyhow::anyhow!("TransferMaster RPC: {}", e))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward an AddMappings request to the Raft leader.
    pub async fn forward_add_mappings(
        &self,
        master: &NodeInfo,
        index_name: &str,
        new_fields: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
        dynamic: &crate::cluster::state::DynamicMapping,
    ) -> Result<(), anyhow::Error> {
        use crate::transport::server::conversions::field_type_to_proto;
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master for AddMappings: {}", e))?;

        let entries: Vec<FieldMappingEntry> = new_fields
            .iter()
            .map(|(name, mapping)| FieldMappingEntry {
                name: name.clone(),
                field_type: field_type_to_proto(&mapping.field_type),
                dimension: mapping.dimension.map(|d| d as u32),
            })
            .collect();

        let request = tonic::Request::new(AddMappingsRequest {
            index_name: index_name.to_string(),
            new_fields: entries,
            dynamic: dynamic.to_string(),
        });
        let resp = client
            .add_mappings(request)
            .await
            .map_err(|e| anyhow::anyhow!("AddMappings RPC: {}", e))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Fetch shard doc counts from a remote node.
    /// Returns a map of (index_name, shard_id) → doc_count.
    pub async fn get_shard_stats(
        &self,
        node: &NodeInfo,
    ) -> Result<HashMap<(String, u32), u64>, anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let resp = client
            .get_shard_stats(tonic::Request::new(ShardStatsRequest {}))
            .await
            .map_err(|e| anyhow::anyhow!("GetShardStats RPC to {}: {}", node.id, e))?;
        let inner = resp.into_inner();
        let map = inner
            .shards
            .into_iter()
            .map(|s| ((s.index_name, s.shard_id), s.doc_count))
            .collect();
        Ok(map)
    }

    /// Fetch per-segment stats from a remote node.
    /// Returns a list of (index_name, shard_id, segment_info) tuples.
    pub async fn get_segment_stats(
        &self,
        node: &NodeInfo,
    ) -> Result<Vec<(String, u32, crate::engine::SegmentInfo)>, anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let resp = client
            .get_segment_stats(tonic::Request::new(SegmentStatsRequest {}))
            .await
            .map_err(|e| anyhow::anyhow!("GetSegmentStats RPC to {}: {}", node.id, e))?;
        let inner = resp.into_inner();
        Ok(inner
            .segments
            .into_iter()
            .map(|segment| {
                (
                    segment.index_name,
                    segment.shard_id,
                    crate::engine::SegmentInfo {
                        segment_id: segment.segment_id,
                        num_docs: segment.num_docs as u32,
                        deleted_docs: segment.deleted_docs as u32,
                    },
                )
            })
            .collect())
    }

    /// Fan out a refresh request to a remote node for a specific index.
    pub async fn forward_refresh(
        &self,
        node: &NodeInfo,
        index_name: &str,
    ) -> Result<(u32, u32), anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let resp = client
            .refresh_index(tonic::Request::new(IndexMaintenanceRequest {
                index_name: index_name.to_string(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("RefreshIndex RPC to {}: {}", node.id, e))?;
        let inner = resp.into_inner();
        Ok((inner.successful_shards, inner.failed_shards))
    }

    /// Fan out a flush request to a remote node for a specific index.
    pub async fn forward_flush(
        &self,
        node: &NodeInfo,
        index_name: &str,
    ) -> Result<(u32, u32), anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let resp = client
            .flush_index(tonic::Request::new(IndexMaintenanceRequest {
                index_name: index_name.to_string(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("FlushIndex RPC to {}: {}", node.id, e))?;
        let inner = resp.into_inner();
        Ok((inner.successful_shards, inner.failed_shards))
    }

    /// Fan out a force-merge request to a remote node for a specific index.
    pub async fn forward_force_merge(
        &self,
        node: &NodeInfo,
        index_name: &str,
        max_num_segments: u32,
    ) -> Result<String, anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let resp = client
            .force_merge_index(tonic::Request::new(ForceMergeRequest {
                index_name: index_name.to_string(),
                max_num_segments,
            }))
            .await
            .map_err(|e| anyhow::anyhow!("ForceMergeIndex RPC to {}: {}", node.id, e))?;
        Ok(resp.into_inner().task_id)
    }

    pub async fn get_task_status(
        &self,
        node: &NodeInfo,
        task_id: &str,
    ) -> Result<Option<crate::tasks::LocalForceMergeTaskSnapshot>, anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let response = client
            .get_task_status(tonic::Request::new(GetTaskStatusRequest {
                task_id: task_id.to_string(),
            }))
            .await
            .map_err(|e| anyhow::anyhow!("GetTaskStatus RPC to {}: {}", node.id, e))?
            .into_inner();

        if !response.found {
            return Ok(None);
        }

        Ok(Some(crate::tasks::LocalForceMergeTaskSnapshot {
            task_id: response.task_id,
            action: response.action,
            node_id: response.node_id,
            index_name: response.index_name,
            max_num_segments: response.max_num_segments as usize,
            status: crate::tasks::TaskStatus::from_wire(&response.status),
            created_at_epoch_ms: response.created_at_epoch_ms,
            started_at_epoch_ms: (response.started_at_epoch_ms != 0)
                .then_some(response.started_at_epoch_ms),
            completed_at_epoch_ms: (response.completed_at_epoch_ms != 0)
                .then_some(response.completed_at_epoch_ms),
            successful_shards: response.successful_shards,
            failed_shards: response.failed_shards,
            error: (!response.error.is_empty()).then_some(response.error),
        }))
    }
}

#[derive(Debug)]
struct DecodedSqlBatchResponse {
    batch: RecordBatch,
    total_hits: usize,
    collected_rows: usize,
    streaming_used: bool,
}

fn decode_sql_batch_response(
    response: SqlRecordBatchResponse,
) -> Result<DecodedSqlBatchResponse, anyhow::Error> {
    if !response.success {
        return Err(anyhow::anyhow!(
            "Shard SQL batch failed: {}",
            response.error
        ));
    }

    let batch = crate::hybrid::arrow_bridge::record_batch_from_ipc(&response.arrow_ipc)?;
    Ok(DecodedSqlBatchResponse {
        batch,
        total_hits: response.total_hits as usize,
        collected_rows: response.collected_rows as usize,
        streaming_used: response.streaming_used,
    })
}

fn decode_search_hits(hits: &[SearchHit]) -> Result<Vec<serde_json::Value>, anyhow::Error> {
    hits.iter()
        .map(|hit| serde_json::from_slice(&hit.source_json).map_err(anyhow::Error::from))
        .collect()
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
        raft_node_id: n.raft_node_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn decode_sql_batch_response_round_trips_arrow_ipc() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3_i64, 7_i64]))])
                .expect("record batch");

        let response = SqlRecordBatchResponse {
            success: true,
            arrow_ipc: crate::hybrid::arrow_bridge::record_batch_to_ipc(&batch)
                .expect("encode batch"),
            total_hits: 9,
            error: String::new(),
            collected_rows: 2,
            streaming_used: true,
        };

        let decoded = decode_sql_batch_response(response).expect("decode batch");
        assert_eq!(decoded.total_hits, 9);
        assert_eq!(decoded.collected_rows, 2);
        assert!(decoded.streaming_used);
        assert_eq!(decoded.batch.num_rows(), 2);
        let values = decoded
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(values.value(0), 3);
        assert_eq!(values.value(1), 7);
    }

    #[test]
    fn decode_sql_batch_response_preserves_non_stream_metadata() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![11_i64]))])
            .expect("record batch");

        let response = SqlRecordBatchResponse {
            success: true,
            arrow_ipc: crate::hybrid::arrow_bridge::record_batch_to_ipc(&batch)
                .expect("encode batch"),
            total_hits: 5,
            error: String::new(),
            collected_rows: 1,
            streaming_used: false,
        };

        let decoded = decode_sql_batch_response(response).expect("decode batch");
        assert_eq!(decoded.total_hits, 5);
        assert_eq!(decoded.collected_rows, 1);
        assert!(!decoded.streaming_used);
        assert_eq!(decoded.batch.num_rows(), 1);
    }

    #[test]
    fn decode_sql_batch_response_returns_rpc_error() {
        let err = match decode_sql_batch_response(SqlRecordBatchResponse {
            success: false,
            arrow_ipc: vec![],
            total_hits: 0,
            error: "boom".to_string(),
            collected_rows: 0,
            streaming_used: false,
        }) {
            Ok(_) => panic!("unsuccessful response should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("boom"));
    }

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

    #[tokio::test]
    async fn forward_create_index_unreachable_master_returns_error() {
        let client = TransportClient::new();
        let master = NodeInfo {
            id: "master".into(),
            name: "master".into(),
            host: "127.0.0.1".into(),
            transport_port: 1, // unreachable
            http_port: 9200,
            roles: vec![],
            raft_node_id: 1,
        };
        let result = client
            .forward_create_index(&master, "test-idx", b"{}")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn forward_delete_index_unreachable_master_returns_error() {
        let client = TransportClient::new();
        let master = NodeInfo {
            id: "master".into(),
            name: "master".into(),
            host: "127.0.0.1".into(),
            transport_port: 1, // unreachable
            http_port: 9200,
            roles: vec![],
            raft_node_id: 1,
        };
        let result = client.forward_delete_index(&master, "test-idx").await;
        assert!(result.is_err());
    }

    #[test]
    fn decode_search_hits_returns_error_on_malformed_payload() {
        let err = decode_search_hits(&[SearchHit {
            source_json: b"not-json".to_vec(),
        }])
        .expect_err("malformed payload should fail");

        let message = err.to_string();
        assert!(message.contains("expected") || message.contains("EOF"));
    }

    #[tokio::test]
    async fn forward_transfer_master_unreachable_returns_error() {
        let client = TransportClient::new();
        let master = NodeInfo {
            id: "master".into(),
            name: "master".into(),
            host: "127.0.0.1".into(),
            transport_port: 1, // unreachable
            http_port: 9200,
            roles: vec![],
            raft_node_id: 1,
        };
        let result = client.forward_transfer_master(&master, "target-node").await;
        assert!(result.is_err());
    }
}
