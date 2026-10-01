//! gRPC transport client — connects to remote nodes via the InternalTransport service.

use crate::cluster::state::{ClusterState, NodeInfo};
use crate::transport::proto::internal_transport_client::InternalTransportClient;
use crate::transport::proto::*;
use crate::transport::server::proto_to_cluster_state;
use crate::transport::state_wait::{AppliedStateInterceptor, decode_state_version};
use anyhow::Context;
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

pub type ConnectedTransportClient = InternalTransportClient<
    tonic::service::interceptor::InterceptedService<Channel, AppliedStateInterceptor>,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaApplyResponse {
    pub processed_checkpoint: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
    pub operation_processed: bool,
    pub operation_persisted: bool,
}

pub struct ShardDocumentRead {
    pub index_uuid: String,
    pub document: Option<crate::engine::DocumentRead>,
}

#[derive(Clone)]
pub struct TransportClient {
    timeout: Duration,
    cluster_manager: Option<Arc<crate::cluster::ClusterManager>>,
    acknowledged_versions: Arc<RwLock<HashMap<String, u64>>>,
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
            cluster_manager: None,
            acknowledged_versions: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(RwLock::new(HashMap::new())),
            tls_connector: None,
        }
    }

    /// Create a transport client with a TLS connector for encrypted inter-node communication.
    pub fn with_tls_connector(connector: Arc<dyn TlsConnector>) -> Self {
        Self {
            timeout: Duration::from_secs(30),
            cluster_manager: None,
            acknowledged_versions: Arc::new(RwLock::new(HashMap::new())),
            channels: Arc::new(RwLock::new(HashMap::new())),
            tls_connector: Some(connector),
        }
    }

    pub fn with_cluster_manager(
        mut self,
        cluster_manager: Arc<crate::cluster::ClusterManager>,
    ) -> Self {
        self.cluster_manager = Some(cluster_manager);
        self
    }

    pub(crate) fn forwarding_request<T>(
        &self,
        message: T,
        index_name: &str,
        shard: Option<(u32, &str)>,
    ) -> tonic::Request<T> {
        use crate::transport::state_wait::*;
        let mut request = crate::transport::request_with_cluster_state_version(message, 0);
        request.metadata_mut().insert_bin(
            INDEX_NAME_HEADER,
            tonic::metadata::MetadataValue::from_bytes(index_name.as_bytes()),
        );
        request.metadata_mut().insert(
            INDEX_STATE_FLOOR_HEADER,
            self.required_state_version(index_name)
                .to_string()
                .parse()
                .expect("u64 is valid ASCII metadata"),
        );
        if let Some(manager) = &self.cluster_manager {
            manager.with_state(|state| {
                request.metadata_mut().insert(
                    STATE_VERSION_HEADER,
                    state
                        .version
                        .to_string()
                        .parse()
                        .expect("u64 is valid ASCII metadata"),
                );
                if let Some(metadata) = state.indices.get(index_name) {
                    request.metadata_mut().insert_bin(
                        INDEX_UUID_HEADER,
                        tonic::metadata::MetadataValue::from_bytes(
                            metadata.uuid.as_str().as_bytes(),
                        ),
                    );
                    if let Some(routing) = shard.and_then(|(id, _)| metadata.shard_routing.get(&id))
                    {
                        request.metadata_mut().insert(
                            PRIMARY_TERM_HEADER,
                            routing
                                .primary_term
                                .to_string()
                                .parse()
                                .expect("u64 is valid ASCII metadata"),
                        );
                    }
                    if let Some(allocation) =
                        shard.and_then(|(id, node)| state.shard_allocation_id(index_name, id, node))
                    {
                        request.metadata_mut().insert(
                            ALLOCATION_ID_HEADER,
                            allocation
                                .to_string()
                                .parse()
                                .expect("u64 is valid ASCII metadata"),
                        );
                    }
                }
            });
        }
        request
    }

    pub(crate) fn required_state_version(&self, index_name: &str) -> u64 {
        self.acknowledged_versions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(index_name)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn acknowledge_state(&self, index_name: &str, version: u64) {
        self.acknowledged_versions
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .entry(index_name.to_string())
            .and_modify(|floor| *floor = (*floor).max(version))
            .or_insert(version);
    }

    fn observe_response_state<T>(
        &self,
        response: &tonic::Response<T>,
        operation: &str,
        index_name: &str,
    ) -> anyhow::Result<u64> {
        let version = decode_state_version(response.metadata())
            .with_context(|| format!("invalid {operation} response cluster state version"))?;
        self.acknowledge_state(index_name, version);
        Ok(version)
    }

    async fn wait_for_response_state<T>(
        &self,
        response: &tonic::Response<T>,
        operation: &str,
        index_name: &str,
    ) -> anyhow::Result<()> {
        let version = self.observe_response_state(response, operation, index_name)?;
        if let Some(manager) = &self.cluster_manager {
            manager.wait_for_version(version).await.map_err(|error| {
                tonic::Status::unavailable(format!(
                    "{}{operation}: {error}",
                    crate::transport::state_wait::STATE_WAIT_STATUS_PREFIX,
                ))
            })?;
        }
        Ok(())
    }

    /// Connect to a remote node's gRPC transport endpoint, reusing cached channels.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<ConnectedTransportClient, tonic::transport::Error> {
        let key = format!("{host}:{port}");

        // Fast path: read lock for cache hit (concurrent, non-blocking)
        {
            let cache = self.channels.read().unwrap_or_else(|e| e.into_inner());
            if let Some(channel) = cache.get(&key) {
                return Ok(InternalTransportClient::with_interceptor(
                    channel.clone(),
                    AppliedStateInterceptor {
                        cluster_manager: self.cluster_manager.clone(),
                    },
                )
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
            tonic::transport::Endpoint::from_shared(format!("{scheme}://{host}:{port}"))?
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

        Ok(InternalTransportClient::with_interceptor(
            channel,
            AppliedStateInterceptor {
                cluster_manager: self.cluster_manager.clone(),
            },
        )
        .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE))
    }

    #[cfg(feature = "protocol-trace")]
    pub fn evict_protocol_trace_channel(&self, host: &str, port: u16) {
        self.channels
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&format!("{host}:{port}"));
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
        self.forward_index_with_condition_to_shard(
            node,
            index_name,
            shard_id,
            doc_id,
            payload,
            crate::engine::WriteCondition::Unconditional,
        )
        .await
    }

    pub async fn forward_index_with_condition_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        payload: &serde_json::Value,
        condition: crate::engine::WriteCondition,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let (if_seq_no, if_primary_term) = condition.expected_version();
        let request = ShardDocRequest {
            index_name: index_name.to_string(),
            shard_id,
            payload_json: serde_json::to_vec(payload)?,
            doc_id: doc_id.to_string(),
            if_seq_no,
            if_primary_term,
            create_only: condition == crate::engine::WriteCondition::Create,
            index_uuid: None,
        };
        self.forward_index_request_to_shard(node, request).await
    }

    pub async fn forward_index_request_to_shard(
        &self,
        node: &NodeInfo,
        request: ShardDocRequest,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let index_name = request.index_name.clone();
        let shard_id = request.shard_id;
        let doc_id = request.doc_id.clone();
        let response = client
            .index_doc(self.forwarding_request(request, &index_name, Some((shard_id, &node.id))))
            .await?
            .into_inner();
        decode_shard_doc_response(&index_name, shard_id, &doc_id, response)
    }

    /// Forward a bulk batch to a specific shard on a node
    pub async fn forward_bulk_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        docs: &[(String, serde_json::Value)],
    ) -> Result<crate::engine::BulkWriteReceipt, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let documents_json: Vec<Vec<u8>> = docs
            .iter()
            .map(|(id, payload)| encode_bulk_document(id, payload))
            .collect::<Result<_, _>>()?;
        let request = self.forwarding_request(
            ShardBulkRequest {
                index_name: index_name.to_string(),
                shard_id,
                documents_json,
                ..Default::default()
            },
            index_name,
            Some((shard_id, &node.id)),
        );
        let response = client.bulk_index(request).await?.into_inner();
        decode_shard_bulk_response(docs, response)
    }

    /// Forward a delete operation to a specific shard on a node
    pub async fn forward_delete_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
    ) -> Result<serde_json::Value, anyhow::Error> {
        self.forward_delete_with_condition_to_shard(
            node,
            index_name,
            shard_id,
            doc_id,
            crate::engine::WriteCondition::Unconditional,
        )
        .await
    }

    pub async fn forward_delete_with_condition_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        condition: crate::engine::WriteCondition,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let (if_seq_no, if_primary_term) = condition.expected_version();
        let request = self.forwarding_request(
            ShardDeleteRequest {
                index_name: index_name.to_string(),
                shard_id,
                doc_id: doc_id.to_string(),
                if_seq_no,
                if_primary_term,
            },
            index_name,
            Some((shard_id, &node.id)),
        );
        let response = client.delete_doc(request).await?.into_inner();
        decode_shard_delete_response(index_name, shard_id, doc_id, response)
    }

    /// Forward a get-by-ID request to a specific shard on a node
    pub async fn forward_get_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        realtime: bool,
    ) -> Result<Option<crate::engine::DocumentRead>, anyhow::Error> {
        Ok(self
            .forward_get_with_index_uuid_to_shard(node, index_name, shard_id, doc_id, realtime)
            .await?
            .document)
    }

    pub async fn forward_get_with_index_uuid_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        doc_id: &str,
        realtime: bool,
    ) -> Result<ShardDocumentRead, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = self.forwarding_request(
            ShardGetRequest {
                index_name: index_name.to_string(),
                shard_id,
                doc_id: doc_id.to_string(),
                realtime: Some(realtime),
            },
            index_name,
            Some((shard_id, &node.id)),
        );
        let response = client.get_doc(request).await?.into_inner();
        if !response.error.is_empty() {
            anyhow::bail!("Get failed: {}", response.error);
        }
        if response.index_uuid.is_empty() {
            anyhow::bail!("shard GET response is missing its index UUID");
        }
        let document = if response.found {
            let source: serde_json::Value = serde_json::from_slice(&response.source_json)?;
            let seq_no = response.seq_no.ok_or_else(|| {
                anyhow::anyhow!("found shard GET response is missing its sequence")
            })?;
            let primary_term = response
                .primary_term
                .filter(|term| *term > 0)
                .ok_or_else(|| {
                    anyhow::anyhow!("found shard GET response is missing its primary term")
                })?;
            Some(crate::engine::DocumentRead {
                source,
                seq_no,
                primary_term,
            })
        } else {
            None
        };
        Ok(ShardDocumentRead {
            index_uuid: response.index_uuid,
            document,
        })
    }

    pub async fn forward_bulk_operations_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        docs: &[(String, serde_json::Value, ShardBulkOperation)],
    ) -> Result<Vec<ShardBulkItemResponse>, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let documents_json = docs
            .iter()
            .map(|(doc_id, source, _)| encode_bulk_document(doc_id, source))
            .collect::<Result<Vec<_>, _>>()?;
        let response = client
            .bulk_index(self.forwarding_request(
                ShardBulkRequest {
                    index_name: index_name.to_string(),
                    shard_id,
                    documents_json,
                    operations: docs.iter().map(|(_, _, operation)| *operation).collect(),
                },
                index_name,
                Some((shard_id, &node.id)),
            ))
            .await?
            .into_inner();
        if !response.success {
            anyhow::bail!("Shard bulk failed: {}", response.error);
        }
        if response.results.len() != docs.len() {
            anyhow::bail!("shard bulk response has inconsistent item count");
        }
        for (item, (doc_id, _, operation)) in response.results.iter().zip(docs) {
            if item.doc_id != *doc_id {
                anyhow::bail!("shard bulk response has inconsistent document identities");
            }
            if item.error.is_empty() {
                let expected = match ShardBulkOpKind::try_from(operation.kind)? {
                    ShardBulkOpKind::Index => matches!(
                        (item.status, item.result.as_str()),
                        (201, "created") | (200, "updated")
                    ),
                    ShardBulkOpKind::Create => item.status == 201 && item.result == "created",
                    ShardBulkOpKind::Delete => matches!(
                        (item.status, item.result.as_str()),
                        (200, "deleted") | (404, "not_found")
                    ),
                };
                if !expected
                    || item.seq_no.is_none()
                    || item.primary_term.is_none_or(|term| term == 0)
                {
                    anyhow::bail!(
                        "shard bulk success has invalid result or missing operation identity"
                    );
                }
            } else if item.status < 400 || item.error_type.is_empty() {
                anyhow::bail!("shard bulk error has invalid status or missing error type");
            }
        }
        Ok(response.results)
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
        let request = self.forwarding_request(
            ShardSearchRequest {
                index_name: index_name.to_string(),
                shard_id,
                query: query.to_string(),
            },
            index_name,
            Some((shard_id, &node.id)),
        );
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
        let request = self.forwarding_request(
            ShardSearchDslRequest {
                index_name: index_name.to_string(),
                shard_id,
                search_request_json: serde_json::to_vec(req)?,
            },
            index_name,
            Some((shard_id, &node.id)),
        );
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

    pub(crate) async fn get_remote_store_leaf_status(
        &self,
        node: &NodeInfo,
        index_name: &str,
        index_uuid: &str,
        splits: &[crate::engine::remote_store::AssignedRemoteSplit],
    ) -> Result<crate::engine::remote_store::LeafStatusSnapshot, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = self.forwarding_request(
            RemoteStoreLeafStatusRequest {
                index_name: index_name.to_string(),
                index_uuid: index_uuid.to_string(),
                splits: splits.iter().map(remote_store_split_to_proto).collect(),
            },
            index_name,
            None,
        );
        let response = client
            .get_remote_store_leaf_status(request)
            .await?
            .into_inner();
        Ok(crate::engine::remote_store::LeafStatusSnapshot {
            root_capable: response.root_capable,
            leaf_capable: response.leaf_capable,
            inflight_bytes: response.inflight_bytes,
            queue_depth: response.queue_depth as usize,
            split_statuses: response
                .split_statuses
                .into_iter()
                .map(|status| {
                    (
                        status.split_id,
                        crate::engine::remote_store::SplitWarmth {
                            artifact_cached: status.artifact_cached,
                            reader_cached: status.reader_cached,
                        },
                    )
                })
                .collect(),
        })
    }

    pub(crate) async fn forward_remote_store_search(
        &self,
        node: &NodeInfo,
        index_name: &str,
        index_uuid: &str,
        req: &crate::search::SearchRequest,
        splits: &[crate::engine::remote_store::AssignedRemoteSplit],
        live_split_ids: &[String],
    ) -> Result<Vec<crate::engine::remote_store::LeafSplitSearchOutcome>, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let request = self.forwarding_request(
            RemoteStoreSearchRequest {
                index_name: index_name.to_string(),
                index_uuid: index_uuid.to_string(),
                search_request_json: serde_json::to_vec(req)?,
                splits: splits.iter().map(remote_store_split_to_proto).collect(),
                live_split_ids: live_split_ids.to_vec(),
            },
            index_name,
            None,
        );
        let response = client
            .search_remote_store_splits(request)
            .await?
            .into_inner();
        response
            .results
            .into_iter()
            .map(|result| {
                let partial_aggs = result
                    .partial_aggs_json
                    .into_iter()
                    .map(|bytes| crate::search::decode_partial_aggs(&bytes))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .fold(std::collections::HashMap::new(), |mut merged, partial| {
                        for (key, value) in partial {
                            merged.insert(key, value);
                        }
                        merged
                    });
                Ok(crate::engine::remote_store::LeafSplitSearchOutcome {
                    split_id: result.split_id,
                    hits: decode_search_hits(&result.hits)?,
                    total_hits: result.total_hits as usize,
                    partial_aggs,
                    error: (!result.success).then_some(result.error),
                })
            })
            .collect()
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
        let request = self.forwarding_request(
            SqlRecordBatchRequest {
                index_name: index_name.to_string(),
                shard_id,
                search_request_json: serde_json::to_vec(req)?,
                columns: columns.to_vec(),
                needs_id,
                needs_score,
                batch_size: 0,
            },
            index_name,
            Some((shard_id, &node.id)),
        );
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
        let request = self.forwarding_request(
            SqlRecordBatchRequest {
                index_name: index_name.to_string(),
                shard_id,
                search_request_json: serde_json::to_vec(req)?,
                columns: columns.to_vec(),
                needs_id,
                needs_score,
                batch_size: batch_size as u32,
            },
            index_name,
            Some((shard_id, &node.id)),
        );
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

    pub(crate) async fn get_cluster_state_version(
        &self,
        node: &NodeInfo,
        local_node_id: &str,
    ) -> anyhow::Result<u64> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let response = client
            .ping(tonic::Request::new(PingRequest {
                source_node_id: local_node_id.to_string(),
            }))
            .await?;
        decode_state_version(response.metadata())
            .context("invalid Ping response cluster state version")
    }

    pub(crate) async fn open_remote_index_primaries(
        &self,
        state: &ClusterState,
        index_name: &str,
        local_node_id: &str,
    ) -> anyhow::Result<()> {
        let metadata = state
            .indices
            .get(index_name)
            .ok_or_else(|| tonic::Status::not_found(format!("no such index [{index_name}]")))?;
        let targets = metadata
            .shard_routing
            .values()
            .map(|routing| routing.primary.as_str())
            .filter(|node| *node != local_node_id)
            .collect::<std::collections::HashSet<_>>();
        let timeout = self.cluster_manager.as_ref().map_or(
            crate::cluster::manager::FORWARDING_STATE_WAIT_TIMEOUT
                + crate::cluster::manager::PRIMARY_OPEN_WAIT_TIMEOUT,
            |manager| manager.forwarding_wait_timeout() + manager.primary_open_wait_timeout(),
        );
        let results = tokio::time::timeout(timeout, futures::future::join_all(targets.into_iter().map(|node_id| async move {
            let node = state.nodes.get(node_id).ok_or_else(|| tonic::Status::unavailable(
                format!("primary node [{node_id}] for index [{index_name}] is absent from cluster state")
            ))?;
            let mut client = self.connect(&node.host, node.transport_port).await
                .with_context(|| format!("connect to primary [{node_id}] for index [{index_name}]"))?;
            let request = self.forwarding_request(IndexMaintenanceRequest {
                index_name: index_name.to_string(),
            }, index_name, None);
            client.open_index(request).await.with_context(|| format!("open primary [{node_id}] for index [{index_name}]"))?;
            Ok::<(), anyhow::Error>(())
        }))).await.map_err(|error| tonic::Status::unavailable(format!(
            "timed out opening primary shards for index [{index_name}] at cluster state version {}: {error}",
            state.version,
        )))?;
        results.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
        Ok(())
    }

    /// Replicate a single document operation to a replica shard on a remote node.
    pub async fn replicate_to_shard(
        &self,
        node: &NodeInfo,
        request: ReplicateDocRequest,
        require_persisted: bool,
    ) -> Result<ReplicaApplyResponse, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let response = client
            .replicate_doc(tonic::Request::new(request))
            .await?
            .into_inner();
        decode_replica_apply_response(
            ReplicaApplyResponseFields {
                success: response.success,
                error: &response.error,
                processed_checkpoint: response.processed_checkpoint,
                persisted_checkpoint: response.persisted_checkpoint,
                operation_processed: response.operation_processed,
                operation_persisted: response.operation_persisted,
            },
            require_persisted,
            "replication",
        )
    }

    /// Replicate a bulk set of document operations to a replica shard on a remote node.
    pub async fn replicate_bulk_to_shard(
        &self,
        node: &NodeInfo,
        request: ReplicateBulkRequest,
        require_persisted: bool,
    ) -> Result<ReplicaApplyResponse, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        let response = client
            .replicate_bulk(tonic::Request::new(request))
            .await?
            .into_inner();
        decode_replica_apply_response(
            ReplicaApplyResponseFields {
                success: response.success,
                error: &response.error,
                processed_checkpoint: response.processed_checkpoint,
                persisted_checkpoint: response.persisted_checkpoint,
                operation_processed: response.all_operations_processed,
                operation_persisted: response.all_operations_persisted,
            },
            require_persisted,
            "bulk replication",
        )
    }

    pub async fn get_shard_sequence_state(
        &self,
        node: &NodeInfo,
        request: GetShardSequenceStateRequest,
    ) -> Result<GetShardSequenceStateResponse, anyhow::Error> {
        let mut client = self.connect(&node.host, node.transport_port).await?;
        Ok(client
            .get_shard_sequence_state(tonic::Request::new(request))
            .await?
            .into_inner())
    }

    pub async fn start_peer_recovery(
        &self,
        primary_node: &NodeInfo,
        request: StartPeerRecoveryRequest,
    ) -> Result<StartPeerRecoveryResponse, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let response = client
            .start_peer_recovery(tonic::Request::new(request))
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        Ok(response)
    }

    pub async fn fetch_recovery_file_chunk(
        &self,
        primary_node: &NodeInfo,
        request: FetchRecoveryFileChunkRequest,
    ) -> Result<FetchRecoveryFileChunkResponse, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let response = client
            .fetch_recovery_file_chunk(tonic::Request::new(request))
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        Ok(response)
    }

    pub async fn fetch_recovery_ops(
        &self,
        primary_node: &NodeInfo,
        request: FetchRecoveryOpsRequest,
    ) -> Result<FetchRecoveryOpsResponse, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let response = client
            .fetch_recovery_ops(tonic::Request::new(request))
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        Ok(response)
    }

    pub async fn prepare_finalize_recovery(
        &self,
        primary_node: &NodeInfo,
        request: PrepareFinalizeRecoveryRequest,
    ) -> Result<PrepareFinalizeRecoveryResponse, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let response = client
            .prepare_finalize_recovery(tonic::Request::new(request))
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        Ok(response)
    }

    pub async fn complete_finalize_recovery(
        &self,
        primary_node: &NodeInfo,
        request: CompleteFinalizeRecoveryRequest,
    ) -> Result<CompleteFinalizeRecoveryResponse, anyhow::Error> {
        let mut client = self
            .connect(&primary_node.host, primary_node.transport_port)
            .await?;
        let response = client
            .complete_finalize_recovery(tonic::Request::new(request))
            .await?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.success {
            return Err(anyhow::anyhow!(
                "peer recovery finalization was not successful"
            ));
        }
        Ok(response)
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
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let settings_json = serde_json::to_vec(settings_body)?;
        let request = tonic::Request::new(UpdateSettingsRequest {
            index_name: index_name.to_string(),
            settings_json,
        });
        let resp = client
            .update_settings(request)
            .await
            .context("UpdateSettings RPC")?;
        if !resp.get_ref().error.is_empty() {
            return Err(anyhow::anyhow!("{}", resp.get_ref().error));
        }
        self.wait_for_response_state(&resp, "UpdateSettings", index_name)
            .await?;
        Ok(())
    }

    pub async fn forward_mark_replica_in_sync(
        &self,
        master: &NodeInfo,
        request: MarkReplicaInSyncRequest,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let response = client
            .mark_replica_in_sync(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("MarkReplicaInSync RPC: {e}"))?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.acknowledged {
            return Err(anyhow::anyhow!("MarkReplicaInSync was not acknowledged"));
        }
        Ok(())
    }

    pub async fn forward_activate_primary(
        &self,
        master: &NodeInfo,
        request: ActivatePrimaryRequest,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let response = client
            .activate_primary(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("ActivatePrimary RPC: {e}"))?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.acknowledged {
            return Err(anyhow::anyhow!("ActivatePrimary was not acknowledged"));
        }
        Ok(())
    }

    pub async fn forward_mark_primary_unavailable(
        &self,
        master: &NodeInfo,
        request: MarkPrimaryUnavailableRequest,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let response = client
            .mark_primary_unavailable(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("MarkPrimaryUnavailable RPC: {e}"))?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.acknowledged {
            return Err(anyhow::anyhow!(
                "MarkPrimaryUnavailable was not acknowledged"
            ));
        }
        Ok(())
    }

    pub async fn forward_mark_primary_available(
        &self,
        master: &NodeInfo,
        request: MarkPrimaryAvailableRequest,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let response = client
            .mark_primary_available(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("MarkPrimaryAvailable RPC: {e}"))?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.acknowledged {
            return Err(anyhow::anyhow!("MarkPrimaryAvailable was not acknowledged"));
        }
        Ok(())
    }

    pub async fn forward_fail_shard_copy(
        &self,
        master: &NodeInfo,
        request: FailShardCopyRequest,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let response = client
            .fail_shard_copy(tonic::Request::new(request))
            .await
            .map_err(|e| anyhow::anyhow!("FailShardCopy RPC: {e}"))?
            .into_inner();
        if !response.error.is_empty() {
            return Err(anyhow::anyhow!("{}", response.error));
        }
        if !response.acknowledged {
            return Err(anyhow::anyhow!("FailShardCopy was not acknowledged"));
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
            .context("connect to master")?;
        let request = tonic::Request::new(CreateIndexRequest {
            index_name: index_name.to_string(),
            body_json: body.to_vec(),
        });
        let resp = client
            .create_index(request)
            .await
            .context("CreateIndex RPC")?;
        if !resp.get_ref().error.is_empty() {
            return Err(anyhow::anyhow!("{}", resp.get_ref().error));
        }
        if !resp.get_ref().acknowledged {
            anyhow::bail!("CreateIndex was not acknowledged");
        }
        let mut response: serde_json::Value = serde_json::from_slice(&resp.get_ref().response_json)
            .context("invalid CreateIndex response JSON")?;
        if response["acknowledged"].as_bool() != Some(true)
            || response["shards_acknowledged"].as_bool().is_none()
            || response["index"].as_str() != Some(index_name)
        {
            anyhow::bail!("invalid acknowledged CreateIndex response for index [{index_name}]");
        }
        if let Err(error) = self
            .wait_for_response_state(&resp, "CreateIndex", index_name)
            .await
        {
            if error
                .chain()
                .filter_map(|cause| cause.downcast_ref::<tonic::Status>())
                .any(crate::transport::state_wait::is_state_wait_timeout)
            {
                warn!(index = index_name, error = %error,
                    "Index creation committed but coordinator application exceeded the metadata deadline");
                response["shards_acknowledged"] = serde_json::Value::Bool(false);
            } else {
                return Err(error);
            }
        }
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
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let request = tonic::Request::new(DeleteIndexRequest {
            index_name: index_name.to_string(),
        });
        let resp = client
            .delete_index(request)
            .await
            .map_err(|e| anyhow::anyhow!("DeleteIndex RPC: {e}"))?;
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
            .map_err(|e| anyhow::anyhow!("connect to master: {e}"))?;
        let request = tonic::Request::new(TransferMasterRequest {
            target_node_id: target_node_id.to_string(),
        });
        let resp = client
            .transfer_master(request)
            .await
            .map_err(|e| anyhow::anyhow!("TransferMaster RPC: {e}"))?;
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
            .map_err(|e| anyhow::anyhow!("connect to master for AddMappings: {e}"))?;

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
            .context("AddMappings RPC")?;
        if !resp.get_ref().error.is_empty() {
            return Err(anyhow::anyhow!("{}", resp.get_ref().error));
        }
        self.observe_response_state(&resp, "AddMappings", index_name)?;
        Ok(())
    }

    /// Forward a dynamic API-key upsert to the Raft leader.
    pub async fn forward_put_api_key(
        &self,
        master: &NodeInfo,
        record: &crate::cluster::state::SecurityApiKeyRecord,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master for PutApiKey: {e}"))?;

        let record_json = serde_json::to_string(record)
            .map_err(|e| anyhow::anyhow!("serialize api key record: {e}"))?;
        let request = tonic::Request::new(PutApiKeyRequest { record_json });
        let resp = client
            .put_api_key(request)
            .await
            .map_err(|e| anyhow::anyhow!("PutApiKey RPC: {e}"))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward a dynamic API-key deletion to the Raft leader.
    pub async fn forward_delete_api_key(
        &self,
        master: &NodeInfo,
        key_id: &str,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master for DeleteApiKey: {e}"))?;

        let request = tonic::Request::new(DeleteApiKeyRequest {
            key_id: key_id.to_string(),
        });
        let resp = client
            .delete_api_key(request)
            .await
            .map_err(|e| anyhow::anyhow!("DeleteApiKey RPC: {e}"))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward a custom-role upsert to the Raft leader.
    pub async fn forward_put_role(
        &self,
        master: &NodeInfo,
        role: &crate::cluster::state::SecurityRoleDefinition,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master for PutRole: {e}"))?;

        let role_json = serde_json::to_string(role)
            .map_err(|e| anyhow::anyhow!("serialize role definition: {e}"))?;
        let request = tonic::Request::new(PutRoleRequest { role_json });
        let resp = client
            .put_role(request)
            .await
            .map_err(|e| anyhow::anyhow!("PutRole RPC: {e}"))?;
        let inner = resp.into_inner();
        if !inner.error.is_empty() {
            return Err(anyhow::anyhow!("{}", inner.error));
        }
        Ok(())
    }

    /// Forward a custom-role deletion to the Raft leader.
    pub async fn forward_delete_role(
        &self,
        master: &NodeInfo,
        name: &str,
    ) -> Result<(), anyhow::Error> {
        let mut client = self
            .connect(&master.host, master.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to master for DeleteRole: {e}"))?;

        let request = tonic::Request::new(DeleteRoleRequest {
            name: name.to_string(),
        });
        let resp = client
            .delete_role(request)
            .await
            .map_err(|e| anyhow::anyhow!("DeleteRole RPC: {e}"))?;
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
        self.fetch_shard_stats(node, None).await
    }

    pub(crate) async fn get_index_shard_stats(
        &self,
        node: &NodeInfo,
        index_name: &str,
    ) -> Result<HashMap<(String, u32), u64>, anyhow::Error> {
        self.fetch_shard_stats(node, Some(index_name)).await
    }

    async fn fetch_shard_stats(
        &self,
        node: &NodeInfo,
        index_name: Option<&str>,
    ) -> Result<HashMap<(String, u32), u64>, anyhow::Error> {
        let mut client = self
            .connect(&node.host, node.transport_port)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {}: {}", node.id, e))?;
        let request = match index_name {
            Some(index) => self.forwarding_request(ShardStatsRequest {}, index, None),
            None => tonic::Request::new(ShardStatsRequest {}),
        };
        let resp = client
            .get_shard_stats(request)
            .await
            .with_context(|| format!("GetShardStats RPC to {}", node.id))?;
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
            .with_context(|| format!("GetSegmentStats RPC to {}", node.id))?;
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
            .refresh_index(self.forwarding_request(
                IndexMaintenanceRequest {
                    index_name: index_name.to_string(),
                },
                index_name,
                None,
            ))
            .await
            .with_context(|| format!("RefreshIndex RPC to {}", node.id))?;
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
            .flush_index(self.forwarding_request(
                IndexMaintenanceRequest {
                    index_name: index_name.to_string(),
                },
                index_name,
                None,
            ))
            .await
            .with_context(|| format!("FlushIndex RPC to {}", node.id))?;
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
            .force_merge_index(self.forwarding_request(
                ForceMergeRequest {
                    index_name: index_name.to_string(),
                    max_num_segments,
                },
                index_name,
                None,
            ))
            .await
            .with_context(|| format!("ForceMergeIndex RPC to {}", node.id))?;
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

fn encode_bulk_document(
    doc_id: &str,
    source: &serde_json::Value,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut buffer = Vec::with_capacity(128 + doc_id.len());
    buffer.extend_from_slice(b"{\"_doc_id\":");
    serde_json::to_writer(&mut buffer, doc_id)?;
    buffer.extend_from_slice(b",\"_source\":");
    serde_json::to_writer(&mut buffer, source)?;
    buffer.push(b'}');
    Ok(buffer)
}

fn decode_shard_doc_response(
    index_name: &str,
    shard_id: u32,
    expected_doc_id: &str,
    response: ShardDocResponse,
) -> Result<serde_json::Value, anyhow::Error> {
    if !response.success {
        anyhow::bail!("Shard index failed: {}", response.error);
    }
    if response.doc_id.is_empty() {
        anyhow::bail!("successful shard index response returned an empty document id");
    }
    if !expected_doc_id.is_empty() && response.doc_id != expected_doc_id {
        anyhow::bail!(
            "successful shard index response returned document id '{}' instead of '{}'",
            response.doc_id,
            expected_doc_id
        );
    }
    let seq_no = response.seq_no.ok_or_else(|| {
        anyhow::anyhow!("successful shard index response is missing its assigned sequence")
    })?;
    let primary_term = response
        .primary_term
        .filter(|term| *term > 0)
        .ok_or_else(|| {
            anyhow::anyhow!("successful shard index response is missing its primary term")
        })?;
    Ok(serde_json::json!({
        "_index": index_name,
        "_id": response.doc_id,
        "_shard": shard_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "result": if response.created { "created" } else { "updated" }
    }))
}

fn decode_shard_bulk_response(
    docs: &[(String, serde_json::Value)],
    response: ShardBulkResponse,
) -> Result<crate::engine::BulkWriteReceipt, anyhow::Error> {
    if !response.success {
        anyhow::bail!("Shard bulk index failed: {}", response.error);
    }
    if response.doc_ids.len() != docs.len()
        || response
            .doc_ids
            .iter()
            .zip(docs)
            .any(|(id, (expected, _))| id != expected)
    {
        anyhow::bail!("successful shard bulk response has inconsistent document identities");
    }
    let receipt = crate::engine::BulkWriteReceipt {
        doc_ids: response.doc_ids,
        start_seq_no: response.start_seq_no,
        primary_term: response
            .primary_term
            .filter(|term| *term > 0)
            .ok_or_else(|| {
                anyhow::anyhow!("successful shard bulk response is missing its primary term")
            })?,
        created: response
            .results
            .iter()
            .map(|item| item.result == "created")
            .collect(),
    };
    receipt.last_seq_no()?;
    for (offset, (item, doc_id)) in response.results.iter().zip(&receipt.doc_ids).enumerate() {
        let expected_seq = receipt
            .start_seq_no
            .and_then(|start| start.checked_add(offset as u64));
        if item.doc_id != *doc_id
            || !item.error.is_empty()
            || item.seq_no != expected_seq
            || item.primary_term != Some(receipt.primary_term)
            || !matches!(
                (item.status, item.result.as_str()),
                (201, "created") | (200, "updated")
            )
        {
            anyhow::bail!("successful shard bulk response has inconsistent item results");
        }
    }
    Ok(receipt)
}

fn decode_shard_delete_response(
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    response: ShardDeleteResponse,
) -> Result<serde_json::Value, anyhow::Error> {
    if !response.success {
        anyhow::bail!("Delete failed: {}", response.error);
    }
    let seq_no = response.seq_no.ok_or_else(|| {
        anyhow::anyhow!("successful shard delete response is missing its assigned sequence")
    })?;
    let primary_term = response
        .primary_term
        .filter(|term| *term > 0)
        .ok_or_else(|| {
            anyhow::anyhow!("successful shard delete response is missing its primary term")
        })?;
    Ok(serde_json::json!({
        "_index": index_name,
        "_id": doc_id,
        "_shard": shard_id,
        "_seq_no": seq_no,
        "_primary_term": primary_term,
        "result": if response.deleted > 0 { "deleted" } else { "not_found" }
    }))
}

struct ReplicaApplyResponseFields<'a> {
    success: bool,
    error: &'a str,
    processed_checkpoint: Option<u64>,
    persisted_checkpoint: Option<u64>,
    operation_processed: bool,
    operation_persisted: bool,
}

fn decode_replica_apply_response(
    response: ReplicaApplyResponseFields<'_>,
    require_persisted: bool,
    label: &str,
) -> Result<ReplicaApplyResponse, anyhow::Error> {
    if !response.success {
        anyhow::bail!("{label} failed: {}", response.error);
    }
    if !response.operation_processed {
        anyhow::bail!("successful {label} response did not prove the operation was processed");
    }
    if response.persisted_checkpoint.is_some() && response.processed_checkpoint.is_none() {
        anyhow::bail!("{label} response has a persisted checkpoint without a processed checkpoint");
    }
    if let (Some(persisted), Some(processed)) =
        (response.persisted_checkpoint, response.processed_checkpoint)
        && persisted > processed
    {
        anyhow::bail!(
            "{label} response persisted checkpoint {persisted} exceeds processed checkpoint {processed}"
        );
    }
    if response.operation_persisted && !response.operation_processed {
        anyhow::bail!("{label} response persisted an unprocessed operation");
    }
    if require_persisted && !response.operation_persisted {
        anyhow::bail!(
            "successful {label} response under request durability did not prove the operation was persisted"
        );
    }
    Ok(ReplicaApplyResponse {
        processed_checkpoint: response.processed_checkpoint,
        persisted_checkpoint: response.persisted_checkpoint,
        operation_processed: response.operation_processed,
        operation_persisted: response.operation_persisted,
    })
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

fn remote_store_split_to_proto(
    split: &crate::engine::remote_store::AssignedRemoteSplit,
) -> RemoteStoreSplitPlan {
    RemoteStoreSplitPlan {
        split_id: split.split_id.clone(),
        bundle_path: split.bundle_path.clone(),
        checksum: split.checksum.clone(),
        size_bytes: split.size_bytes,
    }
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
    fn shard_doc_response_decoder_accepts_server_generated_id() {
        let index = decode_shard_doc_response(
            "idx",
            0,
            "",
            ShardDocResponse {
                success: true,
                doc_id: "generated".into(),
                error: String::new(),
                seq_no: Some(0),
                primary_term: Some(7),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(index["_id"], "generated");
        assert_eq!(index["_seq_no"], 0);
        assert_eq!(index["_primary_term"], 7);
    }

    #[test]
    fn shard_doc_response_decoder_rejects_empty_success_id() {
        for expected_doc_id in ["", "doc"] {
            assert!(
                decode_shard_doc_response(
                    "idx",
                    0,
                    expected_doc_id,
                    ShardDocResponse {
                        success: true,
                        doc_id: String::new(),
                        error: String::new(),
                        seq_no: Some(0),
                        primary_term: Some(7),
                        ..Default::default()
                    },
                )
                .is_err()
            );
        }
    }

    #[test]
    fn write_response_decoders_require_operation_receipts() {
        let index = decode_shard_doc_response(
            "idx",
            0,
            "doc",
            ShardDocResponse {
                success: true,
                doc_id: "doc".into(),
                error: String::new(),
                seq_no: Some(0),
                primary_term: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(index["_seq_no"], 0);
        assert_eq!(index["_primary_term"], 7);
        assert!(
            decode_shard_doc_response(
                "idx",
                0,
                "doc",
                ShardDocResponse {
                    success: true,
                    doc_id: "doc".into(),
                    error: String::new(),
                    seq_no: None,
                    primary_term: Some(7),
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert!(
            decode_shard_doc_response(
                "idx",
                0,
                "doc",
                ShardDocResponse {
                    success: true,
                    doc_id: "other".into(),
                    error: String::new(),
                    seq_no: Some(0),
                    primary_term: Some(7),
                    ..Default::default()
                },
            )
            .is_err()
        );

        let docs = vec![
            ("a".to_string(), serde_json::json!({})),
            ("b".to_string(), serde_json::json!({})),
        ];
        let bulk = decode_shard_bulk_response(
            &docs,
            ShardBulkResponse {
                success: true,
                doc_ids: vec!["a".into(), "b".into()],
                error: String::new(),
                start_seq_no: Some(0),
                primary_term: Some(7),
                results: ["a", "b"]
                    .into_iter()
                    .enumerate()
                    .map(|(offset, doc_id)| ShardBulkItemResponse {
                        doc_id: doc_id.to_string(),
                        status: 201,
                        result: "created".to_string(),
                        seq_no: Some(offset as u64),
                        primary_term: Some(7),
                        ..Default::default()
                    })
                    .collect(),
            },
        )
        .unwrap();
        assert_eq!(bulk.last_seq_no().unwrap(), Some(1));
        assert_eq!(bulk.primary_term, 7);
        for response in [
            ShardBulkResponse {
                success: true,
                doc_ids: vec!["a".into(), "b".into()],
                error: String::new(),
                start_seq_no: None,
                primary_term: Some(7),
                ..Default::default()
            },
            ShardBulkResponse {
                success: true,
                doc_ids: vec!["b".into(), "a".into()],
                error: String::new(),
                start_seq_no: Some(0),
                primary_term: Some(7),
                ..Default::default()
            },
        ] {
            assert!(decode_shard_bulk_response(&docs, response).is_err());
        }
        assert!(
            decode_shard_bulk_response(
                &[],
                ShardBulkResponse {
                    success: true,
                    doc_ids: vec![],
                    error: String::new(),
                    start_seq_no: Some(0),
                    primary_term: Some(7),
                    ..Default::default()
                },
            )
            .is_err()
        );

        let delete = decode_shard_delete_response(
            "idx",
            0,
            "doc",
            ShardDeleteResponse {
                success: true,
                deleted: 1,
                error: String::new(),
                seq_no: Some(0),
                primary_term: Some(7),
            },
        )
        .unwrap();
        assert_eq!(delete["_seq_no"], 0);
        assert_eq!(delete["_primary_term"], 7);
        assert!(
            decode_shard_delete_response(
                "idx",
                0,
                "doc",
                ShardDeleteResponse {
                    success: true,
                    deleted: 1,
                    error: String::new(),
                    seq_no: None,
                    primary_term: Some(7),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn replica_response_decoder_requires_exact_operation_proof() {
        let response = decode_replica_apply_response(
            ReplicaApplyResponseFields {
                success: true,
                error: "",
                processed_checkpoint: None,
                persisted_checkpoint: None,
                operation_processed: true,
                operation_persisted: false,
            },
            false,
            "replication",
        )
        .unwrap();
        assert!(response.operation_processed);
        assert!(!response.operation_persisted);
        assert_eq!(response.processed_checkpoint, None);

        assert!(
            decode_replica_apply_response(
                ReplicaApplyResponseFields {
                    success: true,
                    error: "",
                    processed_checkpoint: Some(10),
                    persisted_checkpoint: Some(10),
                    operation_processed: false,
                    operation_persisted: false,
                },
                false,
                "replication",
            )
            .is_err()
        );
        assert!(
            decode_replica_apply_response(
                ReplicaApplyResponseFields {
                    success: true,
                    error: "",
                    processed_checkpoint: Some(5),
                    persisted_checkpoint: Some(6),
                    operation_processed: true,
                    operation_persisted: true,
                },
                false,
                "replication",
            )
            .is_err()
        );
        assert!(
            decode_replica_apply_response(
                ReplicaApplyResponseFields {
                    success: true,
                    error: "",
                    processed_checkpoint: Some(5),
                    persisted_checkpoint: Some(5),
                    operation_processed: true,
                    operation_persisted: false,
                },
                true,
                "replication",
            )
            .is_err()
        );
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
