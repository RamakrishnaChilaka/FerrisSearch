#![cfg(feature = "protocol-trace")]

use anyhow::{Context, Result};
use ferrissearch::cluster::ClusterManager;
use ferrissearch::cluster::state::{
    DynamicMapping, FieldMapping, FieldType, IndexMetadata, IndexSettings, IndexUuid, NodeInfo,
    NodeRole, ShardRoutingEntry,
};
use ferrissearch::consensus::network::{RaftNetworkConnection, RaftNetworkFactoryImpl};
use ferrissearch::consensus::state_machine::ClusterStateMachine;
use ferrissearch::consensus::store::MemLogStore;
use ferrissearch::consensus::types::{self, ClusterCommand, RaftInstance, TypeConfig};
use ferrissearch::engine::SequenceStats;
use ferrissearch::protocol_trace::{OperationKey, TraceCopySnapshot};
use ferrissearch::shard::{SHARD_COPY_IDENTITY_FILE, ShardCopyIdentity, ShardManager};
use ferrissearch::transport::TransportClient;
use ferrissearch::transport::proto::{
    PingRequest, ShardBulkRequest, ShardDeleteRequest, ShardDocRequest, ShardDocResponse,
    ShardGetRequest, internal_transport_client::InternalTransportClient,
};
use ferrissearch::transport::server::create_transport_service_with_raft;
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::async_runtime::WatchReceiver;
use openraft::{BasicNode, RaftNetworkV2};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;

const INDEX: &str = "live-stale-primary";
const UUID: &str = "live-stale-primary-uuid";
const WAIT: Duration = Duration::from_secs(30);
const NODE_NAMES: [&str; 3] = ["p", "q", "r"];

#[derive(Default)]
struct MetadataPartition {
    isolated: AtomicBool,
    blocked: AtomicU64,
}

impl MetadataPartition {
    fn rejection(&self, source: u64, target: u64) -> Option<std::io::Error> {
        if self.isolated.load(Ordering::Acquire) && (source == 1 || target == 1) {
            self.blocked.fetch_add(1, Ordering::Relaxed);
            Some(std::io::Error::other(format!(
                "test metadata partition: {source} -> {target}"
            )))
        } else {
            None
        }
    }
}

struct PartitionedNetworkFactory {
    source: u64,
    partition: Arc<MetadataPartition>,
}

struct PartitionedConnection {
    source: u64,
    target: u64,
    partition: Arc<MetadataPartition>,
    inner: RaftNetworkConnection,
}

impl RaftNetworkFactory<TypeConfig> for PartitionedNetworkFactory {
    type Network = PartitionedConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let inner = RaftNetworkFactoryImpl.new_client(target, node).await;
        PartitionedConnection {
            source: self.source,
            target,
            partition: self.partition.clone(),
            inner,
        }
    }
}

impl RaftNetworkV2<TypeConfig> for PartitionedConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        if let Some(error) = self.partition.rejection(self.source, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&error)));
        }
        self.inner.append_entries(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        if let Some(error) = self.partition.rejection(self.source, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&error)));
        }
        self.inner.vote(rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: types::Vote,
        snapshot: types::Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        if let Some(error) = self.partition.rejection(self.source, self.target) {
            return Err(StreamingError::Unreachable(Unreachable::new(&error)));
        }
        self.inner
            .full_snapshot(vote, snapshot, cancel, option)
            .await
    }
}

struct TestNode {
    directory: TempDir,
    address: SocketAddr,
    raft: Arc<RaftInstance>,
    manager: Arc<ClusterManager>,
    shards: Arc<ShardManager>,
    transport: TransportClient,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_node(id: u64, partition: Arc<MetadataPartition>) -> Result<TestNode> {
    let directory = tempfile::tempdir()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let state_machine = ClusterStateMachine::new(INDEX.to_string());
    let state = state_machine.state_handle();
    let config = openraft::Config {
        cluster_name: INDEX.to_string(),
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    };
    let raft = Arc::new(
        openraft::Raft::new(
            id,
            Arc::new(config),
            PartitionedNetworkFactory {
                source: id,
                partition,
            },
            MemLogStore::new(),
            state_machine,
        )
        .await?,
    );
    let manager = Arc::new(ClusterManager::with_shared_state(state));
    let shards = Arc::new(ShardManager::new(
        directory.path(),
        Duration::from_secs(600),
    ));
    let transport = TransportClient::new();
    let service = create_transport_service_with_raft(
        manager.clone(),
        shards.clone(),
        transport.clone(),
        raft.clone(),
        Arc::new(ferrissearch::tasks::TaskManager::new()),
        NODE_NAMES[(id - 1) as usize].to_string(),
    );
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    connect(address).await?;
    Ok(TestNode {
        directory,
        address,
        raft,
        manager,
        shards,
        transport,
        server,
    })
}

async fn connect(
    address: SocketAddr,
) -> Result<InternalTransportClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(WAIT)
        .timeout(WAIT)
        .connect()
        .await?;
    Ok(InternalTransportClient::new(channel))
}

fn request<T>(message: T) -> tonic::Request<T> {
    ferrissearch::transport::request_with_cluster_state_version(message, 0)
}

async fn wait_until(label: &str, mut ready: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {label}"))
}

async fn wait_applied(nodes: &[TestNode], indexes: &[usize], version: u64) -> Result<()> {
    wait_until("committed Raft application", || {
        indexes
            .iter()
            .all(|&index| nodes[index].manager.version() >= version)
    })
    .await
}

async fn commit(
    nodes: &[TestNode],
    leader: usize,
    indexes: &[usize],
    command: ClusterCommand,
) -> Result<()> {
    ferrissearch::consensus::client_write_checked(&nodes[leader].raft, command).await?;
    wait_applied(nodes, indexes, nodes[leader].manager.version()).await
}

async fn index_document(address: SocketAddr, doc_id: &str, value: i64) -> Result<ShardDocResponse> {
    Ok(connect(address)
        .await?
        .index_doc(request(ShardDocRequest {
            index_name: INDEX.to_string(),
            shard_id: 0,
            doc_id: doc_id.to_string(),
            payload_json: serde_json::to_vec(&json!({"value": value}))?,
            ..Default::default()
        }))
        .await?
        .into_inner())
}

async fn recover(nodes: &[TestNode], target: usize) -> Result<()> {
    let node = &nodes[target];
    let state = node.manager.get_state();
    let allocation = state
        .shard_allocation_id(INDEX, 0, NODE_NAMES[target])
        .context("recovery target allocation is missing")?;
    ferrissearch::node::start_peer_recovery_for_protocol_trace_test(
        &state,
        NODE_NAMES[target],
        node.manager.clone(),
        node.shards.clone(),
    );
    wait_until("exact-allocation peer recovery admission", || {
        let state = node.manager.get_state();
        state.indices[INDEX].shard_routing[&0]
            .in_sync_replicas
            .iter()
            .any(|name| name == NODE_NAMES[target])
            && state.shard_allocation_id(INDEX, 0, NODE_NAMES[target]) == Some(allocation)
            && node.shards.get_shard(INDEX, 0).is_some()
            && !node.shards.is_peer_recovery_target(INDEX, 0)
    })
    .await
}

struct Observation {
    snapshot: TraceCopySnapshot,
    identity: ShardCopyIdentity,
    sequence: SequenceStats,
    documents: BTreeMap<String, Option<Value>>,
}

impl Observation {
    fn json(&self) -> Value {
        json!({
            "copy": self.snapshot.copy,
            "identity": self.identity,
            "checkpoints": {
                "processed": self.sequence.processed_checkpoint,
                "persisted": self.sequence.persisted_checkpoint,
                "max_seq_no": self.sequence.max_seq_no,
            },
            "documents": self.documents,
            "document_identities": self.snapshot.actual_documents,
            "wal": self.snapshot.wal_entries,
        })
    }
}

async fn observe(node: &TestNode) -> Result<Observation> {
    let shards = node.shards.clone();
    tokio::task::spawn_blocking(move || {
        let snapshot = shards.capture_protocol_trace_copy_state(INDEX, 0)?;
        let engine = shards.get_shard(INDEX, 0).context("copy is not open")?;
        let identity = shards
            .copy_identity(INDEX, 0)
            .context("open copy identity is missing")?;
        let identity_path = shards
            .data_dir()
            .join(UUID)
            .join("shard_0")
            .join(SHARD_COPY_IDENTITY_FILE);
        let durable_identity: ShardCopyIdentity = serde_json::from_slice(
            &std::fs::read(&identity_path)
                .with_context(|| format!("read durable identity {}", identity_path.display()))?,
        )?;
        assert_eq!(identity, durable_identity);
        let sequence = engine.sequence_stats();
        let documents = [
            "counter",
            "delete-sentinel",
            "bulk-sentinel",
            "new-only",
            "old-only",
        ]
        .into_iter()
        .map(|doc| Ok((doc.to_string(), engine.get_document(doc)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Observation {
            snapshot,
            identity,
            sequence,
            documents,
        })
    })
    .await?
}

fn assert_canonical(observed: &Observation, old_term: u64, new_term: u64) {
    assert_eq!(
        observed.documents,
        BTreeMap::from([
            ("counter".to_string(), Some(json!({"value": 1}))),
            ("delete-sentinel".to_string(), Some(json!({"value": 10}))),
            ("bulk-sentinel".to_string(), Some(json!({"value": 21}))),
            ("new-only".to_string(), Some(json!({"value": 42}))),
            ("old-only".to_string(), None),
        ])
    );
    assert_eq!(
        observed.sequence,
        SequenceStats {
            processed_checkpoint: Some(5),
            persisted_checkpoint: Some(5),
            max_seq_no: Some(5),
        }
    );
    assert_eq!(observed.identity.index_uuid, UUID);
    assert_eq!(observed.identity.replica_fence, new_term);
    let identities = observed
        .snapshot
        .live_documents
        .iter()
        .map(|(doc, seq_no, term, _)| (doc.as_str(), (*seq_no, *term)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed.snapshot.live_documents.len(), identities.len());
    assert_eq!(
        identities,
        BTreeMap::from([
            ("counter", (3, new_term)),
            ("delete-sentinel", (1, old_term)),
            ("bulk-sentinel", (4, new_term)),
            ("new-only", (5, new_term)),
        ])
    );
}

async fn record(
    file: &mut tokio::fs::File,
    step: &mut u64,
    event: &str,
    state: Value,
) -> Result<()> {
    let mut line = serde_json::to_vec(&json!({
        "schema": "ferrissearch.live-stale-primary-evidence/v1",
        "step": *step,
        "event": event,
        "state": state,
    }))?;
    line.push(b'\n');
    file.write_all(&line).await?;
    file.flush().await?;
    println!("live stale-primary boundary {step}: {event}");
    *step += 1;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_stale_primary_requests_fail_after_real_raft_promotion() -> Result<()> {
    let partition = Arc::new(MetadataPartition::default());
    let mut nodes = Vec::new();
    for id in 1..=3 {
        nodes.push(start_node(id, partition.clone()).await?);
    }
    ferrissearch::consensus::bootstrap_single_node(&nodes[0].raft, 1, nodes[0].address.to_string())
        .await?;
    wait_until("initial metadata leader", || {
        nodes[0].raft.metrics().borrow_watched().current_leader == Some(1)
    })
    .await?;
    for (index, node) in nodes.iter().enumerate().skip(1) {
        nodes[0]
            .raft
            .add_learner(
                index as u64 + 1,
                BasicNode {
                    addr: node.address.to_string(),
                },
                true,
            )
            .await?;
    }
    nodes[0]
        .raft
        .change_membership(BTreeSet::from([1, 2, 3]), false)
        .await?;
    for (index, node) in nodes.iter().enumerate() {
        commit(
            &nodes,
            0,
            &[0, 1, 2],
            ClusterCommand::AddNode {
                node: NodeInfo {
                    id: NODE_NAMES[index].to_string(),
                    name: NODE_NAMES[index].to_string(),
                    host: "127.0.0.1".to_string(),
                    transport_port: node.address.port(),
                    http_port: 0,
                    roles: vec![NodeRole::Master, NodeRole::Data],
                    raft_node_id: index as u64 + 1,
                },
            },
        )
        .await?;
    }
    commit(
        &nodes,
        0,
        &[0, 1, 2],
        ClusterCommand::SetMaster {
            node_id: "p".to_string(),
        },
    )
    .await?;
    commit(
        &nodes,
        0,
        &[0, 1, 2],
        ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: INDEX.to_string(),
                uuid: IndexUuid::new(UUID),
                number_of_shards: 1,
                number_of_replicas: 2,
                shard_routing: HashMap::from([(
                    0,
                    ShardRoutingEntry {
                        primary: "p".to_string(),
                        primary_term: 1,
                        replicas: vec!["q".to_string(), "r".to_string()],
                        in_sync_replicas: Vec::new(),
                        unassigned_replicas: 0,
                    },
                )]),
                mappings: HashMap::from([(
                    "value".to_string(),
                    FieldMapping {
                        field_type: FieldType::Integer,
                        dimension: None,
                    },
                )]),
                dynamic: DynamicMapping::Strict,
                settings: IndexSettings {
                    refresh_interval_ms: Some(600_000),
                    flush_threshold_bytes: Some(u64::MAX),
                    ..Default::default()
                },
            },
        },
    )
    .await?;

    for (seq_no, (doc, value)) in [
        ("counter", 0),
        ("delete-sentinel", 10),
        ("bulk-sentinel", 20),
    ]
    .into_iter()
    .enumerate()
    {
        let response = index_document(nodes[0].address, doc, value).await?;
        assert!(response.success, "{}", response.error);
        assert_eq!(response.seq_no, Some(seq_no as u64));
    }
    wait_applied(&nodes, &[0, 1, 2], nodes[0].manager.version()).await?;
    recover(&nodes, 1).await?;
    recover(&nodes, 2).await?;
    wait_applied(&nodes, &[0, 1, 2], nodes[0].manager.version()).await?;
    let before = nodes[0].manager.get_state();
    let old_term = before.indices[INDEX].shard_routing[&0].primary_term;
    let old_allocation = before.primary_allocation_id(INDEX, 0).unwrap();
    assert_eq!(
        before.indices[INDEX].shard_routing[&0].in_sync_replicas,
        ["q", "r"]
    );
    for node in &nodes {
        connect(node.address)
            .await?
            .ping(tonic::Request::new(PingRequest {
                source_node_id: "p".to_string(),
            }))
            .await?;
    }

    let output = std::env::var_os("STALE_PRIMARY_EVIDENCE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            nodes[0]
                .directory
                .path()
                .join("stale-primary-evidence.jsonl")
        });
    let mut evidence = tokio::fs::File::create(&output).await?;
    let mut step = 0;
    record(
        &mut evidence,
        &mut step,
        "replicas_admitted",
        json!({"term": old_term, "allocation": old_allocation, "version": before.version}),
    )
    .await?;

    let mut index_pause = nodes[0]
        .transport
        .arm_primary_replication_pause_for_test(OperationKey::new(UUID, 0, old_term, 3))?;
    let address = nodes[0].address;
    let old_index = tokio::spawn(async move { index_document(address, "counter", 999).await });
    tokio::time::timeout(WAIT, index_pause.wait_until_reached()).await??;

    let mut delete_pause = nodes[0]
        .transport
        .arm_primary_replication_pause_for_test(OperationKey::new(UUID, 0, old_term, 4))?;
    let old_delete = tokio::spawn(async move {
        Ok::<_, anyhow::Error>(
            connect(address)
                .await?
                .delete_doc(request(ShardDeleteRequest {
                    index_name: INDEX.to_string(),
                    shard_id: 0,
                    doc_id: "delete-sentinel".to_string(),
                    ..Default::default()
                }))
                .await?
                .into_inner(),
        )
    });
    tokio::time::timeout(WAIT, delete_pause.wait_until_reached()).await??;

    let mut bulk_pause = nodes[0]
        .transport
        .arm_primary_replication_pause_for_test(OperationKey::new(UUID, 0, old_term, 5))?;
    let old_bulk = tokio::spawn(async move {
        let documents_json = ["bulk-sentinel", "old-only"]
            .into_iter()
            .map(|doc| {
                serde_json::to_vec(&json!({
                    "_doc_id": doc,
                    "_source": {"value": 999},
                }))
            })
            .collect::<serde_json::Result<Vec<_>>>()?;
        Ok::<_, anyhow::Error>(
            connect(address)
                .await?
                .bulk_index(request(ShardBulkRequest {
                    index_name: INDEX.to_string(),
                    shard_id: 0,
                    documents_json,
                    ..Default::default()
                }))
                .await?
                .into_inner(),
        )
    });
    tokio::time::timeout(WAIT, bulk_pause.wait_until_reached()).await??;
    assert!(!old_index.is_finished() && !old_delete.is_finished() && !old_bulk.is_finished());
    let paused = observe(&nodes[0]).await?;
    assert_eq!(paused.sequence.processed_checkpoint, Some(6));
    assert_eq!(paused.sequence.persisted_checkpoint, Some(6));
    assert_eq!(paused.snapshot.wal_entries.len(), 7);
    assert_eq!(paused.documents["counter"], Some(json!({"value": 999})));
    assert_eq!(paused.documents["delete-sentinel"], None);
    assert_eq!(paused.documents["old-only"], Some(json!({"value": 999})));
    record(
        &mut evidence,
        &mut step,
        "primary_before_replication",
        paused.json(),
    )
    .await?;

    partition.isolated.store(true, Ordering::Release);
    wait_until("a metadata leader in the surviving quorum", || {
        let q = nodes[1].raft.metrics().borrow_watched().current_leader;
        let r = nodes[2].raft.metrics().borrow_watched().current_leader;
        q == r && matches!(q, Some(2 | 3))
    })
    .await?;
    let leader_id = nodes[1].raft.current_leader().await.unwrap();
    let leader = (leader_id - 1) as usize;
    assert!(partition.blocked.load(Ordering::Relaxed) > 0);
    commit(
        &nodes,
        leader,
        &[1, 2],
        ClusterCommand::SetMaster {
            node_id: NODE_NAMES[leader].to_string(),
        },
    )
    .await?;
    commit(
        &nodes,
        leader,
        &[1, 2],
        ClusterCommand::FailShardCopy {
            index_name: INDEX.to_string(),
            index_uuid: UUID.to_string(),
            shard_id: 0,
            node: "p".to_string(),
            allocation_id: old_allocation,
            expected_primary_term: old_term,
            promote_only: true,
            promotion_candidate: Some("q".to_string()),
        },
    )
    .await?;
    let promoted = nodes[1].manager.get_state();
    assert_eq!(promoted.indices[INDEX].shard_routing[&0].primary, "q");
    assert_eq!(
        promoted.indices[INDEX].shard_routing[&0].in_sync_replicas,
        ["r"]
    );
    for (seq_no, (doc, value)) in [("counter", 1), ("bulk-sentinel", 21), ("new-only", 42)]
        .into_iter()
        .enumerate()
    {
        let response = index_document(nodes[1].address, doc, value).await?;
        assert!(response.success, "{}", response.error);
        assert_eq!(response.seq_no, Some(seq_no as u64 + 3));
    }
    wait_applied(&nodes, &[1, 2], nodes[1].manager.version()).await?;
    let current = nodes[1].manager.get_state();
    let new_term = current.indices[INDEX].shard_routing[&0].primary_term;
    assert!(new_term > old_term);
    let q_before = observe(&nodes[1]).await?;
    let r_before = observe(&nodes[2]).await?;
    assert_canonical(&q_before, old_term, new_term);
    assert_canonical(&r_before, old_term, new_term);
    assert_eq!(q_before.snapshot.wal_entries, r_before.snapshot.wal_entries);
    assert_eq!(
        q_before
            .snapshot
            .wal_entries
            .iter()
            .map(|entry| (entry.seq_no, entry.term))
            .collect::<Vec<_>>(),
        [(3, new_term), (4, new_term), (5, new_term)]
    );
    assert_eq!(nodes[0].manager.version(), before.version);
    assert_eq!(
        nodes[0].manager.get_state().indices[INDEX].shard_routing[&0].primary,
        "p"
    );
    connect(nodes[0].address)
        .await?
        .ping(tonic::Request::new(PingRequest {
            source_node_id: "q".to_string(),
        }))
        .await?;
    record(
        &mut evidence,
        &mut step,
        "new_primary_and_replica_fenced",
        json!({
            "metadata_leader": leader_id,
            "old_primary_applied_version": before.version,
            "blocked_metadata_rpcs": partition.blocked.load(Ordering::Relaxed),
            "q": q_before.json(),
            "r": r_before.json(),
        }),
    )
    .await?;

    index_pause.release()?;
    delete_pause.release()?;
    bulk_pause.release()?;
    let index_result = tokio::time::timeout(WAIT, old_index).await???;
    let delete_result = tokio::time::timeout(WAIT, old_delete).await???;
    let bulk_result = tokio::time::timeout(WAIT, old_bulk).await???;
    assert!(!index_result.success, "{index_result:?}");
    assert!(!delete_result.success, "{delete_result:?}");
    assert!(!bulk_result.success, "{bulk_result:?}");
    assert_eq!(index_result.seq_no, Some(3));
    assert_eq!(delete_result.seq_no, Some(4));
    assert_eq!(bulk_result.start_seq_no, Some(5));
    for (term, error) in [
        (index_result.primary_term, &index_result.error),
        (delete_result.primary_term, &delete_result.error),
        (bulk_result.primary_term, &bulk_result.error),
    ] {
        assert_eq!(term, Some(old_term));
        assert!(error.contains("q:") && error.contains("r:"), "{error}");
    }
    let q_after = observe(&nodes[1]).await?;
    let r_after = observe(&nodes[2]).await?;
    assert_canonical(&q_after, old_term, new_term);
    assert_canonical(&r_after, old_term, new_term);
    assert_eq!(q_before.snapshot.wal_entries, q_after.snapshot.wal_entries);
    assert_eq!(r_before.snapshot.wal_entries, r_after.snapshot.wal_entries);
    assert_eq!(q_before.identity, q_after.identity);
    assert_eq!(r_before.identity, r_after.identity);
    assert_eq!(nodes[0].manager.version(), before.version);
    record(
        &mut evidence,
        &mut step,
        "delayed_old_requests_failed_without_target_mutation",
        json!({
            "index_error": index_result.error,
            "delete_error": delete_result.error,
            "bulk_error": bulk_result.error,
            "q": q_after.json(),
            "r": r_after.json(),
        }),
    )
    .await?;

    let shards = nodes[2].shards.clone();
    tokio::task::spawn_blocking(move || {
        shards.close_protocol_trace_shard_for_restart(INDEX, 0);
    })
    .await?;
    assert!(nodes[2].shards.get_shard(INDEX, 0).is_none());
    let reopened = connect(nodes[2].address)
        .await?
        .get_doc(request(ShardGetRequest {
            index_name: INDEX.to_string(),
            shard_id: 0,
            doc_id: "counter".to_string(),
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert!(reopened.found, "{}", reopened.error);
    assert_eq!(
        serde_json::from_slice::<Value>(&reopened.source_json)?,
        json!({"value": 1})
    );
    let r_reopened = observe(&nodes[2]).await?;
    assert_canonical(&r_reopened, old_term, new_term);
    assert_eq!(
        r_after.snapshot.wal_entries,
        r_reopened.snapshot.wal_entries
    );
    assert_eq!(r_after.identity, r_reopened.identity);
    record(
        &mut evidence,
        &mut step,
        "replica_engine_reopened",
        r_reopened.json(),
    )
    .await?;

    partition.isolated.store(false, Ordering::Release);
    wait_applied(&nodes, &[0, 1, 2], current.version).await?;
    let mut metadata = nodes[leader].manager.get_state().indices[INDEX].clone();
    let routing = metadata.shard_routing.get_mut(&0).unwrap();
    routing.replicas.push("p".to_string());
    routing.unassigned_replicas -= 1;
    commit(
        &nodes,
        leader,
        &[0, 1, 2],
        ClusterCommand::UpdateIndex { metadata },
    )
    .await?;
    let replacement = nodes[0]
        .manager
        .get_state()
        .shard_allocation_id(INDEX, 0, "p")
        .unwrap();
    assert_ne!(replacement, old_allocation);
    recover(&nodes, 0).await?;
    let p_recovered = observe(&nodes[0]).await?;
    assert_canonical(&p_recovered, old_term, new_term);
    assert_eq!(p_recovered.identity.allocation_id, replacement);
    assert!(p_recovered.snapshot.wal_entries.is_empty());
    let mut recovered_documents = p_recovered.snapshot.live_documents.clone();
    let mut canonical_documents = q_after.snapshot.live_documents.clone();
    recovered_documents.sort();
    canonical_documents.sort();
    assert_eq!(recovered_documents, canonical_documents);
    record(
        &mut evidence,
        &mut step,
        "old_primary_peer_recovered_under_fresh_allocation",
        p_recovered.json(),
    )
    .await?;
    for node in &nodes {
        node.raft.shutdown().await?;
    }
    println!("Live stale-primary evidence: {}", output.display());
    Ok(())
}
