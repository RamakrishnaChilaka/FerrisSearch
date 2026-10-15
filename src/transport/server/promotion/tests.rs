use super::*;
use crate::cluster::state::{
    FieldMapping, FieldType, IndexMetadata, IndexUuid, NodeInfo, NodeRole,
};
use crate::consensus::types::{ClusterCommand, RaftInstance};
use crate::engine::{
    ClientOperationIntent, ClientOperationRecord, ClientOperationResult, SearchEngine,
};
use crate::shard::AssignedShardOpen;
use serde_json::json;

async fn leader() -> (Arc<RaftInstance>, Arc<ClusterManager>) {
    let (raft, state) = crate::consensus::create_raft_instance_mem(1, "canonical".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !raft.is_leader() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (raft, Arc::new(ClusterManager::with_shared_state(state)))
}

fn service(
    directory: &std::path::Path,
    cluster: Arc<ClusterManager>,
    shards: Arc<ShardManager>,
    raft: Option<Arc<RaftInstance>>,
    local_node: &str,
) -> TransportService {
    TransportService {
        cluster_manager: cluster,
        shard_manager: shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: Arc::new(crate::storage::StorageManager::new_in_path(directory).unwrap()),
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
        raft,
        local_node_id: local_node.into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    }
}

async fn create_index(raft: &RaftInstance, replica_port: Option<u16>) {
    create_index_with_uuid(raft, replica_port, "uuid-1").await;
}

async fn create_index_with_uuid(raft: &RaftInstance, replica_port: Option<u16>, uuid: &str) {
    let mut nodes = vec!["primary".to_string()];
    for (node, port) in [("primary", 0), ("replica", replica_port.unwrap_or(0))] {
        if node == "replica" && replica_port.is_none() {
            continue;
        }
        raft.client_write(ClusterCommand::AddNode {
            node: NodeInfo {
                id: node.into(),
                name: node.into(),
                host: "127.0.0.1".into(),
                transport_port: port,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: if node == "primary" { 1 } else { 2 },
            },
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    }
    if replica_port.is_some() {
        nodes.push("replica".into());
    }
    let mut metadata =
        IndexMetadata::build_shard_routing("idx", 1, u32::from(replica_port.is_some()), &nodes);
    metadata.uuid = IndexUuid::new(uuid);
    metadata.mappings.insert(
        "value".into(),
        FieldMapping {
            field_type: FieldType::Integer,
            dimension: None,
        },
    );
    raft.client_write(ClusterCommand::CreateIndex { metadata })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    raft.client_write(ClusterCommand::SetMaster {
        node_id: "primary".into(),
    })
    .await
    .unwrap()
    .data
    .into_result()
    .unwrap();
}

fn operation(seq_no: u64, value: u64) -> SequencedOperation {
    SequencedOperation {
        seq_no,
        primary_term: 2,
        document_version: seq_no + 1,
        client_operation: None,
        mutation: DocumentMutation::Index {
            doc_id: "doc".into(),
            source: json!({"value": value}),
        },
    }
}

async fn queued_mutation_witness(revoke: bool) {
    let (raft, cluster) = leader().await;
    create_index(&raft, None).await;
    let dir = tempfile::tempdir().unwrap();
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let mut source = service(
        dir.path(),
        cluster.clone(),
        shards.clone(),
        Some(raft.clone()),
        "primary",
    );
    source.worker_pools = crate::worker::WorkerPools::new(1, 1);
    source.ensure_primary_activated("idx", 0).await.unwrap();
    let engine = shards.get_shard("idx", 0).unwrap();
    let epoch = engine.copy_write_state().primary_epoch().unwrap().unwrap();
    let available = epoch.available_admission_slots_for_test();
    let submitted = source.worker_pools.write_submissions_for_test();
    let pools = source.worker_pools.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let blocker = tokio::spawn(async move {
        pools
            .spawn_write(move || {
                let _ = started.send(());
                let _ = blocked.recv();
            })
            .await
            .unwrap();
    });
    ready.await.unwrap();
    let request_service = source.clone();
    let client = tokio::spawn(async move {
        request_service
            .index_doc(crate::transport::request_with_cluster_state_version(
                ShardDocRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    doc_id: "doc".into(),
                    payload_json: serde_json::to_vec(&json!({"value": 5})).unwrap(),
                    ..Default::default()
                },
                0,
            ))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.worker_pools.write_submissions_for_test() < submitted + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(epoch.available_admission_slots_for_test(), available - 1);
    if revoke {
        let state = cluster.get_state();
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary: "primary".into(),
            allocation_id: state.primary_allocation_id("idx", 0).unwrap(),
            expected_term: 2,
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
        release.send(()).unwrap();
        let response = client.await.unwrap().unwrap().into_inner();
        assert!(!response.success);
        assert_eq!(
            response.failure.unwrap().outcome,
            WriteFailureOutcome::NotExecuted as i32
        );
        assert_eq!(response.seq_no, None);
    } else {
        client.abort();
        assert!(client.await.unwrap_err().is_cancelled());
        assert_eq!(epoch.available_admission_slots_for_test(), available - 1);
        assert!(
            source
                .peer_recovery_exclusive_guard("uuid-1", 0)
                .now_or_never()
                .is_none()
        );
        release.send(()).unwrap();
    }
    blocker.await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while epoch.available_admission_slots_for_test() != available {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        source
            .peer_recovery_exclusive_guard("uuid-1", 0)
            .now_or_never()
            .is_some()
    );
    engine.ensure_write_health().unwrap();
    if revoke {
        assert_eq!(engine.sequence_stats().max_seq_no, None);
        assert!(
            engine
                .get_document_with_metadata("doc", true)
                .unwrap()
                .is_none()
        );
        assert!(epoch.is_revoked());
    } else {
        let document = engine
            .get_document_with_metadata("doc", true)
            .unwrap()
            .unwrap();
        assert_eq!(
            (document.seq_no, document.primary_term, document.version),
            (0, 2, 1)
        );
        assert_eq!(document.source["value"], json!(5));
    }
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_activation_runtime_cancelled_client_retains_queued_worker_permit_and_admission_guard()
 {
    queued_mutation_witness(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_activation_runtime_known_authority_change_rejects_queued_mutation_before_wal() {
    queued_mutation_witness(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_activation_runtime_restart_requires_a_fresh_committed_epoch_and_keeps_data() {
    let (raft, cluster) = leader().await;
    create_index(&raft, None).await;
    let dir = tempfile::tempdir().unwrap();
    let first_term = {
        let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let source = service(
            dir.path(),
            cluster.clone(),
            shards.clone(),
            Some(raft.clone()),
            "primary",
        );
        let active = source.ensure_primary_activated("idx", 0).await.unwrap();
        assert_eq!(active.primary_term, 2);
        let engine = shards.get_shard("idx", 0).unwrap();
        engine
            .add_document_with_receipt_at_term("doc", json!({"value": 7}), active.primary_term)
            .unwrap();
        assert_eq!(
            cluster
                .get_state()
                .shard_allocation_ids("idx", 0)
                .unwrap()
                .primary_authority
                .phase,
            PrimaryAuthorityPhase::Active
        );
        active.primary_term
    };
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let restarted = service(
        dir.path(),
        cluster.clone(),
        shards.clone(),
        Some(raft.clone()),
        "primary",
    );
    let active = restarted.ensure_primary_activated("idx", 0).await.unwrap();
    assert_eq!(active.primary_term, first_term + 1);
    let engine = shards.get_shard("idx", 0).unwrap();
    assert_eq!(
        engine.get_document("doc").unwrap().unwrap()["value"],
        json!(7)
    );
    assert_eq!(engine.sequence_stats().persisted_checkpoint, Some(0));
    let history = cluster
        .get_state()
        .shard_allocation_ids("idx", 0)
        .unwrap()
        .primary_authority
        .history
        .clone()
        .unwrap();
    assert_eq!(history.primary_term, active.primary_term);
    assert_eq!(
        shards.copy_identity("idx", 0).unwrap().canonical_history,
        Some(history)
    );
    assert_eq!(
        restarted
            .ensure_primary_activated("idx", 0)
            .await
            .unwrap()
            .primary_term,
        active.primary_term
    );
    raft.shutdown().await.unwrap();
}

async fn peer_reconciliation_witness(conflicting: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (raft, cluster) = leader().await;
    create_index(&raft, Some(port)).await;
    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    let source = service(
        source_dir.path(),
        cluster.clone(),
        source_shards.clone(),
        Some(raft.clone()),
        "primary",
    );
    let first = source.ensure_primary_activated("idx", 0).await.unwrap();
    assert_eq!(first.primary_term, 2);
    let state = cluster.get_state();
    let replica_allocation = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let initial_history = state
        .shard_allocation_ids("idx", 0)
        .unwrap()
        .primary_authority
        .history
        .clone()
        .unwrap();
    let authority = WriteAuthorityIdentity {
        index_uuid: first.index_uuid.clone(),
        shard_id: 0,
        primary_node_id: "primary".into(),
        primary_allocation_id: first.allocation_id,
        primary_term: first.primary_term,
        activation_id: initial_history.activation_id,
    };
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let mappings = state.indices["idx"].mappings.clone();
    let replica = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &mappings,
            &state.indices["idx"].settings,
            "uuid-1",
            AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_shards
        .persist_copy_canonical_history("idx", replica_allocation, &authority, &initial_history)
        .unwrap();
    raft.client_write(ClusterCommand::MarkReplicaInSync {
        index_name: "idx".into(),
        index_uuid: "uuid-1".into(),
        shard_id: 0,
        replica: "replica".into(),
        allocation_id: replica_allocation,
        primary: "primary".into(),
        primary_term: 2,
    })
    .await
    .unwrap()
    .data
    .into_result()
    .unwrap();
    replica_shards.bind_applied_shard_authority(cluster.clone(), "replica".into());
    let replica_service = service(
        replica_dir.path(),
        cluster.clone(),
        replica_shards.clone(),
        None,
        "replica",
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                InternalTransportServer::new(replica_service)
                    .max_decoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(crate::transport::GRPC_MAX_MESSAGE_SIZE),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .unwrap();
    });
    let primary = source_shards.get_shard("idx", 0).unwrap();
    primary.apply_replica_operation(operation(0, 1)).unwrap();
    replica.apply_replica_operation(operation(0, 1)).unwrap();
    let mut adopted = operation(1, 2);
    let intent = ClientOperationIntent::new(
        "original-adopted".into(),
        ClientOperationIntent::fingerprint_json(&json!({"value": 2})).unwrap(),
    )
    .unwrap();
    adopted.client_operation = Some(
        ClientOperationRecord::new(
            intent,
            ClientOperationResult::Index {
                version: 2,
                created: false,
            },
        )
        .unwrap(),
    );
    if conflicting {
        primary.apply_replica_operation(adopted.clone()).unwrap();
        replica.apply_replica_operation(operation(1, 99)).unwrap();
    } else {
        primary.apply_replica_operation(operation(2, 3)).unwrap();
        replica.apply_replica_operation(adopted.clone()).unwrap();
        replica.apply_replica_operation(operation(2, 3)).unwrap();
    }
    let peer_before = replica.peer_recovery_barrier().unwrap().wal_end;
    primary
        .copy_write_state()
        .primary_epoch()
        .unwrap()
        .unwrap()
        .revoke("fresh canonical reconciliation witness");
    let activated = source.ensure_primary_activated("idx", 0).await.unwrap();
    assert_eq!(activated.primary_term, 3);
    let current = cluster.get_state();
    assert_eq!(
        current
            .shard_allocation_ids("idx", 0)
            .unwrap()
            .primary_authority
            .phase,
        PrimaryAuthorityPhase::Active
    );
    let head = primary.peer_recovery_barrier().unwrap().wal_end;
    let inventory = primary.canonical_inventory(head, 1, 2).unwrap();
    assert_eq!(
        primary
            .canonical_operation_at(inventory.operations[0].cursor, head)
            .unwrap(),
        adopted
    );
    if conflicting {
        assert!(
            current.indices["idx"].shard_routing[&0]
                .in_sync_replicas
                .is_empty()
        );
        assert!(current.shard_allocation_id("idx", 0, "replica").is_none());
        assert_eq!(
            current.indices["idx"].shard_routing[&0].unassigned_replicas,
            1
        );
        assert_eq!(
            replica.peer_recovery_barrier().unwrap().wal_end,
            peer_before
        );
        assert_eq!(
            replica.get_document("doc").unwrap().unwrap()["value"],
            json!(99)
        );
    } else {
        assert_eq!(primary.sequence_stats().persisted_checkpoint, Some(2));
        assert_eq!(replica.sequence_stats().persisted_checkpoint, Some(2));
        assert_eq!(
            current.indices["idx"].shard_routing[&0].in_sync_replicas,
            vec!["replica"]
        );
        assert_eq!(
            source_shards
                .copy_identity("idx", 0)
                .unwrap()
                .canonical_history,
            replica_shards
                .copy_identity("idx", 0)
                .unwrap()
                .canonical_history
        );
        assert_eq!(
            primary.get_document("doc").unwrap().unwrap()["value"],
            json!(3)
        );
        let response = source
            .index_doc(crate::transport::request_with_cluster_state_version(
                ShardDocRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    doc_id: "doc".into(),
                    payload_json: serde_json::to_vec(&json!({"value": 4})).unwrap(),
                    ..Default::default()
                },
                current.version,
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
        assert_eq!(response.primary_term, Some(3));
        assert_eq!(response.seq_no, Some(3));
        let actual = replica
            .get_document_with_metadata("doc", true)
            .unwrap()
            .unwrap();
        assert_eq!(
            (actual.seq_no, actual.primary_term, actual.version),
            (3, 3, 4)
        );
        assert_eq!(actual.source["value"], json!(4));
    }
    stop.send(()).unwrap();
    server.await.unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_activation_runtime_adopts_unique_peer_gap_before_active_and_ack() {
    peer_reconciliation_witness(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_activation_runtime_excludes_conflicting_copy_before_repair_without_rewriting_it()
{
    peer_reconciliation_witness(true).await;
}

struct LiveReplicationFixture {
    _source_dir: tempfile::TempDir,
    _replica_dir: tempfile::TempDir,
    raft: Arc<RaftInstance>,
    cluster: Arc<ClusterManager>,
    source: TransportService,
    target: TransportService,
    replica: Arc<dyn SearchEngine>,
    allocation_id: u64,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

struct PreparedRuntimeWrite {
    primary: ActivatedPrimary,
    engine: Arc<dyn SearchEngine>,
    captured: crate::cluster::state::ClusterState,
    guard: Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
    primary_sequence: crate::engine::SequenceStats,
    replication: crate::replication::ReplicationResult,
}

impl PreparedRuntimeWrite {
    async fn new(fixture: &LiveReplicationFixture, value: u64) -> Self {
        let primary = fixture
            .source
            .ensure_primary_activated("idx", 0)
            .await
            .unwrap();
        let guard = fixture
            .source
            .peer_recovery_write_guard("idx", 0)
            .await
            .unwrap();
        let captured = fixture
            .source
            .validated_primary_write_state("idx", 0, &primary)
            .unwrap();
        let engine = fixture.source.shard_manager.get_shard("idx", 0).unwrap();
        let payload = json!({"value": value});
        let receipt = engine
            .add_document_with_receipt_at_term("ack-doc", payload.clone(), primary.primary_term)
            .unwrap();
        let primary_sequence = engine.sequence_stats();
        let replication = crate::replication::replicate_write_with_durability(
            &fixture.source.transport_client,
            &captured,
            "idx",
            0,
            "ack-doc",
            &payload,
            "index",
            receipt.seq_no,
            receipt.version,
            None,
            receipt.primary_term,
            fixture.source.shard_manager.durability(),
            engine.global_checkpoint(),
        )
        .await;
        Self {
            primary,
            engine,
            captured,
            guard,
            primary_sequence,
            replication,
        }
    }

    async fn acknowledge(
        self,
        source: &TransportService,
        deadline: crate::transport::refresh_deadline::RefreshDeadline,
    ) -> Result<ShardWriteAcknowledgement, String> {
        source
            .acknowledge_replicated_write(
                super::super::write_acknowledgement::WriteAcknowledgementContext {
                    index: "idx",
                    shard_id: 0,
                    engine: &self.engine,
                    primary: &self.primary,
                    captured: &self.captured,
                    guard: &self.guard,
                    primary_sequence: self.primary_sequence,
                    deadline,
                },
                self.replication,
            )
            .await
    }
}

fn acknowledgement_deadline() -> crate::transport::refresh_deadline::RefreshDeadline {
    crate::transport::refresh_deadline::RefreshDeadline::from_request(&Request::new(())).unwrap()
}

fn primary_index_request(id: &str, value: u64) -> Request<ShardDocRequest> {
    crate::transport::request_with_cluster_state_version(
        ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: id.into(),
            payload_json: serde_json::to_vec(&json!({"value": value})).unwrap(),
            ..Default::default()
        },
        0,
    )
}

async fn make_replica_unreachable(fixture: &LiveReplicationFixture) {
    let mut replica = fixture.cluster.get_state().nodes["replica"].clone();
    replica.transport_port = 0;
    fixture
        .raft
        .client_write(ClusterCommand::AddNode { node: replica })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_runtime_publishes_global_progress_durably_and_piggybacks_without_quorum()
 {
    let fixture = LiveReplicationFixture::new().await;
    let first = PreparedRuntimeWrite::new(&fixture, 1).await;
    let engine = first.engine.clone();
    assert!(first.replication.is_ok());
    let report = first
        .acknowledge(&fixture.source, acknowledgement_deadline())
        .await
        .unwrap();
    assert_eq!(report.copies.len(), 2);
    assert_eq!(engine.global_checkpoint(), Some(0));
    assert_eq!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        Some(0)
    );
    assert_eq!(
        fixture
            .target
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        None
    );
    fixture.raft.shutdown().await.unwrap();
    let second = PreparedRuntimeWrite::new(&fixture, 2).await;
    assert!(second.replication.is_ok());
    assert_eq!(
        fixture
            .target
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        Some(0)
    );
    second
        .acknowledge(&fixture.source, acknowledgement_deadline())
        .await
        .unwrap();
    assert_eq!(engine.global_checkpoint(), Some(1));
    let identity: crate::shard::ShardCopyIdentity = serde_json::from_slice(
        &std::fs::read(
            fixture
                ._source_dir
                .path()
                .join("uuid-1/shard_0")
                .join(crate::shard::SHARD_COPY_IDENTITY_FILE),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(identity.safe_global_checkpoint, Some(1));
    let replica_identity: crate::shard::ShardCopyIdentity = serde_json::from_slice(
        &std::fs::read(
            fixture
                ._replica_dir
                .path()
                .join("uuid-1/shard_0")
                .join(crate::shard::SHARD_COPY_IDENTITY_FILE),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(replica_identity.safe_global_checkpoint, Some(0));
    fixture.stop.send(()).unwrap();
    fixture.server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_runtime_partial_fanout_keeps_positive_proof_for_floor_two() {
    let fixture = LiveReplicationFixture::new().await;
    fixture
        .raft
        .client_write(ClusterCommand::AddNode {
            node: NodeInfo {
                id: "unreachable".into(),
                name: "unreachable".into(),
                host: "127.0.0.1".into(),
                transport_port: 0,
                http_port: 0,
                roles: vec![NodeRole::Data],
                raft_node_id: 3,
            },
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let mut metadata = fixture.cluster.get_state().indices["idx"].clone();
    metadata.number_of_replicas = 2;
    metadata.settings.minimum_durable_copies = 2;
    metadata
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .replicas
        .push("unreachable".into());
    fixture
        .raft
        .client_write(ClusterCommand::UpdateIndex { metadata })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let state = fixture.cluster.get_state();
    let allocation_id = state.shard_allocation_id("idx", 0, "unreachable").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let unavailable_copy = Arc::new(ShardManager::new(directory.path(), Duration::from_secs(60)));
    unavailable_copy
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &state.indices["idx"].mappings,
            &state.indices["idx"].settings,
            "uuid-1",
            AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let envelope = crate::replication::capture_replica_authority(&state, "idx", 0).unwrap();
    let authority = live_replication::decode_authority(Some(&envelope), "uuid-1", 0).unwrap();
    unavailable_copy
        .persist_copy_canonical_history(
            "idx",
            allocation_id,
            &authority,
            state
                .shard_allocation_ids("idx", 0)
                .unwrap()
                .primary_authority
                .history
                .as_ref()
                .unwrap(),
        )
        .unwrap();
    fixture
        .raft
        .client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            replica: "unreachable".into(),
            allocation_id,
            primary: "primary".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let prepared = PreparedRuntimeWrite::new(&fixture, 7).await;
    let failure = prepared.replication.as_ref().unwrap_err();
    assert_eq!(failure.acknowledged.len(), 1);
    assert_eq!(failure.acknowledged[0].node_id, "replica");
    assert_eq!(failure.failures.len(), 1);
    assert_eq!(failure.failures[0].node_id, "unreachable");
    let report = prepared
        .acknowledge(&fixture.source, acknowledgement_deadline())
        .await
        .unwrap();
    assert_eq!(report.copies.len(), 3);
    assert_eq!(
        report
            .copies
            .iter()
            .filter(|copy| matches!(
                copy.outcome,
                Some(crate::transport::proto::shard_write_copy_result::Outcome::Acknowledged(true))
            ))
            .count(),
        2
    );
    let failed = report
        .copies
        .iter()
        .find(|copy| copy.node_id == "unreachable")
        .unwrap();
    assert_eq!(failed.allocation_id, allocation_id);
    assert!(matches!(&failed.outcome,
        Some(crate::transport::proto::shard_write_copy_result::Outcome::Error(error))
        if error.contains("committed authority")
    ));
    let state = fixture.cluster.get_state();
    assert_eq!(
        state.indices["idx"].shard_routing[&0].in_sync_replicas,
        vec!["replica"]
    );
    assert!(state.shard_allocation_id("idx", 0, "unreachable").is_none());
    assert_eq!(
        fixture.replica.sequence_stats().persisted_checkpoint,
        Some(0)
    );
    assert!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .pending_exclusions
            .is_empty()
    );
    let next = PreparedRuntimeWrite::new(&fixture, 8)
        .await
        .acknowledge(&fixture.source, acknowledgement_deadline())
        .await
        .unwrap();
    assert_eq!(next.copies.len(), 2);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_runtime_exclusion_cannot_bypass_floor_two() {
    let fixture = LiveReplicationFixture::new().await;
    fixture
        .source
        .update_settings(Request::new(UpdateSettingsRequest {
            index_name: "idx".into(),
            settings_json: serde_json::to_vec(&json!({
                "index": {"minimum_durable_copies": 2}
            }))
            .unwrap(),
        }))
        .await
        .unwrap();
    make_replica_unreachable(&fixture).await;
    let prepared = PreparedRuntimeWrite::new(&fixture, 7).await;
    let engine = prepared.engine.clone();
    let error = prepared
        .acknowledge(&fixture.source, acknowledgement_deadline())
        .await
        .unwrap_err();
    assert!(error.contains("minimum_durable_copies"), "{error}");
    let state = fixture.cluster.get_state();
    assert!(
        state.indices["idx"].shard_routing[&0]
            .in_sync_replicas
            .is_empty()
    );
    assert!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .pending_exclusions
            .is_empty()
    );
    let response = fixture
        .source
        .index_doc(crate::transport::request_with_cluster_state_version(
            ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "later".into(),
                payload_json: serde_json::to_vec(&json!({"value": 9})).unwrap(),
                ..Default::default()
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    assert_eq!(engine.wal_max_seq_no(), Some(0));
    engine.ensure_write_health().unwrap();
    fixture.finish().await;
}

#[test]
fn write_acknowledgement_runtime_cancellation_before_journal_keeps_debt_and_admission_guard() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = LiveReplicationFixture::new().await;
            make_replica_unreachable(&fixture).await;
            let prepared = PreparedRuntimeWrite::new(&fixture, 7).await;
            let epoch = prepared
                .engine
                .copy_write_state()
                .primary_epoch()
                .unwrap()
                .unwrap();
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            let source = fixture.source.clone();
            let request = tokio::spawn(async move {
                prepared
                    .acknowledge(&source, acknowledgement_deadline())
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while epoch.permit().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(
                fixture
                    .source
                    .shard_manager
                    .copy_identity("idx", 0)
                    .unwrap()
                    .pending_exclusions
                    .is_empty()
            );
            assert!(
                fixture
                    .source
                    .ensure_recovery_admission_permitted("idx", 0, 2)
                    .is_err()
            );
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled());
            assert!(
                fixture
                    .source
                    .peer_recovery_exclusive_guard("uuid-1", 0)
                    .now_or_never()
                    .is_none()
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let state = fixture.cluster.get_state();
                    if state.indices["idx"].shard_routing[&0]
                        .in_sync_replicas
                        .is_empty()
                        && epoch.permit().is_ok()
                        && fixture
                            .source
                            .peer_recovery_exclusive_guard("uuid-1", 0)
                            .now_or_never()
                            .is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                fixture
                    .source
                    .shard_manager
                    .copy_identity("idx", 0)
                    .unwrap()
                    .pending_exclusions
                    .is_empty()
            );
            fixture
                .source
                .ensure_recovery_admission_permitted("idx", 0, 2)
                .unwrap();
            fixture.finish().await;
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minimum_durable_copies_runtime_all_write_surfaces_reject_before_wal_but_activation_can_recover()
 {
    let (raft, cluster) = leader().await;
    create_index(&raft, None).await;
    let mut metadata = cluster.get_state().indices["idx"].clone();
    metadata.number_of_replicas = 1;
    metadata.settings.minimum_durable_copies = 2;
    metadata
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .unassigned_replicas = 1;
    raft.client_write(ClusterCommand::UpdateIndex { metadata })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let source = service(
        dir.path(),
        cluster,
        shards.clone(),
        Some(raft.clone()),
        "primary",
    );
    let active = source.ensure_primary_activated("idx", 0).await.unwrap();
    assert_eq!(active.primary_term, 2);
    let engine = shards.get_shard("idx", 0).unwrap();
    let response = source
        .index_doc(crate::transport::request_with_cluster_state_version(
            ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
                ..Default::default()
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!(response.seq_no, None);
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    assert!(response.error.contains("minimum_durable_copies"));
    let response = source
        .bulk_index(crate::transport::request_with_cluster_state_version(
            ShardBulkRequest {
                index_name: "idx".into(),
                shard_id: 0,
                documents_json: vec![serde_json::to_vec(&json!({"value": 2})).unwrap()],
                ..Default::default()
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!(response.start_seq_no, None);
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    let response = source
        .delete_doc(crate::transport::request_with_cluster_state_version(
            ShardDeleteRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                ..Default::default()
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!(response.seq_no, None);
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    let status = source
        .record_client_no_op(crate::transport::request_with_cluster_state_version(
            ShardClientNoOpRequest {
                index_name: "idx".into(),
                shard_id: 0,
                client_operation: Some(crate::transport::client_operation::intent_to_proto(
                    &ClientOperationIntent::new("noop".into(), "a".repeat(64)).unwrap(),
                )),
                result_seq_no: 0,
                result_primary_term: 2,
                result_version: 1,
                index_uuid: Some("uuid-1".into()),
            },
            0,
        ))
        .await
        .unwrap_err();
    let failure = WriteFailure::from_rpc_status(status);
    assert_eq!(failure.outcome, WriteFailureOutcome::NotExecuted);
    assert!(failure.reason.contains("minimum_durable_copies"));
    assert_eq!(engine.wal_max_seq_no(), None);
    engine.ensure_write_health().unwrap();
    assert_eq!(
        source
            .ensure_primary_activated("idx", 0)
            .await
            .unwrap()
            .primary_term,
        2
    );
    let response = source
        .update_settings(Request::new(UpdateSettingsRequest {
            index_name: "idx".into(),
            settings_json: serde_json::to_vec(&json!({
                "index": {"minimum_durable_copies": 1}
            }))
            .unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.acknowledged, "{}", response.error);
    let response = source
        .index_doc(crate::transport::request_with_cluster_state_version(
            ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                payload_json: serde_json::to_vec(&json!({"value": 3})).unwrap(),
                ..Default::default()
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
    raft.shutdown().await.unwrap();
}

impl LiveReplicationFixture {
    async fn new() -> Self {
        Self::with_persistent_raft(false).await
    }

    async fn with_persistent_raft(persistent: bool) -> Self {
        Self::with_persistent_raft_and_uuid(persistent, "uuid-1").await
    }

    async fn with_persistent_raft_and_uuid(persistent: bool, uuid: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let (raft, cluster) = if persistent {
            let (raft, state) = crate::consensus::create_raft_instance(
                1,
                "canonical".into(),
                source_dir.path().to_str().unwrap(),
            )
            .await
            .unwrap();
            crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while !raft.is_leader() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            (raft, Arc::new(ClusterManager::with_shared_state(state)))
        } else {
            leader().await
        };
        create_index_with_uuid(&raft, Some(listener.local_addr().unwrap().port()), uuid).await;
        let source_shards = Arc::new(ShardManager::new(
            source_dir.path(),
            Duration::from_secs(60),
        ));
        let source = service(
            source_dir.path(),
            cluster.clone(),
            source_shards.clone(),
            Some(raft.clone()),
            "primary",
        );
        source.ensure_primary_activated("idx", 0).await.unwrap();
        let state = cluster.get_state();
        let allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
        let envelope = crate::replication::capture_replica_authority(&state, "idx", 0).unwrap();
        let authority = live_replication::decode_authority(Some(&envelope), uuid, 0).unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let replica_shards = Arc::new(ShardManager::new(
            replica_dir.path(),
            Duration::from_secs(60),
        ));
        let replica = replica_shards
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &state.indices["idx"].mappings,
                &state.indices["idx"].settings,
                uuid,
                AssignedShardOpen {
                    allocation_id,
                    primary_term: authority.primary_term,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        replica_shards
            .persist_copy_canonical_history(
                "idx",
                allocation_id,
                &authority,
                state
                    .shard_allocation_ids("idx", 0)
                    .unwrap()
                    .primary_authority
                    .history
                    .as_ref()
                    .unwrap(),
            )
            .unwrap();
        raft.client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: uuid.into(),
            shard_id: 0,
            replica: "replica".into(),
            allocation_id,
            primary: "primary".into(),
            primary_term: authority.primary_term,
        })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
        replica_shards.bind_applied_shard_authority(cluster.clone(), "replica".into());
        let target = service(
            replica_dir.path(),
            cluster.clone(),
            replica_shards,
            None,
            "replica",
        );
        let server_service = target.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(InternalTransportServer::new(server_service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        Self {
            _source_dir: source_dir,
            _replica_dir: replica_dir,
            raft,
            cluster,
            source,
            target,
            replica,
            allocation_id,
            stop,
            server,
        }
    }

    fn request(&self, seq_no: u64) -> ReplicateDocRequest {
        ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            index_uuid: "uuid-1".into(),
            target_allocation_id: Some(self.allocation_id),
            primary_term: Some(2),
            doc_id: "doc".into(),
            payload_json: serde_json::to_vec(&json!({"value": seq_no + 1})).unwrap(),
            op: "index".into(),
            seq_no,
            document_version: Some(seq_no + 1),
            client_operation: None,
            authority: Some(
                crate::replication::capture_replica_authority(&self.cluster.get_state(), "idx", 0)
                    .unwrap(),
            ),
        }
    }

    async fn reserve_next_term(&self) {
        let state = self.cluster.get_state();
        self.raft
            .client_write(ClusterCommand::ActivatePrimary {
                index_name: "idx".into(),
                index_uuid: "uuid-1".into(),
                shard_id: 0,
                primary: "primary".into(),
                allocation_id: state.primary_allocation_id("idx", 0).unwrap(),
                expected_term: state.indices["idx"].shard_routing[&0].primary_term,
            })
            .await
            .unwrap()
            .data
            .into_result()
            .unwrap();
    }

    async fn finish(self) {
        self.stop.send(()).unwrap();
        self.server.await.unwrap();
        self.raft.shutdown().await.unwrap();
    }
}

#[cfg(feature = "protocol-trace")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_authority_protocol_trace_runtime_canonical_and_observed_exclusion() {
    let directory = tempfile::tempdir().unwrap();
    let output = std::env::var_os("FS013_AUTHORITY_EVIDENCE_OUTPUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| directory.path().join("authority.json"));
    let uuid = uuid::Uuid::new_v4().to_string();
    let trace = crate::protocol_trace::authority::start_serialized_witness(
        crate::protocol_trace::authority::Config {
            output: output.clone(),
            run_id: uuid::Uuid::new_v4().to_string(),
            index_uuid: uuid.clone(),
            shard: 0,
            nodes: vec!["primary".into(), "replica".into()],
        },
    )
    .unwrap();
    let fixture = LiveReplicationFixture::with_persistent_raft_and_uuid(false, &uuid).await;
    let first = fixture
        .source
        .index_doc(primary_index_request("before-reactivation", 1))
        .await
        .unwrap()
        .into_inner();
    assert!(first.success, "{}", first.error);
    fixture
        .source
        .shard_manager
        .get_shard("idx", 0)
        .unwrap()
        .copy_write_state()
        .primary_epoch()
        .unwrap()
        .unwrap()
        .revoke("serialized canonical reactivation witness");
    assert_eq!(
        fixture
            .source
            .ensure_primary_activated("idx", 0)
            .await
            .unwrap()
            .primary_term,
        3
    );
    let second = fixture
        .source
        .index_doc(primary_index_request("after-reactivation", 2))
        .await
        .unwrap()
        .into_inner();
    assert!(second.success, "{}", second.error);
    make_replica_unreachable(&fixture).await;
    let degraded = fixture
        .source
        .index_doc(primary_index_request("after-exclusion", 3))
        .await
        .unwrap()
        .into_inner();
    assert!(degraded.success, "{}", degraded.error);
    assert_eq!(degraded.write_acknowledgement.unwrap().copies.len(), 2);
    assert!(
        fixture.cluster.get_state().indices["idx"]
            .in_sync_replica_nodes(0)
            .is_empty()
    );
    fixture
        .source
        .ensure_recovery_admission_permitted("idx", 0, 3)
        .unwrap();
    fixture.finish().await;
    trace.finish().unwrap();
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let events = document["events"].as_array().unwrap();
    for expected in [
        "canonical_promoting",
        "canonical_frozen",
        "canonical_sealed",
        "canonical_active",
        "exclusion_closed",
        "exclusion_journaled",
        "exclusion_proposed",
        "exclusion_committed",
        "exclusion_settled",
        "write_certified",
    ] {
        assert!(
            events.iter().any(|event| event["event"] == expected),
            "{expected} is absent"
        );
    }
    let checked = std::process::Command::new("python3")
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("scripts/tla/check_write_authority_trace.py"),
        )
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    eprintln!(
        "FS013 serialized canonical/exclusion authority witness: {}",
        output.display()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_handler_healthy_copies_ack_without_metadata_quorum() {
    let fixture = LiveReplicationFixture::new().await;
    let version = fixture.cluster.get_state().version;
    fixture.raft.shutdown().await.unwrap();
    let response = fixture
        .source
        .index_doc(primary_index_request("healthy", 7))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
    let report = response.write_acknowledgement.unwrap();
    assert_eq!(report.copies.len(), 2);
    assert!(report.copies.iter().all(|copy| matches!(
        copy.outcome,
        Some(crate::transport::proto::shard_write_copy_result::Outcome::Acknowledged(true))
    )));
    assert_eq!(fixture.cluster.get_state().version, version);
    assert_eq!(
        fixture
            .replica
            .get_document_with_metadata("healthy", true)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 7})
    );
    assert_eq!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        Some(0)
    );
    fixture.stop.send(()).unwrap();
    fixture.server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_handler_exact_operation_above_gap_does_not_require_a_prefix() {
    let fixture = LiveReplicationFixture::new().await;
    let engine = fixture.source.shard_manager.get_shard("idx", 0).unwrap();
    let missing = engine
        .add_document_with_receipt_at_term("missing", json!({"value": 0}), 2)
        .unwrap();
    assert_eq!(missing.seq_no, 0);
    let response = fixture
        .source
        .index_doc(primary_index_request("above-gap", 1))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!(response.seq_no, Some(1));
    let report = response.write_acknowledgement.unwrap();
    assert_eq!(report.copies.len(), 2);
    assert!(report.copies.iter().all(|copy| matches!(
        copy.outcome,
        Some(crate::transport::proto::shard_write_copy_result::Outcome::Acknowledged(true))
    )));
    assert_eq!(fixture.replica.sequence_stats().max_seq_no, Some(1));
    assert_eq!(fixture.replica.sequence_stats().persisted_checkpoint, None);
    assert_eq!(
        fixture
            .replica
            .get_document_with_metadata("above-gap", true)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 1})
    );
    assert_eq!(engine.global_checkpoint(), None);
    assert_eq!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        None
    );
    crate::replication::replicate_write_with_durability(
        &fixture.source.transport_client,
        &fixture.cluster.get_state(),
        "idx",
        0,
        "missing",
        &json!({"value": 0}),
        "index",
        missing.seq_no,
        missing.version,
        None,
        missing.primary_term,
        fixture.source.shard_manager.durability(),
        None,
    )
    .await
    .unwrap();
    let response = fixture
        .source
        .index_doc(primary_index_request("after-gap", 2))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!(response.seq_no, Some(2));
    assert_eq!(
        fixture.replica.sequence_stats().persisted_checkpoint,
        Some(2)
    );
    assert_eq!(engine.global_checkpoint(), Some(2));
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_handler_global_publication_io_failure_is_indeterminate_and_sticky() {
    let fixture = LiveReplicationFixture::new().await;
    let engine = fixture.source.shard_manager.get_shard("idx", 0).unwrap();
    let temporary = fixture
        ._source_dir
        .path()
        .join("uuid-1/shard_0")
        .join(format!("{}.tmp", crate::shard::SHARD_COPY_IDENTITY_FILE));
    std::fs::create_dir(&temporary).unwrap();
    let response = fixture
        .source
        .index_doc(primary_index_request("metadata-failed", 1))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::Indeterminate as i32
    );
    assert!(
        response.error.contains("Is a directory"),
        "{}",
        response.error
    );
    assert!(response.write_acknowledgement.is_none());
    assert!(engine.ensure_write_health().is_err());
    assert_eq!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .safe_global_checkpoint,
        None
    );
    assert_eq!(
        fixture.replica.sequence_stats().persisted_checkpoint,
        Some(0)
    );
    let read_error = fixture
        .source
        .get_doc(crate::transport::request_with_cluster_state_version(
            ShardGetRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "metadata-failed".into(),
                realtime: Some(true),
            },
            0,
        ))
        .await
        .unwrap_err();
    assert_eq!(read_error.code(), tonic::Code::Aborted, "{read_error}");
    assert!(
        read_error
            .message()
            .contains("copy authority publication failed")
    );
    std::fs::remove_dir(temporary).unwrap();
    let before = engine.sequence_stats();
    let rejected = fixture
        .source
        .index_doc(primary_index_request("not-executed", 2))
        .await
        .unwrap()
        .into_inner();
    assert!(!rejected.success);
    assert_eq!(
        rejected.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    assert_eq!(rejected.seq_no, None);
    assert_eq!(engine.sequence_stats(), before);
    assert!(engine.ensure_write_health().is_err());
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_acknowledgement_handler_deadline_debt_survives_disk_raft_and_copy_restart() {
    let fixture = LiveReplicationFixture::with_persistent_raft(true).await;
    make_replica_unreachable(&fixture).await;
    let engine = fixture.source.shard_manager.get_shard("idx", 0).unwrap();
    let epoch = engine.copy_write_state().primary_epoch().unwrap().unwrap();
    fixture.raft.shutdown().await.unwrap();
    let mut request = primary_index_request("uncertain", 1);
    request.set_timeout(Duration::from_millis(300));
    let failed = fixture
        .source
        .index_doc(request)
        .await
        .unwrap()
        .into_inner();
    assert!(!failed.success);
    assert_eq!((failed.seq_no, failed.primary_term), (Some(0), Some(2)));
    assert_eq!(
        failed.failure.unwrap().outcome,
        WriteFailureOutcome::Indeterminate as i32
    );
    assert!(
        failed.error.contains("exclusion remains sticky"),
        "{}",
        failed.error
    );
    assert!(failed.write_acknowledgement.is_none());
    let identity_path = fixture
        ._source_dir
        .path()
        .join("uuid-1/shard_0")
        .join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let identity: crate::shard::ShardCopyIdentity =
        serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(identity.pending_exclusions.len(), 1);
    let pending = &identity.pending_exclusions[0];
    assert_eq!(pending.authority, epoch.identity);
    assert_eq!(
        pending.failed_copies[0].allocation_id,
        fixture.allocation_id
    );
    assert!(epoch.permit().is_err());
    assert!(engine.ensure_write_health().is_ok());
    assert!(
        fixture
            .source
            .ensure_recovery_admission_permitted("idx", 0, 2)
            .is_err()
    );
    assert!(
        fixture
            .source
            .peer_recovery_exclusive_guard("uuid-1", 0)
            .now_or_never()
            .is_none()
    );
    let before = engine.sequence_stats();
    let rejected = tokio::time::timeout(
        Duration::from_secs(12),
        fixture
            .source
            .index_doc(primary_index_request("blocked-by-debt", 2)),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert!(!rejected.success);
    assert_eq!(
        rejected.failure.unwrap().outcome,
        WriteFailureOutcome::NotExecuted as i32
    );
    assert_eq!(rejected.seq_no, None);
    assert!(rejected.error.contains("exclusion"), "{}", rejected.error);
    assert_eq!(engine.sequence_stats(), before);
    assert!(
        fixture
            .source
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .pending_exclusions
            .iter()
            .any(|entry| entry.transition_id == pending.transition_id)
    );
    drop(
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture.source.peer_recovery_exclusive_guard("uuid-1", 0),
        )
        .await
        .unwrap(),
    );
    fixture.stop.send(()).unwrap();
    fixture.server.await.unwrap();
    let directory = fixture._source_dir;
    drop(fixture.target);
    drop(fixture.replica);
    drop(fixture.source);
    drop(fixture.cluster);
    drop(fixture.raft);
    drop(engine);
    drop(epoch);

    let (raft, applied) = crate::consensus::create_raft_instance(
        1,
        "canonical".into(),
        directory.path().to_str().unwrap(),
    )
    .await
    .unwrap();
    let cluster = Arc::new(ClusterManager::with_shared_state(applied));
    tokio::time::timeout(Duration::from_secs(15), async {
        while !raft.is_leader() || !cluster.get_state().indices.contains_key("idx") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let shards = Arc::new(ShardManager::new(directory.path(), Duration::from_secs(60)));
    let restarted = service(
        directory.path(),
        cluster.clone(),
        shards.clone(),
        Some(raft.clone()),
        "primary",
    );
    let active = restarted.ensure_primary_activated("idx", 0).await.unwrap();
    assert_eq!(active.primary_term, 3);
    let recovered = shards.get_shard("idx", 0).unwrap();
    assert_eq!(
        recovered
            .get_document_with_metadata("uncertain", true)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 1})
    );
    assert!(
        shards
            .copy_identity("idx", 0)
            .unwrap()
            .pending_exclusions
            .is_empty()
    );
    assert!(
        cluster.get_state().indices["idx"]
            .in_sync_replica_nodes(0)
            .is_empty()
    );
    let response = restarted
        .index_doc(primary_index_request("after-restart", 3))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.primary_term), (Some(1), Some(3)));
    assert_eq!(response.write_acknowledgement.unwrap().copies.len(), 1);
    assert!(
        recovered
            .get_document_with_metadata("blocked-by-debt", true)
            .unwrap()
            .is_none()
    );
    raft.shutdown().await.unwrap();
}

#[test]
fn write_acknowledgement_runtime_global_publication_timeout_retains_worker_authority_and_guard() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = LiveReplicationFixture::new().await;
            let prepared = PreparedRuntimeWrite::new(&fixture, 1).await;
            assert!(prepared.replication.is_ok());
            let engine = prepared.engine.clone();
            let epoch = engine.copy_write_state().primary_epoch().unwrap().unwrap();
            let available_during_write = epoch.available_admission_slots_for_test();
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            let mut budget = Request::new(());
            budget.set_timeout(Duration::from_millis(200));
            let error = prepared
                .acknowledge(
                    &fixture.source,
                    crate::transport::refresh_deadline::RefreshDeadline::from_request(&budget)
                        .unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                error.contains("during durable global progress publication"),
                "{error}"
            );
            assert_eq!(engine.global_checkpoint(), None);
            assert_eq!(
                fixture
                    .source
                    .shard_manager
                    .copy_identity("idx", 0)
                    .unwrap()
                    .safe_global_checkpoint,
                None
            );
            assert_eq!(
                epoch.available_admission_slots_for_test(),
                available_during_write
            );
            assert!(
                fixture
                    .source
                    .peer_recovery_exclusive_guard("uuid-1", 0)
                    .now_or_never()
                    .is_none()
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fixture
                        .source
                        .shard_manager
                        .copy_identity("idx", 0)
                        .unwrap()
                        .safe_global_checkpoint
                        == Some(0)
                        && epoch.available_admission_slots_for_test() == available_during_write + 1
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                fixture
                    .source
                    .peer_recovery_exclusive_guard("uuid-1", 0)
                    .now_or_never()
                    .is_some()
            );
            let identity: crate::shard::ShardCopyIdentity = serde_json::from_slice(
                &std::fs::read(
                    fixture
                        ._source_dir
                        .path()
                        .join("uuid-1/shard_0")
                        .join(crate::shard::SHARD_COPY_IDENTITY_FILE),
                )
                .unwrap(),
            )
            .unwrap();
            assert_eq!(identity.safe_global_checkpoint, Some(0));
            assert!(engine.ensure_write_health().is_ok());
            let next = fixture
                .source
                .index_doc(primary_index_request("after-timeout", 2))
                .await
                .unwrap()
                .into_inner();
            assert!(next.success, "{}", next.error);
            assert_eq!(next.seq_no, Some(1));
            fixture.finish().await;
        });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_replication_runtime_rejects_missing_or_foreign_authority_before_wal() {
    let fixture = LiveReplicationFixture::new().await;
    let initial_fence = fixture
        .target
        .shard_manager
        .copy_identity("idx", 0)
        .unwrap()
        .replica_fence;
    for variant in 0..7 {
        let mut request = fixture.request(0);
        match variant {
            0 => request.authority = None,
            1 => request.authority.as_mut().unwrap().primary_node_id = "other".into(),
            2 => request.authority.as_mut().unwrap().primary_allocation_id += 1,
            3 => request.authority.as_mut().unwrap().activation_id += 1,
            4 => request.authority.as_mut().unwrap().primary_term += 1,
            5 => request.authority.as_mut().unwrap().captured_state_version = 0,
            6 => request.primary_term = Some(3),
            _ => unreachable!(),
        }
        match fixture.target.replicate_doc(Request::new(request)).await {
            Ok(response) => assert!(!response.into_inner().success, "variant {variant}"),
            Err(error) => assert_eq!(error.code(), tonic::Code::InvalidArgument),
        }
        assert_eq!(fixture.replica.wal_max_seq_no(), None, "variant {variant}");
        fixture.replica.ensure_write_health().unwrap();
        assert_eq!(
            fixture
                .target
                .shard_manager
                .copy_identity("idx", 0)
                .unwrap()
                .replica_fence,
            initial_fence,
        );
    }
    let mut operation = fixture.request(0);
    let envelope = operation.authority.clone();
    operation.authority.as_mut().unwrap().activation_id += 1;
    let error = fixture
        .target
        .replicate_bulk(Request::new(ReplicateBulkRequest {
            index_name: "idx".into(),
            shard_id: 0,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(fixture.allocation_id),
            authority: envelope,
            ops: vec![operation],
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("identity"));
    assert_eq!(fixture.replica.wal_max_seq_no(), None);
    fixture.replica.ensure_write_health().unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_replication_runtime_known_promoting_authority_rejects_without_mutation() {
    let fixture = LiveReplicationFixture::new().await;
    let old = fixture.request(0);
    fixture.reserve_next_term().await;
    let response = fixture
        .target
        .replicate_doc(Request::new(old.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert!(!response.operation_persisted && !response.operation_processed);
    let mut promoting = old;
    let state = fixture.cluster.get_state();
    let envelope = promoting.authority.as_mut().unwrap();
    envelope.primary_term = 3;
    envelope.activation_id = state
        .shard_allocation_ids("idx", 0)
        .unwrap()
        .primary_authority
        .activation_id
        .unwrap();
    envelope.captured_state_version = state.version;
    let response = fixture
        .target
        .replicate_doc(Request::new(promoting))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!(fixture.replica.wal_max_seq_no(), None);
    assert_eq!(
        fixture
            .target
            .shard_manager
            .copy_identity("idx", 0)
            .unwrap()
            .replica_fence,
        2
    );
    fixture.replica.ensure_write_health().unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_replication_runtime_authority_change_after_apply_revokes_proof_not_disk_health() {
    let fixture = LiveReplicationFixture::new().await;
    let request = fixture.request(0);
    let authority =
        live_replication::decode_authority(request.authority.as_ref(), "uuid-1", 0).unwrap();
    let shards = fixture.target.shard_manager.clone();
    let allocation_id = fixture.allocation_id;
    let raft = fixture.raft.clone();
    let primary_allocation = authority.primary_allocation_id;
    let runtime = tokio::runtime::Handle::current();
    let error = tokio::task::spawn_blocking(move || {
        shards.apply_replica_operation(
            "idx",
            0,
            crate::shard::ReplicaApplyContext {
                index_uuid: "uuid-1",
                allocation_id,
                applied_view_term: 2,
                message_term: 2,
                authority: Some(&authority),
            },
            |engine| {
                let receipt = engine.apply_replica_operation(operation(0, 7))?;
                assert!(receipt.operation_persisted);
                runtime
                    .block_on(raft.client_write(ClusterCommand::ActivatePrimary {
                        index_name: "idx".into(),
                        index_uuid: "uuid-1".into(),
                        shard_id: 0,
                        primary: "primary".into(),
                        allocation_id: primary_allocation,
                        expected_term: 2,
                    }))
                    .unwrap()
                    .data
                    .into_result()
                    .unwrap();
                Ok(receipt)
            },
        )
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("authority"), "{error:#}");
    assert_eq!(
        fixture.replica.sequence_stats().persisted_checkpoint,
        Some(0)
    );
    fixture.replica.ensure_write_health().unwrap();
    let authority =
        live_replication::decode_authority(request.authority.as_ref(), "uuid-1", 0).unwrap();
    assert!(
        fixture
            .target
            .validate_replica_write_proof("idx", 0, allocation_id, &authority, true)
            .is_err()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_replication_runtime_prior_term_retries_prove_original_index_delete_and_noop() {
    let fixture = LiveReplicationFixture::new().await;
    let intent = |id: &str| {
        ClientOperationIntent::new(
            id.into(),
            ClientOperationIntent::fingerprint_json(&json!({"id": id})).unwrap(),
        )
        .unwrap()
    };
    let index_intent = intent("index");
    let index = ShardDocRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: "doc".into(),
        payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
        client_operation: Some(crate::transport::client_operation::intent_to_proto(
            &index_intent,
        )),
        ..Default::default()
    };
    let response = fixture
        .source
        .index_doc(crate::transport::request_with_cluster_state_version(
            index.clone(),
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!(response.seq_no, Some(0));
    let noop = ShardClientNoOpRequest {
        index_name: "idx".into(),
        shard_id: 0,
        client_operation: Some(crate::transport::client_operation::intent_to_proto(
            &intent("noop"),
        )),
        result_seq_no: 0,
        result_primary_term: 2,
        result_version: 1,
        index_uuid: Some("uuid-1".into()),
    };
    let response = fixture
        .source
        .record_client_no_op(crate::transport::request_with_cluster_state_version(
            noop.clone(),
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!(
        (response.operation_seq_no, response.seq_no),
        (Some(1), Some(0))
    );
    let delete = ShardDeleteRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: "absent".into(),
        client_operation: Some(crate::transport::client_operation::intent_to_proto(
            &intent("delete"),
        )),
        ..Default::default()
    };
    let response = fixture
        .source
        .delete_doc(crate::transport::request_with_cluster_state_version(
            delete.clone(),
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.deleted), (Some(2), 0));
    fixture
        .source
        .shard_manager
        .get_shard("idx", 0)
        .unwrap()
        .copy_write_state()
        .primary_epoch()
        .unwrap()
        .unwrap()
        .revoke("prior-term retry witness");
    assert_eq!(
        fixture
            .source
            .ensure_primary_activated("idx", 0)
            .await
            .unwrap()
            .primary_term,
        3
    );
    let replica_end = fixture.replica.peer_recovery_barrier().unwrap().wal_end;
    let mut retry = index.clone();
    retry.payload_json = serde_json::to_vec(&json!({"value": 999})).unwrap();
    let response = fixture
        .source
        .index_doc(crate::transport::request_with_cluster_state_version(
            retry, 0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
    let response = fixture
        .source
        .delete_doc(crate::transport::request_with_cluster_state_version(
            delete, 0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    assert_eq!((response.seq_no, response.primary_term), (Some(2), Some(2)));
    let response = fixture
        .source
        .record_client_no_op(crate::transport::request_with_cluster_state_version(
            noop.clone(),
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.success && response.deduplicated,
        "{}",
        response.error
    );
    assert_eq!(
        (
            response.operation_seq_no,
            response.operation_primary_term,
            response.seq_no
        ),
        (Some(1), Some(2), Some(0))
    );
    let response = fixture
        .source
        .record_client_no_op(crate::transport::request_with_cluster_state_version(
            ShardClientNoOpRequest {
                client_operation: index.client_operation.clone(),
                ..noop
            },
            0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.success && response.deduplicated,
        "{}",
        response.error
    );
    assert_eq!(
        (response.operation_seq_no, response.operation_primary_term),
        (Some(0), Some(2))
    );
    assert_eq!(
        response.client_operation.unwrap().result_kind,
        ClientOperationResultKind::ClientOperationIndex as i32
    );
    assert_eq!(
        fixture.replica.peer_recovery_barrier().unwrap().wal_end,
        replica_end
    );
    let actual = fixture
        .replica
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(actual.source["value"], json!(1));
    assert_eq!(
        (actual.seq_no, actual.primary_term, actual.version),
        (0, 2, 1)
    );
    fixture.raft.shutdown().await.unwrap();
    fixture
        .replica
        .fail_stop_writes("required old-operation proof is unavailable");
    let response = fixture
        .source
        .index_doc(crate::transport::request_with_cluster_state_version(
            index, 0,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!response.success);
    assert_eq!((response.seq_no, response.primary_term), (Some(0), Some(2)));
    assert_eq!(
        response.failure.unwrap().outcome,
        WriteFailureOutcome::Indeterminate as i32
    );
    fixture.stop.send(()).unwrap();
    fixture.server.await.unwrap();
}
