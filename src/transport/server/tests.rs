use super::*;
use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{
    ClusterState as DomainClusterState, FieldMapping, FieldType,
    IndexMetadata as DomainIndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
};
use crate::engine::{CompositeEngine, SearchEngine, tantivy::HotEngine};
use crate::shard::ShardManager;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

fn test_storage_manager(data_dir: &std::path::Path) -> Arc<crate::storage::StorageManager> {
    Arc::new(crate::storage::StorageManager::new_in_path(data_dir).unwrap())
}

fn test_remote_store_reader_cache() -> Arc<crate::engine::remote_store::RemoteSplitReaderCache> {
    Arc::new(crate::engine::remote_store::RemoteSplitReaderCache::default())
}

#[cfg(feature = "protocol-trace")]
#[test]
fn protocol_trace_replica_rejection_reasons_cover_pre_apply_boundaries() {
    for (message, expected) in [
        ("shard copy collision quarantine is active", "quarantined"),
        (
            "replication primary term 1 is below local fence 2",
            "term_fence",
        ),
        ("replication allocation mismatch", "identity_mismatch"),
        (
            "replica is installing a peer recovery snapshot",
            "recovery_gate",
        ),
        ("replica shard engine is not open", "copy_unavailable"),
        ("injected WAL sync failure", "apply_failure"),
    ] {
        assert_eq!(
            TransportService::protocol_trace_replica_rejection_reason(&anyhow::anyhow!(message)),
            expected
        );
    }
}

fn gap_test_state(replica_port: u16) -> DomainClusterState {
    let mut state = DomainClusterState::new("gap-probe".into());
    for (node_id, transport_port) in [("source", 0), ("replica", replica_port)] {
        state.add_node(DomainNodeInfo {
            id: node_id.into(),
            name: node_id.into(),
            host: "127.0.0.1".into(),
            transport_port,
            http_port: 0,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
    }
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "source".into(),
                primary_term: 2,
                replicas: vec!["replica".into()],
                in_sync_replicas: vec!["replica".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    state.master_node = Some("source".into());
    state
}

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
            primary_term: 7,
            replicas: vec!["node-2".into()],
            in_sync_replicas: vec!["node-2".into()],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".into(),
            primary_term: 11,
            replicas: vec!["node-1".into()],
            in_sync_replicas: vec!["node-1".into()],
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
        uuid: crate::cluster::state::IndexUuid::new("products-uuid"),
        number_of_shards: 2,
        number_of_replicas: 1,
        shard_routing,
        mappings,
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings {
            engine: crate::cluster::state::IndexEngine::LocalShards,
            refresh_interval_ms: Some(1500),
            flush_threshold_bytes: Some(65_536),
            remote_store: None,
        },
    });
    {
        let allocations = cs.shard_allocations.get_mut("products").unwrap();
        let shard0 = allocations.get_mut(&0).unwrap();
        shard0.primary = Some(101);
        shard0.replicas.insert("node-2".into(), 102);
        shard0.initial_allocation_id = 100;
        shard0.primary_initialized = true;
        shard0.primary_unavailable = true;
        let shard1 = allocations.get_mut(&1).unwrap();
        shard1.primary = Some(201);
        shard1.replicas.insert("node-1".into(), 202);
        shard1.initial_allocation_id = 200;
    }
    cs.version = 42; // reset again after add_index

    cs
}

#[tokio::test]
async fn add_mappings_rejects_reserved_metadata_names_before_raft() {
    let dir = tempfile::tempdir().unwrap();
    let cluster_manager = Arc::new(ClusterManager::new("mapping-validation".into()));
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    for field in [
        "_id",
        "_doc_id",
        "_source",
        "_seq_no",
        "_primary_term",
        "_version",
        "_index",
        "_routing",
    ] {
        let error = service
            .add_mappings(Request::new(AddMappingsRequest {
                index_name: "idx".into(),
                new_fields: vec![FieldMappingEntry {
                    name: field.to_string(),
                    field_type: "keyword".into(),
                    dimension: None,
                }],
                dynamic: "true".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            format!(
                "Field [{field}] is a metadata field and cannot be added inside a document. Use the index API request parameters."
            )
        );
    }
}

#[tokio::test]
async fn add_mappings_accepts_only_plain_text_for_builtin_body() {
    let (raft, shared_state) = crate::consensus::create_raft_instance_mem(1, "body-mapping".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }

    let mut state = DomainClusterState::new("body-mapping".into());
    state.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Master, NodeRole::Data],
        raft_node_id: 1,
    });
    state.master_node = Some("node-1".into());
    state.add_index(DomainIndexMetadata::build_shard_routing(
        "idx",
        1,
        0,
        &["node-1".into()],
    ));
    *shared_state.write().unwrap() = state;

    let dir = tempfile::tempdir().unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let response = service
        .add_mappings(Request::new(AddMappingsRequest {
            index_name: "idx".into(),
            new_fields: vec![FieldMappingEntry {
                name: "body".into(),
                field_type: "text".into(),
                dimension: None,
            }],
            dynamic: "true".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.acknowledged);
    assert_eq!(
        shared_state.read().unwrap().indices["idx"].mappings["body"].field_type,
        FieldType::Text
    );

    for (field_type, dimension) in [("keyword", None), ("integer", None), ("text", Some(3))] {
        let error = service
            .add_mappings(Request::new(AddMappingsRequest {
                index_name: "idx".into(),
                new_fields: vec![FieldMappingEntry {
                    name: "body".into(),
                    field_type: field_type.into(),
                    dimension,
                }],
                dynamic: "true".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "Field [body] is the built-in catch-all text field and can only be mapped as [text]"
        );
    }
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
    assert_eq!(shard0.primary_term, 7);
    assert_eq!(shard0.replicas, vec!["node-2".to_string()]);
    assert_eq!(shard0.in_sync_replicas, vec!["node-2".to_string()]);
    assert_eq!(restored.primary_allocation_id("products", 0), Some(101));
    assert_eq!(
        restored.shard_allocation_id("products", 0, "node-2"),
        Some(102)
    );
    assert!(restored.primary_initialized("products", 0));
    assert!(restored.primary_unavailable("products", 0));

    let shard1 = idx.shard_routing.get(&1).unwrap();
    assert_eq!(shard1.primary, "node-2");
    assert_eq!(shard1.primary_term, 11);
    assert_eq!(shard1.replicas, vec!["node-1".to_string()]);
    assert_eq!(shard1.in_sync_replicas, vec!["node-1".to_string()]);
    assert_eq!(shard1.unassigned_replicas, 1);
    assert_eq!(restored.primary_allocation_id("products", 1), Some(201));
    assert_eq!(
        restored.shard_allocation_id("products", 1, "node-1"),
        Some(202)
    );
}

fn shard_assignment_mut(
    state: &mut crate::transport::proto::ClusterState,
    shard_id: u32,
) -> &mut crate::transport::proto::ShardAssignment {
    state.indices[0]
        .shards
        .iter_mut()
        .find(|assignment| assignment.shard_id == shard_id)
        .unwrap()
}

#[test]
fn cluster_state_snapshot_without_in_sync_membership_fails_closed() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0)
        .in_sync_replica_node_ids
        .clear();

    let restored = proto_to_cluster_state(&proto).unwrap();
    assert!(
        restored.indices["products"].shard_routing[&0]
            .in_sync_replicas
            .is_empty()
    );
}

#[test]
fn cluster_state_snapshot_without_primary_term_is_rejected() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).primary_term = 0;

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("has no primary term"));
}

#[test]
fn old_cluster_state_wire_format_is_rejected() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    proto.format_version = 0;

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("recreate the index"));
}

#[test]
fn cluster_state_snapshot_without_allocation_identity_is_rejected() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).initial_allocation_id = None;

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("missing allocation identity"));
}

#[test]
fn cluster_state_snapshot_rejects_missing_replica_allocation_id() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).replica_allocations[0].allocation_id = None;

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("missing an allocation ID"));
}

#[test]
fn cluster_state_snapshot_rejects_in_sync_primary() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).in_sync_replica_node_ids = vec!["node-1".into()];

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("cannot also be an in-sync replica")
    );
}

#[test]
fn cluster_state_snapshot_rejects_unassigned_in_sync_node() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).in_sync_replica_node_ids = vec!["node-3".into()];

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("is not present in the replica assignments")
    );
}

#[test]
fn cluster_state_snapshot_rejects_duplicate_in_sync_node() {
    let original = make_full_cluster_state();
    let mut proto = cluster_state_to_proto(&original);
    shard_assignment_mut(&mut proto, 0).in_sync_replica_node_ids =
        vec!["node-2".into(), "node-2".into()];

    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("duplicate in-sync replica"));
}

#[test]
fn roundtrip_preserves_index_mappings_and_settings() {
    let original = make_full_cluster_state();
    let proto = cluster_state_to_proto(&original);
    let restored = proto_to_cluster_state(&proto).unwrap();

    let idx = restored.indices.get("products").unwrap();
    assert_eq!(idx.uuid.as_str(), "products-uuid");
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(DomainIndexMetadata {
        name: "logs".into(),
        uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: std::collections::HashMap::new(),
        dynamic: Default::default(),
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

#[test]
fn roundtrip_dynamic_security_state() {
    use crate::cluster::state::{SecurityApiKeyRecord, SecurityRoleDefinition};

    let mut cs = DomainClusterState::new("test".into());
    cs.api_keys.insert(
        "key-1".into(),
        SecurityApiKeyRecord {
            id: "key-1".into(),
            name: "ingest-bot".into(),
            hash_sha256: "a".repeat(64),
            roles: vec!["write".into()],
            indices: vec!["logs-*".into()],
            created_at_millis: 1_700_000_000_000,
        },
    );
    cs.roles.insert(
        "log-reader".into(),
        SecurityRoleDefinition {
            name: "log-reader".into(),
            cluster: vec!["monitor".into()],
            indices: vec!["logs-*".into()],
            index_privileges: vec!["read".into()],
        },
    );

    let proto = cluster_state_to_proto(&cs);
    // Snapshot carries the dynamic security state losslessly.
    assert_eq!(proto.api_keys_json.len(), 1);
    assert_eq!(proto.roles_json.len(), 1);

    let restored = proto_to_cluster_state(&proto).unwrap();
    assert_eq!(restored.api_keys, cs.api_keys);
    assert_eq!(restored.roles, cs.roles);
    // The stored value is the hash, never a plaintext secret.
    assert_eq!(restored.api_keys["key-1"].hash_sha256, "a".repeat(64));
}

#[test]
fn proto_to_cluster_state_rejects_malformed_api_key_json() {
    let mut proto = cluster_state_to_proto(&DomainClusterState::new("test".into()));
    proto.api_keys_json.push("{not valid json".into());
    let err = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ── advance_global_checkpoint tests ────────────────────────────────

fn make_checkpoint_engine() -> (tempfile::TempDir, Arc<dyn crate::engine::SearchEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        crate::engine::CompositeEngine::new(dir.path(), std::time::Duration::from_secs(60))
            .unwrap();
    (dir, Arc::new(engine))
}

fn replica_checkpoint(
    node_id: &str,
    processed_checkpoint: Option<u64>,
    persisted_checkpoint: Option<u64>,
) -> crate::shard::ReplicaCheckpointUpdate {
    crate::shard::ReplicaCheckpointUpdate {
        node_id: node_id.to_string(),
        allocation_id: 1,
        processed_checkpoint,
        persisted_checkpoint,
    }
}

#[test]
fn advance_global_checkpoint_no_replicas_uses_primary() {
    let (_dir, engine) = make_checkpoint_engine();
    TransportService::advance_global_checkpoint(&engine, Some(10), &[]);
    assert_eq!(engine.global_checkpoint(), Some(10));
}

#[test]
fn advance_global_checkpoint_min_of_primary_and_replicas() {
    let (_dir, engine) = make_checkpoint_engine();
    let replicas = vec![
        replica_checkpoint("r1", Some(5), Some(5)),
        replica_checkpoint("r2", Some(8), Some(8)),
    ];
    TransportService::advance_global_checkpoint(&engine, Some(10), &replicas);
    assert_eq!(
        engine.global_checkpoint(),
        Some(5),
        "should be min(10, 5, 8) = 5"
    );
}

#[test]
fn advance_global_checkpoint_primary_lower_than_replicas() {
    let (_dir, engine) = make_checkpoint_engine();
    let replicas = vec![replica_checkpoint("r1", Some(20), Some(20))];
    TransportService::advance_global_checkpoint(&engine, Some(3), &replicas);
    assert_eq!(
        engine.global_checkpoint(),
        Some(3),
        "primary is the bottleneck"
    );
}

#[test]
fn advance_global_checkpoint_never_goes_backward() {
    let (_dir, engine) = make_checkpoint_engine();
    // Set to 10 first
    TransportService::advance_global_checkpoint(&engine, Some(10), &[]);
    assert_eq!(engine.global_checkpoint(), Some(10));

    // Try to set lower — should stay at 10
    let replicas = vec![replica_checkpoint("r1", Some(5), Some(5))];
    TransportService::advance_global_checkpoint(&engine, Some(5), &replicas);
    assert_eq!(
        engine.global_checkpoint(),
        Some(10),
        "should never go backward"
    );
}

#[test]
fn advance_global_checkpoint_advances_forward() {
    let (_dir, engine) = make_checkpoint_engine();
    TransportService::advance_global_checkpoint(&engine, Some(5), &[]);
    assert_eq!(engine.global_checkpoint(), Some(5));

    TransportService::advance_global_checkpoint(&engine, Some(10), &[]);
    assert_eq!(engine.global_checkpoint(), Some(10));
}

#[test]
fn advance_global_checkpoint_single_lagging_replica() {
    let (_dir, engine) = make_checkpoint_engine();
    let replicas = vec![
        replica_checkpoint("fast", Some(100), Some(100)),
        replica_checkpoint("slow", Some(2), Some(2)),
        replica_checkpoint("medium", Some(50), Some(50)),
    ];
    TransportService::advance_global_checkpoint(&engine, Some(100), &replicas);
    assert_eq!(
        engine.global_checkpoint(),
        Some(2),
        "slowest replica determines global checkpoint"
    );
}

#[tokio::test]
async fn d1_commit3_async_processed_checkpoint_is_not_a_global_persistence_proof() {
    let dir = tempfile::tempdir().unwrap();
    let engine: Arc<dyn crate::engine::SearchEngine> = Arc::new(
        crate::engine::CompositeEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &HashMap::new(),
            crate::wal::TranslogDurability::Async {
                sync_interval_ms: 3_600_000,
            },
            Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap(),
    );
    engine
        .add_document_with_receipt("a", json!({"value": 1}))
        .unwrap();
    engine
        .add_document_with_receipt("b", json!({"value": 2}))
        .unwrap();
    let sequence = engine.sequence_stats();
    assert_eq!(sequence.processed_checkpoint, Some(1));
    assert_eq!(sequence.persisted_checkpoint, None);

    TransportService::advance_global_checkpoint(
        &engine,
        sequence.persisted_checkpoint,
        &[replica_checkpoint("replica", Some(1), None)],
    );

    assert_eq!(engine.global_checkpoint(), None);
}

#[tokio::test]
async fn expired_gap_probe_clears_an_idle_observation_when_replica_caught_up() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = listener.local_addr().unwrap().port();
    let state = gap_test_state(replica_port);

    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_engine = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: 1,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_engine
        .apply_replica_batch(
            (0..=5)
                .map(|seq_no| crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 2,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"seq": seq_no}),
                    },
                })
                .collect(),
        )
        .unwrap();
    let replica_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    replica_manager.update_state(state.clone());
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    source_shards.isr_tracker.update_replica_checkpoints_at(
        "idx",
        0,
        crate::shard::ReplicaCheckpointContext {
            index_uuid: "uuid-1",
            primary_term: 2,
            primary_processed_checkpoint: Some(5),
        },
        &[crate::shard::ReplicaCheckpointUpdate {
            node_id: "replica".into(),
            allocation_id: 1,
            processed_checkpoint: Some(0),
            persisted_checkpoint: Some(0),
        }],
        std::time::Instant::now() - Duration::from_secs(61),
    );
    assert_eq!(
        source_shards.isr_tracker.gap_observations("idx", 0).len(),
        1
    );
    let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    source_manager.update_state(state);
    let source_service = TransportService {
        cluster_manager: source_manager,
        shard_manager: source_shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    source_service.reconcile_replica_gaps().await;

    assert!(
        source_shards
            .isr_tracker
            .gap_observations("idx", 0)
            .is_empty()
    );
}

#[tokio::test]
async fn version_map_capacity_rejection_is_resource_exhausted_before_wal_append() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .initialize_copy_identity_for_test("idx", 0, "uuid-1", 1, 1)
        .unwrap();
    let engine = Arc::new(
        CompositeEngine::new(dir.path().join("uuid-1/shard_0"), Duration::from_secs(60)).unwrap(),
    );
    engine.text_engine().set_version_map_max_bytes_for_test(1);
    shard_manager.insert_shard_for_test("idx", 0, engine.clone());

    let mut state = DomainClusterState::new("capacity".into());
    state.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 0,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let error = service
        .index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "doc".into(),
            payload_json: serde_json::to_vec(&json!({})).unwrap(),
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .is_empty()
    );
}

#[tokio::test]
async fn get_or_open_search_shard_reopens_persisted_shard_via_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "test-uuid-reopen";
    {
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.register_index_uuid("restart-idx", test_uuid);
        let engine = manager
            .open_shard_with_settings(
                "restart-idx",
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                test_uuid,
            )
            .unwrap();
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "restart-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new(test_uuid),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.register_index_uuid("restart-idx", test_uuid);
        let engine = manager
            .open_shard_with_settings(
                "restart-idx",
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                test_uuid,
            )
            .unwrap();
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "restart-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new(test_uuid),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
async fn get_segment_stats_only_reports_open_shard_segments() {
    let dir = tempfile::tempdir().unwrap();

    let sm = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    sm.open_shard("open-idx", 0).unwrap();
    let engine = sm.get_shard("open-idx", 0).expect("open shard");
    engine
        .add_document("d1", json!({"title": "segment row"}))
        .unwrap();
    engine.refresh().unwrap();

    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::new("segment-stats-cluster".into())),
        shard_manager: sm,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let response = service
        .get_segment_stats(Request::new(SegmentStatsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.segments.len(), 1);
    assert_eq!(response.segments[0].index_name, "open-idx");
    assert_eq!(response.segments[0].shard_id, 0);
    assert_eq!(response.segments[0].num_docs, 1);
    assert_eq!(response.segments[0].deleted_docs, 0);
}

#[tokio::test]
async fn get_or_open_search_shard_returns_not_found_for_unknown_shard() {
    let dir = tempfile::tempdir().unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::new("missing-unit".into())),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
async fn ping_rejects_unregistered_source_node() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ClusterManager::new("ping-test".into()));
    manager.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Data],
        raft_node_id: 1,
    });
    let service = TransportService {
        cluster_manager: manager,
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let err = service
        .ping(Request::new(PingRequest {
            source_node_id: "node-2".into(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("not registered"));
}

#[tokio::test]
async fn ping_updates_last_seen_for_registered_source_node() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ClusterManager::new("ping-test".into()));
    manager.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Data],
        raft_node_id: 1,
    });
    let service = TransportService {
        cluster_manager: Arc::clone(&manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    service
        .ping(Request::new(PingRequest {
            source_node_id: "node-1".into(),
        }))
        .await
        .unwrap();

    let state = manager.get_state();
    assert!(state.last_seen.contains_key("node-1"));
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        2,
        ShardRoutingEntry {
            primary: "node-3".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "maint-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
        number_of_shards: 3,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: sm,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
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
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    shard_routing.insert(
        1,
        ShardRoutingEntry {
            primary: "node-2".into(),
            primary_term: 1,
            replicas: vec!["node-1".into()],
            in_sync_replicas: vec!["node-1".into()],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "rep-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
        number_of_shards: 2,
        number_of_replicas: 1,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: sm,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let (successful, failed) =
        service.run_maintenance_on_assigned_shards("rep-idx", |e| e.refresh());
    // Shard 0 primary + shard 1 replica = 2
    assert_eq!(successful, 2, "primary + replica assigned here");
    assert_eq!(failed, 0);
}

#[tokio::test]
async fn flush_index_reopens_assigned_shard_before_running_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "test-uuid-flush";
    {
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        manager.register_index_uuid("maint-idx", test_uuid);
        let engine = manager
            .open_shard_with_settings(
                "maint-idx",
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                test_uuid,
            )
            .unwrap();
        engine
            .add_document("d1", json!({"title": "flush reopen"}))
            .unwrap();
        engine.refresh().unwrap();
    }

    let mut cluster_state = DomainClusterState::new("maint-cluster".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "maint-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new(test_uuid),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let response = service
        .flush_index(Request::new(IndexMaintenanceRequest {
            index_name: "maint-idx".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.successful_shards, 1);
    assert_eq!(response.failed_shards, 0);
    assert!(service.shard_manager.get_shard("maint-idx", 0).is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_refresh_does_not_exhaust_write_pool_for_replica_apply() {
    let dir = tempfile::tempdir().unwrap();
    let maintenance_dir = dir.path().join("maintenance-shard");
    let write_dir = dir.path().join("write-shard");
    std::fs::create_dir_all(&maintenance_dir).unwrap();
    std::fs::create_dir_all(&write_dir).unwrap();

    let maintenance_engine =
        Arc::new(HotEngine::new(&maintenance_dir, Duration::from_secs(60)).unwrap());
    let write_engine = Arc::new(HotEngine::new(&write_dir, Duration::from_secs(60)).unwrap());

    let (refresh_started_tx, refresh_started_rx) = tokio::sync::oneshot::channel();
    maintenance_engine.notify_before_refresh_writer_for_test(refresh_started_tx);

    let (writer_locked_tx, writer_locked_rx) = tokio::sync::oneshot::channel();
    let (release_writer_tx, release_writer_rx) = std::sync::mpsc::channel();
    let locked_engine = maintenance_engine.clone();
    let writer_thread = std::thread::spawn(move || {
        let _writer = locked_engine.writer_lock_for_test();
        let _ = writer_locked_tx.send(());
        release_writer_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("test must release the blocked maintenance writer");
    });
    tokio::time::timeout(Duration::from_secs(5), writer_locked_rx)
        .await
        .expect("writer lock acquisition should be bounded")
        .expect("writer lock holder should signal readiness");

    let shard_manager = Arc::new(ShardManager::new(
        dir.path().join("shards"),
        Duration::from_secs(60),
    ));
    shard_manager.register_index_uuid("maintenance-idx", "maintenance-uuid");
    shard_manager.register_index_uuid("write-idx", "write-uuid");
    shard_manager.insert_shard_for_test("maintenance-idx", 0, maintenance_engine);
    shard_manager.insert_shard_for_test("write-idx", 0, write_engine.clone());

    let mut cluster_state = DomainClusterState::new("maintenance-pool-cluster".into());
    for (index_name, index_uuid) in [
        ("maintenance-idx", "maintenance-uuid"),
        ("write-idx", "write-uuid"),
    ] {
        cluster_state.add_index(DomainIndexMetadata {
            name: index_name.into(),
            uuid: crate::cluster::state::IndexUuid::new(index_uuid),
            number_of_shards: 1,
            number_of_replicas: 0,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: "node-1".into(),
                    primary_term: 1,
                    replicas: vec![],
                    in_sync_replicas: vec![],
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: crate::cluster::state::IndexSettings::default(),
        });
    }
    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(1, 1),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let refresh_service = service.clone();
    let refresh_task = tokio::spawn(async move {
        refresh_service
            .refresh_index(Request::new(IndexMaintenanceRequest {
                index_name: "maintenance-idx".into(),
            }))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), refresh_started_rx)
        .await
        .expect("refresh should reach the blocked writer")
        .expect("refresh start signal should be delivered");

    let replica_result = tokio::time::timeout(
        Duration::from_secs(5),
        service.replicate_doc(Request::new(ReplicateDocRequest {
            index_name: "write-idx".into(),
            shard_id: 0,
            op: "index".into(),
            doc_id: "replica-doc".into(),
            payload_json: serde_json::to_vec(&json!({"value": "written"})).unwrap(),
            seq_no: 7,
            index_uuid: "write-uuid".into(),
            primary_term: Some(1),
            target_allocation_id: Some(1),
        })),
    )
    .await;

    release_writer_tx
        .send(())
        .expect("blocked maintenance writer should still be waiting");
    let refresh_result = tokio::time::timeout(Duration::from_secs(5), refresh_task).await;
    writer_thread.join().unwrap();

    let replica_response = replica_result
        .expect("replica apply must not wait for unrelated maintenance")
        .expect("replica apply RPC should succeed")
        .into_inner();
    assert!(replica_response.success, "{}", replica_response.error);

    let refresh_response = refresh_result
        .expect("refresh should complete after releasing the writer")
        .expect("refresh task should not panic")
        .expect("refresh RPC should succeed")
        .into_inner();
    assert_eq!(refresh_response.successful_shards, 1);
    assert_eq!(refresh_response.failed_shards, 0);

    crate::worker::spawn_engine_maintenance("test visibility refresh", {
        let write_engine = write_engine.clone();
        move || write_engine.refresh()
    })
    .await
    .unwrap();
    assert_eq!(
        write_engine.get_document("replica-doc").unwrap().unwrap()["value"],
        "written"
    );
}

#[tokio::test]
async fn force_merge_rpc_returns_immediately_after_enqueue() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster_state = DomainClusterState::new("force-merge-cluster".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "force-merge-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("force-merge-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let error = service
        .force_merge_index(Request::new(ForceMergeRequest {
            index_name: "force-merge-idx".into(),
            max_num_segments: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    let response = service
        .force_merge_index(Request::new(ForceMergeRequest {
            index_name: "force-merge-idx".into(),
            max_num_segments: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(!response.task_id.is_empty());
    assert!(
        service
            .task_manager
            .get_local_force_merge(&response.task_id)
            .is_some()
    );
}

#[tokio::test]
async fn get_task_status_rpc_returns_local_force_merge_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::new("task-status-cluster".into())),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let task_id = service
        .task_manager
        .create_local_force_merge("node-1", "idx", 1);
    service.task_manager.mark_running(&task_id);

    let response = service
        .get_task_status(Request::new(GetTaskStatusRequest { task_id }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.found);
    assert_eq!(response.status, "running");
    assert_eq!(response.node_id, "node-1");
    assert_eq!(response.index_name, "idx");
}

#[tokio::test]
async fn force_merge_task_counts_missing_assigned_shard_as_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster_state = DomainClusterState::new("force-merge-cluster".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "force-merge-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("missing-force-merge-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let response = service
        .force_merge_index(Request::new(ForceMergeRequest {
            index_name: "force-merge-idx".into(),
            max_num_segments: 1,
        }))
        .await
        .unwrap()
        .into_inner();

    let task = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(task) = service
                .task_manager
                .get_local_force_merge(&response.task_id)
                && matches!(
                    task.status,
                    crate::tasks::TaskStatus::Completed | crate::tasks::TaskStatus::Failed
                )
            {
                break task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("force-merge task should reach a terminal state");

    assert_eq!(task.status, crate::tasks::TaskStatus::Failed);
    assert_eq!(task.successful_shards, 0);
    assert_eq!(task.failed_shards, 1);
}

#[tokio::test]
async fn flush_index_refuses_to_create_missing_uuid_dir() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster_state = DomainClusterState::new("maint-cluster".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cluster_state.add_index(DomainIndexMetadata {
        name: "maint-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("missing-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let manager = ClusterManager::new(cluster_state.cluster_name.clone());
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let response = service
        .flush_index(Request::new(IndexMaintenanceRequest {
            index_name: "maint-idx".into(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.successful_shards, 0);
    assert_eq!(response.failed_shards, 0);
    assert!(service.shard_manager.get_shard("maint-idx", 0).is_none());
    assert!(!dir.path().join("missing-uuid").join("shard_0").exists());
}

#[test]
fn validate_join_identity_rejects_zero_raft_id() {
    let state = DomainClusterState::new("test".into());
    let error = validate_join_identity(&state, "node-a", 0).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("nonzero raft_node_id"));
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
            dynamic: "false".into(),
        }],
        api_keys_json: vec![],
        roles_json: vec![],
        format_version: 1,
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

#[test]
fn roundtrip_preserves_dynamic_mapping_true() {
    let mut cs = DomainClusterState::new("dyn-test".into());
    let mut meta = DomainIndexMetadata {
        name: "dyn-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("d-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: std::collections::HashMap::new(),
        mappings: std::collections::HashMap::new(),
        dynamic: crate::cluster::state::DynamicMapping::True,
        settings: crate::cluster::state::IndexSettings::default(),
    };
    meta.shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "n1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(meta);
    cs.version = 1;

    let proto = cluster_state_to_proto(&cs);
    let restored = proto_to_cluster_state(&proto).unwrap();
    assert_eq!(
        restored.indices["dyn-idx"].dynamic,
        crate::cluster::state::DynamicMapping::True
    );
}

#[test]
fn roundtrip_preserves_dynamic_mapping_strict() {
    let mut cs = DomainClusterState::new("dyn-test".into());
    let mut meta = DomainIndexMetadata {
        name: "strict-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("s-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: std::collections::HashMap::new(),
        mappings: std::collections::HashMap::new(),
        dynamic: crate::cluster::state::DynamicMapping::Strict,
        settings: crate::cluster::state::IndexSettings::default(),
    };
    meta.shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "n1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(meta);
    cs.version = 1;

    let proto = cluster_state_to_proto(&cs);
    let restored = proto_to_cluster_state(&proto).unwrap();
    assert_eq!(
        restored.indices["strict-idx"].dynamic,
        crate::cluster::state::DynamicMapping::Strict
    );
}

#[test]
fn empty_dynamic_wire_value_is_rejected() {
    let mut cs = DomainClusterState::new("dyn-test".into());
    let mut meta = DomainIndexMetadata {
        name: "legacy-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("l-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: std::collections::HashMap::new(),
        mappings: std::collections::HashMap::new(),
        dynamic: crate::cluster::state::DynamicMapping::False,
        settings: crate::cluster::state::IndexSettings::default(),
    };
    meta.shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "n1".into(),
            primary_term: 1,
            replicas: vec![],
            in_sync_replicas: vec![],
            unassigned_replicas: 0,
        },
    );
    cs.add_index(meta);
    cs.version = 1;

    let mut proto = cluster_state_to_proto(&cs);
    proto.indices[0].dynamic.clear();
    let error = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("dynamic mapping"));
}

#[test]
fn roundtrip_preserves_remote_store_engine() {
    let mut cs = DomainClusterState::new("engine-test".into());
    let meta = DomainIndexMetadata {
        name: "remote-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("remote-uuid"),
        number_of_shards: 0,
        number_of_replicas: 0,
        shard_routing: std::collections::HashMap::new(),
        mappings: std::collections::HashMap::new(),
        dynamic: crate::cluster::state::DynamicMapping::False,
        settings: crate::cluster::state::IndexSettings {
            engine: crate::cluster::state::IndexEngine::RemoteStore,
            remote_store: Some(crate::cluster::state::RemoteStoreSettings {
                object_store_uri: Some("s3://bucket/remote".into()),
                manifest_path: Some("manifests/11.json".into()),
                manifest_generation: Some(11),
                manifest_checksum: Some("sha256:manifest-11".into()),
                manifest_refresh_ms: Some(1000),
                hotcache_bytes: Some(4 * 1024 * 1024),
                split_cache_bytes: Some(64 * 1024 * 1024),
            }),
            ..Default::default()
        },
    };
    cs.add_index(meta);

    let proto = cluster_state_to_proto(&cs);
    let restored = proto_to_cluster_state(&proto).unwrap();
    assert_eq!(
        restored.indices["remote-idx"].settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    );
    assert_eq!(
        restored.indices["remote-idx"]
            .settings
            .remote_store
            .as_ref()
            .and_then(|settings| settings.manifest_generation),
        Some(11)
    );
}

#[test]
fn roundtrip_rejects_unknown_non_empty_engine() {
    let proto = ClusterState {
        cluster_name: "engine-test".into(),
        version: 1,
        master_node: None,
        nodes: vec![],
        indices: vec![IndexMetadata {
            name: "mystery-idx".into(),
            uuid: "mystery-uuid".into(),
            number_of_shards: 0,
            number_of_replicas: 0,
            shards: vec![],
            mappings: vec![],
            settings: Some(crate::transport::proto::IndexSettings {
                engine: "alien_store".into(),
                refresh_interval_ms: None,
                flush_threshold_bytes: None,
                remote_store: None,
            }),
            dynamic: "false".into(),
        }],
        api_keys_json: vec![],
        roles_json: vec![],
        format_version: 1,
    };

    let err = proto_to_cluster_state(&proto).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("unknown engine 'alien_store'"));
}

#[tokio::test]
async fn create_index_returns_internal_when_no_data_nodes_are_available() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster_state = DomainClusterState::new("create-index-test".into());
    cluster_state.add_node(DomainNodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![NodeRole::Master],
        raft_node_id: 1,
    });
    cluster_state.master_node = Some("node-1".into());

    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, cluster_state.cluster_name.clone())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".into())
        .await
        .unwrap();
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let manager = ClusterManager::with_shared_state(shared_state);
    manager.update_state(cluster_state);

    let service = TransportService {
        cluster_manager: Arc::new(manager),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let err = service
        .create_index(Request::new(CreateIndexRequest {
            index_name: "idx".into(),
            body_json: serde_json::to_vec(&json!({
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }))
            .unwrap(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(
        err.message()
            .contains("No data nodes available to assign shards")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_fence_persist_failure_does_not_fail_the_shard_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) = crate::consensus::create_raft_instance_mem(1, "fence-io".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "test Raft leader was not elected"
        );
        tokio::task::yield_now().await;
    }

    let metadata = DomainIndexMetadata {
        name: "fence-io".into(),
        uuid: crate::cluster::state::IndexUuid::new("fence-io-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    let index_uuid = metadata.uuid.to_string();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let allocation_id = shared_state
        .read()
        .unwrap()
        .primary_allocation_id("fence-io", 0)
        .unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .open_assigned_shard_with_settings(
            "fence-io",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            &index_uuid,
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: "fence-io".into(),
            index_uuid: index_uuid.clone(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );

    let identity_temp = dir
        .path()
        .join(&index_uuid)
        .join("shard_0")
        .join(format!("{}.tmp", crate::shard::SHARD_COPY_IDENTITY_FILE));
    std::fs::create_dir(&identity_temp).unwrap();
    let cluster_manager = Arc::new(ClusterManager::with_shared_state(shared_state.clone()));
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let error = match service.ensure_primary_activated("fence-io", 0).await {
        Ok(_) => panic!("injected fence persistence failure must reject activation"),
        Err(error) => error,
    };
    assert!(error.contains("failed to persist primary fence"));
    assert_eq!(
        shared_state
            .read()
            .unwrap()
            .primary_allocation_id("fence-io", 0),
        Some(allocation_id),
        "transient fence persistence errors must not alter routing"
    );
}

#[tokio::test]
async fn uninitialized_copy_failure_is_not_queued_for_reporting() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("uninitialized-failure".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let shard_dir = dir.path().join("uuid-1/shard_0");
    std::fs::create_dir_all(&shard_dir).unwrap();
    std::fs::write(shard_dir.join("legacy-data"), b"not an empty initial copy").unwrap();
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let service = TransportService {
        cluster_manager,
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    assert!(
        service.ensure_primary_activated("idx", 0).await.is_err(),
        "invalid initial storage must still fail closed"
    );
    assert!(
        service
            .primary_activation_state
            .failed_copy_reports
            .lock()
            .await
            .is_empty(),
        "uninitialized copies must not enqueue FailShardCopy reports"
    );
}

#[tokio::test]
async fn request_path_respects_assigned_open_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("request-backoff".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(
        3,
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );
    shard_manager.inject_assigned_open_io_failures(5, usize::MAX);
    let service = TransportService {
        cluster_manager,
        shard_manager: shard_manager.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    for _ in 0..8 {
        assert!(service.ensure_primary_activated("idx", 0).await.is_err());
    }
    assert_eq!(
        shard_manager.assigned_open_attempts_for_test(),
        1,
        "requests during backoff must not repeat the full assigned open"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_replica_io_is_failed_out_of_routing() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) = crate::consensus::create_raft_instance_mem(1, "replica-io".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let metadata = DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: vec!["node-2".into()],
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::CreateIndex {
            metadata: metadata.clone(),
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let primary_allocation = shared_state
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let replica_allocation = shared_state
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id: replica_allocation,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let replica_engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_engine.inject_wal_write_failures_for_test(28, 3);
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-2".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    for _ in 0..3 {
        let response = service
            .replicate_doc(Request::new(ReplicateDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
                op: "index".into(),
                seq_no: 0,
                index_uuid: "uuid-1".into(),
                primary_term: Some(2),
                target_allocation_id: Some(replica_allocation),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
    }
    assert!(
        shared_state.read().unwrap().indices["idx"].shard_routing[&0]
            .in_sync_replicas
            .is_empty()
    );
    let current_state = shared_state.read().unwrap().clone();
    assert!(
        crate::replication::replicate_write(
            &crate::transport::TransportClient::new(),
            &current_state,
            "idx",
            0,
            "next",
            &json!({"value": 2}),
            "index",
            1,
            2,
        )
        .await
        .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_replica_bulk_wal_io_remains_typed_for_escalation() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("replica-bulk-io".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["node-2".into()],
                in_sync_replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);

    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let replica_engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_engine.inject_wal_write_failures_for_test(28, 3);
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-2".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let mut errors = Vec::new();
    for _ in 0..3 {
        let operation = ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "doc".into(),
            payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
            op: "index".into(),
            seq_no: 0,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        };
        let response = service
            .replicate_bulk(Request::new(ReplicateBulkRequest {
                index_name: "idx".into(),
                shard_id: 0,
                ops: vec![operation],
                index_uuid: "uuid-1".into(),
                primary_term: Some(2),
                target_allocation_id: Some(allocation_id),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
        errors.push(response.error);
    }
    assert!(!errors[0].contains("persistent shard copy I/O failure"));
    assert!(!errors[1].contains("persistent shard copy I/O failure"));
    assert!(
        errors[2].contains("persistent shard copy I/O failure"),
        "{}",
        errors[2]
    );
}

fn replica_json_test_service() -> (tempfile::TempDir, TransportService, u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("replica-json".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["node-2".into()],
                in_sync_replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-2".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    (dir, service, allocation_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_index_payload_json_is_decoded_once_per_operation() {
    let (_dir, service, allocation_id) = replica_json_test_service();
    let marker = uuid::Uuid::new_v4().to_string();
    let single_payload = serde_json::to_vec(&json!({
        "marker": marker,
        "kind": "single"
    }))
    .unwrap();
    start_tracking_replica_index_payload(&single_payload);
    let response = service
        .replicate_doc(Request::new(ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "single".into(),
            payload_json: single_payload,
            op: "index".into(),
            seq_no: 0,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    let single_parse_count = stop_tracking_replica_index_payload();

    let bulk_payload = serde_json::to_vec(&json!({
        "marker": marker,
        "kind": "bulk"
    }))
    .unwrap();
    let ops = (1..=3)
        .map(|seq_no| ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: format!("bulk-{seq_no}"),
            payload_json: bulk_payload.clone(),
            op: "index".into(),
            seq_no,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        })
        .collect();
    start_tracking_replica_index_payload(&bulk_payload);
    let response = service
        .replicate_bulk(Request::new(ReplicateBulkRequest {
            index_name: "idx".into(),
            shard_id: 0,
            ops,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    let bulk_parse_count = stop_tracking_replica_index_payload();
    assert_eq!((single_parse_count, bulk_parse_count), (1, 3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_bulk_accepts_non_contiguous_promotion_noops() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("promotion-noops".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["node-2".into()],
                in_sync_replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);

    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let replica_engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-2".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let noop = |seq_no| ReplicateDocRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: String::new(),
        payload_json: serde_json::to_vec(&json!({"_reason": "promotion gap"})).unwrap(),
        op: "noop".into(),
        seq_no,
        index_uuid: "uuid-1".into(),
        primary_term: Some(2),
        target_allocation_id: Some(allocation_id),
    };

    let response = service
        .replicate_bulk(Request::new(ReplicateBulkRequest {
            index_name: "idx".into(),
            shard_id: 0,
            ops: vec![noop(0), noop(2)],
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.success, "{}", response.error);
    assert!(response.all_operations_processed);
    assert!(response.all_operations_persisted);
    assert_eq!(
        replica_engine.sequence_stats().processed_checkpoint,
        Some(0)
    );
    assert_eq!(replica_engine.sequence_stats().max_seq_no, Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_noop_batches_reach_live_replica_in_two_requests() {
    const OLD_TERM_OPS: u64 = 1_031;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = listener.local_addr().unwrap().port();
    let mut state = gap_test_state(replica_port);
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let source_allocation = state.shard_allocation_id("idx", 0, "source").unwrap();
    let replica_allocation = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let history = (0..OLD_TERM_OPS)
        .map(|index| crate::engine::SequencedOperation {
            seq_no: index * 2,
            primary_term: 1,
            mutation: crate::engine::DocumentMutation::Index {
                doc_id: format!("doc-{index}"),
                source: json!({"value": index}),
            },
        })
        .collect::<Vec<_>>();
    let max_seq_no = (OLD_TERM_OPS - 1) * 2;

    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_engine = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_engine.apply_replica_batch(history.clone()).unwrap();
    replica_engine.refresh().unwrap();
    let replica_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    replica_manager.update_state(state.clone());
    let replica_activation_state = new_primary_activation_state();
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: replica_activation_state.clone(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    let source_engine = source_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: source_allocation,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    source_engine.apply_replica_batch(history).unwrap();
    source_engine.refresh().unwrap();
    let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    source_manager.update_state(state);
    let source_service = TransportService {
        cluster_manager: source_manager,
        shard_manager: source_shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    source_service
        .activate_primary_for_lifecycle("idx", 0)
        .await
        .unwrap();
    server.abort();

    assert_eq!(
        replica_activation_state
            .promotion_noop_bulk_requests_received
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert_eq!(
        source_engine.sequence_stats().processed_checkpoint,
        Some(max_seq_no)
    );
    assert_eq!(
        replica_engine.sequence_stats().processed_checkpoint,
        Some(max_seq_no)
    );
    assert_eq!(replica_engine.sequence_stats().max_seq_no, Some(max_seq_no));
    assert!(
        source_shards
            .isr_tracker
            .gap_observations("idx", 0)
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_promotion_noop_fanout_is_retried_end_to_end() {
    let unavailable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = unavailable_listener.local_addr().unwrap().port();
    drop(unavailable_listener);

    let state = gap_test_state(replica_port);
    let allocation_id = state.shard_allocation_id("idx", 0, "source").unwrap();
    let replica_allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let seed_gap = |engine: &Arc<dyn SearchEngine>| {
        for seq_no in [0, 2] {
            engine
                .apply_replica_operation(crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"seq": seq_no}),
                    },
                })
                .unwrap();
        }
        assert_eq!(engine.sequence_stats().processed_checkpoint, Some(0));
        assert_eq!(engine.sequence_stats().max_seq_no, Some(2));
    };

    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    let source_engine = source_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    seed_gap(&source_engine);
    let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    source_manager.update_state(state.clone());
    let source_service = TransportService {
        cluster_manager: source_manager,
        shard_manager: source_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    tokio::time::timeout(
        Duration::from_secs(30),
        source_service.activate_primary_for_lifecycle("idx", 0),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(source_engine.sequence_stats().processed_checkpoint, Some(2));
    assert_eq!(
        source_service
            .shard_manager
            .isr_tracker
            .gap_observations("idx", 0)
            .len(),
        1,
        "the unavailable replica must retain a gap observation"
    );

    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_engine = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    seed_gap(&replica_engine);
    replica_shards
        .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), replica_allocation_id, 2)
        .await
        .unwrap();
    let replica_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    replica_manager.update_state(state);
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", replica_port))
        .await
        .unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    tokio::time::timeout(
        Duration::from_secs(30),
        source_service.activate_primary_for_lifecycle("idx", 0),
    )
    .await
    .unwrap()
    .unwrap();

    let sequence = replica_engine.sequence_stats();
    assert_eq!(sequence.processed_checkpoint, Some(2));
    assert_eq!(sequence.persisted_checkpoint, Some(2));
    assert!(
        source_service
            .shard_manager
            .isr_tracker
            .gap_observations("idx", 0)
            .is_empty(),
        "successful NoOp redelivery must close the replica gap observation"
    );
    let operations = replica_engine
        .retained_recovery_ops(0, 10, 1024 * 1024)
        .unwrap()
        .operations;
    let replicated_noop = operations
        .iter()
        .find(|operation| operation.seq_no == 1)
        .expect("promotion NoOp must reach the replica on activation retry");
    assert_eq!(replicated_noop.primary_term, 2);
    assert_eq!(replicated_noop.op, crate::wal::WalOperation::NoOp);
    assert_eq!(replicated_noop.payload["_reason"], "promotion gap");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_noop_retry_on_one_shard_does_not_block_other_shards() {
    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = unavailable.local_addr().unwrap().port();
    drop(unavailable);

    let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let blackhole_port = blackhole.local_addr().unwrap().port();
    let (blackhole_connected_tx, blackhole_connected_rx) = tokio::sync::oneshot::channel();
    let blackhole_task = tokio::spawn(async move {
        let (socket, _) = blackhole.accept().await.unwrap();
        let _ = blackhole_connected_tx.send(());
        let _socket = socket;
        std::future::pending::<()>().await;
    });

    let mut state = gap_test_state(replica_port);
    state.add_node(DomainNodeInfo {
        id: "replica2".into(),
        name: "replica2".into(),
        host: "127.0.0.1".into(),
        transport_port: blackhole_port,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    state.add_index(DomainIndexMetadata {
        name: "idx2".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-2"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "source".into(),
                primary_term: 2,
                replicas: vec!["replica2".into()],
                in_sync_replicas: vec!["replica2".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });

    let seed_gap = |engine: &Arc<dyn SearchEngine>| {
        for seq_no in [0, 2] {
            engine
                .apply_replica_operation(crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"seq": seq_no}),
                    },
                })
                .unwrap();
        }
    };

    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    for (index, index_uuid) in [("idx", "uuid-1"), ("idx2", "uuid-2")] {
        let engine = source_shards
            .open_assigned_shard_with_settings(
                index,
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                index_uuid,
                crate::shard::AssignedShardOpen {
                    allocation_id: state.shard_allocation_id(index, 0, "source").unwrap(),
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        seed_gap(&engine);
    }
    let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    source_manager.update_state(state.clone());
    let source_service = Arc::new(TransportService {
        cluster_manager: source_manager,
        shard_manager: source_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        source_service.activate_primary_for_lifecycle("idx", 0),
    )
    .await
    .unwrap()
    .unwrap();

    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_allocation = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let replica_engine = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    seed_gap(&replica_engine);
    replica_shards
        .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), replica_allocation, 2)
        .await
        .unwrap();
    let replica_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    replica_manager.update_state(state);
    let replica_activation_state = new_primary_activation_state();
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: replica_activation_state.clone(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let replica_listener = tokio::net::TcpListener::bind(("127.0.0.1", replica_port))
        .await
        .unwrap();
    let replica_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                replica_listener,
            ))
            .await
            .unwrap();
    });

    let blocked_service = source_service.clone();
    let blocked_activation = tokio::spawn(async move {
        blocked_service
            .activate_primary_for_lifecycle("idx2", 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), blackhole_connected_rx)
        .await
        .expect("idx2 NoOp retry never reached the black-hole replica")
        .unwrap();

    let started = std::time::Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        source_service.index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
            doc_id: "healthy-write".into(),
        })),
    )
    .await
    .expect("a NoOp retry on idx2 blocked a write to idx")
    .unwrap()
    .into_inner();
    let elapsed = started.elapsed();

    blocked_activation.abort();
    blackhole_task.abort();
    replica_server.abort();

    assert!(response.success, "{}", response.error);
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    assert_eq!(
        replica_activation_state
            .promotion_noop_bulk_requests_received
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promoted_primary_replays_multiple_batches_and_reopens_cleanly() {
    const DOCUMENT_COUNT: u64 = 1_001;

    let mut state = gap_test_state(1);
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    state
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .in_sync_replicas
        .clear();
    state.indices.get_mut("idx").unwrap().mappings.insert(
        "value".into(),
        FieldMapping {
            field_type: FieldType::Integer,
            dimension: None,
        },
    );
    state
        .indices
        .get_mut("idx")
        .unwrap()
        .settings
        .refresh_interval_ms = Some(3_600_000);
    let allocation_id = state.shard_allocation_id("idx", 0, "source").unwrap();
    let mappings = state.indices["idx"].mappings.clone();
    let settings = state.indices["idx"].settings.clone();
    let dir = tempfile::tempdir().unwrap();

    {
        let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(3600)));
        let engine = shards
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &mappings,
                &settings,
                "uuid-1",
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .apply_replica_batch(
                (0..DOCUMENT_COUNT)
                    .map(|seq_no| crate::engine::SequencedOperation {
                        seq_no,
                        primary_term: 1,
                        mutation: crate::engine::DocumentMutation::Index {
                            doc_id: format!("doc-{seq_no}"),
                            source: json!({"value": seq_no}),
                        },
                    })
                    .collect(),
            )
            .unwrap();

        let manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
        manager.update_state(state.clone());
        let service = TransportService {
            cluster_manager: manager,
            shard_manager: shards,
            transport_client: crate::transport::TransportClient::new(),
            storage_manager: test_storage_manager(dir.path()),
            remote_store_reader_cache: test_remote_store_reader_cache(),
            raft: None,
            local_node_id: "source".into(),
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            task_manager: Arc::new(crate::tasks::TaskManager::new()),
            primary_activation_state: new_primary_activation_state(),
            peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
            join_lock: new_join_lock(),
        };

        service
            .activate_primary_for_lifecycle("idx", 0)
            .await
            .unwrap();
        let response = service
            .index_doc(Request::new(ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "after-promotion".into(),
                payload_json: serde_json::to_vec(&json!({"value": DOCUMENT_COUNT})).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), DOCUMENT_COUNT + 1);
        for seq_no in 0..DOCUMENT_COUNT {
            assert_eq!(
                engine
                    .get_document(&format!("doc-{seq_no}"))
                    .unwrap()
                    .unwrap()["value"],
                json!(seq_no)
            );
        }
        assert_eq!(
            engine.get_document("after-promotion").unwrap().unwrap()["value"],
            json!(DOCUMENT_COUNT)
        );
    }

    let reopened_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(3600)));
    let reopened = reopened_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &mappings,
            &settings,
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: false,
            },
        )
        .unwrap();
    assert_eq!(reopened.doc_count(), DOCUMENT_COUNT + 1);
    assert_eq!(
        reopened.sequence_stats().processed_checkpoint,
        Some(DOCUMENT_COUNT)
    );
    for seq_no in 0..DOCUMENT_COUNT {
        assert_eq!(
            reopened
                .get_document(&format!("doc-{seq_no}"))
                .unwrap()
                .unwrap()["value"],
            json!(seq_no)
        );
    }
    assert_eq!(
        reopened.get_document("after-promotion").unwrap().unwrap()["value"],
        json!(DOCUMENT_COUNT)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_only_primary_fault_stays_unavailable_without_term_flapping_and_clears_on_write() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) = crate::consensus::create_raft_instance_mem(1, "primary-io".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let metadata = DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let allocation_id = shared_state
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );

    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let primary_engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    service.ensure_primary_activated("idx", 0).await.unwrap();
    let active_term = shared_state.read().unwrap().indices["idx"].shard_routing[&0].primary_term;
    primary_engine.inject_wal_write_failures_for_test(28, usize::MAX);
    for _ in 0..3 {
        let response = service
            .index_doc(Request::new(ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
    }
    assert_eq!(
        shared_state.read().unwrap().primary_allocation_id("idx", 0),
        Some(allocation_id)
    );
    assert!(
        shared_state.read().unwrap().primary_unavailable("idx", 0),
        "failed single-copy primary must be status-red without routing mutation"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    let unavailable_log_id = service
        .raft
        .as_ref()
        .unwrap()
        .metrics()
        .borrow_watched()
        .last_applied;

    for interval in 0..2 {
        service
            .primary_activation_state
            .failed_copy_reports
            .lock()
            .await
            .clear();
        service
            .activate_primary_for_lifecycle("idx", 0)
            .await
            .unwrap();
        for attempt in 0..3 {
            let response = service
                .index_doc(Request::new(ShardDocRequest {
                    index_name: "idx".into(),
                    shard_id: 0,
                    doc_id: format!("still-failing-{interval}-{attempt}"),
                    payload_json: serde_json::to_vec(&json!({"value": 2})).unwrap(),
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!response.success);
        }
        let state = shared_state.read().unwrap();
        assert_eq!(
            state.indices["idx"].shard_routing[&0].primary_term,
            active_term
        );
        assert!(state.primary_unavailable("idx", 0));
        assert_eq!(
            service
                .raft
                .as_ref()
                .unwrap()
                .metrics()
                .borrow_watched()
                .last_applied,
            unavailable_log_id,
            "an already-unavailable allocation must not receive another Raft report"
        );
    }

    let version_before_repair = shared_state.read().unwrap().version;
    primary_engine.inject_wal_write_failures_for_test(28, 0);
    let repaired = service
        .index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "repaired".into(),
            payload_json: serde_json::to_vec(&json!({"value": 3})).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(repaired.success, "{}", repaired.error);
    let clear_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let cleared = {
            let state = shared_state.read().unwrap();
            (!state.primary_unavailable("idx", 0)).then_some((
                state.indices["idx"].shard_routing[&0].primary_term,
                state.version,
            ))
        };
        if let Some((primary_term, version)) = cleared {
            assert_eq!(primary_term, active_term);
            assert_eq!(
                version,
                version_before_repair + 1,
                "the first successful write should commit one status-only clear"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < clear_deadline,
            "background primary-available report did not clear the flag"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_write_does_not_wait_for_primary_available_report() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("available-report".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.primary_allocation_id("idx", 0).unwrap();
    {
        let allocation = state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        allocation.primary_initialized = true;
        allocation.primary_unavailable = true;
    }
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let _blocked_report = service
        .primary_activation_state
        .available_primary_reports
        .lock()
        .await;
    let response = tokio::time::timeout(
        Duration::from_millis(250),
        service.index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "fast-response".into(),
            payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
        })),
    )
    .await
    .expect("a successful write must not wait for primary-available reporting")
    .unwrap()
    .into_inner();
    assert!(response.success, "{}", response.error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_write_does_not_spawn_primary_available_report() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("ordinary-write".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.primary_allocation_id("idx", 0).unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let _blocked_report = service
        .primary_activation_state
        .available_primary_reports
        .lock()
        .await;
    let response = service
        .index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "ordinary".into(),
            payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.success, "{}", response.error);
    tokio::task::yield_now().await;
    assert_eq!(
        service
            .primary_activation_state
            .available_report_tasks_spawned
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "healthy writes must not spawn primary-availability reporting"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repaired_open_fault_reactivates_primary_and_clears_unavailable_status() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, "primary-open-repair".into())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    let metadata = DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let allocation_id = shared_state
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager: shard_manager.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    service.ensure_primary_activated("idx", 0).await.unwrap();
    let active_term = shared_state.read().unwrap().indices["idx"].shard_routing[&0].primary_term;

    shard_manager.quarantine_shard_copy("idx", 0);
    shard_manager.inject_assigned_open_io_failures(5, 3);
    for _ in 0..3 {
        assert!(
            service
                .activate_primary_for_lifecycle("idx", 0)
                .await
                .is_err()
        );
    }
    {
        let state = shared_state.read().unwrap();
        assert_eq!(
            state.indices["idx"].shard_routing[&0].primary_term,
            active_term
        );
        assert!(state.primary_unavailable("idx", 0));
    }

    shard_manager.inject_assigned_open_io_failures(5, 0);
    service
        .activate_primary_for_lifecycle("idx", 0)
        .await
        .unwrap();
    let state = shared_state.read().unwrap();
    assert_eq!(
        state.indices["idx"].shard_routing[&0].primary_term,
        active_term + 1
    );
    assert!(!state.primary_unavailable("idx", 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_apply_escalation_keeps_reads_open_without_immediate_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("apply-quarantine".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.primary_allocation_id("idx", 0).unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    engine
        .add_document_with_receipt("baseline", json!({"value": 0}))
        .unwrap();
    engine.refresh().unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager: shard_manager.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    engine.inject_engine_apply_failures_for_test(28, 3);
    for attempt in 0..3 {
        let response = service
            .index_doc(Request::new(ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: format!("failed-after-wal-{attempt}"),
                payload_json: serde_json::to_vec(&json!({"value": attempt})).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
    }
    assert!(engine.get_document("baseline").unwrap().is_some());
    for attempt in 0..3 {
        let document = engine
            .get_document(&format!("failed-after-wal-{attempt}"))
            .unwrap();
        if attempt < 2 {
            assert!(
                document.is_some(),
                "the next write must rebuild and replay the prior post-WAL gap"
            );
        } else {
            assert!(
                document.is_none(),
                "the currently failing post-WAL operation must remain invisible"
            );
        }
    }
    let wal_operations = engine
        .retained_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
    for attempt in 0..2 {
        assert!(wal_operations.iter().any(|operation| {
            operation.payload["_doc_id"] == format!("failed-after-wal-{attempt}")
        }));
    }
    assert!(
        wal_operations
            .iter()
            .all(|operation| operation.payload["_doc_id"] != "failed-after-wal-2"),
        "retained recovery must not serve an operation the source has not processed"
    );
    let current = shard_manager.get_shard("idx", 0).unwrap();
    assert!(Arc::ptr_eq(&current, &engine));
    let same_engine = service.get_or_open_shard("idx", 0).await.unwrap();
    assert!(Arc::ptr_eq(&same_engine, &engine));
    assert!(same_engine.get_document("baseline").unwrap().is_some());
    for attempt in 0..3 {
        let document = same_engine
            .get_document(&format!("failed-after-wal-{attempt}"))
            .unwrap();
        assert_eq!(
            document.is_some(),
            attempt < 2,
            "only operations replayed by a later writer rebuild may be visible"
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_commit_failure_recovers_writes_and_deletes_before_promotion() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("replica-commit-recovery".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["node-2".into()],
                in_sync_replicas: vec!["node-2".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let replica_allocation = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager: cluster_manager.clone(),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-2".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let replicate = |doc_id: String,
                     seq_no: u64,
                     op: &str,
                     payload: serde_json::Value|
     -> ReplicateDocRequest {
        ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            op: op.into(),
            seq_no,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(replica_allocation),
        }
    };
    assert!(
        service
            .replicate_doc(Request::new(replicate(
                "pre-fault".into(),
                0,
                "index",
                json!({"value": 0}),
            )))
            .await
            .unwrap()
            .into_inner()
            .success
    );
    engine.refresh().unwrap();
    assert!(
        service
            .replicate_doc(Request::new(replicate(
                "victim".into(),
                1,
                "index",
                json!({"value": 1}),
            )))
            .await
            .unwrap()
            .into_inner()
            .success
    );
    engine.refresh().unwrap();
    assert!(
        service
            .replicate_doc(Request::new(replicate(
                "victim".into(),
                2,
                "delete",
                json!({}),
            )))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    let index_dir = dir.path().join("uuid-1/shard_0/index");
    std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    assert!(
        service
            .replicate_doc(Request::new(replicate(
                "during-fault".into(),
                3,
                "index",
                json!({"value": 3}),
            )))
            .await
            .unwrap()
            .into_inner()
            .success
    );
    std::thread::sleep(Duration::from_millis(300));
    let failed_commit = engine.refresh();
    std::fs::set_permissions(&index_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(failed_commit.is_err());

    let mut acknowledged = vec!["pre-fault".to_string(), "during-fault".to_string()];
    for offset in 0..5 {
        let id = format!("acked-{offset}");
        let response = service
            .replicate_doc(Request::new(replicate(
                id.clone(),
                offset + 4,
                "index",
                json!({"value": offset + 4}),
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(response.success, "{}", response.error);
        acknowledged.push(id);
    }
    engine.refresh().unwrap();

    let mut promoted = cluster_manager.get_state();
    {
        let routing = promoted
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap();
        routing.primary = "node-2".into();
        routing.primary_term = 3;
        routing.replicas.clear();
        routing.in_sync_replicas.clear();
    }
    {
        let allocations = promoted
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        allocations.replicas.remove("node-2");
        allocations.primary = Some(replica_allocation);
    }
    cluster_manager.update_state(promoted);
    let primary_write = service
        .index_doc(Request::new(ShardDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "after-promotion".into(),
            payload_json: serde_json::to_vec(&json!({"value": 7})).unwrap(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(primary_write.success, "{}", primary_write.error);
    acknowledged.push("after-promotion".into());
    engine.refresh().unwrap();
    for id in acknowledged {
        assert!(
            engine.get_document(&id).unwrap().is_some(),
            "promoted replica lost acknowledged document {id}"
        );
    }
    assert!(
        engine.get_document("victim").unwrap().is_none(),
        "promoted replica resurrected an acknowledged delete"
    );
}

#[tokio::test]
async fn definitive_failure_quarantine_happens_only_after_report_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = DomainClusterState::new("definitive-quarantine".into());
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: Vec::new(),
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let allocation_id = state.primary_allocation_id("idx", 0).unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state);
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    let service = TransportService {
        cluster_manager,
        shard_manager: shard_manager.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let error: anyhow::Error = serde_json::from_str::<serde_json::Value>("{not-json")
        .unwrap_err()
        .into();
    service
        .primary_activation_state
        .failed_copy_reports
        .lock()
        .await
        .insert(
            ("uuid-1".into(), 0, allocation_id),
            std::time::Instant::now(),
        );
    service
        .report_local_copy_failure("idx", "uuid-1", 0, allocation_id, 2, &error)
        .await;
    assert!(shard_manager.get_shard("idx", 0).is_some());

    service
        .primary_activation_state
        .failed_copy_reports
        .lock()
        .await
        .clear();
    service
        .report_local_copy_failure("idx", "uuid-1", 0, allocation_id, 2, &error)
        .await;
    assert!(shard_manager.get_shard("idx", 0).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_primary_apply_io_promotes_live_in_sync_replica() {
    let dir = tempfile::tempdir().unwrap();
    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, "primary-apply-io".into())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    for node_id in ["node-1", "node-2"] {
        assert_eq!(
            raft.client_write(crate::consensus::types::ClusterCommand::AddNode {
                node: DomainNodeInfo {
                    id: node_id.into(),
                    name: node_id.into(),
                    host: "127.0.0.1".into(),
                    transport_port: 0,
                    http_port: 0,
                    roles: vec![NodeRole::Data],
                    raft_node_id: 0,
                },
            })
            .await
            .unwrap()
            .data,
            crate::consensus::types::ClusterResponse::Ok
        );
    }
    let metadata = DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 1,
                replicas: vec!["node-2".into()],
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let primary_allocation = shared_state
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let replica_allocation = shared_state
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(crate::consensus::types::ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "uuid-1".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id: replica_allocation,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        crate::consensus::types::ClusterResponse::Ok
    );
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let primary_engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id: primary_allocation,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    primary_engine.inject_wal_write_failures_for_test(28, 3);
    shard_manager.isr_tracker.update_replica_checkpoint(
        "idx",
        "uuid-1",
        0,
        2,
        Some(10),
        crate::shard::ReplicaCheckpointUpdate {
            node_id: "node-2".into(),
            allocation_id: replica_allocation,
            processed_checkpoint: Some(10),
            persisted_checkpoint: Some(10),
        },
    );
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    for _ in 0..3 {
        let response = service
            .index_doc(Request::new(ShardDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc".into(),
                payload_json: serde_json::to_vec(&json!({"value": 1})).unwrap(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
    }
    assert_eq!(
        shared_state.read().unwrap().indices["idx"].shard_routing[&0].primary,
        "node-2"
    );
}

#[test]
fn leader_selects_live_highest_checkpoint_promotion_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.isr_tracker.update_replica_checkpoints(
        "idx",
        "uuid-1",
        0,
        2,
        Some(100),
        &[
            replica_checkpoint("dead-node", Some(100), Some(100)),
            replica_checkpoint("node-2", Some(10), Some(10)),
            replica_checkpoint("node-3", Some(20), Some(20)),
        ],
    );
    let mut state = DomainClusterState::new("candidate-ranking".into());
    for node_id in ["node-1", "node-2", "node-3"] {
        state.add_node(DomainNodeInfo {
            id: node_id.into(),
            name: node_id.into(),
            host: "127.0.0.1".into(),
            transport_port: 0,
            http_port: 0,
            roles: vec![NodeRole::Data],
            raft_node_id: 0,
        });
    }
    state.add_index(DomainIndexMetadata {
        name: "idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("uuid-1"),
        number_of_shards: 1,
        number_of_replicas: 3,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["dead-node".into(), "node-2".into(), "node-3".into()],
                in_sync_replicas: vec!["dead-node".into(), "node-2".into(), "node-3".into()],
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: crate::cluster::state::IndexSettings::default(),
    });
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::new(state.cluster_name.clone())),
        shard_manager,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    assert_eq!(
        service.select_live_promotion_candidate(&state, "idx", 0),
        Some("node-3".into())
    );
}

#[tokio::test]
async fn search_remote_store_splits_requires_local_index_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let service = TransportService {
        cluster_manager: Arc::new(ClusterManager::new("remote-store-metadata".into())),
        shard_manager: Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60))),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "node-1".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let err = service
        .search_remote_store_splits(Request::new(RemoteStoreSearchRequest {
            index_name: "remotehits".into(),
            index_uuid: "idx-1".into(),
            search_request_json: serde_json::to_vec(&crate::search::SearchRequest {
                query: crate::search::QueryClause::MatchAll(serde_json::json!({})),
                size: 10,
                from: 0,
                knn: None,
                sort: Vec::new(),
                search_after: None,
                aggs: HashMap::new(),
            })
            .unwrap(),
            splits: Vec::new(),
            live_split_ids: Vec::new(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::NotFound);
    assert!(err.message().contains("index [remotehits] not found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_c3_collision_with_lagging_view_quarantines_copy() {
    let mut state = gap_test_state(0);
    state
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary_term = 1;
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let engine = shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    engine
        .apply_replica_batch(
            (0..=5)
                .map(|seq_no| crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"term": 1, "seq": seq_no}),
                    },
                })
                .collect(),
        )
        .unwrap();
    let manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    manager.update_state(state);
    let service = TransportService {
        cluster_manager: manager,
        shard_manager: shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    match service
        .replicate_doc(Request::new(ReplicateDocRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "doc-5".into(),
            payload_json: serde_json::to_vec(&json!({"term": 2})).unwrap(),
            op: "index".into(),
            seq_no: 5,
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        }))
        .await
    {
        Ok(response) => assert!(!response.into_inner().success),
        Err(status) => assert_eq!(status.code(), tonic::Code::DataLoss),
    }
    assert!(
        shards.get_shard("idx", 0).is_none(),
        "a definitive collision must quarantine the copy even when its routing view lags"
    );
}

#[derive(Clone, Default)]
struct ReviewC3LogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for ReviewC3LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn review_c3_unopened_copy_gap_probe_is_transient() {
    let logs = ReviewC3LogBuffer::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = listener.local_addr().unwrap().port();
    let mut state = gap_test_state(replica_port);
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    replica_manager.update_state(state.clone());
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    source_shards.isr_tracker.update_replica_checkpoints_at(
        "idx",
        0,
        crate::shard::ReplicaCheckpointContext {
            index_uuid: "uuid-1",
            primary_term: 2,
            primary_processed_checkpoint: Some(5),
        },
        &[crate::shard::ReplicaCheckpointUpdate {
            node_id: "replica".into(),
            allocation_id,
            processed_checkpoint: Some(4),
            persisted_checkpoint: Some(4),
        }],
        std::time::Instant::now() - Duration::from_secs(61),
    );
    let source_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    source_manager.update_state(state);
    let source_service = TransportService {
        cluster_manager: source_manager,
        shard_manager: source_shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    source_service.reconcile_replica_gaps().await;
    server.abort();

    let captured = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(
        captured.contains("Replica gap probe failed transiently"),
        "{captured}"
    );
    assert!(
        !captured.contains("Failed to remove replica after the fixed gap target remained unmet"),
        "{captured}"
    );
    assert_eq!(
        source_shards.isr_tracker.gap_observations("idx", 0).len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_c3_primary_reports_collision_from_lagging_view_replica() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_port = listener.local_addr().unwrap().port();
    let mut authoritative = gap_test_state(replica_port);
    authoritative
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = authoritative
        .shard_allocation_id("idx", 0, "replica")
        .unwrap();

    let mut lagged = authoritative.clone();
    lagged
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary_term = 1;
    let replica_dir = tempfile::tempdir().unwrap();
    let replica_shards = Arc::new(ShardManager::new(
        replica_dir.path(),
        Duration::from_secs(60),
    ));
    let replica_engine = replica_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &crate::cluster::state::IndexSettings::default(),
            "uuid-1",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 1,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    replica_engine
        .apply_replica_batch(
            (0..=5)
                .map(|seq_no| crate::engine::SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: crate::engine::DocumentMutation::Index {
                        doc_id: format!("doc-{seq_no}"),
                        source: json!({"term": 1, "seq": seq_no}),
                    },
                })
                .collect(),
        )
        .unwrap();
    let replica_manager = Arc::new(ClusterManager::new(lagged.cluster_name.clone()));
    replica_manager.update_state(lagged);
    let replica_service = TransportService {
        cluster_manager: replica_manager,
        shard_manager: replica_shards,
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(replica_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let replica_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(InternalTransportServer::new(replica_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let (raft, shared_state) =
        crate::consensus::create_raft_instance_mem(1, authoritative.cluster_name.clone())
            .await
            .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:0".into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !raft.is_leader() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    *shared_state.write().unwrap() = authoritative.clone();

    let primary_dir = tempfile::tempdir().unwrap();
    let primary_service = TransportService {
        cluster_manager: Arc::new(ClusterManager::with_shared_state(shared_state.clone())),
        shard_manager: Arc::new(ShardManager::new(
            primary_dir.path(),
            Duration::from_secs(60),
        )),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(primary_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: Some(raft),
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let failures = crate::replication::replicate_write_with_durability(
        &primary_service.transport_client,
        &authoritative,
        "idx",
        0,
        "doc-5",
        &json!({"term": 2}),
        "index",
        5,
        2,
        crate::wal::TranslogDurability::Request,
    )
    .await
    .unwrap_err();
    assert_eq!(failures.len(), 1);
    assert!(failures[0].definitive);
    assert_eq!(failures[0].allocation_id, Some(allocation_id));

    primary_service
        .report_definitive_replica_failures(&authoritative, "idx", 0, 2, &failures)
        .await;
    replica_server.abort();

    let state = shared_state.read().unwrap();
    let routing = &state.indices["idx"].shard_routing[&0];
    assert!(routing.replicas.is_empty());
    assert!(routing.in_sync_replicas.is_empty());
    assert_eq!(routing.unassigned_replicas, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_c3_gap_probes_run_concurrently_with_short_timeout() {
    let listener_one = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_two = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port_one = listener_one.local_addr().unwrap().port();
    let port_two = listener_two.local_addr().unwrap().port();
    let blackhole = |listener: tokio::net::TcpListener| {
        tokio::spawn(async move {
            let mut sockets = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                sockets.push(socket);
            }
        })
    };
    let blackhole_one = blackhole(listener_one);
    let blackhole_two = blackhole(listener_two);

    let mut state = gap_test_state(port_one);
    state.add_node(DomainNodeInfo {
        id: "replica-2".into(),
        name: "replica-2".into(),
        host: "127.0.0.1".into(),
        transport_port: port_two,
        http_port: 0,
        roles: vec![NodeRole::Data],
        raft_node_id: 0,
    });
    let allocation_two = 2;
    {
        let routing = state
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap();
        routing.replicas.push("replica-2".into());
        routing.in_sync_replicas.push("replica-2".into());
    }
    {
        let allocations = state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap();
        allocations
            .replicas
            .insert("replica-2".into(), allocation_two);
        allocations.primary_initialized = true;
    }
    let allocation_one = state.shard_allocation_id("idx", 0, "replica").unwrap();

    let source_dir = tempfile::tempdir().unwrap();
    let source_shards = Arc::new(ShardManager::new(
        source_dir.path(),
        Duration::from_secs(60),
    ));
    source_shards.isr_tracker.update_replica_checkpoints_at(
        "idx",
        0,
        crate::shard::ReplicaCheckpointContext {
            index_uuid: "uuid-1",
            primary_term: 2,
            primary_processed_checkpoint: Some(5),
        },
        &[
            crate::shard::ReplicaCheckpointUpdate {
                node_id: "replica".into(),
                allocation_id: allocation_one,
                processed_checkpoint: Some(4),
                persisted_checkpoint: Some(4),
            },
            crate::shard::ReplicaCheckpointUpdate {
                node_id: "replica-2".into(),
                allocation_id: allocation_two,
                processed_checkpoint: Some(4),
                persisted_checkpoint: Some(4),
            },
        ],
        std::time::Instant::now() - Duration::from_secs(61),
    );
    let manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    manager.update_state(state);
    let service = TransportService {
        cluster_manager: manager,
        shard_manager: source_shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(source_dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "source".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };

    let started = std::time::Instant::now();
    service
        .reconcile_replica_gaps_with_probe_timeout(Duration::from_millis(300))
        .await;
    let elapsed = started.elapsed();
    blackhole_one.abort();
    blackhole_two.abort();

    assert!(
        elapsed < Duration::from_millis(550),
        "two concurrent 300ms probes took {elapsed:?}"
    );
    assert_eq!(
        source_shards.isr_tracker.gap_observations("idx", 0).len(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collision_quarantine_rejects_reopen_and_follow_up_replication() {
    let mut state = gap_test_state(0);
    state
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary_term = 1;
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    {
        let engine = shards
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                "uuid-1",
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .apply_replica_batch(
                (0..=5)
                    .map(|seq_no| crate::engine::SequencedOperation {
                        seq_no,
                        primary_term: 1,
                        mutation: crate::engine::DocumentMutation::Index {
                            doc_id: format!("doc-{seq_no}"),
                            source: json!({"term": 1, "seq": seq_no}),
                        },
                    })
                    .collect(),
            )
            .unwrap();
    }
    let manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    manager.update_state(state);
    let service = TransportService {
        cluster_manager: manager,
        shard_manager: shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let request = |seq_no: u64| ReplicateDocRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: format!("doc-{seq_no}"),
        payload_json: serde_json::to_vec(&json!({"term": 2, "seq": seq_no})).unwrap(),
        op: "index".into(),
        seq_no,
        index_uuid: "uuid-1".into(),
        primary_term: Some(2),
        target_allocation_id: Some(allocation_id),
    };

    let collision = service
        .replicate_doc(Request::new(request(5)))
        .await
        .unwrap_err();
    assert_eq!(collision.code(), tonic::Code::DataLoss);
    assert!(shards.get_shard("idx", 0).is_none());

    let identity_path = dir
        .path()
        .join("uuid-1")
        .join("shard_0")
        .join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let identity: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(identity["allocation_id"], allocation_id);
    assert_eq!(identity["collision_quarantined"], true);

    let follow_up = service
        .replicate_doc(Request::new(request(6)))
        .await
        .unwrap_err();
    assert_eq!(follow_up.code(), tonic::Code::DataLoss);
    assert!(
        follow_up
            .message()
            .contains("collision quarantine is active")
    );
    assert!(shards.get_shard("idx", 0).is_none());

    let second_follow_up = service
        .replicate_doc(Request::new(request(7)))
        .await
        .unwrap_err();
    assert_eq!(second_follow_up.code(), tonic::Code::DataLoss);
    assert!(
        second_follow_up
            .message()
            .contains("collision quarantine is active")
    );

    let bulk_follow_up = service
        .replicate_bulk(Request::new(ReplicateBulkRequest {
            index_name: "idx".into(),
            shard_id: 0,
            ops: vec![request(8)],
            index_uuid: "uuid-1".into(),
            primary_term: Some(2),
            target_allocation_id: Some(allocation_id),
        }))
        .await
        .unwrap_err();
    assert_eq!(bulk_follow_up.code(), tonic::Code::DataLoss);
    assert!(
        bulk_follow_up
            .message()
            .contains("collision quarantine is active")
    );
    assert!(shards.get_shard("idx", 0).is_none());

    drop(service);
    drop(shards);
    let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
    let error = match restarted.open_assigned_shard_with_settings(
        "idx",
        0,
        &HashMap::new(),
        &crate::cluster::state::IndexSettings::default(),
        "uuid-1",
        crate::shard::AssignedShardOpen {
            allocation_id,
            primary_term: 1,
            allow_empty_creation: false,
        },
    ) {
        Ok(_) => panic!("a collision-quarantined copy must not reopen"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("collision quarantine is active"));
    assert!(restarted.get_shard("idx", 0).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collision_marker_persist_failure_keeps_transport_quarantined() {
    let mut state = gap_test_state(0);
    state
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary_term = 1;
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = state.shard_allocation_id("idx", 0, "replica").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let shards = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    {
        let engine = shards
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &crate::cluster::state::IndexSettings::default(),
                "uuid-1",
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .apply_replica_batch(
                (0..=5)
                    .map(|seq_no| crate::engine::SequencedOperation {
                        seq_no,
                        primary_term: 1,
                        mutation: crate::engine::DocumentMutation::Index {
                            doc_id: format!("doc-{seq_no}"),
                            source: json!({"term": 1, "seq": seq_no}),
                        },
                    })
                    .collect(),
            )
            .unwrap();
        engine.refresh().unwrap();
    }
    let manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    manager.update_state(state);
    let service = TransportService {
        cluster_manager: manager,
        shard_manager: shards.clone(),
        transport_client: crate::transport::TransportClient::new(),
        storage_manager: test_storage_manager(dir.path()),
        remote_store_reader_cache: test_remote_store_reader_cache(),
        raft: None,
        local_node_id: "replica".into(),
        worker_pools: crate::worker::WorkerPools::new(2, 2),
        task_manager: Arc::new(crate::tasks::TaskManager::new()),
        primary_activation_state: new_primary_activation_state(),
        peer_recovery_state: peer_recovery::new_peer_recovery_transport_state(),
        join_lock: new_join_lock(),
    };
    let request = |seq_no: u64| ReplicateDocRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: format!("doc-{seq_no}"),
        payload_json: serde_json::to_vec(&json!({"term": 2, "seq": seq_no})).unwrap(),
        op: "index".into(),
        seq_no,
        index_uuid: "uuid-1".into(),
        primary_term: Some(2),
        target_allocation_id: Some(allocation_id),
    };
    shards.inject_collision_quarantine_persist_failures(28, 2);

    let collision = service
        .replicate_doc(Request::new(request(5)))
        .await
        .unwrap_err();
    assert_eq!(collision.code(), tonic::Code::DataLoss);

    let durable: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join("uuid-1/shard_0")
                .join(crate::shard::SHARD_COPY_IDENTITY_FILE),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(durable["collision_quarantined"], false);
    assert!(
        shards
            .copy_identity("idx", 0)
            .is_some_and(|identity| identity.collision_quarantined)
    );
    assert!(shards.get_shard("idx", 0).is_none());

    let follow_up = service
        .replicate_doc(Request::new(request(6)))
        .await
        .unwrap_err();
    assert_eq!(follow_up.code(), tonic::Code::DataLoss);
    assert!(
        follow_up
            .message()
            .contains("collision quarantine is active")
    );
    assert!(shards.get_shard("idx", 0).is_none());
    let durable: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join("uuid-1/shard_0")
                .join(crate::shard::SHARD_COPY_IDENTITY_FILE),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(durable["collision_quarantined"], false);

    let read = service
        .get_doc(Request::new(ShardGetRequest {
            index_name: "idx".into(),
            shard_id: 0,
            doc_id: "doc-5".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!read.found);
    assert!(read.error.contains("collision quarantine is active"));
}
