use super::*;
use crate::cluster::manager::ClusterManager;
use crate::cluster::state::{
    ClusterState as DomainClusterState, FieldMapping, FieldType,
    IndexMetadata as DomainIndexMetadata, NodeInfo as DomainNodeInfo, NodeRole, ShardRoutingEntry,
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
        uuid: crate::cluster::state::IndexUuid::new("products-uuid"),
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
            replicas: vec![],
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
        uuid: crate::cluster::state::IndexUuid::new(test_uuid),
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
        uuid: crate::cluster::state::IndexUuid::new(test_uuid),
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
        uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
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
        uuid: crate::cluster::state::IndexUuid::new("test-uuid"),
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

#[tokio::test]
async fn flush_index_reopens_assigned_shard_before_running_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let test_uuid = "test-uuid-flush";
    {
        let shard_dir = dir.path().join(test_uuid).join("shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let engine = CompositeEngine::new(&shard_dir, Duration::from_secs(60)).unwrap();
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
            replicas: vec![],
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

#[tokio::test]
async fn flush_index_refuses_to_create_missing_uuid_dir() {
    let dir = tempfile::tempdir().unwrap();
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
    cluster_state.add_index(DomainIndexMetadata {
        name: "maint-idx".into(),
        uuid: crate::cluster::state::IndexUuid::new("missing-uuid"),
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
