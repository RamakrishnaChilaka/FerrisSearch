use super::*;
use crate::cluster::state::{IndexMetadata, IndexSettings, IndexUuid, ShardRoutingEntry};
use crate::consensus::types::ClusterResponse;
use crate::transport::proto::internal_transport_server::InternalTransport;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn apply_index(
    engine: &Arc<dyn crate::engine::SearchEngine>,
    doc_id: &str,
    source: serde_json::Value,
    seq_no: u64,
    primary_term: u64,
) {
    engine
        .apply_replica_operation(crate::engine::SequencedOperation {
            seq_no,
            primary_term,
            mutation: crate::engine::DocumentMutation::Index {
                doc_id: doc_id.to_string(),
                source,
            },
        })
        .unwrap();
}

#[tokio::test]
async fn node_rejects_excessive_peer_recovery_concurrency() {
    let config = crate::config::AppConfig {
        max_concurrent_peer_recoveries: 65,
        ..Default::default()
    };
    let error = match Node::new(config).await {
        Ok(_) => panic!("excessive peer recovery concurrency must be rejected"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("max_concurrent_peer_recoveries must be between 0 and 64")
    );
}

#[test]
fn dead_node_removal_waits_for_routing_update_success() {
    assert!(dead_node_removal_allowed(false));
    assert!(
        !dead_node_removal_allowed(true),
        "node removal must be deferred when promotion/routing persistence fails"
    );
}

#[test]
fn failed_copy_reports_are_throttled_per_allocation() {
    let mut reports = std::collections::HashMap::new();
    let now = std::time::Instant::now();
    let key = ("uuid-1".to_string(), 0, "node-1".to_string(), 7);
    assert!(should_attempt_failed_copy_report(
        &mut reports,
        key.clone(),
        now
    ));
    assert!(!should_attempt_failed_copy_report(
        &mut reports,
        key.clone(),
        now + Duration::from_secs(59),
    ));
    assert!(should_attempt_failed_copy_report(
        &mut reports,
        key,
        now + Duration::from_secs(60),
    ));
    assert!(should_attempt_failed_copy_report(
        &mut reports,
        ("uuid-1".to_string(), 0, "node-1".to_string(), 8),
        now + Duration::from_secs(1),
    ));
}

#[tokio::test]
async fn reconciliation_closes_engine_after_local_allocation_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let mut state = crate::cluster::state::ClusterState::new("removed-copy".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    });
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();

    {
        let routing = state
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap();
        routing.replicas.clear();
        routing.in_sync_replicas.clear();
        routing.unassigned_replicas = 1;
    }
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .replicas
        .remove("node-2");

    assert!(
        open_local_assigned_shards(
            &state,
            "node-2",
            &shard_manager,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .is_empty()
    );
    assert!(shard_manager.get_shard("idx", 0).is_none());
    assert!(dir.path().join("idx-uuid/shard_0").exists());
}

#[tokio::test]
async fn issue_152_reconciliation_closes_deleted_index_and_wal() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let mut state = crate::cluster::state::ClusterState::new("recreate".into());
    let metadata = IndexMetadata::build_shard_routing("idx", 1, 0, &["node-2".into()]);
    let old_uuid = metadata.uuid.clone();
    state.add_index(metadata);
    let guarded = std::sync::Mutex::new(std::collections::HashSet::new());
    assert!(open_local_assigned_shards(&state, "node-2", &manager, &guarded).is_empty());
    let old_engine = manager.get_shard("idx", 0).unwrap();
    apply_index(&old_engine, "old", serde_json::json!({"value": 1}), 0, 1);
    let weak = Arc::downgrade(&old_engine);
    drop(old_engine);
    state.indices.remove("idx");
    state.shard_allocations.remove("idx");
    state.version += 1;

    assert!(open_local_assigned_shards(&state, "node-2", &manager, &guarded).is_empty());
    assert!(
        manager.get_shard("idx", 0).is_none(),
        "applied deletion must retire an engine absent from metadata"
    );
    assert!(
        weak.upgrade().is_none(),
        "the manager must release the old engine and WAL"
    );
    assert!(manager.copy_identity("idx", 0).is_none());
    assert!(dir.path().join(&old_uuid).join("shard_0").is_dir());
}

#[tokio::test]
async fn issue_152_reconciliation_replaces_uuid_without_deleting_other_data() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let mut state = crate::cluster::state::ClusterState::new("recreate".into());
    let old = IndexMetadata::build_shard_routing("idx", 1, 0, &["node-2".into()]);
    let old_uuid = old.uuid.clone();
    state.add_index(old);
    let guarded = std::sync::Mutex::new(std::collections::HashSet::new());
    assert!(open_local_assigned_shards(&state, "node-2", &manager, &guarded).is_empty());
    let old_engine = manager.get_shard("idx", 0).unwrap();
    apply_index(&old_engine, "old", serde_json::json!({"value": 1}), 0, 1);
    drop(old_engine);
    let old_identity_path = dir
        .path()
        .join(&old_uuid)
        .join("shard_0")
        .join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let old_identity = std::fs::read(&old_identity_path).unwrap();
    let new = IndexMetadata::build_shard_routing("idx", 1, 0, &["node-2".into()]);
    let new_uuid = new.uuid.clone();
    state.add_index_with_allocation_id(new, 2).unwrap();
    state.version += 1;

    let failures = open_local_assigned_shards(&state, "node-2", &manager, &guarded);
    assert!(failures.is_empty(), "{failures:?}");
    let identity = manager.copy_identity("idx", 0).unwrap();
    assert_eq!(identity.index_uuid, new_uuid.as_str());
    assert_eq!(identity.allocation_id, 2);
    let engine = manager.get_shard("idx", 0).unwrap();
    assert!(
        engine
            .get_document_with_metadata("old", true)
            .unwrap()
            .is_none()
    );
    apply_index(&engine, "new", serde_json::json!({"value": 2}), 0, 1);
    assert_eq!(
        engine
            .get_document_with_metadata("new", true)
            .unwrap()
            .unwrap()
            .source,
        serde_json::json!({"value": 2})
    );
    assert_eq!(std::fs::read(old_identity_path).unwrap(), old_identity);
    assert!(dir.path().join(&new_uuid).join("shard_0").is_dir());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collision_quarantine_keeps_lifecycle_failure_reportable_until_removal() {
    let (raft, state_handle) =
        crate::consensus::create_raft_instance_mem(1, "collision-quarantine".into())
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
    assert_eq!(
        raft.client_write(ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "idx".into(),
                uuid: IndexUuid::new("idx-uuid"),
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
                settings: IndexSettings::default(),
            },
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    let primary_allocation = state_handle
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let allocation_id = state_handle
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );

    let dir = tempfile::tempdir().unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    for seq_no in 0..=5 {
        apply_index(
            &engine,
            &format!("doc-{seq_no}"),
            serde_json::json!({"term": 2, "seq": seq_no}),
            seq_no,
            2,
        );
    }
    engine.refresh().unwrap();

    let cluster_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
    let remote_store_resources = crate::transport::server::RemoteStoreTransportResources {
        storage_manager: Arc::new(crate::storage::StorageManager::new_in_path(dir.path()).unwrap()),
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
    };
    let (_transport_server, transport_service) =
        crate::transport::server::create_transport_service_with_raft_and_storage_handle(
            cluster_manager.clone(),
            shard_manager.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(crate::tasks::TaskManager::new()),
            remote_store_resources,
            "node-2".into(),
        );
    let collision = transport_service
        .replicate_doc(tonic::Request::new(
            crate::transport::proto::ReplicateDocRequest {
                index_name: "idx".into(),
                shard_id: 0,
                doc_id: "doc-5".into(),
                payload_json: serde_json::to_vec(&serde_json::json!({
                    "term": 3,
                    "seq": 5
                }))
                .unwrap(),
                op: "index".into(),
                seq_no: 5,
                index_uuid: "idx-uuid".into(),
                primary_term: Some(3),
                target_allocation_id: Some(allocation_id),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(collision.code(), tonic::Code::DataLoss);

    let identity_path = dir
        .path()
        .join("idx-uuid/shard_0")
        .join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let identity: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(identity["collision_quarantined"], true);

    let mut lagging_state = state_handle.read().unwrap().clone();
    lagging_state
        .indices
        .get_mut("idx")
        .unwrap()
        .shard_routing
        .get_mut(&0)
        .unwrap()
        .primary_term = 1;
    let stale_failure = open_local_assigned_shards(
        &lagging_state,
        "node-2",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    )
    .into_iter()
    .next()
    .expect("collision marker must remain reportable under a lagging routing view");
    assert_eq!(stale_failure.primary_term, 1);
    assert!(
        stale_failure
            .reason
            .contains("collision quarantine is active")
    );

    let mut recent_reports = std::collections::HashMap::new();
    report_failed_shard_copies(
        vec![stale_failure],
        cluster_manager.as_ref(),
        &shard_manager,
        &TransportClient::new(),
        raft.as_ref(),
        &mut recent_reports,
    )
    .await;
    assert!(
        state_handle.read().unwrap().indices["idx"].shard_routing[&0].is_replica_in_sync("node-2"),
        "the stale first report must not remove the current allocation"
    );

    let current_state = state_handle.read().unwrap().clone();
    let current_failure = open_local_assigned_shards(
        &current_state,
        "node-2",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    )
    .into_iter()
    .next()
    .expect("collision marker must be reported again after routing catches up");
    assert_eq!(current_failure.primary_term, 2);
    assert!(
        current_failure
            .reason
            .contains("collision quarantine is active")
    );
    report_failed_shard_copies(
        vec![current_failure],
        cluster_manager.as_ref(),
        &shard_manager,
        &TransportClient::new(),
        raft.as_ref(),
        &mut recent_reports,
    )
    .await;
    let removed_state = state_handle.read().unwrap().clone();
    assert!(
        !removed_state.indices["idx"].shard_routing[&0]
            .replicas
            .iter()
            .any(|node| node == "node-2")
    );
    assert_eq!(removed_state.shard_allocation_id("idx", 0, "node-2"), None);

    assert!(
        open_local_assigned_shards(
            &removed_state,
            "node-2",
            &shard_manager,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .is_empty()
    );
    assert!(shard_manager.get_shard("idx", 0).is_none());
    let identity: serde_json::Value =
        serde_json::from_slice(&std::fs::read(identity_path).unwrap()).unwrap();
    assert_eq!(identity["collision_quarantined"], true);
}

#[tokio::test]
async fn open_local_assigned_shards_opens_unopened_local_shards() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            primary_term: 1,
            replicas: vec!["node-2".into()],
            in_sync_replicas: vec!["node-2".into()],
            unassigned_replicas: 0,
        },
    );
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    std::fs::create_dir_all(dir.path().join("idx-uuid").join("shard_0")).unwrap();

    assert!(shard_manager.get_shard("idx", 0).is_none());
    open_local_assigned_shards(
        &state,
        "node-1",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert!(shard_manager.get_shard("idx", 0).is_some());
}

#[test]
fn open_local_assigned_shards_skips_missing_expected_uuid_dir_for_recovered_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("expected-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    let guarded = collect_guarded_startup_shards(&state, "node-1");
    open_local_assigned_shards(
        &state,
        "node-1",
        &shard_manager,
        &std::sync::Mutex::new(guarded),
    );
    assert!(shard_manager.get_shard("idx", 0).is_none());
    assert!(!dir.path().join("expected-uuid").exists());
}

#[tokio::test]
async fn failed_recovery_marker_reports_only_the_matching_inactive_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 1,
        shard_routing: HashMap::from([(
            0,
            ShardRoutingEntry {
                primary: "node-1".into(),
                primary_term: 2,
                replicas: vec!["node-2".into()],
                in_sync_replicas: Vec::new(),
                unassigned_replicas: 0,
            },
        )]),
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    let marker_dir = dir.path().join("idx-uuid/shard_0");
    std::fs::create_dir_all(&marker_dir).unwrap();
    std::fs::write(
        marker_dir.join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "index_uuid": "idx-uuid",
            "allocation_id": allocation_id,
        }))
        .unwrap(),
    )
    .unwrap();

    let shard_manager = Arc::new(shard_manager);
    assert!(
        shard_manager
            .begin_peer_recovery_target_blocking("idx".into(), 0, "idx-uuid".into(), allocation_id,)
            .await
            .unwrap()
    );
    assert!(
        open_local_assigned_shards(
            &state,
            "node-2",
            shard_manager.as_ref(),
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .is_empty(),
        "an active out-of-sync recovery target must not be failed"
    );
    shard_manager.end_peer_recovery_target("idx", 0);

    let failures = open_local_assigned_shards(
        &state,
        "node-2",
        shard_manager.as_ref(),
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].allocation_id, allocation_id);

    std::fs::write(
        marker_dir.join(crate::shard::PEER_RECOVERY_IN_PROGRESS_MARKER),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "index_uuid": "idx-uuid",
            "allocation_id": allocation_id + 1,
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(
        open_local_assigned_shards(
            &state,
            "node-2",
            shard_manager.as_ref(),
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .is_empty(),
        "a stale install marker must not fail the replacement allocation"
    );
}

#[tokio::test]
async fn open_local_assigned_shards_creates_missing_dir_for_new_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("fresh-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    open_local_assigned_shards(
        &state,
        "node-1",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert!(shard_manager.get_shard("idx", 0).is_some());
    assert!(dir.path().join("fresh-uuid").join("shard_0").exists());
}

#[tokio::test]
async fn recovered_node_only_guards_assignments_from_local_recovered_state() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let recovered_state = crate::cluster::state::ClusterState::new("node-test".into());

    let mut authoritative_state = crate::cluster::state::ClusterState::new("node-test".into());
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
    authoritative_state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("fresh-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    let guarded = build_guarded_startup_shards(Some(&recovered_state), "node-1");
    open_local_assigned_shards(&authoritative_state, "node-1", &shard_manager, &guarded);

    assert!(shard_manager.get_shard("idx", 0).is_some());
    assert!(dir.path().join("fresh-uuid").join("shard_0").exists());
}

#[tokio::test]
async fn recovered_startup_shards_remain_guarded_across_reopen_attempts() {
    // Simulate a recovered node whose authoritative startup state still
    // assigns shard 0 locally, but the expected UUID directory is gone.
    // Reconciliation must keep refusing to create a fresh shard dir for
    // that recovered startup assignment, even on later lifecycle ticks.
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let mut state = crate::cluster::state::ClusterState::new("guard-clear-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("my-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    // Directory does NOT exist on disk — guard should block every reopen attempt.
    let guarded = build_guarded_startup_shards(Some(&state), "node-1");
    open_local_assigned_shards(&state, "node-1", &shard_manager, &guarded);
    assert!(
        shard_manager.get_shard("idx", 0).is_none(),
        "guard should prevent opening a shard with missing dir"
    );
    assert!(!dir.path().join("my-uuid").join("shard_0").exists());

    // Simulate a later lifecycle reconciliation with the same
    // authoritative assignment. The recovered-startup guard must still
    // prevent recreating an empty shard directory.
    open_local_assigned_shards(&state, "node-1", &shard_manager, &guarded);
    assert!(
        shard_manager.get_shard("idx", 0).is_none(),
        "recovered startup assignments must remain guarded until real local data exists"
    );
    assert!(!dir.path().join("my-uuid").join("shard_0").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn open_local_assigned_shards_blocking_does_not_starve_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    std::fs::create_dir_all(dir.path().join("idx-uuid").join("shard_0")).unwrap();

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let manager = shard_manager.clone();
    let task = tokio::spawn(async move {
        let _ = started_tx.send(());
        open_local_assigned_shards_blocking(
            state,
            "node-1".into(),
            manager,
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        )
        .await;
    });

    let start = std::time::Instant::now();
    started_rx.await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "blocking lifecycle shard-open wrapper stalled the async runtime for {elapsed:?}"
    );

    task.await.unwrap();
    assert!(shard_manager.get_shard("idx", 0).is_some());
}

#[test]
fn cleanup_orphaned_data_if_authoritative_skips_empty_state() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let live_uuid = "live-uuid";

    std::fs::create_dir_all(dir.path().join(live_uuid).join("shard_0")).unwrap();

    let state = crate::cluster::state::ClusterState::new("node-test".into());
    assert!(!cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        false,
        &snapshot_uuid_dirs(dir.path()),
    ));
    assert!(dir.path().join(live_uuid).exists());
}

#[test]
fn cleanup_orphaned_data_if_authoritative_allows_empty_authoritative_state() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let orphan_uuid = "orphan-uuid";

    std::fs::create_dir_all(dir.path().join(orphan_uuid).join("shard_0")).unwrap();

    let state = crate::cluster::state::ClusterState::new("node-test".into());
    assert!(cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        true,
        &snapshot_uuid_dirs(dir.path()),
    ));
    assert!(!dir.path().join(orphan_uuid).exists());
}

#[test]
fn cleanup_orphaned_data_if_authoritative_keeps_known_uuid_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let known_uuid = "known-uuid";
    let orphan_uuid = "orphan-uuid";

    std::fs::create_dir_all(dir.path().join(known_uuid).join("shard_0")).unwrap();
    std::fs::create_dir_all(dir.path().join(orphan_uuid).join("shard_0")).unwrap();

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new(known_uuid),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    assert!(cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        true,
        &snapshot_uuid_dirs(dir.path()),
    ));
    assert!(dir.path().join(known_uuid).exists());
    assert!(!dir.path().join(orphan_uuid).exists());
}

#[test]
fn cleanup_orphaned_data_if_authoritative_skips_when_local_uuid_dir_missing() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    std::fs::create_dir_all(dir.path().join("old-live-uuid").join("shard_0")).unwrap();

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("expected-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    assert!(!cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        true,
        &snapshot_uuid_dirs(dir.path()),
    ));
    assert!(dir.path().join("old-live-uuid").exists());
}

#[test]
fn cleanup_skips_when_uuid_dir_was_freshly_created() {
    // Simulates the unsafe sequence that caused data loss:
    // 1. Old data lives under UUID "old-data-uuid"
    // 2. Raft state says shards belong under "new-raft-uuid"
    // 3. An unsafe reopen path creates "new-raft-uuid" before cleanup runs
    // 4. Orphan cleanup must NOT delete "old-data-uuid" because
    //    "new-raft-uuid" was freshly created (not pre-existing).
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    // Old data on disk (from a previous index creation)
    std::fs::create_dir_all(dir.path().join("old-data-uuid").join("shard_0")).unwrap();

    // Snapshot BEFORE opening shards — only old-data-uuid exists
    let pre_existing = snapshot_uuid_dirs(dir.path());
    assert!(pre_existing.contains("old-data-uuid"));
    assert!(!pre_existing.contains("new-raft-uuid"));

    // Simulate guard cleared + shard opened: create the new UUID dir
    std::fs::create_dir_all(dir.path().join("new-raft-uuid").join("shard_0")).unwrap();

    // Cluster state says shards are under new-raft-uuid
    let mut state = crate::cluster::state::ClusterState::new("guard-clear-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("new-raft-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    // Cleanup must SKIP because new-raft-uuid was not pre-existing
    assert!(!cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        true,
        &pre_existing,
    ));

    // old-data-uuid must survive — it has the real data!
    assert!(
        dir.path().join("old-data-uuid").exists(),
        "old data directory must NOT be deleted when the expected UUID dir was freshly created"
    );
}

#[test]
fn cleanup_runs_when_uuid_dir_was_pre_existing() {
    // On a subsequent restart, the UUID dir IS pre-existing → cleanup allowed.
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    // Both dirs exist on disk
    std::fs::create_dir_all(dir.path().join("current-uuid").join("shard_0")).unwrap();
    std::fs::create_dir_all(dir.path().join("stale-orphan").join("shard_0")).unwrap();

    let pre_existing = snapshot_uuid_dirs(dir.path());
    assert!(pre_existing.contains("current-uuid"));

    let mut state = crate::cluster::state::ClusterState::new("restart-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("current-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });

    // Cleanup should proceed and remove stale-orphan
    assert!(cleanup_orphaned_data_if_authoritative(
        Some(&state),
        "node-1",
        &shard_manager,
        true,
        &pre_existing,
    ));
    assert!(dir.path().join("current-uuid").exists());
    assert!(
        !dir.path().join("stale-orphan").exists(),
        "stale orphan should be cleaned up when expected UUID was pre-existing"
    );
}

#[test]
fn two_restart_recovery_sequence_preserves_old_data_and_never_creates_fresh_uuid_dir() {
    // This models the exact node-1 failure mode across two restarts:
    // 1. Old shard data exists only under an old UUID directory.
    // 2. Authoritative cluster state points at a different UUID.
    // 3. Startup reconciliation must refuse to create the new UUID dir.
    // 4. A second restart must still preserve the old data and avoid
    //    turning it into an orphan eligible for cleanup.
    let dir = tempfile::tempdir().unwrap();

    std::fs::create_dir_all(dir.path().join("old-data-uuid").join("shard_0")).unwrap();

    let mut state = crate::cluster::state::ClusterState::new("two-restart-test".into());
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
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("new-raft-uuid"),
        number_of_shards: 1,
        number_of_replicas: 0,
        shard_routing,
        mappings: HashMap::new(),
        dynamic: Default::default(),
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    // First restart: startup guard must keep the missing authoritative
    // UUID dir fail-closed and prevent orphan cleanup from touching old data.
    {
        let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let guard = build_guarded_startup_shards(Some(&state), "node-1");
        let pre_existing = snapshot_uuid_dirs(dir.path());

        assert!(pre_existing.contains("old-data-uuid"));
        assert!(!pre_existing.contains("new-raft-uuid"));

        open_local_assigned_shards(&state, "node-1", &shard_manager, &guard);
        assert!(shard_manager.get_shard("idx", 0).is_none());
        assert!(!dir.path().join("new-raft-uuid").exists());

        assert!(!cleanup_orphaned_data_if_authoritative(
            Some(&state),
            "node-1",
            &shard_manager,
            true,
            &pre_existing,
        ));
        assert!(dir.path().join("old-data-uuid").exists());
        assert!(!dir.path().join("new-raft-uuid").exists());
    }

    // Second restart: if the first restart had recreated the new UUID dir,
    // it would now appear pre-existing and cleanup could delete old-data-uuid.
    // The invariant is that the new dir never appears in the first place.
    {
        let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        let guard = build_guarded_startup_shards(Some(&state), "node-1");
        let pre_existing = snapshot_uuid_dirs(dir.path());

        assert!(pre_existing.contains("old-data-uuid"));
        assert!(!pre_existing.contains("new-raft-uuid"));

        open_local_assigned_shards(&state, "node-1", &shard_manager, &guard);
        assert!(shard_manager.get_shard("idx", 0).is_none());
        assert!(!dir.path().join("new-raft-uuid").exists());

        assert!(!cleanup_orphaned_data_if_authoritative(
            Some(&state),
            "node-1",
            &shard_manager,
            true,
            &pre_existing,
        ));
        assert!(dir.path().join("old-data-uuid").exists());
        assert!(!dir.path().join("new-raft-uuid").exists());
    }
}

#[test]
fn ping_rejection_requires_rejoin_only_for_not_found_status() {
    let rejected = anyhow::Error::from(tonic::Status::not_found("unknown node"));
    let unavailable = anyhow::Error::from(tonic::Status::unavailable("master down"));
    let transport = anyhow::anyhow!("transport connect failed");

    assert!(ping_rejection_requires_rejoin(&rejected));
    assert!(!ping_rejection_requires_rejoin(&unavailable));
    assert!(!ping_rejection_requires_rejoin(&transport));
}

#[test]
fn follower_join_retry_remaining_enforces_backoff_window() {
    let now = Instant::now();
    let min_interval = Duration::from_secs(15);

    assert_eq!(follower_join_retry_remaining(None, now, min_interval), None);

    let recent = now.checked_sub(Duration::from_secs(5)).unwrap();
    assert_eq!(
        follower_join_retry_remaining(Some(recent), now, min_interval),
        Some(Duration::from_secs(10))
    );

    let old = now.checked_sub(Duration::from_secs(20)).unwrap();
    assert_eq!(
        follower_join_retry_remaining(Some(old), now, min_interval),
        None
    );
}

#[test]
fn should_retry_cluster_join_only_when_local_node_missing() {
    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
    assert!(should_retry_cluster_join(&state, "node-1"));

    state.add_node(crate::cluster::state::NodeInfo {
        id: "node-1".into(),
        name: "node-1".into(),
        host: "127.0.0.1".into(),
        transport_port: 9300,
        http_port: 9200,
        roles: vec![crate::cluster::state::NodeRole::Data],
        raft_node_id: 1,
    });

    assert!(!should_retry_cluster_join(&state, "node-1"));
}

#[test]
fn remote_seed_hosts_excludes_local_transport_port() {
    let seeds = vec![
        "127.0.0.1:9300".to_string(),
        "127.0.0.1:9301".to_string(),
        "127.0.0.1:9302".to_string(),
    ];

    let remote = remote_seed_hosts(&seeds, 9301);

    assert_eq!(
        remote,
        vec!["127.0.0.1:9300".to_string(), "127.0.0.1:9302".to_string()]
    );
}

#[test]
fn remote_seed_hosts_all_nodes_have_reachable_peers() {
    // When seed_hosts includes all 3 transport ports, every node
    // must have at least one remote seed after filtering itself out.
    // This is the fix for the reverse-start-order bug: if seed_hosts
    // only had ["127.0.0.1:9300"], node-1 filtered it out and got
    // an empty list, causing premature single-node bootstrap.
    let seeds = vec![
        "127.0.0.1:9300".to_string(),
        "127.0.0.1:9301".to_string(),
        "127.0.0.1:9302".to_string(),
    ];

    for port in [9300, 9301, 9302] {
        let remote = remote_seed_hosts(&seeds, port);
        assert_eq!(
            remote.len(),
            2,
            "node on port {port} must have 2 remote seeds, got {}",
            remote.len()
        );
        assert!(
            !remote.iter().any(|s| s.ends_with(&format!(":{port}"))),
            "node on port {port} must not have itself in remote seeds"
        );
    }
}

#[test]
fn remote_seed_hosts_single_seed_leaves_self_empty() {
    // Documents the old bug: with only one seed matching the local port,
    // the node has zero remote seeds and will bootstrap solo.
    let seeds = vec!["127.0.0.1:9300".to_string()];
    let remote = remote_seed_hosts(&seeds, 9300);
    assert!(
        remote.is_empty(),
        "single seed matching local port must be empty (triggers bootstrap)"
    );

    // But node-2 still has a seed to try
    let remote2 = remote_seed_hosts(&seeds, 9301);
    assert_eq!(remote2.len(), 1);
}

#[test]
fn resolve_transport_tls_paths_returns_none_when_disabled() {
    let config = AppConfig::default();
    assert!(resolve_transport_tls_paths(&config).unwrap().is_none());
}

#[tokio::test]
async fn node_new_wires_disabled_column_cache_budget() {
    let data_dir = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_dir: data_dir.path().to_string_lossy().into_owned(),
        column_cache_size_percent: 0,
        ..AppConfig::default()
    };

    let node = Node::new(config).await.unwrap();

    assert_eq!(node.shard_manager.column_cache_max_capacity(), 0);
}

#[test]
fn resolve_transport_tls_paths_requires_all_files_when_enabled() {
    let mut config = AppConfig {
        transport_tls_enabled: true,
        ..AppConfig::default()
    };

    let err = resolve_transport_tls_paths(&config).unwrap_err();
    assert!(err.to_string().contains("transport_tls_ca_file"));

    config.transport_tls_ca_file = Some("/tmp/ca.pem".into());
    let err = resolve_transport_tls_paths(&config).unwrap_err();
    assert!(err.to_string().contains("transport_tls_cert_file"));

    config.transport_tls_cert_file = Some("/tmp/node.pem".into());
    let err = resolve_transport_tls_paths(&config).unwrap_err();
    assert!(err.to_string().contains("transport_tls_key_file"));
}

#[cfg(not(feature = "transport-tls"))]
#[test]
fn resolve_transport_tls_paths_requires_transport_tls_feature() {
    let config = AppConfig {
        transport_tls_enabled: true,
        transport_tls_ca_file: Some("/tmp/ca.pem".into()),
        transport_tls_cert_file: Some("/tmp/node.pem".into()),
        transport_tls_key_file: Some("/tmp/node-key.pem".into()),
        ..AppConfig::default()
    };

    let err = resolve_transport_tls_paths(&config).unwrap_err();
    assert!(
        err.to_string()
            .contains("requires building with --features transport-tls")
    );
}

#[cfg(feature = "transport-tls")]
#[test]
fn resolve_transport_tls_paths_returns_paths_when_feature_enabled() {
    let config = AppConfig {
        transport_tls_enabled: true,
        transport_tls_ca_file: Some("/tmp/ca.pem".into()),
        transport_tls_cert_file: Some("/tmp/node.pem".into()),
        transport_tls_key_file: Some("/tmp/node-key.pem".into()),
        ..AppConfig::default()
    };

    let tls = resolve_transport_tls_paths(&config).unwrap().unwrap();
    assert_eq!(tls.ca, "/tmp/ca.pem");
    assert_eq!(tls.cert, "/tmp/node.pem");
    assert_eq!(tls.key, "/tmp/node-key.pem");
}

#[test]
fn resolve_http_tls_paths_returns_none_when_disabled() {
    let config = AppConfig::default();
    assert!(resolve_http_tls_paths(&config).unwrap().is_none());
}

#[test]
fn resolve_http_tls_paths_requires_all_files_when_enabled() {
    let mut config = AppConfig {
        http_tls_enabled: true,
        ..AppConfig::default()
    };

    let err = resolve_http_tls_paths(&config).unwrap_err();
    assert!(err.to_string().contains("http_tls_cert_file"));

    config.http_tls_cert_file = Some("/tmp/http.pem".into());
    let err = resolve_http_tls_paths(&config).unwrap_err();
    assert!(err.to_string().contains("http_tls_key_file"));
}

#[cfg(not(feature = "http-tls"))]
#[test]
fn resolve_http_tls_paths_requires_http_tls_feature() {
    let config = AppConfig {
        http_tls_enabled: true,
        http_tls_cert_file: Some("/tmp/http.pem".into()),
        http_tls_key_file: Some("/tmp/http-key.pem".into()),
        ..AppConfig::default()
    };

    let err = resolve_http_tls_paths(&config).unwrap_err();
    assert!(
        err.to_string()
            .contains("requires building with --features http-tls")
    );
}

#[cfg(feature = "http-tls")]
#[test]
fn resolve_http_tls_paths_returns_paths_when_feature_enabled() {
    let config = AppConfig {
        http_tls_enabled: true,
        http_tls_cert_file: Some("/tmp/http.pem".into()),
        http_tls_key_file: Some("/tmp/http-key.pem".into()),
        ..AppConfig::default()
    };

    let tls = resolve_http_tls_paths(&config).unwrap().unwrap();
    assert_eq!(tls.cert, "/tmp/http.pem");
    assert_eq!(tls.key, "/tmp/http-key.pem");
}

#[tokio::test]
async fn try_join_cluster_short_circuits_when_raft_leader() {
    // Bootstrap a single-node Raft so it becomes leader immediately.
    let (raft, _state) = crate::consensus::create_raft_instance_mem(1, "leader-skip-test".into())
        .await
        .unwrap();
    crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19399".into())
        .await
        .unwrap();
    for _ in 0..50 {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(raft.is_leader(), "node must be leader for this test");

    // Seed list points to unreachable addresses — without the leadership
    // short-circuit, this would block for the full 20 attempts with
    // connection timeouts (~100s).
    let seeds = vec!["127.0.0.1:19777".to_string(), "127.0.0.1:19778".to_string()];
    let node = NodeInfo {
        id: "leader-node".into(),
        name: "leader-node".into(),
        host: "127.0.0.1".into(),
        transport_port: 19399,
        http_port: 19200,
        roles: vec![NodeRole::Master, NodeRole::Data],
        raft_node_id: 1,
    };
    let client = TransportClient::new();
    let start = Instant::now();
    let result = try_join_cluster(&client, &seeds, &node, 1, 20, Some(Arc::clone(&raft))).await;
    let elapsed = start.elapsed();

    // Should return None (leader doesn't need seed-based join) within
    // milliseconds, not the ~100s it would take without the check.
    assert!(result.is_none());
    assert!(
        elapsed < Duration::from_secs(2),
        "leadership short-circuit should return immediately, but took {elapsed:?}"
    );
}

#[tokio::test]
async fn try_join_cluster_no_short_circuit_without_raft() {
    // With raft=None, the function should try all attempts normally.
    // Using empty seeds so it returns immediately.
    let seeds: Vec<String> = vec![];
    let node = NodeInfo {
        id: "test-node".into(),
        name: "test-node".into(),
        host: "127.0.0.1".into(),
        transport_port: 19399,
        http_port: 19200,
        roles: vec![NodeRole::Master, NodeRole::Data],
        raft_node_id: 1,
    };
    let client = TransportClient::new();
    let result = try_join_cluster(&client, &seeds, &node, 1, 5, None).await;
    assert!(result.is_none(), "empty seeds should return None");
}

#[tokio::test]
async fn corrupt_in_sync_replica_copy_is_reported_as_definitive() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = crate::cluster::state::ClusterState::new("corrupt-probe".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;
    let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
    {
        let first = ShardManager::new(dir.path(), Duration::from_secs(60));
        let engine = first
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        apply_index(&engine, "acked", serde_json::json!({"value": 1}), 0, 1);
    }
    let manifest = dir.path().join("idx-uuid/shard_0/translog.manifest");
    assert!(manifest.exists());
    std::fs::write(&manifest, b"{not-a-manifest").unwrap();

    let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
    restarted.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut reported = 0;
    for _ in 0..3 {
        reported += open_local_assigned_shards(
            &state,
            "node-2",
            &restarted,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .len();
    }
    let error = match restarted.open_assigned_shard_with_settings(
        "idx",
        0,
        &HashMap::new(),
        &IndexSettings::default(),
        "idx-uuid",
        crate::shard::AssignedShardOpen {
            allocation_id,
            primary_term: 2,
            allow_empty_creation: false,
        },
    ) {
        Ok(_) => panic!("corrupt copy unexpectedly opened"),
        Err(error) => error,
    };
    assert!(
        reported > 0,
        "a deterministically corrupt authoritative copy must be failed out"
    );
    assert!(ShardManager::is_definitive_copy_failure(&error));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_in_sync_replica_is_failed_and_replication_resumes() {
    let (raft, state_handle) =
        crate::consensus::create_raft_instance_mem(1, "corrupt-replica".into())
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
    let metadata = IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(ClusterCommand::CreateIndex {
            metadata: metadata.clone(),
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    let primary_allocation = state_handle
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let replica_allocation = state_handle
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id: replica_allocation,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );

    let dir = tempfile::tempdir().unwrap();
    {
        let first = ShardManager::new(dir.path(), Duration::from_secs(60));
        first
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id: replica_allocation,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
    }
    std::fs::write(
        dir.path().join("idx-uuid/shard_0/translog.manifest"),
        b"{not-a-manifest",
    )
    .unwrap();
    let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
    restarted.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let failure = open_local_assigned_shards(
        &state_handle.read().unwrap().clone(),
        "node-2",
        &restarted,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    )
    .into_iter()
    .next()
    .expect("corrupt replica must produce a failure report");
    assert!(!failure.promote_only);
    report_failed_shard_copies(
        vec![failure],
        &ClusterManager::with_shared_state(state_handle.clone()),
        &restarted,
        &TransportClient::new(),
        raft.as_ref(),
        &mut std::collections::HashMap::new(),
    )
    .await;
    let state = state_handle.read().unwrap().clone();
    assert!(
        state.indices["idx"].shard_routing[&0]
            .in_sync_replicas
            .is_empty()
    );
    assert!(
        crate::replication::replicate_write(
            &TransportClient::new(),
            &state,
            "idx",
            0,
            "doc",
            &serde_json::json!({"value": 2}),
            "index",
            1,
            2,
        )
        .await
        .is_ok(),
        "writes must no longer wait for the corrupt replica"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_primary_with_in_sync_replica_is_promoted() {
    let (raft, state_handle) =
        crate::consensus::create_raft_instance_mem(1, "corrupt-primary".into())
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
            raft.client_write(ClusterCommand::AddNode {
                node: crate::cluster::state::NodeInfo {
                    id: node_id.into(),
                    name: node_id.into(),
                    host: "127.0.0.1".into(),
                    transport_port: 0,
                    http_port: 0,
                    roles: vec![crate::cluster::state::NodeRole::Data],
                    raft_node_id: 0,
                },
            })
            .await
            .unwrap()
            .data,
            ClusterResponse::Ok
        );
    }
    let metadata = IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(ClusterCommand::CreateIndex { metadata })
            .await
            .unwrap()
            .data,
        ClusterResponse::Ok
    );
    let primary_allocation = state_handle
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let replica_allocation = state_handle
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id: replica_allocation,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );

    let dir = tempfile::tempdir().unwrap();
    {
        let first = ShardManager::new(dir.path(), Duration::from_secs(60));
        first
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id: primary_allocation,
                    primary_term: 2,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
    }
    std::fs::write(
        dir.path().join("idx-uuid/shard_0/translog.manifest"),
        b"{not-a-manifest",
    )
    .unwrap();
    let restarted = ShardManager::new(dir.path(), Duration::from_secs(60));
    restarted.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let failure = open_local_assigned_shards(
        &state_handle.read().unwrap().clone(),
        "node-1",
        &restarted,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    )
    .into_iter()
    .next()
    .expect("corrupt primary must produce a promote-only failure");
    assert!(failure.promote_only);
    report_failed_shard_copies(
        vec![failure],
        &ClusterManager::with_shared_state(state_handle.clone()),
        &restarted,
        &TransportClient::new(),
        raft.as_ref(),
        &mut std::collections::HashMap::new(),
    )
    .await;
    assert_eq!(
        state_handle.read().unwrap().indices["idx"].shard_routing[&0].primary,
        "node-2"
    );
}

#[tokio::test]
async fn published_pending_marker_recovers_in_memory_state_without_restart() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let engine = manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    apply_index(&engine, "preserved", serde_json::json!({"value": 1}), 0, 1);
    engine.refresh().unwrap();
    assert!(
        manager
            .begin_peer_recovery_target_blocking("idx".into(), 0, "idx-uuid".into(), 7)
            .await
            .unwrap()
    );
    let shard_dir = dir.path().join("idx-uuid/shard_0");
    std::fs::write(
        shard_dir.join(crate::shard::PEER_RECOVERY_AWAITING_MEMBERSHIP_MARKER),
        serde_json::to_vec(&serde_json::json!({
            "index_uuid": "idx-uuid",
            "allocation_id": 7,
            "primary_node_id": "node-1",
            "primary_term": 2,
        }))
        .unwrap(),
    )
    .unwrap();

    manager
        .reset_peer_recovery_target_for_retry_blocking("idx".into(), 0, "idx-uuid".into(), 7)
        .await
        .unwrap();
    assert!(
        manager
            .peer_recovery_target_states()
            .iter()
            .all(|(_, state)| !matches!(
                state,
                crate::shard::PeerRecoveryTargetState::Recovering { .. }
            ))
    );

    let mut state = ClusterState::new("pending-split".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    });
    let allocations = state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap();
    allocations.primary_initialized = true;
    allocations.replicas.insert("node-2".into(), 7);
    let cluster_manager = Arc::new(ClusterManager::new(state.cluster_name.clone()));
    cluster_manager.update_state(state.clone());
    super::peer_recovery::PeerRecoveryDriver::new(1).reconcile(
        &state,
        "node-2",
        cluster_manager,
        manager.clone(),
        TransportClient::new(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while manager.is_peer_recovery_target("idx", 0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "published marker did not reconcile without restart"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        engine.get_document("preserved").unwrap().is_some(),
        "reconciliation must retain the finalized copy"
    );
}

#[test]
fn assigned_open_backoff_bounds_retries() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    manager.set_copy_retry_policy_for_test(
        3,
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );
    manager.inject_assigned_open_io_failures(5, usize::MAX);
    let mut state = ClusterState::new("retry-bound".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    for _ in 0..8 {
        assert!(
            open_local_assigned_shards(
                &state,
                "node-2",
                &manager,
                &std::sync::Mutex::new(std::collections::HashSet::new()),
            )
            .is_empty()
        );
    }
    assert_eq!(
        manager.assigned_open_attempts_for_test(),
        1,
        "backoff must suppress repeated full opens"
    );
}

#[test]
fn persistent_io_escalates_with_role_specific_failure_mode() {
    fn state(primary: &str, replicas: Vec<String>, in_sync: Vec<String>) -> ClusterState {
        let mut state = ClusterState::new("persistent-io".into());
        state.add_index(IndexMetadata {
            name: "idx".into(),
            uuid: IndexUuid::new("idx-uuid"),
            number_of_shards: 1,
            number_of_replicas: replicas.len() as u32,
            shard_routing: HashMap::from([(
                0,
                ShardRoutingEntry {
                    primary: primary.into(),
                    primary_term: 2,
                    replicas,
                    in_sync_replicas: in_sync,
                    unassigned_replicas: 0,
                },
            )]),
            mappings: HashMap::new(),
            dynamic: Default::default(),
            settings: IndexSettings::default(),
        });
        state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_initialized = true;
        state
    }

    let replica_dir = tempfile::tempdir().unwrap();
    let replica = ShardManager::new(replica_dir.path(), Duration::from_secs(60));
    replica.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    replica.inject_assigned_open_io_failures(5, usize::MAX);
    let replica_state = state("node-1", vec!["node-2".into()], vec!["node-2".into()]);
    let mut replica_failures = Vec::new();
    for _ in 0..3 {
        replica_failures = open_local_assigned_shards(
            &replica_state,
            "node-2",
            &replica,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        );
    }
    assert_eq!(replica_failures.len(), 1);
    assert!(!replica_failures[0].promote_only);
    assert!(replica_failures[0].quarantine);
    assert!(
        replica_failures[0]
            .reason
            .contains("persistent shard copy I/O")
    );

    let primary_dir = tempfile::tempdir().unwrap();
    let primary = ShardManager::new(primary_dir.path(), Duration::from_secs(60));
    primary.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    primary.inject_assigned_open_io_failures(5, usize::MAX);
    let primary_state = state("node-1", Vec::new(), Vec::new());
    let mut primary_failures = Vec::new();
    for _ in 0..3 {
        primary_failures = open_local_assigned_shards(
            &primary_state,
            "node-1",
            &primary,
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        );
    }
    assert_eq!(primary_failures.len(), 1);
    assert!(primary_failures[0].promote_only);
    assert!(primary_failures[0].quarantine);
}

#[test]
fn persistent_io_requires_both_count_and_time_budget() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    manager.set_copy_retry_policy_for_test(
        3,
        Duration::from_millis(50),
        Duration::ZERO,
        Duration::ZERO,
    );
    manager.inject_assigned_open_io_failures(5, usize::MAX);
    let mut state = ClusterState::new("persistent-window".into());
    state.add_index(IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    });
    state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_initialized = true;

    for _ in 0..3 {
        assert!(
            open_local_assigned_shards(
                &state,
                "node-2",
                &manager,
                &std::sync::Mutex::new(std::collections::HashSet::new()),
            )
            .is_empty(),
            "attempt count alone must not exhaust the retry window"
        );
    }
    std::thread::sleep(Duration::from_millis(60));
    let failures = open_local_assigned_shards(
        &state,
        "node-2",
        &manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert_eq!(failures.len(), 1);
    assert!(failures[0].reason.contains("persistent shard copy I/O"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_primary_restart_activates_and_resolves_pending_target() {
    let (raft, state_handle) =
        crate::consensus::create_raft_instance_mem(1, "idle-primary-restart".into())
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

    let metadata = IndexMetadata {
        name: "idx".into(),
        uuid: IndexUuid::new("idx-uuid"),
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
        settings: IndexSettings::default(),
    };
    assert_eq!(
        raft.client_write(ClusterCommand::CreateIndex {
            metadata: metadata.clone(),
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    let primary_allocation = state_handle
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let replica_allocation = state_handle
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    let primary_dir = tempfile::tempdir().unwrap();
    {
        let first_primary = ShardManager::new(primary_dir.path(), Duration::from_secs(60));
        first_primary
            .open_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                "idx-uuid",
                crate::shard::AssignedShardOpen {
                    allocation_id: primary_allocation,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
    }
    assert_eq!(
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );

    let target_dir = tempfile::tempdir().unwrap();
    let target_shards = Arc::new(ShardManager::new(
        target_dir.path(),
        Duration::from_secs(60),
    ));
    target_shards
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id: replica_allocation,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    assert!(target_shards.begin_peer_recovery_target("idx", 0));
    target_shards
        .mark_peer_recovery_awaiting_membership_blocking(
            "idx".into(),
            0,
            crate::shard::PeerRecoveryAwaitingMembership {
                index_uuid: "idx-uuid".into(),
                allocation_id: replica_allocation,
                primary_node_id: "node-1".into(),
                primary_term: 2,
            },
        )
        .await
        .unwrap();

    let restarted_primary = Arc::new(ShardManager::new(
        primary_dir.path(),
        Duration::from_secs(60),
    ));
    let state = state_handle.read().unwrap().clone();
    assert!(
        open_local_assigned_shards(
            &state,
            "node-1",
            restarted_primary.as_ref(),
            &std::sync::Mutex::new(std::collections::HashSet::new()),
        )
        .is_empty()
    );
    let primary_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
    let (_server, activation_service) =
        crate::transport::server::create_transport_service_with_raft_and_storage_handle(
            primary_manager,
            restarted_primary,
            TransportClient::new(),
            raft,
            Arc::new(crate::tasks::TaskManager::new()),
            crate::transport::server::RemoteStoreTransportResources {
                storage_manager: Arc::new(
                    crate::storage::StorageManager::new_in_path(primary_dir.path()).unwrap(),
                ),
                remote_store_reader_cache: Arc::new(
                    crate::engine::remote_store::RemoteSplitReaderCache::default(),
                ),
            },
            "node-1".into(),
        );
    activate_local_primaries(&state, "node-1", &activation_service).await;
    let activated = state_handle.read().unwrap().clone();
    assert_eq!(activated.indices["idx"].shard_routing[&0].primary_term, 3);
    activate_local_primaries(&activated, "node-1", &activation_service).await;
    assert_eq!(
        state_handle.read().unwrap().indices["idx"].shard_routing[&0].primary_term,
        3,
        "lifecycle activation must be idempotent for the activated term"
    );

    let target_manager = Arc::new(ClusterManager::new(activated.cluster_name.clone()));
    target_manager.update_state(activated.clone());
    super::peer_recovery::PeerRecoveryDriver::new(1).reconcile(
        &activated,
        "node-2",
        target_manager,
        target_shards.clone(),
        TransportClient::new(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while target_shards.is_peer_recovery_target("idx", 0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle primary activation did not resolve the pending target"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn failed_lifecycle_report_keeps_unpersisted_collision_quarantine() {
    let (raft, state_handle) =
        crate::consensus::create_raft_instance_mem(1, "collision-quarantine".into())
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
    assert_eq!(
        raft.client_write(ClusterCommand::CreateIndex {
            metadata: IndexMetadata {
                name: "idx".into(),
                uuid: IndexUuid::new("idx-uuid"),
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
                settings: IndexSettings::default(),
            },
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    let primary_allocation = state_handle
        .read()
        .unwrap()
        .primary_allocation_id("idx", 0)
        .unwrap();
    let allocation_id = state_handle
        .read()
        .unwrap()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    assert_eq!(
        raft.client_write(ClusterCommand::ActivatePrimary {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            primary: "node-1".into(),
            allocation_id: primary_allocation,
            expected_term: 1,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );
    assert_eq!(
        raft.client_write(ClusterCommand::MarkReplicaInSync {
            index_name: "idx".into(),
            index_uuid: "idx-uuid".into(),
            shard_id: 0,
            replica: "node-2".into(),
            allocation_id,
            primary: "node-1".into(),
            primary_term: 2,
        })
        .await
        .unwrap()
        .data,
        ClusterResponse::Ok
    );

    let dir = tempfile::tempdir().unwrap();
    let shard_manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    shard_manager.set_copy_retry_policy_for_test(3, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let engine = shard_manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "idx-uuid",
            crate::shard::AssignedShardOpen {
                allocation_id,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    for seq_no in 0..=5 {
        apply_index(
            &engine,
            &format!("doc-{seq_no}"),
            serde_json::json!({"term": 2, "seq": seq_no}),
            seq_no,
            2,
        );
    }
    engine.refresh().unwrap();
    drop(engine);

    let cluster_manager = Arc::new(ClusterManager::with_shared_state(state_handle.clone()));
    let remote_store_resources = crate::transport::server::RemoteStoreTransportResources {
        storage_manager: Arc::new(crate::storage::StorageManager::new_in_path(dir.path()).unwrap()),
        remote_store_reader_cache: Arc::new(
            crate::engine::remote_store::RemoteSplitReaderCache::default(),
        ),
    };
    let (_transport_server, transport_service) =
        crate::transport::server::create_transport_service_with_raft_and_storage_handle(
            cluster_manager.clone(),
            shard_manager.clone(),
            TransportClient::new(),
            raft.clone(),
            Arc::new(crate::tasks::TaskManager::new()),
            remote_store_resources,
            "node-2".into(),
        );
    let replicate = |doc_id: &str, seq_no: u64| crate::transport::proto::ReplicateDocRequest {
        index_name: "idx".into(),
        shard_id: 0,
        doc_id: doc_id.into(),
        payload_json: serde_json::to_vec(&serde_json::json!({"term": 3, "seq": seq_no})).unwrap(),
        op: "index".into(),
        seq_no,
        index_uuid: "idx-uuid".into(),
        primary_term: Some(3),
        target_allocation_id: Some(allocation_id),
    };

    // ENOSPC exactly once, while persisting the collision marker.
    shard_manager.inject_collision_quarantine_persist_failures(28, 1);
    let collision = transport_service
        .replicate_doc(tonic::Request::new(replicate("doc-5", 5)))
        .await
        .unwrap_err();
    assert_eq!(collision.code(), tonic::Code::DataLoss);
    let identity_path = dir
        .path()
        .join("idx-uuid/shard_0")
        .join(crate::shard::SHARD_COPY_IDENTITY_FILE);
    let durable: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_ne!(
        durable["collision_quarantined"], true,
        "marker write was injected to fail"
    );
    assert!(
        shard_manager
            .copy_identity("idx", 0)
            .expect("253b583 keeps the marked identity in memory")
            .collision_quarantined
    );
    assert!(shard_manager.get_shard("idx", 0).is_none());
    assert!(
        state_handle.read().unwrap().indices["idx"].shard_routing[&0].is_replica_in_sync("node-2"),
        "the replica's lagging-view transport report is skipped, so the copy is still in sync"
    );

    // First lifecycle tick: open is rejected by the cached marker and the
    // failure is reportable.
    let current_state = state_handle.read().unwrap().clone();
    let first_tick = open_local_assigned_shards(
        &current_state,
        "node-2",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert_eq!(first_tick.len(), 1);
    assert!(
        first_tick[0]
            .reason
            .contains("collision quarantine is active")
    );
    assert!(
        !first_tick[0].quarantine,
        "collision quarantine must not fall back to generic quarantine"
    );

    // The report cannot be applied during this tick (no Raft leader known).
    let mut leaderless_state = current_state.clone();
    leaderless_state.master_node = None;
    let leaderless_manager =
        ClusterManager::with_shared_state(Arc::new(std::sync::RwLock::new(leaderless_state)));
    let (follower_raft, _) =
        crate::consensus::create_raft_instance_mem(2, "collision-quarantine-follower".into())
            .await
            .unwrap();
    assert!(!follower_raft.is_leader());
    let mut recent_reports = HashMap::new();
    report_failed_shard_copies(
        first_tick,
        &leaderless_manager,
        &shard_manager,
        &TransportClient::new(),
        follower_raft.as_ref(),
        &mut recent_reports,
    )
    .await;
    assert!(
        state_handle.read().unwrap().indices["idx"].shard_routing[&0].is_replica_in_sync("node-2"),
        "the failed report leaves the allocation in sync"
    );
    let cached_after_report = shard_manager
        .copy_identity("idx", 0)
        .map(|identity| identity.collision_quarantined);
    assert_eq!(
        cached_after_report,
        Some(true),
        "a failed lifecycle report must keep the in-memory collision marker"
    );

    // Second lifecycle tick, same process, marker still not durable.
    let second_tick = open_local_assigned_shards(
        &current_state,
        "node-2",
        &shard_manager,
        &std::sync::Mutex::new(std::collections::HashSet::new()),
    );
    assert_eq!(
        second_tick.len(),
        1,
        "the quarantined copy must fail its next open and be reported again"
    );
    assert!(
        second_tick[0]
            .reason
            .contains("collision quarantine is active")
    );
    assert!(!second_tick[0].quarantine);
    assert!(
        shard_manager.get_shard("idx", 0).is_none(),
        "the divergent copy must stay closed"
    );

    let follow_up = transport_service
        .replicate_doc(tonic::Request::new(replicate("doc-6", 6)))
        .await;
    match follow_up {
        Err(status) => assert_eq!(status.code(), tonic::Code::DataLoss, "{status:?}"),
        Ok(response) => panic!(
            "replication to a quarantined copy must fail closed: {:?}",
            response.get_ref()
        ),
    }
    assert!(shard_manager.get_shard("idx", 0).is_none());
    let durable: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
    assert_eq!(
        durable["collision_quarantined"], true,
        "the next rejection must persist the collision marker"
    );
}
