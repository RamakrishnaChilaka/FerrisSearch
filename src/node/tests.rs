use super::*;
use crate::cluster::state::{IndexMetadata, IndexSettings, IndexUuid, ShardRoutingEntry};
use crate::engine::CompositeEngine;
use crate::transport::proto::RecoverReplicaOp;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn make_engine() -> (tempfile::TempDir, Arc<dyn crate::engine::SearchEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
    (dir, Arc::new(engine))
}

#[test]
fn apply_recovery_ops_indexes_documents() {
    let (_dir, engine) = make_engine();
    let ops = vec![
        RecoverReplicaOp {
            seq_no: 0,
            op: "index".into(),
            doc_id: "r1".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"x": 1})).unwrap(),
        },
        RecoverReplicaOp {
            seq_no: 1,
            op: "index".into(),
            doc_id: "r2".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"x": 2})).unwrap(),
        },
    ];

    apply_recovery_ops(&engine, &ops);

    engine.refresh().unwrap();
    assert!(engine.get_document("r1").unwrap().is_some());
    assert!(engine.get_document("r2").unwrap().is_some());
    assert!(engine.local_checkpoint() >= 1);
}

#[test]
fn apply_recovery_ops_deletes_documents() {
    let (_dir, engine) = make_engine();
    engine
        .add_document("d1", serde_json::json!({"x": 1}))
        .unwrap();
    engine.refresh().unwrap();
    assert!(engine.get_document("d1").unwrap().is_some());

    let ops = vec![RecoverReplicaOp {
        seq_no: 5,
        op: "delete".into(),
        doc_id: "d1".into(),
        payload_json: vec![],
    }];

    apply_recovery_ops(&engine, &ops);

    engine.refresh().unwrap();
    assert!(
        engine.get_document("d1").unwrap().is_none(),
        "document should be deleted"
    );
    assert!(engine.local_checkpoint() >= 5);
}

#[test]
fn apply_recovery_ops_skips_unknown_ops() {
    let (_dir, engine) = make_engine();
    let cp_before = engine.local_checkpoint();

    let ops = vec![RecoverReplicaOp {
        seq_no: 10,
        op: "unknown_op".into(),
        doc_id: "x".into(),
        payload_json: vec![],
    }];

    apply_recovery_ops(&engine, &ops);

    // Checkpoint should not change for unknown ops
    assert_eq!(engine.local_checkpoint(), cp_before);
}

#[test]
fn apply_recovery_ops_empty_is_noop() {
    let (_dir, engine) = make_engine();
    let cp_before = engine.local_checkpoint();
    apply_recovery_ops(&engine, &[]);
    assert_eq!(engine.local_checkpoint(), cp_before);
}

#[test]
fn apply_recovery_ops_mixed_index_and_delete() {
    let (_dir, engine) = make_engine();

    let ops = vec![
        RecoverReplicaOp {
            seq_no: 0,
            op: "index".into(),
            doc_id: "m1".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"v": 1})).unwrap(),
        },
        RecoverReplicaOp {
            seq_no: 1,
            op: "index".into(),
            doc_id: "m2".into(),
            payload_json: serde_json::to_vec(&serde_json::json!({"v": 2})).unwrap(),
        },
        RecoverReplicaOp {
            seq_no: 2,
            op: "delete".into(),
            doc_id: "m1".into(),
            payload_json: vec![],
        },
    ];

    apply_recovery_ops(&engine, &ops);

    engine.refresh().unwrap();
    assert!(
        engine.get_document("m1").unwrap().is_none(),
        "m1 should be deleted"
    );
    assert!(
        engine.get_document("m2").unwrap().is_some(),
        "m2 should exist"
    );
    assert!(engine.local_checkpoint() >= 2);
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
            replicas: vec!["node-2".into()],
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
            replicas: vec![],
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
        settings: IndexSettings::default(),
    });

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
async fn open_local_assigned_shards_creates_missing_dir_for_new_assignment() {
    let dir = tempfile::tempdir().unwrap();
    let shard_manager = ShardManager::new(dir.path(), Duration::from_secs(60));

    let mut state = crate::cluster::state::ClusterState::new("node-test".into());
    let mut shard_routing = HashMap::new();
    shard_routing.insert(
        0,
        ShardRoutingEntry {
            primary: "node-1".into(),
            replicas: vec![],
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
            replicas: vec![],
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
            replicas: vec![],
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
        settings: IndexSettings::default(),
    });

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
            replicas: vec![],
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
        "blocking lifecycle shard-open wrapper stalled the async runtime for {:?}",
        elapsed
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
            replicas: vec![],
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
            replicas: vec![],
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
            replicas: vec![],
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
            replicas: vec![],
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
            replicas: vec![],
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
        settings: IndexSettings::default(),
    });

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
        "leadership short-circuit should return immediately, but took {:?}",
        elapsed
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
