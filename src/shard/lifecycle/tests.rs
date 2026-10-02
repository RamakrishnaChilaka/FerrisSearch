use super::*;
use crate::cluster::state::IndexMetadata;
use crate::consensus::types::ClusterCommand;

#[tokio::test(flavor = "current_thread")]
async fn issue_152_incarnation_cleanup_waits_off_tokio_workers() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    manager
        .open_shard_with_settings_blocking(
            "idx".into(),
            0,
            HashMap::new(),
            IndexSettings::default(),
            "old-uuid",
        )
        .await
        .unwrap();
    let lock = manager.index_lifecycle_lock("idx");
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = lock.write().unwrap();
        held_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    held_rx.await.unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    *manager.index_close_before_lock_sender.lock().unwrap() = Some(entered_tx);
    let closing = manager.clone();
    let cleanup = tokio::spawn(async move {
        closing
            .reconcile_index_incarnations_blocking(ClusterState::new("recreate".into()))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !cleanup.is_finished(),
        "cleanup must still be blocked by the lifecycle lock"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), tokio::spawn(async { 42 }))
            .await
            .unwrap()
            .unwrap(),
        42
    );
    release_tx.send(()).unwrap();
    cleanup.await.unwrap().unwrap();
    tokio::task::spawn_blocking(move || holder.join().unwrap())
        .await
        .unwrap();
    assert!(manager.get_shard("idx", 0).is_none());
    assert!(manager.copy_identity("idx", 0).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_stale_quarantine_preserves_new_uuid_and_allocation() {
    for old_uuid in ["old-uuid", "new-uuid"] {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let engine = manager
            .open_primary_assigned_shard_with_settings_blocking(
                "idx".into(),
                0,
                HashMap::new(),
                IndexSettings::default(),
                "new-uuid",
                AssignedShardOpen {
                    allocation_id: 9,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .await
            .unwrap();
        let writer = engine.clone();
        tokio::task::spawn_blocking(move || {
            writer
                .add_document_with_receipt_at_term("new", serde_json::json!({"value": 2}), 1)
                .unwrap();
        })
        .await
        .unwrap();
        manager
            .quarantine_shard_copy_for_allocation_blocking("idx".into(), 0, old_uuid.into(), 8)
            .await
            .unwrap();
        let serving = manager
            .get_shard("idx", 0)
            .expect("stale quarantine must preserve the current copy");
        assert!(Arc::ptr_eq(&serving, &engine));
        let identity = manager.copy_identity("idx", 0).unwrap();
        assert_eq!(identity.index_uuid, "new-uuid");
        assert_eq!(identity.allocation_id, 9);
        let source = tokio::task::spawn_blocking(move || {
            serving
                .get_document_with_metadata("new", true)
                .unwrap()
                .unwrap()
                .source
        })
        .await
        .unwrap();
        assert_eq!(source, serde_json::json!({"value": 2}));
        manager
            .quarantine_shard_copy_for_allocation_blocking("idx".into(), 0, "new-uuid".into(), 9)
            .await
            .unwrap();
        assert!(manager.get_shard("idx", 0).is_none());
        assert!(manager.copy_identity("idx", 0).is_none());
        let path = dir.path().join("new-uuid/shard_0");
        tokio::task::spawn_blocking(move || {
            drop(engine);
            assert!(
                path.is_dir(),
                "quarantine must retain the exact copy's disk evidence"
            );
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_152_delayed_initial_open_cannot_publish_old_incarnation() {
    let (raft, state_handle) = crate::consensus::create_raft_instance_mem(1, "recreate".into())
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
    let cluster = Arc::new(ClusterManager::with_shared_state(state_handle));
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    manager.bind_applied_shard_authority(cluster.clone(), "node-2".into());
    let old = IndexMetadata::build_shard_routing("idx", 1, 0, &["node-2".into()]);
    let old_uuid = old.uuid.clone();
    raft.client_write(ClusterCommand::CreateIndex { metadata: old })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let old_allocation = cluster
        .get_state()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *manager.open_before_lock_sender.lock().unwrap() = Some(entered_tx);
    *manager.open_before_lock_release.lock().unwrap() = Some(release_rx);
    let opening = manager.clone();
    let captured_uuid = old_uuid.clone();
    let old_open = tokio::spawn(async move {
        opening
            .open_primary_assigned_shard_with_settings_blocking(
                "idx".into(),
                0,
                HashMap::new(),
                IndexSettings::default(),
                captured_uuid,
                AssignedShardOpen {
                    allocation_id: old_allocation,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .await
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .await
        .unwrap();
    raft.client_write(ClusterCommand::DeleteIndex {
        index_name: "idx".into(),
    })
    .await
    .unwrap();
    let new = IndexMetadata::build_shard_routing("idx", 1, 0, &["node-2".into()]);
    let new_uuid = new.uuid.clone();
    raft.client_write(ClusterCommand::CreateIndex { metadata: new })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let allocation_id = cluster
        .get_state()
        .shard_allocation_id("idx", 0, "node-2")
        .unwrap();
    let opening = manager.clone();
    let expected_uuid = new_uuid.clone();
    let new_open = tokio::spawn(async move {
        opening
            .open_primary_assigned_shard_with_settings_blocking(
                "idx".into(),
                0,
                HashMap::new(),
                IndexSettings::default(),
                expected_uuid,
                AssignedShardOpen {
                    allocation_id,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .await
    });
    release_tx.send(()).unwrap();
    let error = match old_open.await.unwrap() {
        Ok(_) => panic!("the delayed old allocation was opened"),
        Err(error) => error,
    };
    assert!(error.is::<ShardReopenAborted>(), "{error:#}");
    let new_engine = new_open.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &manager.get_shard("idx", 0).unwrap(),
        &new_engine
    ));
    let identity = manager.copy_identity("idx", 0).unwrap();
    assert_eq!(identity.index_uuid, new_uuid.as_str());
    assert_eq!(identity.allocation_id, allocation_id);
    let path = dir.path().join(old_uuid);
    assert!(
        !tokio::task::spawn_blocking(move || path.exists())
            .await
            .unwrap()
    );
    raft.shutdown().await.unwrap();
}
