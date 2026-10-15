use super::*;
use crate::cluster::state::{ClusterState, IndexMetadata, IndexUuid};
use crate::engine::DocumentMutation;
use serde_json::json;

fn operation(seq_no: u64) -> SequencedOperation {
    SequencedOperation {
        seq_no,
        primary_term: 2,
        document_version: seq_no + 1,
        client_operation: None,
        mutation: DocumentMutation::Index {
            doc_id: "doc".into(),
            source: json!({"value": seq_no + 1}),
        },
    }
}

fn authority() -> WriteAuthorityIdentity {
    WriteAuthorityIdentity {
        index_uuid: "uuid-1".into(),
        shard_id: 0,
        primary_node_id: "primary".into(),
        primary_allocation_id: 7,
        primary_term: 3,
        activation_id: 9,
    }
}

fn state() -> Arc<RwLock<ClusterState>> {
    let mut metadata =
        IndexMetadata::build_shard_routing("idx", 1, 1, &["primary".into(), "replica".into()]);
    metadata.uuid = IndexUuid::new("uuid-1");
    let routing = metadata.shard_routing.get_mut(&0).unwrap();
    routing.primary_term = 3;
    routing.in_sync_replicas = vec!["replica".into()];
    let mut state = ClusterState::new("test".into());
    state.add_index_with_allocation_id(metadata, 7).unwrap();
    let ids = state
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap();
    ids.primary_initialized = true;
    ids.primary_authority.reserve(9);
    ids.replicas.insert("replica".into(), 8);
    state.version = 10;
    Arc::new(RwLock::new(state))
}

fn request(token: Option<&str>, command: PromotionCommand) -> PromotionRequest {
    PromotionRequest {
        index: "idx".into(),
        authority: authority(),
        target_allocation_id: 8,
        sender_state_version: 10,
        token: token.map(str::to_string),
        command,
    }
}

fn open(manager: &ShardManager, create: bool) -> Arc<dyn SearchEngine> {
    manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 8,
                primary_term: 2,
                allow_empty_creation: create,
            },
        )
        .unwrap()
}

#[tokio::test]
async fn promotion_protocol_cleanup_waits_for_active_then_releases_pins_without_a_release_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let engine = open(&manager, true);
    let shared = state();
    manager.bind_applied_shard_authority(
        Arc::new(crate::cluster::ClusterManager::with_shared_state(
            shared.clone(),
        )),
        "replica".into(),
    );
    let PromotionReply::Frozen(frozen) = manager
        .reconcile_primary_history(request(None, PromotionCommand::Freeze))
        .unwrap()
    else {
        panic!("freeze");
    };
    let history = CanonicalHistoryAccumulator::new(&authority(), None)
        .unwrap()
        .finish(None)
        .unwrap();
    manager
        .reconcile_primary_history(request(
            Some(&frozen.token),
            PromotionCommand::Seal {
                history: history.clone(),
            },
        ))
        .unwrap();
    manager.reconcile_frozen_histories_blocking().await.unwrap();
    assert_eq!(manager.frozen_histories.lock().unwrap().len(), 1);
    assert_eq!(engine.copy_write_state().canonical_history().unwrap(), None);
    shared
        .write()
        .unwrap()
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_authority
        .finish(history.clone());
    manager.reconcile_frozen_histories_blocking().await.unwrap();
    assert!(manager.frozen_histories.lock().unwrap().is_empty());
    assert_eq!(
        engine.copy_write_state().canonical_history().unwrap(),
        Some(history)
    );
    assert!(matches!(
        manager
            .reconcile_primary_history(request(Some(&frozen.token), PromotionCommand::Release))
            .unwrap(),
        PromotionReply::Released,
    ));
    {
        let mut state = shared.write().unwrap();
        state
            .indices
            .get_mut("idx")
            .unwrap()
            .shard_routing
            .get_mut(&0)
            .unwrap()
            .primary_term = 4;
        state
            .shard_allocations
            .get_mut("idx")
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .primary_authority
            .reserve(10);
    }
    assert!(
        manager
            .reconcile_primary_history(request(Some(&frozen.token), PromotionCommand::Release))
            .is_err()
    );
}

#[tokio::test]
async fn promotion_protocol_fences_freezes_resyncs_and_seals_without_destructive_repair() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    let engine = open(&manager, true);
    engine.apply_replica_operation(operation(2)).unwrap();
    let state = state();
    let cluster = Arc::new(crate::cluster::ClusterManager::with_shared_state(
        state.clone(),
    ));
    manager.bind_applied_shard_authority(cluster, "replica".into());
    let PromotionReply::Frozen(frozen) = manager
        .reconcile_primary_history(request(None, PromotionCommand::Freeze))
        .unwrap()
    else {
        panic!("freeze did not return a report");
    };
    assert_eq!(frozen.max_next, 3);
    assert_eq!(frozen.persisted_next, 0);
    assert_eq!(manager.copy_identity("idx", 0).unwrap().replica_fence, 3);
    assert!(
        manager
            .apply_replica_operation(
                "idx",
                0,
                ReplicaApplyContext {
                    index_uuid: "uuid-1",
                    allocation_id: 8,
                    applied_view_term: 2,
                    message_term: 2,
                    authority: None,
                },
                |engine| engine.apply_replica_operation(operation(0)),
            )
            .is_err()
    );
    let PromotionReply::Frozen(repeated) = manager
        .reconcile_primary_history(request(None, PromotionCommand::Freeze))
        .unwrap()
    else {
        panic!("repeat freeze did not return a report");
    };
    assert_eq!(frozen, repeated);
    let wrong_token = uuid::Uuid::new_v4().to_string();
    assert!(
        manager
            .reconcile_primary_history(request(
                Some(&wrong_token),
                PromotionCommand::Inventory {
                    start_seq_no: 0,
                    end_seq_no: 3,
                    view: HistoryView::Frozen
                },
            ))
            .is_err()
    );
    let PromotionReply::Appended { persisted_next, .. } = manager
        .reconcile_primary_history(request(
            Some(&frozen.token),
            PromotionCommand::Append {
                operations: vec![operation(0), operation(1)],
            },
        ))
        .unwrap()
    else {
        panic!("append did not return durable progress");
    };
    assert_eq!(persisted_next, 3);
    let PromotionReply::Inventory(inventory) = manager
        .reconcile_primary_history(request(
            Some(&frozen.token),
            PromotionCommand::Inventory {
                start_seq_no: 0,
                end_seq_no: 3,
                view: HistoryView::Current,
            },
        ))
        .unwrap()
    else {
        panic!("inventory reply missing");
    };
    let mut digest = CanonicalHistoryAccumulator::new(&authority(), None).unwrap();
    for operation in &inventory.operations {
        digest.record(operation).unwrap();
    }
    let history = digest.finish(Some(2)).unwrap();
    let mut wrong = history.clone();
    wrong.digest = [9; 32];
    assert!(
        manager
            .reconcile_primary_history(request(
                Some(&frozen.token),
                PromotionCommand::Seal { history: wrong },
            ))
            .is_err()
    );
    assert!(
        manager
            .copy_identity("idx", 0)
            .unwrap()
            .canonical_history
            .is_none()
    );
    manager
        .reconcile_primary_history(request(
            Some(&frozen.token),
            PromotionCommand::Seal {
                history: history.clone(),
            },
        ))
        .unwrap();
    assert_eq!(
        manager.copy_identity("idx", 0).unwrap().canonical_history,
        Some(history.clone())
    );
    assert!(
        manager
            .reconcile_primary_history(request(
                Some(&frozen.token),
                PromotionCommand::Append {
                    operations: vec![operation(3)]
                },
            ))
            .is_err()
    );
    state
        .write()
        .unwrap()
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .primary_authority
        .finish(history.clone());
    manager
        .reconcile_primary_history(request(Some(&frozen.token), PromotionCommand::Release))
        .unwrap();
    assert_eq!(
        engine.copy_write_state().canonical_history().unwrap(),
        Some(history)
    );
    assert!(manager.frozen_histories.lock().unwrap().is_empty());
    engine.refresh().unwrap();
    assert_eq!(
        engine.get_document("doc").unwrap().unwrap()["value"],
        json!(3)
    );
    assert!(engine.ensure_write_health().is_ok());
}

#[tokio::test]
async fn promotion_protocol_process_token_and_allocation_do_not_survive_restart_or_aba() {
    let dir = tempfile::tempdir().unwrap();
    let state = state();
    let token = {
        let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
        open(&manager, true);
        manager.bind_applied_shard_authority(
            Arc::new(crate::cluster::ClusterManager::with_shared_state(
                state.clone(),
            )),
            "replica".into(),
        );
        let PromotionReply::Frozen(frozen) = manager
            .reconcile_primary_history(request(None, PromotionCommand::Freeze))
            .unwrap()
        else {
            panic!("freeze");
        };
        frozen.token
    };
    let manager = ShardManager::new(dir.path(), Duration::from_secs(60));
    manager.bind_applied_shard_authority(
        Arc::new(crate::cluster::ClusterManager::with_shared_state(
            state.clone(),
        )),
        "replica".into(),
    );
    open(&manager, false);
    assert!(
        manager
            .reconcile_primary_history(request(
                Some(&token),
                PromotionCommand::Inventory {
                    start_seq_no: 0,
                    end_seq_no: 1,
                    view: HistoryView::Frozen
                },
            ))
            .is_err()
    );
    let PromotionReply::Frozen(frozen) = manager
        .reconcile_primary_history(request(None, PromotionCommand::Freeze))
        .unwrap()
    else {
        panic!("freeze");
    };
    assert_ne!(token, frozen.token);
    state
        .write()
        .unwrap()
        .shard_allocations
        .get_mut("idx")
        .unwrap()
        .get_mut(&0)
        .unwrap()
        .replicas
        .insert("replica".into(), 11);
    assert!(
        manager
            .reconcile_primary_history(request(
                Some(&frozen.token),
                PromotionCommand::Append {
                    operations: vec![operation(0)]
                },
            ))
            .is_err()
    );
    assert_eq!(manager.copy_identity("idx", 0).unwrap().allocation_id, 8);
}
