use super::*;
use crate::cluster::state::IndexMetadata;
use crate::consensus::types::ClusterCommand;
use crate::engine::{
    BulkWriteReceipt, DeleteWriteReceipt, DocumentRead, IndexWriteReceipt, ReplicaApplyReceipt,
    ReplicaBulkApplyReceipt, SequencedOperation, WriteCondition,
};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_followup_b1_delayed_old_registration_preserves_current_engine() {
    let (raft, state_handle) = crate::consensus::create_raft_instance_mem(1, "review".into())
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
    let old = IndexMetadata::build_shard_routing("idx", 2, 0, &["node-2".into()]);
    let old_uuid = old.uuid.to_string();
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
    *manager.open_after_validation_gate.lock().unwrap() = Some(OpenValidationGate {
        index_uuid: old_uuid.clone(),
        entered: entered_tx,
        release: release_rx,
    });
    let opening = manager.clone();
    let captured = old_uuid.clone();
    let old_open = tokio::spawn(async move {
        opening
            .open_primary_assigned_shard_with_settings_blocking(
                "idx".into(),
                0,
                HashMap::new(),
                IndexSettings::default(),
                captured,
                AssignedShardOpen {
                    allocation_id: old_allocation,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .await
    });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    raft.client_write(ClusterCommand::DeleteIndex {
        index_name: "idx".into(),
    })
    .await
    .unwrap();
    let new = IndexMetadata::build_shard_routing("idx", 2, 0, &["node-2".into()]);
    let new_uuid = new.uuid.to_string();
    raft.client_write(ClusterCommand::CreateIndex { metadata: new })
        .await
        .unwrap()
        .data
        .into_result()
        .unwrap();
    let state = cluster.get_state();
    let new_allocation = state.shard_allocation_id("idx", 1, "node-2").unwrap();
    let lifecycle = manager.clone();
    let lifecycle_state = state.clone();
    let lifecycle_uuid = new_uuid.clone();
    let new_engine = tokio::task::spawn_blocking(move || {
        lifecycle
            .reconcile_index_incarnations(&lifecycle_state)
            .unwrap();
        let engine = lifecycle
            .open_primary_assigned_shard_with_settings(
                "idx",
                1,
                &HashMap::new(),
                &IndexSettings::default(),
                &lifecycle_uuid,
                AssignedShardOpen {
                    allocation_id: new_allocation,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .unwrap();
        engine
            .add_document_with_receipt_at_term("new", json!({"value": 2}), 1)
            .unwrap();
        engine
    })
    .await
    .unwrap();
    assert_eq!(
        manager.index_uuid("idx").as_deref(),
        Some(new_uuid.as_str())
    );
    release_tx.send(()).unwrap();
    let error = match old_open.await.unwrap() {
        Ok(_) => panic!("the obsolete open was published"),
        Err(error) => error,
    };
    assert!(error.is::<ShardReopenAborted>(), "{error:#}");
    println!(
        "B1 registry after old open: {:?}; current UUID={new_uuid}",
        manager.index_uuid("idx")
    );
    let checking = manager.clone();
    let expected = new_uuid.clone();
    let reopen_validation = tokio::task::spawn_blocking(move || {
        checking.ensure_reopen_target("idx", 1, &expected, new_allocation)
    })
    .await
    .unwrap();
    println!("B1 current reopen validation: {reopen_validation:?}");
    assert!(
        manager.copy_identity("idx", 0).is_none(),
        "an aborted old open must not cache a foreign identity before retirement"
    );
    manager
        .reconcile_index_incarnations_blocking(cluster.get_state())
        .await
        .unwrap();
    let served = manager.get_shard("idx", 1);
    println!(
        "B1 current engine after retirement served={}",
        served.is_some()
    );
    assert!(
        served.is_some(),
        "retiring {old_uuid} evicted the current {new_uuid} engine"
    );
    assert!(Arc::ptr_eq(&served.unwrap(), &new_engine));
    assert_eq!(
        manager.index_uuid("idx").as_deref(),
        Some(new_uuid.as_str())
    );
    reopen_validation.unwrap();
    let reopening = manager.clone();
    let expected = new_uuid.clone();
    let held = new_engine.clone();
    tokio::task::spawn_blocking(move || {
        let engine = reopening
            .open_primary_assigned_shard_with_settings(
                "idx",
                1,
                &HashMap::new(),
                &IndexSettings::default(),
                &expected,
                AssignedShardOpen {
                    allocation_id: new_allocation,
                    primary_term: 1,
                    allow_empty_creation: false,
                },
            )
            .expect("a retained current engine must not be reopened into LockBusy");
        assert!(Arc::ptr_eq(&engine, &held));
        assert_eq!(
            engine
                .get_document_with_metadata("new", true)
                .unwrap()
                .unwrap()
                .source,
            json!({"value": 2})
        );
        let allocation_id = state.shard_allocation_id("idx", 0, "node-2").unwrap();
        let sibling = reopening
            .open_primary_assigned_shard_with_settings(
                "idx",
                0,
                &HashMap::new(),
                &IndexSettings::default(),
                &expected,
                AssignedShardOpen {
                    allocation_id,
                    primary_term: 1,
                    allow_empty_creation: true,
                },
            )
            .expect("the aborted old open must not leave a foreign cached identity");
        assert!(
            sibling
                .get_document_with_metadata("old", true)
                .unwrap()
                .is_none()
        );
    })
    .await
    .unwrap();
    raft.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_followup_b1_retirement_preserves_other_uuid_engine_settings_and_isr() {
    for registered_uuid in ["old-uuid", "new-uuid"] {
        let dir = tempfile::tempdir().unwrap();
        let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
        let preparing = manager.clone();
        let (old, current, settings) = tokio::task::spawn_blocking(move || {
            let old = preparing
                .open_shard_with_settings(
                    "idx",
                    0,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "old-uuid",
                )
                .unwrap();
            let current = preparing
                .open_shard_with_settings(
                    "idx",
                    1,
                    &HashMap::new(),
                    &IndexSettings::default(),
                    "new-uuid",
                )
                .unwrap();
            current
                .add_document_with_receipt_at_term("new", json!({"value": 2}), 1)
                .unwrap();
            preparing.isr_tracker.update_replica_checkpoint(
                "idx",
                "new-uuid",
                1,
                1,
                Some(0),
                ReplicaCheckpointUpdate {
                    node_id: "replica".into(),
                    allocation_id: 9,
                    processed_checkpoint: Some(0),
                    persisted_checkpoint: Some(0),
                },
            );
            let settings = preparing.get_settings_manager("idx").unwrap();
            preparing.register_index_uuid("idx", registered_uuid);
            (old, current, settings)
        })
        .await
        .unwrap();
        let closing = manager.clone();
        tokio::task::spawn_blocking(move || {
            closing
                .close_index_incarnation("idx", "old-uuid", "review_uuid_retirement", false, None)
                .unwrap();
        })
        .await
        .unwrap();
        assert!(manager.get_shard("idx", 0).is_none());
        let serving = manager
            .get_shard("idx", 1)
            .expect("current UUID must remain served");
        assert!(Arc::ptr_eq(&serving, &current));
        assert!(Arc::ptr_eq(
            &manager.get_settings_manager("idx").unwrap(),
            &settings
        ));
        assert_eq!(
            manager.isr_tracker.replica_checkpoints("idx", 1),
            vec![("replica".into(), 0)]
        );
        assert_eq!(manager.index_uuid("idx").as_deref(), Some("new-uuid"));
        tokio::task::spawn_blocking(move || {
            assert_eq!(
                serving
                    .get_document_with_metadata("new", true)
                    .unwrap()
                    .unwrap()
                    .source,
                json!({"value": 2})
            );
            drop(old);
            drop(current);
        })
        .await
        .unwrap();
    }
}

struct DropGateEngine {
    inner: Arc<dyn SearchEngine>,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Drop for DropGateEngine {
    fn drop(&mut self) {
        self.entered.take().unwrap().send(()).unwrap();
        self.release
            .get_mut()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
    }
}

impl SearchEngine for DropGateEngine {
    fn add_document_with_receipt_at_term(
        &self,
        id: &str,
        payload: Value,
        term: u64,
    ) -> Result<IndexWriteReceipt> {
        self.inner
            .add_document_with_receipt_at_term(id, payload, term)
    }
    fn add_document_with_condition_at_term(
        &self,
        id: &str,
        payload: Value,
        term: u64,
        condition: WriteCondition,
    ) -> Result<IndexWriteReceipt> {
        self.inner
            .add_document_with_condition_at_term(id, payload, term, condition)
    }
    fn bulk_add_documents_with_receipt_at_term(
        &self,
        docs: Vec<(String, Value)>,
        term: u64,
    ) -> Result<BulkWriteReceipt> {
        self.inner
            .bulk_add_documents_with_receipt_at_term(docs, term)
    }
    fn delete_document_with_receipt_at_term(
        &self,
        id: &str,
        term: u64,
    ) -> Result<DeleteWriteReceipt> {
        self.inner.delete_document_with_receipt_at_term(id, term)
    }
    fn delete_document_with_condition_at_term(
        &self,
        id: &str,
        term: u64,
        condition: WriteCondition,
    ) -> Result<DeleteWriteReceipt> {
        self.inner
            .delete_document_with_condition_at_term(id, term, condition)
    }
    fn apply_replica_operation(&self, op: SequencedOperation) -> Result<ReplicaApplyReceipt> {
        self.inner.apply_replica_operation(op)
    }
    fn apply_replica_batch(&self, ops: Vec<SequencedOperation>) -> Result<ReplicaBulkApplyReceipt> {
        self.inner.apply_replica_batch(ops)
    }
    fn get_document(&self, id: &str) -> Result<Option<Value>> {
        self.inner.get_document(id)
    }
    fn get_document_with_metadata(&self, id: &str, realtime: bool) -> Result<Option<DocumentRead>> {
        self.inner.get_document_with_metadata(id, realtime)
    }
    fn refresh(&self) -> Result<()> {
        self.inner.refresh()
    }
    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
    fn force_merge(&self, segments: usize) -> Result<()> {
        self.inner.force_merge(segments)
    }
    fn search(&self, query: &str) -> Result<Vec<Value>> {
        self.inner.search(query)
    }
    fn search_query(
        &self,
        request: &crate::search::SearchRequest,
    ) -> Result<(
        Vec<Value>,
        usize,
        HashMap<String, crate::search::PartialAggResult>,
    )> {
        self.inner.search_query(request)
    }
    fn doc_count(&self) -> u64 {
        self.inner.doc_count()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_followup_n3_engine_teardown_does_not_hold_node_wide_serving_lock() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let old = manager
        .open_shard_with_settings_blocking(
            "retired".into(),
            0,
            HashMap::new(),
            IndexSettings::default(),
            "retired-uuid",
        )
        .await
        .unwrap();
    manager
        .open_shard_with_settings_blocking(
            "unrelated".into(),
            0,
            HashMap::new(),
            IndexSettings::default(),
            "unrelated-uuid",
        )
        .await
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    manager.shards.write().unwrap().insert(
        ShardKey::new("retired", 0),
        Arc::new(DropGateEngine {
            inner: old,
            entered: Some(entered_tx),
            release: Mutex::new(release_rx),
        }),
    );
    let retiring = manager.clone();
    let retirement =
        tokio::spawn(async move { retiring.close_index_shards_blocking("retired".into()).await });
    tokio::time::timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let reading = manager.clone();
    let mut lookup = tokio::task::spawn_blocking(move || {
        reading
            .get_shard("unrelated", 0)
            .expect("unrelated copy remains served")
    });
    let responsive = tokio::time::timeout(Duration::from_secs(1), &mut lookup).await;
    release_tx.send(()).unwrap();
    retirement.await.unwrap().unwrap();
    if responsive.is_err() {
        lookup.await.unwrap();
        panic!("engine teardown held the node-wide serving-map lock");
    }
    responsive.unwrap().unwrap();
}
