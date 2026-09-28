use ferrissearch::engine::column_cache::ColumnCache;
use ferrissearch::engine::{CompositeEngine, SearchEngine};
use ferrissearch::wal::TranslogDurability;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

fn open_engine(path: &std::path::Path) -> CompositeEngine {
    CompositeEngine::new(path, Duration::from_secs(3600)).unwrap()
}

fn apply_index(
    engine: &dyn SearchEngine,
    doc_id: &str,
    source: serde_json::Value,
    seq_no: u64,
    primary_term: u64,
) -> ferrissearch::engine::ReplicaApplyReceipt {
    engine
        .apply_replica_operation(ferrissearch::engine::SequencedOperation {
            seq_no,
            primary_term,
            mutation: ferrissearch::engine::DocumentMutation::Index {
                doc_id: doc_id.to_string(),
                source,
            },
        })
        .unwrap()
}

#[test]
fn restart_replays_historical_older_term_entries_after_a_fence_raise() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "a", json!({"v": 0}), 0, 1);
        apply_index(&engine, "b", json!({"v": 2}), 2, 1);
        engine.reconcile_term_sequence_state(2, Some(2)).unwrap();
        apply_index(&engine, "c", json!({"v": 3}), 3, 2);
        engine.refresh().unwrap();
    }

    let reopened = open_engine(dir.path());
    assert_eq!(reopened.sequence_stats().processed_checkpoint, Some(0));
    assert_eq!(reopened.sequence_stats().max_seq_no, Some(3));
}

#[test]
fn stale_primary_term_is_rejected_before_wal_append() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    engine
        .add_document_with_receipt_at_term("a", json!({"v": 0}), 1)
        .unwrap();
    engine.reconcile_term_sequence_state(2, Some(0)).unwrap();
    let before = engine
        .peer_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;

    assert!(
        engine
            .add_document_with_receipt_at_term("stale", json!({"v": 1}), 1)
            .is_err()
    );
    let after = engine
        .peer_recovery_ops(0, usize::MAX, usize::MAX)
        .unwrap()
        .operations;
    assert_eq!(after.len(), before.len());
    assert!(
        after
            .iter()
            .all(|entry| entry.payload["_doc_id"] != "stale")
    );
}

#[test]
fn full_flush_retains_history_above_a_processed_gap() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "a", json!({"v": 0}), 0, 1);
        apply_index(&engine, "b", json!({"v": 2}), 2, 1);
        engine.flush().unwrap();
        let retained = engine
            .peer_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations;
        assert!(retained.iter().any(|entry| entry.seq_no == 2));
    }

    let reopened = open_engine(dir.path());
    apply_index(&reopened, "gap", json!({"v": 1}), 1, 1);
    assert_eq!(reopened.sequence_stats().processed_checkpoint, Some(2));
}

#[test]
fn checkpoint_flush_caps_pruning_at_the_processed_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "a", json!({"v": 0}), 0, 1);
        apply_index(&engine, "b", json!({"v": 2}), 2, 1);
        engine.reconcile_term_sequence_state(2, Some(2)).unwrap();
        let receipt = engine
            .add_document_with_receipt_at_term("c", json!({"v": 3}), 2)
            .unwrap();
        engine
            .text_engine()
            .flush_with_global_checkpoint(receipt.seq_no)
            .unwrap();
        let retained = engine
            .peer_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations;
        assert!(retained.iter().any(|entry| entry.seq_no == 2));
        assert!(retained.iter().any(|entry| entry.seq_no == 3));
    }

    let reopened = open_engine(dir.path());
    apply_index(&reopened, "gap", json!({"v": 1}), 1, 2);
    assert_eq!(reopened.sequence_stats().processed_checkpoint, Some(3));
}

#[tokio::test]
async fn async_unsynced_write_does_not_advance_persisted_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new_with_mappings(
        dir.path(),
        Duration::from_secs(3600),
        &HashMap::new(),
        TranslogDurability::Async {
            sync_interval_ms: 3_600_000,
        },
        Arc::new(ColumnCache::new(0, 0)),
    )
    .unwrap();
    engine
        .add_document_with_receipt_at_term("a", json!({"v": 0}), 1)
        .unwrap();

    assert_eq!(engine.sequence_stats().processed_checkpoint, Some(0));
    assert_eq!(engine.sequence_stats().persisted_checkpoint, None);
}
