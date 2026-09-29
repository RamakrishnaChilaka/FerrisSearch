use ferrissearch::engine::{CompositeEngine, DocumentMutation, SearchEngine, SequencedOperation};
use ferrissearch::wal::{WalCursor, WalOperation};
use serde_json::json;
use std::time::Duration;

fn open_engine(path: &std::path::Path) -> CompositeEngine {
    CompositeEngine::new(path, Duration::from_secs(3600)).unwrap()
}

fn apply_index(engine: &dyn SearchEngine, doc_id: &str, seq_no: u64, primary_term: u64) {
    engine
        .apply_replica_operation(SequencedOperation {
            seq_no,
            primary_term,
            mutation: DocumentMutation::Index {
                doc_id: doc_id.to_string(),
                source: json!({"seq": seq_no}),
            },
        })
        .unwrap();
}

#[test]
fn physical_wal_cursor_does_not_skip_a_late_lower_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    apply_index(&engine, "ten", 10, 1);
    apply_index(&engine, "six", 6, 1);

    let barrier = engine.peer_recovery_barrier().unwrap();
    let mut cursor = WalCursor {
        generation_id: 0,
        byte_offset: 0,
    };
    let mut recovered = Vec::new();
    loop {
        let batch = engine
            .peer_recovery_ops(cursor, Some(barrier.wal_end), 1, usize::MAX)
            .unwrap();
        recovered.extend(batch.operations.into_iter().map(|entry| entry.seq_no));
        cursor = batch.next_cursor;
        if batch.complete {
            break;
        }
    }

    assert_eq!(cursor, barrier.wal_end);
    assert_eq!(recovered, vec![10, 6]);
}

#[test]
fn promotion_replays_then_fills_gaps_with_durable_noops() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "zero", 0, 1);
        apply_index(&engine, "two", 2, 1);
        engine.reconcile_term_sequence_state(2, Some(2)).unwrap();

        let noops = engine.prepare_primary_activation(2).unwrap();

        assert_eq!(
            noops
                .iter()
                .map(|operation| operation.seq_no)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(engine.sequence_stats().processed_checkpoint, Some(2));
        assert_eq!(engine.sequence_stats().persisted_checkpoint, Some(2));
        let wal = engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations;
        assert!(
            wal.iter()
                .any(|entry| entry.seq_no == 1 && entry.op == WalOperation::NoOp)
        );
    }

    let reopened = open_engine(dir.path());
    assert_eq!(reopened.sequence_stats().processed_checkpoint, Some(2));
    assert_eq!(reopened.get_document("zero").unwrap().unwrap()["seq"], 0);
    assert_eq!(reopened.get_document("two").unwrap().unwrap()["seq"], 2);
}
