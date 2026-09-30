use ferrissearch::engine::{
    ApplyOutcome, CompositeEngine, DocumentMutation, SearchEngine, SequencedOperation,
};
use ferrissearch::wal::{HotTranslog, SequencedWalEntry, WalOperation, WriteAheadLog};
use serde_json::{Value, json};
use std::time::Duration;

fn wal_bytes(directory: &std::path::Path) -> Vec<u8> {
    std::fs::read(directory.join("translog-00000000000000000000.bin")).unwrap()
}

fn source() -> Value {
    json!({
        "body": "quotes \" slash \\ newline \n",
        "integer": 9_007_199_254_740_993u64,
        "negative": -9_007_199_254_740_993i64,
        "nested": {"null": null, "labels": ["a", "b"], "flag": true}
    })
}

#[test]
fn primary_single_wal_bytes_match_owned_document_envelopes() {
    let actual_dir = tempfile::tempdir().unwrap();
    let reference_dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new(actual_dir.path(), Duration::from_secs(3_600)).unwrap();
    let reference = HotTranslog::open(reference_dir.path()).unwrap();
    let doc_id = "escaped\"id\\with\nline";

    let receipt = engine
        .add_document_with_receipt_at_term(doc_id, source(), 7)
        .unwrap();
    assert_eq!(receipt.doc_id, doc_id);
    assert_eq!(receipt.seq_no, 0);
    assert_eq!(receipt.primary_term, 7);
    reference
        .append(
            7,
            WalOperation::Index,
            json!({"_doc_id": doc_id, "_source": source()}),
        )
        .unwrap();
    assert_eq!(
        wal_bytes(actual_dir.path()),
        wal_bytes(reference_dir.path())
    );
    engine.refresh().unwrap();
    assert_eq!(engine.get_document(doc_id).unwrap(), Some(source()));

    let receipt = engine
        .delete_document_with_receipt_at_term(doc_id, 7)
        .unwrap();
    assert_eq!(receipt.seq_no, 1);
    assert_eq!(receipt.primary_term, 7);
    reference
        .append(7, WalOperation::Delete, json!({"_doc_id": doc_id}))
        .unwrap();
    assert_eq!(
        wal_bytes(actual_dir.path()),
        wal_bytes(reference_dir.path())
    );
    engine.refresh().unwrap();
    assert!(engine.get_document(doc_id).unwrap().is_none());
}

#[test]
fn primary_bulk_wal_bytes_and_duplicate_id_order_are_unchanged() {
    let actual_dir = tempfile::tempdir().unwrap();
    let reference_dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new(actual_dir.path(), Duration::from_secs(3_600)).unwrap();
    let reference = HotTranslog::open(reference_dir.path()).unwrap();
    let docs = vec![
        ("same".to_string(), source()),
        ("other".to_string(), json!({"body": "second", "nested": {}})),
        (
            "same".to_string(),
            json!({"body": "latest", "nested": null}),
        ),
    ];
    let expected = docs
        .iter()
        .map(|(doc_id, payload)| {
            (
                WalOperation::Index,
                json!({"_doc_id": doc_id, "_source": payload}),
            )
        })
        .collect::<Vec<_>>();
    let receipt = engine
        .bulk_add_documents_with_receipt_at_term(docs, 7)
        .unwrap();
    assert_eq!(receipt.doc_ids, ["same", "other", "same"]);
    assert_eq!(receipt.start_seq_no, Some(0));
    assert_eq!(receipt.last_seq_no().unwrap(), Some(2));
    assert_eq!(receipt.primary_term, 7);
    assert_eq!(
        reference.write_bulk_with_receipt(7, &expected).unwrap(),
        Some(0)
    );
    assert_eq!(
        wal_bytes(actual_dir.path()),
        wal_bytes(reference_dir.path())
    );
    engine.refresh().unwrap();
    assert_eq!(engine.doc_count(), 2);
    assert_eq!(
        engine.get_document("same").unwrap(),
        Some(json!({"body": "latest", "nested": null}))
    );
}

#[test]
fn replica_single_wal_bytes_and_sequence_receipts_are_unchanged() {
    let actual_dir = tempfile::tempdir().unwrap();
    let reference_dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new(actual_dir.path(), Duration::from_secs(3_600)).unwrap();
    let reference = HotTranslog::open(reference_dir.path()).unwrap();
    let newest = SequencedOperation {
        seq_no: 1,
        primary_term: 7,
        mutation: DocumentMutation::Index {
            doc_id: "same".into(),
            source: source(),
        },
    };
    let receipt = engine.apply_replica_operation(newest.clone()).unwrap();
    assert_eq!(receipt.outcome, ApplyOutcome::Applied);
    assert!(receipt.operation_processed);
    assert!(receipt.operation_persisted);
    assert_eq!(receipt.sequence.processed_checkpoint, None);
    reference
        .append_with_seq(
            1,
            7,
            WalOperation::Index,
            json!({"_doc_id": "same", "_source": source()}),
        )
        .unwrap();

    let receipt = engine
        .apply_replica_operation(SequencedOperation {
            seq_no: 0,
            primary_term: 7,
            mutation: DocumentMutation::Index {
                doc_id: "same".into(),
                source: json!({"body": "older"}),
            },
        })
        .unwrap();
    assert_eq!(receipt.outcome, ApplyOutcome::Stale);
    assert!(receipt.operation_persisted);
    assert_eq!(receipt.sequence.processed_checkpoint, Some(1));
    reference
        .append_with_seq(
            0,
            7,
            WalOperation::Index,
            json!({"_doc_id": "same", "_source": {"body": "older"}}),
        )
        .unwrap();
    assert_eq!(
        wal_bytes(actual_dir.path()),
        wal_bytes(reference_dir.path())
    );
    engine.refresh().unwrap();
    assert_eq!(engine.get_document("same").unwrap(), Some(source()));
    let before = wal_bytes(actual_dir.path());
    assert_eq!(
        engine.apply_replica_operation(newest).unwrap().outcome,
        ApplyOutcome::Redelivery
    );
    assert_eq!(wal_bytes(actual_dir.path()), before);
}

#[test]
fn replica_bulk_keeps_physical_wal_order_stale_apply_and_redelivery() {
    let actual_dir = tempfile::tempdir().unwrap();
    let reference_dir = tempfile::tempdir().unwrap();
    let engine = CompositeEngine::new(actual_dir.path(), Duration::from_secs(3_600)).unwrap();
    let reference = HotTranslog::open(reference_dir.path()).unwrap();
    let operations = vec![
        SequencedOperation {
            seq_no: 2,
            primary_term: 7,
            mutation: DocumentMutation::Index {
                doc_id: "same".into(),
                source: source(),
            },
        },
        SequencedOperation {
            seq_no: 0,
            primary_term: 7,
            mutation: DocumentMutation::Index {
                doc_id: "same".into(),
                source: json!({"body": "older"}),
            },
        },
        SequencedOperation {
            seq_no: 1,
            primary_term: 7,
            mutation: DocumentMutation::NoOp {
                reason: "gap \"fill\"".into(),
            },
        },
        SequencedOperation {
            seq_no: 3,
            primary_term: 7,
            mutation: DocumentMutation::Delete {
                doc_id: "missing".into(),
            },
        },
    ];
    let expected = operations
        .iter()
        .map(|operation| {
            let (op, payload) = match &operation.mutation {
                DocumentMutation::Index { doc_id, source } => (
                    WalOperation::Index,
                    json!({"_doc_id": doc_id, "_source": source}),
                ),
                DocumentMutation::Delete { doc_id } => {
                    (WalOperation::Delete, json!({"_doc_id": doc_id}))
                }
                DocumentMutation::NoOp { reason } => {
                    (WalOperation::NoOp, json!({"_reason": reason}))
                }
            };
            SequencedWalEntry {
                seq_no: operation.seq_no,
                primary_term: operation.primary_term,
                op,
                payload,
            }
        })
        .collect::<Vec<_>>();
    let receipt = engine.apply_replica_batch(operations.clone()).unwrap();
    assert_eq!(
        receipt.outcomes,
        [
            ApplyOutcome::Applied,
            ApplyOutcome::Stale,
            ApplyOutcome::NoOp,
            ApplyOutcome::Applied
        ]
    );
    assert!(receipt.all_operations_processed);
    assert!(receipt.all_operations_persisted);
    assert_eq!(receipt.sequence.processed_checkpoint, Some(3));
    reference.append_batch_with_seq(&expected).unwrap();
    assert_eq!(
        wal_bytes(actual_dir.path()),
        wal_bytes(reference_dir.path())
    );
    engine.refresh().unwrap();
    assert_eq!(engine.get_document("same").unwrap(), Some(source()));

    let before = wal_bytes(actual_dir.path());
    let receipt = engine.apply_replica_batch(operations).unwrap();
    assert_eq!(receipt.outcomes, [ApplyOutcome::Redelivery; 4]);
    assert!(receipt.all_operations_persisted);
    assert_eq!(wal_bytes(actual_dir.path()), before);
}
