use ferrissearch::engine::{
    CompositeEngine, DocumentMutation, SearchEngine, SequencedOperation, VersionConflictError,
    WriteCondition,
};
use ferrissearch::wal::{HotTranslog, WalOperation, WriteAheadLog};
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn open(path: &std::path::Path) -> CompositeEngine {
    CompositeEngine::new(path, Duration::from_secs(3600)).unwrap()
}

#[test]
fn writes_regression_conflicts_do_not_allocate_sequences_or_append_wal() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    let first = engine
        .add_document_with_receipt("doc", json!({"value": 1}))
        .unwrap();
    let before = engine.text_engine().translog_size_bytes();
    for condition in [
        WriteCondition::Create,
        WriteCondition::IfMatch {
            seq_no: first.seq_no + 1,
            primary_term: first.primary_term,
        },
        WriteCondition::IfMatch {
            seq_no: first.seq_no,
            primary_term: first.primary_term + 1,
        },
    ] {
        let error = engine
            .add_document_with_condition_at_term(
                "doc",
                json!({"wrong": true}),
                first.primary_term,
                condition,
            )
            .unwrap_err();
        assert!(error.is::<VersionConflictError>(), "{error:#}");
        assert_eq!(engine.text_engine().translog_size_bytes(), before);
        assert_eq!(engine.wal_max_seq_no(), Some(first.seq_no));
    }
    let error = engine
        .delete_document_with_condition_at_term(
            "doc",
            first.primary_term,
            WriteCondition::IfMatch {
                seq_no: first.seq_no + 1,
                primary_term: first.primary_term,
            },
        )
        .unwrap_err();
    assert!(error.is::<VersionConflictError>());
    assert_eq!(engine.text_engine().translog_size_bytes(), before);
    let second = engine
        .add_document_with_condition_at_term(
            "doc",
            json!({"value": 2}),
            first.primary_term,
            WriteCondition::IfMatch {
                seq_no: first.seq_no,
                primary_term: first.primary_term,
            },
        )
        .unwrap();
    assert!(!second.created);
    assert_eq!(second.seq_no, first.seq_no + 1);
}

#[test]
fn writes_regression_exactly_one_concurrent_conditional_write_wins() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(directory.path()));
    let first = engine
        .add_document_with_receipt("doc", json!({"value": 0}))
        .unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let writers = (1..=8)
        .map(|value| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let current = engine
                    .get_document_with_metadata("doc", true)
                    .unwrap()
                    .unwrap();
                barrier.wait();
                let result = engine.add_document_with_condition_at_term(
                    "doc",
                    json!({"value": value}),
                    current.primary_term,
                    WriteCondition::IfMatch {
                        seq_no: current.seq_no,
                        primary_term: current.primary_term,
                    },
                );
                (value, result)
            })
        })
        .collect::<Vec<_>>();
    let mut winner = None;
    for writer in writers {
        let (value, result) = writer.join().unwrap();
        match result {
            Ok(receipt) => {
                assert!(winner.replace(value).is_none());
                assert_eq!(receipt.seq_no, first.seq_no + 1);
            }
            Err(error) => assert!(error.is::<VersionConflictError>(), "{error:#}"),
        }
    }
    let document = engine
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(document.source["value"], winner.unwrap());
    assert_eq!(document.seq_no, first.seq_no + 1);
    assert_eq!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .len(),
        2
    );
}

#[test]
fn writes_regression_replay_flush_and_committed_conditions_preserve_identity() {
    let directory = tempfile::tempdir().unwrap();
    let last = {
        let engine = open(directory.path());
        engine
            .add_document_with_receipt("doc", json!({"value": 1}))
            .unwrap();
        engine
            .add_document_with_receipt("doc", json!({"value": 2}))
            .unwrap()
    };
    let engine = open(directory.path());
    let document = engine
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(document.source, json!({"value": 2}));
    assert_eq!(document.seq_no, last.seq_no);
    engine.flush().unwrap();
    assert!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .is_empty()
    );
    let document = engine
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(document.seq_no, last.seq_no);
    assert_eq!(document.primary_term, last.primary_term);
    engine.refresh().unwrap();
    let next = engine
        .add_document_with_condition_at_term(
            "doc",
            json!({"value": 3}),
            last.primary_term,
            WriteCondition::IfMatch {
                seq_no: last.seq_no,
                primary_term: last.primary_term,
            },
        )
        .unwrap();
    assert_eq!(next.seq_no, last.seq_no + 1);
    assert_eq!(
        engine
            .get_document_with_metadata("doc", false)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 2})
    );
    assert_eq!(
        engine
            .get_document_with_metadata("doc", true)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 3})
    );
}

#[test]
fn writes_regression_primary_bulk_and_out_of_order_replica_reads_use_correct_wal_frames() {
    let primary_directory = tempfile::tempdir().unwrap();
    let primary = open(primary_directory.path());
    let receipt = primary
        .bulk_add_documents_with_receipt(vec![
            ("doc".into(), json!({"value": 1})),
            ("doc".into(), json!({"value": 2})),
            ("other".into(), json!({"value": 3})),
        ])
        .unwrap();
    assert_eq!(receipt.created, [true, false, true]);
    let current = primary
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(current.source, json!({"value": 2}));
    assert_eq!(current.seq_no, 1);
    let replica_directory = tempfile::tempdir().unwrap();
    let replica = open(replica_directory.path());
    replica
        .apply_replica_batch(vec![
            SequencedOperation {
                seq_no: 2,
                primary_term: receipt.primary_term,
                mutation: DocumentMutation::Index {
                    doc_id: "other".into(),
                    source: json!({"value": 3}),
                },
            },
            SequencedOperation {
                seq_no: 1,
                primary_term: receipt.primary_term,
                mutation: DocumentMutation::Index {
                    doc_id: "doc".into(),
                    source: json!({"value": 2}),
                },
            },
            SequencedOperation {
                seq_no: 0,
                primary_term: receipt.primary_term,
                mutation: DocumentMutation::Index {
                    doc_id: "doc".into(),
                    source: json!({"value": 1}),
                },
            },
        ])
        .unwrap();
    assert_eq!(
        replica
            .get_document_with_metadata("doc", true)
            .unwrap()
            .unwrap(),
        current
    );
    replica
        .apply_replica_operation(SequencedOperation {
            seq_no: 3,
            primary_term: receipt.primary_term,
            mutation: DocumentMutation::Delete {
                doc_id: "doc".into(),
            },
        })
        .unwrap();
    assert!(
        replica
            .get_document_with_metadata("doc", true)
            .unwrap()
            .is_none()
    );
    replica.flush().unwrap();
    assert!(
        replica
            .get_document_with_metadata("doc", true)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        replica
            .get_document_with_metadata("other", true)
            .unwrap()
            .unwrap()
            .source,
        json!({"value": 3})
    );
}

#[test]
fn writes_regression_replay_positions_follow_physical_order_across_committed_skips() {
    let directory = tempfile::tempdir().unwrap();
    {
        let engine = open(directory.path());
        for (seq_no, doc_id) in [(2, "a"), (0, "a")] {
            engine
                .apply_replica_operation(SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: DocumentMutation::Index {
                        doc_id: doc_id.to_string(),
                        source: json!({"value": seq_no}),
                    },
                })
                .unwrap();
        }
        engine.refresh().unwrap();
        for (seq_no, doc_id) in [(4, "b"), (1, "a")] {
            engine
                .apply_replica_operation(SequencedOperation {
                    seq_no,
                    primary_term: 1,
                    mutation: DocumentMutation::Index {
                        doc_id: doc_id.to_string(),
                        source: json!({"value": seq_no}),
                    },
                })
                .unwrap();
        }
    }
    let engine = open(directory.path());
    let a = engine
        .get_document_with_metadata("a", true)
        .unwrap()
        .unwrap();
    let b = engine
        .get_document_with_metadata("b", true)
        .unwrap()
        .unwrap();
    assert_eq!((a.seq_no, a.source["value"].as_u64()), (2, Some(2)));
    assert_eq!((b.seq_no, b.source["value"].as_u64()), (4, Some(4)));
}

#[test]
fn writes_regression_wal_positions_survive_batches_and_detect_truncation() {
    let directory = tempfile::tempdir().unwrap();
    let wal = HotTranslog::open(directory.path()).unwrap();
    let start = wal.recovery_read_snapshot().unwrap().end_cursor();
    wal.write_bulk_with_receipt(
        1,
        &[
            (
                WalOperation::Index,
                json!({"_doc_id": "a", "_source": {"value": 1}}),
            ),
            (
                WalOperation::Index,
                json!({"_doc_id": "b", "_source": {"value": 2}}),
            ),
        ],
    )
    .unwrap();
    let positions = wal.entry_positions(start, 2).unwrap();
    assert_eq!(positions[0], start);
    assert!(positions[1].byte_offset > positions[0].byte_offset);
    for (seq_no, position) in positions.into_iter().enumerate() {
        assert_eq!(
            wal.read_entry_at(position).unwrap().unwrap().seq_no,
            seq_no as u64
        );
        assert_eq!(
            wal.find_entry_position(seq_no as u64, 1).unwrap(),
            Some(position)
        );
    }
    wal.truncate_below(1).unwrap();
    assert!(wal.read_entry_at(start).unwrap().is_none());
    let next = wal.recovery_read_snapshot().unwrap().end_cursor();
    let entry = wal
        .append(
            1,
            WalOperation::Index,
            json!({"_doc_id": "c", "_source": {"value": 3}}),
        )
        .unwrap();
    assert_eq!(entry.seq_no, 2);
    assert_eq!(
        wal.read_entry_at(next).unwrap().unwrap().payload["_source"],
        json!({"value": 3})
    );
}
