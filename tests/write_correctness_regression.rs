use ferrissearch::engine::{
    CompositeEngine, DocumentMutation, SearchEngine, SequencedOperation, VersionConflictError,
    WriteCondition,
};
use ferrissearch::wal::{HotTranslog, WalOperation, WriteAheadLog};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

fn open(path: &std::path::Path) -> CompositeEngine {
    CompositeEngine::new(path, Duration::from_secs(3600)).unwrap()
}

#[test]
fn writes_regression_realtime_get_never_precedes_acknowledged_cas_during_churn() {
    const WRITERS: usize = 6;
    const WRITES_PER_WRITER: usize = 20;

    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(directory.path()));
    let old = engine
        .add_document_with_receipt("old", json!({"stable": true}))
        .unwrap();
    engine.refresh().unwrap();
    let initial = engine
        .add_document_with_receipt("counter", json!({"n": 0}))
        .unwrap();
    let acknowledged = Arc::new(AtomicU64::new(initial.seq_no));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(WRITERS + 4));
    let maintenance = {
        let engine = engine.clone();
        let stop = stop.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let mut rounds = 0;
            while !stop.load(Ordering::Acquire) || rounds < 2 {
                if rounds % 2 == 0 {
                    engine.refresh().unwrap();
                } else {
                    engine.flush().unwrap();
                }
                rounds += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
            rounds
        })
    };
    let readers = (0..2)
        .map(|_| {
            let engine = engine.clone();
            let acknowledged = acknowledged.clone();
            let stop = stop.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut reads = 0;
                while !stop.load(Ordering::Acquire) || reads < 20 {
                    let floor = acknowledged.load(Ordering::Acquire);
                    let document = engine
                        .get_document_with_metadata("counter", true)
                        .unwrap()
                        .expect("acknowledged counter must remain present");
                    assert!(
                        document.seq_no >= floor,
                        "GET at {} preceded the acknowledged version {floor}",
                        document.seq_no
                    );
                    assert_eq!(document.primary_term, initial.primary_term);
                    assert_eq!(
                        document.seq_no,
                        initial.seq_no + document.source["n"].as_u64().unwrap()
                    );
                    let stable = engine
                        .get_document_with_metadata("old", true)
                        .unwrap()
                        .unwrap();
                    assert_eq!(stable.source, json!({"stable": true}));
                    assert_eq!(stable.seq_no, old.seq_no);
                    reads += 1;
                    std::thread::yield_now();
                }
                reads
            })
        })
        .collect::<Vec<_>>();
    let writers = (0..WRITERS)
        .map(|writer| {
            let engine = engine.clone();
            let acknowledged = acknowledged.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for write in 0..WRITES_PER_WRITER {
                    let mut completed = false;
                    for _ in 0..1_000 {
                        let floor = acknowledged.load(Ordering::Acquire);
                        let current = engine
                            .get_document_with_metadata("counter", true)
                            .unwrap()
                            .unwrap();
                        assert!(current.seq_no >= floor);
                        let condition = WriteCondition::IfMatch {
                            seq_no: current.seq_no,
                            primary_term: current.primary_term,
                        };
                        let mut source = current.source;
                        source["n"] = json!(source["n"].as_u64().unwrap() + 1);
                        source[format!("writer-{writer}-write-{write}")] = json!(true);
                        match engine.add_document_with_condition_at_term(
                            "counter",
                            source,
                            current.primary_term,
                            condition,
                        ) {
                            Ok(receipt) => {
                                acknowledged.fetch_max(receipt.seq_no, Ordering::AcqRel);
                                completed = true;
                                break;
                            }
                            Err(error) => {
                                assert!(error.is::<VersionConflictError>(), "{error:#}");
                            }
                        }
                    }
                    assert!(completed, "CAS writer {writer} exhausted its retry budget");
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let results = writers
        .into_iter()
        .map(|writer| writer.join())
        .collect::<Vec<_>>();
    stop.store(true, Ordering::Release);
    let read_results = readers
        .into_iter()
        .map(|reader| reader.join())
        .collect::<Vec<_>>();
    let rounds = maintenance.join().unwrap();
    for result in results {
        result.unwrap();
    }
    for result in read_results {
        assert!(result.unwrap() >= 20);
    }
    assert!(rounds >= 2);
    let document = engine
        .get_document_with_metadata("counter", true)
        .unwrap()
        .unwrap();
    assert_eq!(document.source["n"], json!(WRITERS * WRITES_PER_WRITER));
    assert_eq!(document.seq_no, acknowledged.load(Ordering::Acquire));
    for writer in 0..WRITERS {
        for write in 0..WRITES_PER_WRITER {
            assert_eq!(
                document.source[format!("writer-{writer}-write-{write}")],
                true
            );
        }
    }
    engine.refresh().unwrap();
    assert_eq!(
        engine
            .get_document_with_metadata("counter", false)
            .unwrap()
            .unwrap(),
        document
    );
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

#[test]
fn writes_regression_bulk_reuses_initial_versions_without_changing_ordered_results() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    engine
        .add_document("existing", json!({"value": 1}))
        .unwrap();
    engine.add_document("deleted", json!({"value": 1})).unwrap();
    engine.refresh().unwrap();
    engine.delete_document("deleted").unwrap();
    let receipt = engine
        .bulk_add_documents_with_receipt(vec![
            ("existing".into(), json!({"value": 2})),
            ("new".into(), json!({"value": 3})),
            ("new".into(), json!({"value": 4})),
            ("deleted".into(), json!({"value": 5})),
            ("deleted".into(), json!({"value": 6})),
        ])
        .unwrap();
    assert_eq!(receipt.created, vec![false, true, false, true, false]);
    for (doc_id, value, offset) in [("existing", 2, 0), ("new", 4, 2), ("deleted", 6, 4)] {
        let document = engine
            .get_document_with_metadata(doc_id, true)
            .unwrap()
            .unwrap();
        assert_eq!(document.source, json!({"value": value}));
        assert_eq!(document.seq_no, receipt.start_seq_no.unwrap() + offset);
    }
}

#[test]
fn writes_regression_wal_positions_cross_buffer_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let wal = HotTranslog::open(directory.path()).unwrap();
    let operations = [64, 16_384, 4_096, 20_000]
        .into_iter()
        .enumerate()
        .map(|(id, size)| {
            (
                WalOperation::Index,
                json!({"_doc_id": id.to_string(), "_source": {"body": "x".repeat(size)}}),
            )
        })
        .collect::<Vec<_>>();
    let start = wal.recovery_read_snapshot().unwrap().end_cursor();
    wal.write_bulk_with_receipt(1, &operations).unwrap();
    let positions = wal.entry_positions(start, operations.len()).unwrap();
    assert_eq!(positions[0], start);
    for (offset, position) in positions.iter().enumerate() {
        let entry = wal.read_entry_at(*position).unwrap().unwrap();
        assert_eq!(entry.seq_no, offset as u64);
        assert_eq!(entry.payload, operations[offset].1);
        if offset > 0 {
            assert!(position.byte_offset > positions[offset - 1].byte_offset);
        }
    }
}

#[test]
fn writes_regression_reactivation_after_unpublished_commit_keeps_realtime_occ() {
    // The unpublished commit races Tantivy's commit watcher, which can reload
    // the reader first. Repeat the schedule so the stale-reader window is hit
    // on every run of the unfixed code.
    for attempt in 0..10 {
        reactivation_after_unpublished_commit_keeps_realtime_occ(attempt);
    }
}

fn reactivation_after_unpublished_commit_keeps_realtime_occ(attempt: usize) {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(&directory.path().join("shard"));
    let first = engine
        .add_document_with_receipt("doc", json!({"v": 1}))
        .unwrap();
    engine.refresh().unwrap();
    let second = engine
        .add_document_with_receipt("doc", json!({"v": 2}))
        .unwrap();
    engine
        .add_document_with_receipt("fresh", json!({"n": 1}))
        .unwrap();
    // A peer-recovery source snapshot commits both writes without publishing a reader.
    drop(
        engine
            .prepare_peer_recovery_snapshot(&directory.path().join("snapshot"))
            .unwrap(),
    );
    // A settlement term bump re-activates this engine; the rebuild resets the live map.
    let term = second.primary_term + 1;
    assert!(engine.prepare_primary_activation(term).unwrap().is_empty());

    let current = engine
        .get_document_with_metadata("doc", true)
        .unwrap()
        .unwrap();
    assert_eq!(current.source, json!({"v": 2}), "attempt {attempt}");
    assert_eq!(
        (current.seq_no, current.primary_term),
        (second.seq_no, second.primary_term)
    );
    assert!(
        engine
            .get_document_with_metadata("fresh", true)
            .unwrap()
            .is_some()
    );
    let stale_cas = engine
        .add_document_with_condition_at_term(
            "doc",
            json!({"v": 1, "lost_v2": true}),
            term,
            WriteCondition::IfMatch {
                seq_no: first.seq_no,
                primary_term: first.primary_term,
            },
        )
        .unwrap_err();
    assert!(stale_cas.is::<VersionConflictError>(), "{stale_cas:#}");
    let duplicate_create = engine
        .add_document_with_condition_at_term("fresh", json!({"n": 0}), term, WriteCondition::Create)
        .unwrap_err();
    assert!(
        duplicate_create.is::<VersionConflictError>(),
        "{duplicate_create:#}"
    );
}
