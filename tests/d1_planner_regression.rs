use ferrissearch::engine::{CompositeEngine, SearchEngine};
use serde_json::json;
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

fn apply_delete(
    engine: &dyn SearchEngine,
    doc_id: &str,
    seq_no: u64,
    primary_term: u64,
) -> ferrissearch::engine::ReplicaApplyReceipt {
    engine
        .apply_replica_operation(ferrissearch::engine::SequencedOperation {
            seq_no,
            primary_term,
            mutation: ferrissearch::engine::DocumentMutation::Delete {
                doc_id: doc_id.to_string(),
            },
        })
        .unwrap()
}

#[test]
fn newer_index_survives_late_older_index_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "doc", json!({"value": 2}), 1, 1);
        apply_index(&engine, "doc", json!({"value": 1}), 0, 1);
        engine.refresh().unwrap();
        assert_eq!(engine.get_document("doc").unwrap().unwrap()["value"], 2);
    }

    let reopened = open_engine(dir.path());
    assert_eq!(reopened.get_document("doc").unwrap().unwrap()["value"], 2);
}

#[test]
fn uncommitted_out_of_order_restart_keeps_the_newer_value() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "doc", json!({"value": 2}), 1, 1);
        apply_index(&engine, "doc", json!({"value": 1}), 0, 1);
    }

    let reopened = open_engine(dir.path());
    reopened.refresh().unwrap();
    assert_eq!(reopened.get_document("doc").unwrap().unwrap()["value"], 2);
}

#[test]
fn delete_survives_late_older_index_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "doc", json!({"value": 0}), 0, 1);
        apply_delete(&engine, "doc", 2, 1);
        apply_index(&engine, "doc", json!({"value": 1}), 1, 1);
        engine.refresh().unwrap();
        assert!(engine.get_document("doc").unwrap().is_none());
    }

    let reopened = open_engine(dir.path());
    assert!(reopened.get_document("doc").unwrap().is_none());
}

#[test]
fn duplicate_delivery_does_not_append_twice_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_engine(dir.path());
        apply_index(&engine, "x", json!({"value": 1}), 0, 1);
        apply_index(&engine, "y", json!({"value": 1}), 1, 1);
        apply_index(&engine, "x", json!({"value": 1}), 0, 1);
        assert_eq!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .len(),
            2
        );
    }

    let reopened = open_engine(dir.path());
    reopened.refresh().unwrap();
    assert_eq!(reopened.get_document("x").unwrap().unwrap()["value"], 1);
    assert_eq!(reopened.get_document("y").unwrap().unwrap()["value"], 1);
}

#[test]
fn incompatible_same_term_redelivery_fails_without_another_wal_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_engine(dir.path());
    apply_index(&engine, "doc", json!({"value": 1}), 0, 1);
    assert!(
        engine
            .apply_replica_operation(ferrissearch::engine::SequencedOperation {
                seq_no: 0,
                primary_term: 1,
                mutation: ferrissearch::engine::DocumentMutation::Index {
                    doc_id: "doc".into(),
                    source: json!({"value": 2}),
                },
            })
            .is_err()
    );
    assert_eq!(
        engine
            .retained_recovery_ops(0, usize::MAX, usize::MAX)
            .unwrap()
            .operations
            .len(),
        1
    );
}

#[test]
fn new_local_index_schema_contains_sequence_identity_fields() {
    let dir = tempfile::tempdir().unwrap();
    let _engine = open_engine(dir.path());
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("index/meta.json")).unwrap())
            .unwrap();
    let names = meta["schema"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["name"].as_str())
        .collect::<Vec<_>>();
    assert!(names.contains(&"_seq_no"));
    assert!(names.contains(&"_primary_term"));
}
