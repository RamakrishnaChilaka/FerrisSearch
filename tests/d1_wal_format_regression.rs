use ferrissearch::cluster::state::IndexSettings;
use ferrissearch::shard::{AssignedShardOpen, SHARD_COPY_IDENTITY_FILE, ShardManager};
use ferrissearch::wal::HotTranslog;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const MANIFEST_FILE: &str = "translog.manifest";
const GENERATION_FILE: &str = "translog-00000000000000000000.bin";

#[test]
fn new_translog_persists_v2_manifest_with_entry_version_and_min_max_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let _translog = HotTranslog::open(dir.path()).unwrap();

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap()).unwrap();
    assert_eq!(manifest["version"], 2);
    assert_eq!(manifest["entry_format_version"], 2);
    assert_eq!(
        manifest["generations"][0]["min_seq_no"],
        serde_json::Value::Null
    );
    assert_eq!(
        manifest["generations"][0]["max_seq_no"],
        serde_json::Value::Null
    );
    assert!(manifest["generations"][0].get("first_seq_no").is_none());
    assert!(manifest["generations"][0].get("last_seq_no").is_none());
}

#[test]
fn direct_open_rejects_even_an_empty_v1_manifest_without_migration_authority() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(GENERATION_FILE), []).unwrap();
    std::fs::write(
        dir.path().join(MANIFEST_FILE),
        serde_json::to_vec(&json!({
            "version": 1,
            "active_generation_id": 0,
            "next_generation_id": 1,
            "generations": [{
                "id": 0,
                "first_seq_no": null,
                "last_seq_no": null,
                "size_bytes": 0
            }]
        }))
        .unwrap(),
    )
    .unwrap();

    let error = HotTranslog::open(dir.path())
        .err()
        .expect("v1 WAL must fail closed");
    let error = format!("{error:#}");
    assert!(
        error.contains("unsupported translog manifest") && error.contains("version 1"),
        "unexpected legacy WAL error: {error}"
    );
}

#[tokio::test]
async fn fence_raise_persists_the_pre_raise_maximum_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(ShardManager::new(dir.path(), Duration::from_secs(60)));
    let engine = manager
        .open_assigned_shard_with_settings(
            "idx",
            0,
            &HashMap::new(),
            &IndexSettings::default(),
            "uuid-1",
            AssignedShardOpen {
                allocation_id: 7,
                primary_term: 2,
                allow_empty_creation: true,
            },
        )
        .unwrap();
    engine
        .add_document_with_seq("before-raise", json!({"value": 0}), 0)
        .unwrap();

    manager
        .raise_copy_fence_blocking("idx".into(), 0, "uuid-1".into(), 7, 5)
        .await
        .unwrap();

    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join("uuid-1/shard_0")
                .join(SHARD_COPY_IDENTITY_FILE),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(identity["replica_fence"], 5);
    assert_eq!(identity["fence_max_seq_no"], 0);
}
