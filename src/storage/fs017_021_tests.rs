use super::*;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const SUBPROCESS_ENV_DIR: &str = "FERRIS_FS019_SUBPROCESS_STORAGE_DIR";
const SUBPROCESS_ENV_WORKDIR: &str = "FERRIS_FS019_SUBPROCESS_WORKDIR";
const SUBPROCESS_ENV_INDEX_UUID: &str = "FERRIS_FS019_SUBPROCESS_INDEX_UUID";
const SUBPROCESS_ENV_OP_ID: &str = "FERRIS_FS019_SUBPROCESS_OP_ID";
const SUBPROCESS_ENV_EPOCH: &str = "FERRIS_FS019_SUBPROCESS_EPOCH";

fn make_split(
    index_uuid: &str,
    split_id: &str,
    op_id: &str,
    checksum_char: char,
) -> RemoteSplitManifest {
    let mut tags = BTreeMap::new();
    if !op_id.is_empty() {
        tags.insert("operation_id".to_string(), op_id.to_string());
    }
    RemoteSplitManifest {
        split_id: split_id.to_string(),
        state: RemoteSplitState::Published,
        bundle_path: StorageManager::split_bundle_key(index_uuid, split_id),
        bundle_etag: None,
        hotcache_path: None,
        checksum: format!("sha256:{}", checksum_char.to_string().repeat(64)),
        doc_count: 5,
        size_bytes: 64,
        uncompressed_bytes: 128,
        hotcache_bytes: None,
        time_range: None,
        tags,
        field_ranges: BTreeMap::new(),
        field_terms: BTreeMap::new(),
    }
}

async fn upload_mini_bundle(
    manager: &StorageManager,
    index_uuid: &str,
    split_id: &str,
    op_id: &str,
    payload_marker: &str,
) -> RemoteSplitManifest {
    let stage = manager.staging_dir(index_uuid, split_id);
    if stage.exists() {
        std::fs::remove_dir_all(&stage).unwrap();
    }
    std::fs::create_dir_all(stage.join("index")).unwrap();
    std::fs::write(
        stage.join("index/meta.json"),
        format!("{{\"marker\":\"{payload_marker}\"}}"),
    )
    .unwrap();
    let (bundle_path, checksum, size_bytes) = manager
        .upload_split_bundle(index_uuid, split_id, &stage)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&stage);
    let mut tags = BTreeMap::new();
    tags.insert("operation_id".to_string(), op_id.to_string());
    RemoteSplitManifest {
        split_id: split_id.to_string(),
        state: RemoteSplitState::Published,
        bundle_path,
        bundle_etag: None,
        hotcache_path: None,
        checksum,
        doc_count: 2,
        size_bytes,
        uncompressed_bytes: size_bytes,
        hotcache_bytes: None,
        time_range: None,
        tags,
        field_ranges: BTreeMap::new(),
        field_terms: BTreeMap::new(),
    }
}

async fn stage_intent_and_prepared(
    manager: &StorageManager,
    index_uuid: &str,
    op_id: &str,
    fingerprint_seed: &str,
    now_ms: u64,
) -> (String, PublicationIntentRecord, PublicationPreparedRecord) {
    let split_id = deterministic_publication_split_id(index_uuid, op_id);
    let fp = compute_publication_request_fingerprint(
        index_uuid,
        1,
        "sha256:v1",
        &[(
            deterministic_publication_doc_id(index_uuid, op_id, 0),
            serde_json::json!({"title": fingerprint_seed}),
        )],
    )
    .unwrap();
    let intent = PublicationIntentRecord {
        format_version: PUBLICATION_RECORD_FORMAT_VERSION,
        index_uuid: index_uuid.to_string(),
        index_name: "events".to_string(),
        operation_id: op_id.to_string(),
        request_fingerprint: fp.clone(),
        schema_generation: 1,
        schema_hash: "sha256:v1".to_string(),
        split_id: split_id.clone(),
        planned_bundle_path: StorageManager::split_bundle_key(index_uuid, &split_id),
        doc_count: 2,
        producer_node_id: "node-1".to_string(),
        created_at: "2026-10-16T00:00:00Z".to_string(),
        created_at_epoch_ms: now_ms,
    };
    let intent = manager.stage_publication_intent(&intent).await.unwrap();
    let split = upload_mini_bundle(manager, index_uuid, &split_id, op_id, fingerprint_seed).await;
    let prepared = PublicationPreparedRecord {
        format_version: PUBLICATION_RECORD_FORMAT_VERSION,
        index_uuid: index_uuid.to_string(),
        index_name: "events".to_string(),
        operation_id: op_id.to_string(),
        request_fingerprint: fp.clone(),
        schema_generation: 1,
        schema_hash: "sha256:v1".to_string(),
        producer_node_id: "node-1".to_string(),
        prepared_at: "2026-10-16T00:00:01Z".to_string(),
        prepared_at_epoch_ms: now_ms,
        split,
    };
    let prepared = manager.write_publication_prepared(&prepared).await.unwrap();
    (fp, intent, prepared)
}

#[tokio::test]
async fn fs017_manifest_and_pointer_version_window_and_broken_lineage_rejection() {
    let temp_dir = TempDir::new().unwrap();
    let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
    let index_uuid = "idx-fs017";

    // 1. Format capability window checks.
    assert!(REMOTE_MANIFEST_FORMAT_CAPABILITY.supports_read(2));
    assert!(REMOTE_MANIFEST_FORMAT_CAPABILITY.supports_read(3));
    assert!(!REMOTE_MANIFEST_FORMAT_CAPABILITY.supports_read(1));
    assert!(!REMOTE_MANIFEST_FORMAT_CAPABILITY.supports_read(4));
    assert_eq!(REMOTE_MANIFEST_FORMAT_CAPABILITY.write_version, 3);

    // 2. Old supported v2 manifest + v2 pointer within the compatibility window [2, 3] remain readable.
    let v2_manifest_key = manifest_generation_key(index_uuid, 1);
    let v2_manifest_json = serde_json::json!({
        "version": 2,
        "engine": "remote_store",
        "index_uuid": index_uuid,
        "index_name": "events",
        "generation": 1,
        "published_at": "2026-10-16T00:00:00Z",
        "schema_hash": "sha256:v1",
        "splits": [make_split(index_uuid, "split-v2", "", 'a')]
    });
    manager
        .put(
            &manager.object_path(&v2_manifest_key).unwrap(),
            serde_json::to_vec(&v2_manifest_json).unwrap(),
        )
        .await
        .unwrap();
    let v2_pointer_json = serde_json::json!({
        "version": 2,
        "current_generation": 1,
        "manifest_path": v2_manifest_key,
    });
    manager
        .put(
            &manager
                .object_path(&manifest_pointer_key(index_uuid))
                .unwrap(),
            serde_json::to_vec(&v2_pointer_json).unwrap(),
        )
        .await
        .unwrap();

    let loaded_v2 = manager
        .load_current_manifest(index_uuid, Some("sha256:v1"))
        .await
        .expect("v2 manifest inside compatibility window must remain readable")
        .expect("v2 manifest exists");
    assert_eq!(loaded_v2.version, 2);
    assert_eq!(loaded_v2.generation, 1);
    assert_eq!(loaded_v2.schema_generation, 1);

    // 3. Unsupported future version (v4) fails closed with UnsupportedIndexFormatError.
    let mut v4_pointer = v2_pointer_json.clone();
    v4_pointer["version"] = serde_json::json!(4);
    manager
        .put(
            &manager
                .object_path(&manifest_pointer_key(index_uuid))
                .unwrap(),
            serde_json::to_vec(&v4_pointer).unwrap(),
        )
        .await
        .unwrap();
    let err = manager
        .load_current_manifest(index_uuid, None)
        .await
        .unwrap_err();
    assert!(err.is::<crate::common::UnsupportedIndexFormatError>());

    // 4. Generation-0 empty fenced pointer returns Ok(None) from load_current_manifest.
    let idx_fenced = "idx-gen0-fenced";
    let (fenced_ptr, _) = manager.fence_manifest_pointer(idx_fenced, 1).await.unwrap();
    assert_eq!(fenced_ptr.current_generation, 0);
    assert_eq!(fenced_ptr.pointer_revision, 1);
    assert_eq!(fenced_ptr.writer_epoch, 1);
    assert!(
        manager
            .load_current_manifest(idx_fenced, None)
            .await
            .unwrap()
            .is_none()
    );

    // 5. Generation 2 with missing parent manifest object in storage fails lineage verification.
    let idx_broken = "idx-broken-lineage";
    let broken_gen2 = RemoteStoreManifest {
        version: REMOTE_MANIFEST_FORMAT_VERSION,
        engine: crate::cluster::state::IndexEngine::RemoteStore,
        index_uuid: idx_broken.into(),
        index_name: "events".into(),
        schema_generation: 1,
        schema_hash: "sha256:v1".into(),
        generation: 2,
        parent_generation: 1,
        parent_manifest_path: Some(manifest_operation_key(idx_broken, 1, "missing-op-1")),
        writer_epoch: 1,
        operation_id: "op-2".into(),
        request_fingerprint: format!("sha256:{}", "b".repeat(64)),
        published_at: "2026-10-16T00:00:00Z".into(),
        settings: None,
        splits: vec![make_split(idx_broken, "split-2", "op-2", 'b')],
    };
    manager.publish_manifest(&broken_gen2).await.unwrap();
    let lineage_err = manager
        .load_current_manifest(idx_broken, Some("sha256:v1"))
        .await
        .unwrap_err();
    assert!(
        lineage_err.to_string().contains("broken manifest lineage"),
        "expected broken manifest lineage error, got: {lineage_err:#}"
    );
}

async fn run_crash_and_failover_suite(make_manager: impl Fn() -> StorageManager) {
    let manager = make_manager();
    let index_uuid = format!("idx-fs018-{}", Uuid::new_v4());
    let now_ms = 1_700_000_000_000u64;

    // Fence initial pointer at epoch 1.
    let (fenced_e1, _) = manager
        .fence_manifest_pointer(&index_uuid, 1)
        .await
        .unwrap();
    assert_eq!(fenced_e1.current_generation, 0);
    assert_eq!(fenced_e1.pointer_revision, 1);

    // Phase 1 & 2 & 3: Stage op-1 and inject crash before manifest creation, then resume.
    let (fp1, _, _) =
        stage_intent_and_prepared(&manager, &index_uuid, "op-1", "doc-batch-1", now_ms).await;
    manager.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::AfterPreparedBeforeManifestCreated,
    ));
    let req1 = SequencerCommitRequest {
        index_uuid: index_uuid.clone(),
        index_name: "events".into(),
        operation_id: "op-1".into(),
        request_fingerprint: fp1.clone(),
        schema_generation: 1,
        schema_hash: "sha256:v1".into(),
        writer_epoch: 1,
        lease_id: 1,
        sequencer_node_id: "node-1".into(),
        fenced_pointer_revision: fenced_e1.pointer_revision,
        settings: None,
    };
    let crash_err = manager
        .commit_prepared_publication(&req1)
        .await
        .unwrap_err();
    assert!(
        crash_err
            .to_string()
            .contains("AfterPreparedBeforeManifestCreated")
    );

    // Retry op-1 with the same fingerprint: commits generation 1.
    let outcome1 = manager.commit_prepared_publication(&req1).await.unwrap();
    assert_eq!(outcome1.generation, 1);
    assert_eq!(outcome1.parent_generation, 0);
    assert!(!outcome1.replayed);

    // Duplicate retry of op-1 with same fingerprint returns replayed=true at generation 1.
    let replay1 = manager.commit_prepared_publication(&req1).await.unwrap();
    assert_eq!(replay1.generation, 1);
    assert_eq!(replay1.split_id, outcome1.split_id);
    assert!(replay1.replayed);

    // Conflicting retry of op-1 with different fingerprint fails with OperationConflict (409).
    let mut bad_req1 = req1.clone();
    bad_req1.request_fingerprint = format!("sha256:{}", "f".repeat(64));
    let conflict_err = manager
        .commit_prepared_publication(&bad_req1)
        .await
        .unwrap_err();
    assert!(matches!(
        conflict_err.downcast_ref::<PublicationError>(),
        Some(PublicationError::OperationConflict { .. })
    ));

    // Phase 4: Stale epoch race after candidate manifest creation + new epoch pointer fence!
    let (fp_stale, _, _) = stage_intent_and_prepared(
        &manager,
        &index_uuid,
        "op-2-stale-first",
        "doc-batch-2",
        now_ms,
    )
    .await;
    let req_stale_e1 = SequencerCommitRequest {
        index_uuid: index_uuid.clone(),
        index_name: "events".into(),
        operation_id: "op-2-stale-first".into(),
        request_fingerprint: fp_stale.clone(),
        schema_generation: 1,
        schema_hash: "sha256:v1".into(),
        writer_epoch: 1,
        lease_id: 1,
        sequencer_node_id: "node-1".into(),
        fenced_pointer_revision: fenced_e1.pointer_revision,
        settings: None,
    };
    manager.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::AfterManifestCreatedBeforePointerCas,
    ));
    let err = manager
        .commit_prepared_publication(&req_stale_e1)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("AfterManifestCreatedBeforePointerCas")
    );

    // Now Sequencer 2 acquires epoch 2 and fences the pointer before Epoch 1 can CAS!
    let manager_node2 = make_manager();
    let (fenced_e2, _) = manager_node2
        .fence_manifest_pointer(&index_uuid, 2)
        .await
        .unwrap();
    assert_eq!(fenced_e2.writer_epoch, 2);
    assert_eq!(fenced_e2.current_generation, 1);
    assert_eq!(fenced_e2.pointer_revision, outcome1.pointer_revision + 1);

    // Sequencer 2 commits op-2-winner at generation 2 under epoch 2.
    let (fp_winner, _, _) = stage_intent_and_prepared(
        &manager_node2,
        &index_uuid,
        "op-2-winner",
        "doc-batch-winner",
        now_ms,
    )
    .await;
    let req_winner_e2 = SequencerCommitRequest {
        index_uuid: index_uuid.clone(),
        index_name: "events".into(),
        operation_id: "op-2-winner".into(),
        request_fingerprint: fp_winner,
        schema_generation: 1,
        schema_hash: "sha256:v1".into(),
        writer_epoch: 2,
        lease_id: 2,
        sequencer_node_id: "node-2".into(),
        fenced_pointer_revision: fenced_e2.pointer_revision,
        settings: None,
    };
    let outcome2 = manager_node2
        .commit_prepared_publication(&req_winner_e2)
        .await
        .unwrap();
    assert_eq!(outcome2.generation, 2);
    assert_eq!(outcome2.parent_generation, 1);
    assert_eq!(outcome2.writer_epoch, 2);

    // Old Sequencer 1 wakes up and tries to commit op-2-stale-first under stale epoch 1: MUST FAIL!
    let stale_err = manager
        .commit_prepared_publication(&req_stale_e1)
        .await
        .unwrap_err();
    assert!(
        matches!(
            stale_err.downcast_ref::<PublicationError>(),
            Some(PublicationError::StaleWriterEpoch {
                attempted_epoch: 1,
                current_epoch: 2,
                ..
            })
        ),
        "expected StaleWriterEpoch, got: {stale_err:#}"
    );

    // Client retries op-2-stale-first on active Sequencer 2 (epoch 2): reuses prepared bundle
    // and rebases cleanly onto generation 2 -> commits generation 3!
    let req_rebased_e2 = SequencerCommitRequest {
        writer_epoch: 2,
        lease_id: 2,
        sequencer_node_id: "node-2".into(),
        fenced_pointer_revision: fenced_e2.pointer_revision,
        ..req_stale_e1
    };
    let outcome3 = manager_node2
        .commit_prepared_publication(&req_rebased_e2)
        .await
        .unwrap();
    assert_eq!(outcome3.generation, 3);
    assert_eq!(outcome3.parent_generation, 2);
    assert_eq!(outcome3.writer_epoch, 2);
    assert!(!outcome3.replayed);

    // Phase 5: Ambiguous Indeterminate pointer CAS before commit vs after commit,
    // and crash after pointer CAS before committed.json!
    let (fp4, _, _) = stage_intent_and_prepared(
        &manager_node2,
        &index_uuid,
        "op-4-indeterminate",
        "doc-batch-4",
        now_ms,
    )
    .await;
    let req4 = SequencerCommitRequest {
        index_uuid: index_uuid.clone(),
        index_name: "events".into(),
        operation_id: "op-4-indeterminate".into(),
        request_fingerprint: fp4.clone(),
        schema_generation: 1,
        schema_hash: "sha256:v1".into(),
        writer_epoch: 2,
        lease_id: 2,
        sequencer_node_id: "node-2".into(),
        fenced_pointer_revision: fenced_e2.pointer_revision,
        settings: None,
    };

    // 5a: Indeterminate before commit returns PublicationError::Indeterminate and marks inventory Unresolved.
    manager_node2.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::ForcePointerCasIndeterminateBeforeCommit,
    ));
    let ind_err = manager_node2
        .commit_prepared_publication(&req4)
        .await
        .unwrap_err();
    assert!(matches!(
        ind_err.downcast_ref::<PublicationError>(),
        Some(PublicationError::Indeterminate { operation_id, .. }) if operation_id == "op-4-indeterminate"
    ));

    let inv_unresolved = manager_node2
        .build_reachability_inventory(&ReachabilityInventoryOptions {
            now_epoch_ms: now_ms + 1_000_000,
            ..ReachabilityInventoryOptions::for_index(&index_uuid, Some("events".into()))
        })
        .await
        .unwrap();
    assert!(inv_unresolved.has_unresolved_publications);

    // 5b: Indeterminate after commit resolves against the pointer chain, repairs committed.json, and returns replayed=true!
    manager_node2.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::ForcePointerCasIndeterminateAfterCommit,
    ));
    let resolved_outcome4 = manager_node2
        .commit_prepared_publication(&req4)
        .await
        .unwrap();
    assert_eq!(resolved_outcome4.generation, 4);
    assert!(resolved_outcome4.replayed);
    assert!(
        manager_node2
            .load_publication_committed(&index_uuid, "op-4-indeterminate")
            .await
            .unwrap()
            .is_some(),
        "resolution after indeterminate CAS must repair committed.json"
    );

    // 5c: Crash after pointer CAS before committed.json is repaired on retry.
    let (fp5, _, _) = stage_intent_and_prepared(
        &manager_node2,
        &index_uuid,
        "op-5-crash-after-cas",
        "doc-batch-5",
        now_ms,
    )
    .await;
    let req5 = SequencerCommitRequest {
        operation_id: "op-5-crash-after-cas".into(),
        request_fingerprint: fp5,
        ..req4
    };
    manager_node2.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::AfterPointerCommittedBeforeCommitRecord,
    ));
    let err5 = manager_node2
        .commit_prepared_publication(&req5)
        .await
        .unwrap_err();
    assert!(
        err5.to_string()
            .contains("AfterPointerCommittedBeforeCommitRecord")
    );
    assert!(
        manager_node2
            .load_publication_committed(&index_uuid, "op-5-crash-after-cas")
            .await
            .unwrap()
            .is_none()
    );
    let repaired5 = manager_node2
        .commit_prepared_publication(&req5)
        .await
        .unwrap();
    assert_eq!(repaired5.generation, 5);
    assert!(repaired5.replayed);
    assert!(
        manager_node2
            .load_publication_committed(&index_uuid, "op-5-crash-after-cas")
            .await
            .unwrap()
            .is_some()
    );

    // Final manifest must contain all 5 committed splits and pass full lineage verification.
    let final_manifest = manager_node2
        .load_current_manifest(&index_uuid, Some("sha256:v1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_manifest.generation, 5);
    assert_eq!(final_manifest.splits.len(), 5);
}

#[tokio::test]
async fn fs018_019_crash_and_retry_at_every_publication_phase_on_local_and_remote_stores() {
    let local_dir = TempDir::new().unwrap();
    let local_root = local_dir.path().join("_remote_store");
    let local_work = local_dir.path().join("_workdir");
    run_crash_and_failover_suite(|| {
        StorageManager::new(
            local_root.to_string_lossy().into_owned(),
            local_work.clone(),
        )
        .unwrap()
    })
    .await;

    let remote_dir = TempDir::new().unwrap();
    let mem_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    run_crash_and_failover_suite(|| {
        StorageManager::new_with_object_store(
            "s3://test-bucket/fs018".into(),
            Arc::clone(&mem_store),
            remote_dir.path().join("work"),
            None,
        )
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn fs019_subprocess_worker_entry_point() {
    let Ok(storage_dir) = std::env::var(SUBPROCESS_ENV_DIR) else {
        return;
    };
    let workdir = PathBuf::from(std::env::var(SUBPROCESS_ENV_WORKDIR).unwrap());
    let index_uuid = std::env::var(SUBPROCESS_ENV_INDEX_UUID).unwrap();
    let op_id = std::env::var(SUBPROCESS_ENV_OP_ID).unwrap();
    let epoch: u64 = std::env::var(SUBPROCESS_ENV_EPOCH)
        .unwrap()
        .parse()
        .unwrap();

    let manager = StorageManager::new(storage_dir, workdir).unwrap();
    let now_ms = 1_700_000_000_000u64;
    let (fp, _, _) = stage_intent_and_prepared(
        &manager,
        &index_uuid,
        &op_id,
        &format!("payload-{op_id}"),
        now_ms,
    )
    .await;

    for _ in 0..32 {
        let (fenced_ptr, _) = manager
            .fence_manifest_pointer(&index_uuid, epoch)
            .await
            .unwrap();
        let req = SequencerCommitRequest {
            index_uuid: index_uuid.clone(),
            index_name: "events".into(),
            operation_id: op_id.clone(),
            request_fingerprint: fp.clone(),
            schema_generation: 1,
            schema_hash: "sha256:v1".into(),
            writer_epoch: epoch,
            lease_id: epoch,
            sequencer_node_id: format!("proc-{op_id}"),
            fenced_pointer_revision: fenced_ptr.pointer_revision,
            settings: None,
        };
        match manager.commit_prepared_publication(&req).await {
            Ok(_) => return,
            Err(err) => {
                if matches!(
                    err.downcast_ref::<PublicationError>(),
                    Some(
                        PublicationError::PointerConflict { .. }
                            | PublicationError::UnfencedPointer { .. }
                    )
                ) {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    continue;
                }
                panic!("subprocess publication failed unexpectedly: {err:#}");
            }
        }
    }
    panic!("subprocess failed to commit after 32 attempts");
}

#[tokio::test]
async fn fs019_multi_process_producers_and_sequencer_failover_preserve_every_split() {
    if std::env::var(SUBPROCESS_ENV_DIR).is_ok() {
        return;
    }
    let temp_dir = TempDir::new().unwrap();
    let storage_root = temp_dir.path().join("_remote_store");
    std::fs::create_dir_all(&storage_root).unwrap();
    let index_uuid = "idx-multiprocess-fs019";
    let exe = std::env::current_exe().unwrap();

    // Round 1: 2 child processes produce and commit under epoch 1.
    let mut children = Vec::new();
    for i in 1..=2 {
        let workdir = temp_dir.path().join(format!("work-e1-{i}"));
        let op_id = format!("proc-op-{i}");
        let child = std::process::Command::new(&exe)
            .args([
                "--exact",
                "storage::fs017_021_tests::fs019_subprocess_worker_entry_point",
                "--nocapture",
            ])
            .env(SUBPROCESS_ENV_DIR, &storage_root)
            .env(SUBPROCESS_ENV_WORKDIR, &workdir)
            .env(SUBPROCESS_ENV_INDEX_UUID, index_uuid)
            .env(SUBPROCESS_ENV_OP_ID, &op_id)
            .env(SUBPROCESS_ENV_EPOCH, "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        children.push(child);
    }
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "epoch-1 subprocess failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Round 2: Failover to epoch 2 across 2 more child processes.
    let mut children_e2 = Vec::new();
    for i in 3..=4 {
        let workdir = temp_dir.path().join(format!("work-e2-{i}"));
        let op_id = format!("proc-op-{i}");
        let child = std::process::Command::new(&exe)
            .args([
                "--exact",
                "storage::fs017_021_tests::fs019_subprocess_worker_entry_point",
                "--nocapture",
            ])
            .env(SUBPROCESS_ENV_DIR, &storage_root)
            .env(SUBPROCESS_ENV_WORKDIR, &workdir)
            .env(SUBPROCESS_ENV_INDEX_UUID, index_uuid)
            .env(SUBPROCESS_ENV_OP_ID, &op_id)
            .env(SUBPROCESS_ENV_EPOCH, "2")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        children_e2.push(child);
    }
    for child in children_e2 {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "epoch-2 subprocess failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let verifier = StorageManager::new(
        storage_root.to_string_lossy().into_owned(),
        temp_dir.path().join("verifier-work"),
    )
    .unwrap();
    let final_manifest = verifier
        .load_current_manifest(index_uuid, Some("sha256:v1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_manifest.generation, 4);
    assert_eq!(final_manifest.writer_epoch, 2);
    assert_eq!(final_manifest.splits.len(), 4);
    let committed_ops: HashSet<String> = final_manifest
        .splits
        .iter()
        .filter_map(|s| s.tags.get("operation_id").cloned())
        .collect();
    assert_eq!(
        committed_ops,
        HashSet::from([
            "proc-op-1".to_string(),
            "proc-op-2".to_string(),
            "proc-op-3".to_string(),
            "proc-op-4".to_string(),
        ])
    );
}

#[tokio::test]
async fn fs020_021_inventory_and_janitor_enforce_retention_pins_sweep_grace_rate_limits_and_restart()
 {
    let temp_dir = TempDir::new().unwrap();
    let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
    let index_uuid = "idx-fs020-021";
    let base_now_ms = 1_700_000_000_000u64;

    // 1. Commit 2 generations normally under epoch 2 (after an epoch-1 superseded candidate).
    let (fenced_e1, _) = manager.fence_manifest_pointer(index_uuid, 1).await.unwrap();
    let (fp1, _, _) =
        stage_intent_and_prepared(&manager, index_uuid, "op-gen1", "docs-1", base_now_ms).await;
    let out1 = manager
        .commit_prepared_publication(&SequencerCommitRequest {
            index_uuid: index_uuid.into(),
            index_name: "events".into(),
            operation_id: "op-gen1".into(),
            request_fingerprint: fp1,
            schema_generation: 1,
            schema_hash: "sha256:v1".into(),
            writer_epoch: 1,
            lease_id: 1,
            sequencer_node_id: "node-1".into(),
            fenced_pointer_revision: fenced_e1.pointer_revision,
            settings: None,
        })
        .await
        .unwrap();
    assert_eq!(out1.generation, 1);

    // Create a superseded candidate at generation 2 under epoch 1 before epoch 2 fences.
    let (fp_sup, _, _) = stage_intent_and_prepared(
        &manager,
        index_uuid,
        "op-superseded",
        "docs-superseded",
        base_now_ms,
    )
    .await;
    manager.set_publication_crash_stage_for_test(Some(
        PublicationCrashStage::AfterManifestCreatedBeforePointerCas,
    ));
    let _ = manager
        .commit_prepared_publication(&SequencerCommitRequest {
            index_uuid: index_uuid.into(),
            index_name: "events".into(),
            operation_id: "op-superseded".into(),
            request_fingerprint: fp_sup.clone(),
            schema_generation: 1,
            schema_hash: "sha256:v1".into(),
            writer_epoch: 1,
            lease_id: 1,
            sequencer_node_id: "node-1".into(),
            fenced_pointer_revision: fenced_e1.pointer_revision,
            settings: None,
        })
        .await;

    // Fence epoch 2 and commit op-gen2 at generation 2, then force op-superseded to record Superseded attempt.
    let (fenced_e2, _) = manager.fence_manifest_pointer(index_uuid, 2).await.unwrap();
    let (fp2, _, _) =
        stage_intent_and_prepared(&manager, index_uuid, "op-gen2", "docs-2", base_now_ms).await;
    let out2 = manager
        .commit_prepared_publication(&SequencerCommitRequest {
            index_uuid: index_uuid.into(),
            index_name: "events".into(),
            operation_id: "op-gen2".into(),
            request_fingerprint: fp2,
            schema_generation: 1,
            schema_hash: "sha256:v1".into(),
            writer_epoch: 2,
            lease_id: 2,
            sequencer_node_id: "node-2".into(),
            fenced_pointer_revision: fenced_e2.pointer_revision,
            settings: None,
        })
        .await
        .unwrap();
    assert_eq!(out2.generation, 2);

    // Create an abandoned staging intent + bundle (op-abandoned), a pinned orphan split (op-pinned-orphan),
    // an unknown object, and a corrupt attempt object.
    let (_, _, prep_abandoned) = stage_intent_and_prepared(
        &manager,
        index_uuid,
        "op-abandoned",
        "docs-abandoned",
        base_now_ms,
    )
    .await;
    let (_, _, prep_pinned) = stage_intent_and_prepared(
        &manager,
        index_uuid,
        "op-pinned-orphan",
        "docs-pinned",
        base_now_ms,
    )
    .await;
    let unknown_key = format!("{index_uuid}/custom-notes/readme.txt");
    manager
        .put(
            &manager.object_path(&unknown_key).unwrap(),
            "keep-me-conservatively".to_string(),
        )
        .await
        .unwrap();

    // Pin prep_pinned's split on the StorageManager so inventory & janitor treat it as Pinned.
    let _local_pin = manager.pin_cached_split(index_uuid, &prep_pinned.split.split_id);

    // 2. Within active grace window (`now_epoch_ms == current`), uncommitted artifacts are Staging/Prepared (not reclaimable).
    let active_now = now_epoch_ms();
    let mut inv_opts = ReachabilityInventoryOptions::for_index(index_uuid, Some("events".into()));
    inv_opts.now_epoch_ms = active_now;
    inv_opts.active_grace_ms = 60_000;
    inv_opts.retention_horizon_ms = 300_000;
    let report_within_grace = manager
        .build_reachability_inventory(&inv_opts)
        .await
        .unwrap();
    assert!(report_within_grace.listing_complete);
    assert!(report_within_grace.lineage_healthy);
    assert_eq!(report_within_grace.summary.reclaimable_objects, 0);

    // 3. Advance clock past active_grace_ms and retention_horizon_ms.
    let future_now = active_now + 600_000;
    inv_opts.now_epoch_ms = future_now;
    let report_after_horizon = manager
        .build_reachability_inventory(&inv_opts)
        .await
        .unwrap();
    assert!(report_after_horizon.summary.reclaimable_objects > 0);

    // Verify specific classifications in the inventory:
    let by_path: BTreeMap<String, InventoryObjectEntry> = report_after_horizon
        .objects
        .iter()
        .cloned()
        .map(|e| (e.path.clone(), e))
        .collect();
    assert_eq!(
        by_path[&manifest_pointer_key(index_uuid)].classification,
        ObjectReachabilityClass::Committed
    );
    assert_eq!(
        by_path[&out2.manifest_path].classification,
        ObjectReachabilityClass::Committed
    );
    assert_eq!(
        by_path[&out1.manifest_path].classification,
        ObjectReachabilityClass::Retained
    );
    assert_eq!(
        by_path[&manifest_operation_key(index_uuid, 2, "op-superseded")].classification,
        ObjectReachabilityClass::Superseded
    );
    assert!(by_path[&manifest_operation_key(index_uuid, 2, "op-superseded")].reclaimable);
    assert_eq!(
        by_path[&prep_abandoned.split.bundle_path].classification,
        ObjectReachabilityClass::Abandoned
    );
    assert!(by_path[&prep_abandoned.split.bundle_path].reclaimable);
    assert_eq!(
        by_path[&prep_pinned.split.bundle_path].classification,
        ObjectReachabilityClass::Pinned
    );
    assert!(!by_path[&prep_pinned.split.bundle_path].reclaimable);
    assert_eq!(
        by_path[&unknown_key].classification,
        ObjectReachabilityClass::Unknown
    );
    assert!(!by_path[&unknown_key].reclaimable);

    // 4. Truncated / partial listing disables reclamation and blocks destructive janitor.
    let mut partial_opts = inv_opts.clone();
    partial_opts.max_reported_objects = 3;
    let truncated_report = manager
        .build_reachability_inventory(&partial_opts)
        .await
        .unwrap();
    assert!(truncated_report.truncated);
    assert!(!truncated_report.listing_complete);
    assert_eq!(truncated_report.summary.reclaimable_objects, 0);

    let blocked_janitor = manager
        .run_janitor(&JanitorOptions {
            dry_run: false,
            inventory_options: partial_opts,
            max_deletes_per_run: 64,
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 0,
        })
        .await
        .unwrap();
    assert!(blocked_janitor.blocked);
    assert_eq!(blocked_janitor.deleted_objects, 0);

    // 5. Default dry_run=true janitor explains candidates and deletes nothing.
    let dry_run_report = manager
        .run_janitor(&JanitorOptions {
            dry_run: true,
            inventory_options: inv_opts.clone(),
            max_deletes_per_run: 64,
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 10_000,
        })
        .await
        .unwrap();
    assert!(dry_run_report.dry_run);
    assert!(dry_run_report.would_delete_objects > 0);
    assert_eq!(dry_run_report.deleted_objects, 0);

    // 6. Destructive mode with sweep_grace_ms=10_000: Pass 1 durably marks candidates without deleting.
    let mark_pass = manager
        .run_janitor(&JanitorOptions {
            dry_run: false,
            inventory_options: inv_opts.clone(),
            max_deletes_per_run: 64,
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 10_000,
        })
        .await
        .unwrap();
    assert_eq!(mark_pass.deleted_objects, 0);
    assert!(mark_pass.marked_objects > 0);

    // Simulate process restart by constructing a fresh StorageManager on the same path.
    let restarted_manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
    let _restarted_pin =
        restarted_manager.pin_cached_split(index_uuid, &prep_pinned.split.split_id);

    // Now pin prep_abandoned's split BEFORE the sweep pass to prove false-positive prevention,
    // and inject a transient delete failure on op-superseded's candidate manifest!
    let delayed_reader_pin =
        restarted_manager.pin_cached_split(index_uuid, &prep_abandoned.split.split_id);
    let superseded_manifest_path = manifest_operation_key(index_uuid, 2, "op-superseded");
    restarted_manager.fail_janitor_delete_path_for_test(&superseded_manifest_path);

    let mut sweep_inv_opts = inv_opts.clone();
    sweep_inv_opts.now_epoch_ms = future_now + 20_000;
    let sweep_pass_1 = restarted_manager
        .run_janitor(&JanitorOptions {
            dry_run: false,
            inventory_options: sweep_inv_opts.clone(),
            max_deletes_per_run: 2, // Also test rate limiting!
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 10_000,
        })
        .await
        .unwrap();
    assert!(
        sweep_pass_1.false_positives_prevented >= 1,
        "delayed reader pin on marked split must trigger false_positives_prevented"
    );
    assert_eq!(sweep_pass_1.delete_failures, 1);
    assert!(sweep_pass_1.deleted_objects <= 2);

    // Clear transient delete failure and drop delayed_reader_pin, then complete sweep.
    drop(delayed_reader_pin);
    restarted_manager.clear_janitor_delete_failures_for_test();
    sweep_inv_opts.now_epoch_ms = future_now + 40_000;
    let _ = restarted_manager
        .run_janitor(&JanitorOptions {
            dry_run: false,
            inventory_options: sweep_inv_opts.clone(),
            max_deletes_per_run: 64,
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 10_000,
        })
        .await
        .unwrap();
    sweep_inv_opts.now_epoch_ms = future_now + 60_000;
    let final_sweep = restarted_manager
        .run_janitor(&JanitorOptions {
            dry_run: false,
            inventory_options: sweep_inv_opts,
            max_deletes_per_run: 64,
            max_delete_bytes_per_run: 1024 * 1024,
            sweep_grace_ms: 10_000,
        })
        .await
        .unwrap();
    assert_eq!(final_sweep.delete_failures, 0);

    // Verify committed splits, retained generation-1 manifest, pinned orphan split, and unknown file remain intact!
    let loaded_current = restarted_manager
        .load_current_manifest(index_uuid, Some("sha256:v1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded_current.generation, 2);
    assert_eq!(loaded_current.splits.len(), 2);
    assert!(
        restarted_manager
            .get_bytes(&restarted_manager.object_path(&unknown_key).unwrap())
            .await
            .is_ok()
    );
    assert!(
        restarted_manager
            .get_bytes(
                &restarted_manager
                    .object_path(&prep_pinned.split.bundle_path)
                    .unwrap()
            )
            .await
            .is_ok()
    );
    // And superseded manifest + unpinned abandoned bundle were deleted!
    assert!(
        restarted_manager
            .get_bytes(
                &restarted_manager
                    .object_path(&superseded_manifest_path)
                    .unwrap()
            )
            .await
            .is_err()
    );
    assert!(
        restarted_manager
            .get_bytes(
                &restarted_manager
                    .object_path(&prep_abandoned.split.bundle_path)
                    .unwrap()
            )
            .await
            .is_err()
    );
}
