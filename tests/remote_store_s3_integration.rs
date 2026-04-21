//! Integration tests for the S3 storage backend against a real S3-compatible
//! object store (tested against MinIO / RustFS).
//!
//! These tests are gated behind the `FERRIS_RUSTFS_ENDPOINT` environment
//! variable so a plain `cargo test` does not require a running object store.
//!
//! To run locally:
//!
//! ```bash
//! ./scripts/rustfs_dev.sh up
//! eval "$(./scripts/rustfs_dev.sh env)"
//! export FERRIS_RUSTFS_ENDPOINT="$AWS_ENDPOINT_URL"
//! cargo test --test remote_store_s3_integration -- --test-threads=1
//! ```
//!
//! Tests run single-threaded because they mutate `AWS_*` process environment
//! variables that the `object_store::aws` builder reads at construction time.

use ferrissearch::cluster::state::IndexEngine;
use ferrissearch::storage::{
    RemoteSplitManifest, RemoteSplitState, RemoteStoreManifest, StorageManager,
};
use std::collections::BTreeMap;
use uuid::Uuid;

const BUCKET_ENV: &str = "FERRIS_REMOTE_STORE_BUCKET";
const ENDPOINT_ENV: &str = "FERRIS_RUSTFS_ENDPOINT";
const DEFAULT_BUCKET: &str = "ferrissearch-dev";
const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
const DEFAULT_SECRET_KEY: &str = "rustfsadmin";

/// Returns `None` when the S3 endpoint is not configured in the env so
/// individual tests can `return` early without failing.
fn s3_env() -> Option<(String, String)> {
    let endpoint = std::env::var(ENDPOINT_ENV).ok()?;
    let bucket = std::env::var(BUCKET_ENV).unwrap_or_else(|_| DEFAULT_BUCKET.to_string());

    // Set defaults for creds/region when not already provided. These are
    // the dev-server defaults from scripts/rustfs_dev.sh.
    // SAFETY: tests run with --test-threads=1 per the module docstring.
    unsafe {
        if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            std::env::set_var("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY);
        }
        if std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
            std::env::set_var("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY);
        }
        if std::env::var("AWS_REGION").is_err() && std::env::var("AWS_DEFAULT_REGION").is_err() {
            std::env::set_var("AWS_REGION", "us-east-1");
        }
        std::env::set_var("AWS_ENDPOINT_URL", &endpoint);
    }

    Some((endpoint, bucket))
}

fn random_prefix() -> String {
    format!("ferris-it/{}", Uuid::new_v4())
}

fn sample_manifest(index_uuid: &str, generation: u64) -> RemoteStoreManifest {
    RemoteStoreManifest {
        version: 1,
        engine: IndexEngine::RemoteStore,
        index_uuid: index_uuid.into(),
        index_name: "events".into(),
        generation,
        published_at: "2026-04-21T00:00:00Z".into(),
        schema_hash: "sha256:v1".into(),
        settings: None,
        splits: vec![RemoteSplitManifest {
            split_id: format!("split-{}", generation),
            state: RemoteSplitState::Published,
            bundle_path: format!("splits/split-{}/bundle", generation),
            bundle_etag: None,
            hotcache_path: None,
            checksum: format!("sha256:split-{}", generation),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 32,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
        }],
    }
}

#[tokio::test]
async fn s3_backend_reports_as_remote_and_exposes_s3_uri() {
    let Some((_, bucket)) = s3_env() else {
        eprintln!("skip: {} not set", ENDPOINT_ENV);
        return;
    };
    let prefix = random_prefix();
    let uri = format!("s3://{}/{}", bucket, prefix);

    let manager = StorageManager::new(uri.clone()).expect("s3 manager should build");

    assert!(!manager.is_local());
    assert!(manager.root().is_none());
    assert_eq!(manager.uri(), uri);
}

#[tokio::test]
async fn s3_backend_publishes_and_loads_manifest_roundtrip() {
    let Some((_, bucket)) = s3_env() else {
        eprintln!("skip: {} not set", ENDPOINT_ENV);
        return;
    };
    let prefix = random_prefix();
    let uri = format!("s3://{}/{}", bucket, prefix);
    let manager = StorageManager::new(uri).expect("s3 manager should build");

    let index_uuid = Uuid::new_v4().to_string();
    let manifest_v1 = sample_manifest(&index_uuid, 1);

    // Publish generation 1.
    let pointer = manager
        .publish_manifest(&manifest_v1)
        .await
        .expect("publish manifest v1 against s3");
    assert_eq!(pointer.current_generation, 1);

    // Load current manifest back.
    let loaded = manager
        .load_current_manifest(&index_uuid, Some("sha256:v1"))
        .await
        .expect("load current manifest")
        .expect("manifest should exist");
    assert_eq!(loaded, manifest_v1);
}

#[tokio::test]
async fn s3_backend_append_split_and_publish_bumps_generation() {
    let Some((_, bucket)) = s3_env() else {
        eprintln!("skip: {} not set", ENDPOINT_ENV);
        return;
    };
    let prefix = random_prefix();
    let uri = format!("s3://{}/{}", bucket, prefix);
    let manager = StorageManager::new(uri).expect("s3 manager should build");

    let index_uuid = Uuid::new_v4().to_string();

    let first_split = RemoteSplitManifest {
        split_id: "split-a".into(),
        state: RemoteSplitState::Published,
        bundle_path: "splits/split-a/bundle".into(),
        bundle_etag: None,
        hotcache_path: None,
        checksum: "sha256:a".into(),
        doc_count: 1,
        size_bytes: 16,
        uncompressed_bytes: 32,
        hotcache_bytes: None,
        time_range: None,
        tags: BTreeMap::new(),
    };
    let m1 = manager
        .append_split_and_publish(&index_uuid, "events", "sha256:v1", None, first_split)
        .await
        .expect("first append");
    assert_eq!(m1.generation, 1);
    assert_eq!(m1.splits.len(), 1);

    let second_split = RemoteSplitManifest {
        split_id: "split-b".into(),
        state: RemoteSplitState::Published,
        bundle_path: "splits/split-b/bundle".into(),
        bundle_etag: None,
        hotcache_path: None,
        checksum: "sha256:b".into(),
        doc_count: 2,
        size_bytes: 32,
        uncompressed_bytes: 64,
        hotcache_bytes: None,
        time_range: None,
        tags: BTreeMap::new(),
    };
    let m2 = manager
        .append_split_and_publish(&index_uuid, "events", "sha256:v1", None, second_split)
        .await
        .expect("second append");
    assert_eq!(m2.generation, 2);
    assert_eq!(m2.splits.len(), 2);

    // Reloading via the pointer must see generation 2.
    let loaded = manager
        .load_current_manifest(&index_uuid, Some("sha256:v1"))
        .await
        .expect("load current")
        .expect("manifest should exist");
    assert_eq!(loaded.generation, 2);
    assert_eq!(loaded.splits.len(), 2);
}

#[tokio::test]
async fn s3_backend_load_current_manifest_returns_none_for_fresh_index() {
    let Some((_, bucket)) = s3_env() else {
        eprintln!("skip: {} not set", ENDPOINT_ENV);
        return;
    };
    let prefix = random_prefix();
    let uri = format!("s3://{}/{}", bucket, prefix);
    let manager = StorageManager::new(uri).expect("s3 manager should build");

    let index_uuid = Uuid::new_v4().to_string();
    let loaded = manager
        .load_current_manifest(&index_uuid, None)
        .await
        .expect("load should succeed with no manifest present");
    assert!(loaded.is_none());
}

#[tokio::test]
async fn s3_backend_append_rejects_schema_mismatch() {
    let Some((_, bucket)) = s3_env() else {
        eprintln!("skip: {} not set", ENDPOINT_ENV);
        return;
    };
    let prefix = random_prefix();
    let uri = format!("s3://{}/{}", bucket, prefix);
    let manager = StorageManager::new(uri).expect("s3 manager should build");

    let index_uuid = Uuid::new_v4().to_string();

    let split = RemoteSplitManifest {
        split_id: "split-a".into(),
        state: RemoteSplitState::Published,
        bundle_path: "splits/split-a/bundle".into(),
        bundle_etag: None,
        hotcache_path: None,
        checksum: "sha256:a".into(),
        doc_count: 1,
        size_bytes: 16,
        uncompressed_bytes: 32,
        hotcache_bytes: None,
        time_range: None,
        tags: BTreeMap::new(),
    };
    manager
        .append_split_and_publish(&index_uuid, "events", "sha256:v1", None, split.clone())
        .await
        .expect("first append");

    let err = manager
        .append_split_and_publish(&index_uuid, "events", "sha256:v2", None, split)
        .await
        .expect_err("schema mismatch must reject");
    assert!(
        err.to_string().contains("schema_hash mismatch"),
        "unexpected error: {err}"
    );
}
