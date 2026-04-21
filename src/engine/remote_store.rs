//! Read-path for the `remote_store` engine.
//!
//! Loads the current published manifest for an index, opens each published
//! split as a local `HotEngine`, runs the DSL search against every split, and
//! merges the per-split results into a `DistributedDslSearchResult` that the
//! existing coordinator post-processing can consume.
//!
//! Limitations (intentional for this slice):
//! - Runs on the coordinator only; no leaf gRPC fan-out.
//! - No split-bundle extraction (tarball support); `bundle_path` must refer
//!   to an on-disk directory laid out like a `HotEngine` data-dir (i.e.,
//!   contains an `index/` subdirectory with `meta.json`).
//! - No hotcache, no LRU split cache, no rendezvous hashing.
//! - k-NN vector search is not supported yet (remote_store is for
//!   inverted-index splits).
//! - `schema_hash` from the manifest is not validated against the live
//!   index mappings today. It is safe to skip: `remote_store` indices reject
//!   writes (including dynamic-mapping `AddMappings`), so mappings cannot
//!   change after creation. When the split publish path lands, this module
//!   must start passing a computed schema hash to
//!   `StorageManager::load_current_manifest` so stale manifests are rejected.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::StatusCode;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::api::AppState;
use crate::api::index::DistributedDslSearchResult;
use crate::cluster::state::IndexMetadata;
use crate::search::{PartialAggResult, SearchRequest};

/// Resolve a manifest-supplied `bundle_path` against `storage_root` while
/// rejecting anything that would escape the root. Manifests may be written
/// by external components (and, with cloud object stores, by other tenants),
/// so `bundle_path` is treated as untrusted input.
///
/// Rejects:
/// - empty paths
/// - absolute paths (`/foo`, `C:\foo`, or leading path separators)
/// - any `..` or root components via `Path::components`
///
/// Returns the resolved absolute `PathBuf` when the path is safely contained.
fn resolve_split_dir(storage_root: &Path, bundle_path: &str) -> Result<PathBuf, String> {
    if bundle_path.is_empty() {
        return Err("bundle_path is empty".into());
    }

    let rel = Path::new(bundle_path);
    if rel.is_absolute() {
        return Err(format!("bundle_path must be relative: {}", bundle_path));
    }

    for component in rel.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "bundle_path contains disallowed component: {}",
                    bundle_path
                ));
            }
        }
    }

    Ok(storage_root.join(rel))
}

/// Entry point invoked from `execute_distributed_dsl_search` when an index's
/// engine is `IndexEngine::RemoteStore`.
pub(crate) async fn search(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    search_req: &SearchRequest,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    if search_req.knn.is_some() {
        return Err(crate::api::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "illegal_argument_exception",
            "k-NN search is not supported on remote_store indices yet",
        ));
    }

    // Load the current manifest. A missing pointer is not an error — it just
    // means the index has no published splits yet, so the query returns zero
    // hits and zero shards.
    //
    // Pass the computed schema_hash so `load_manifest` fails closed if the
    // published manifest was built against a different mapping shape. Safe
    // today because `remote_store` rejects writes (including `AddMappings`),
    // so mappings are immutable post-create and the hashes should always
    // line up — but we want the guard in place before dynamic mapping or
    // non-backward-compatible schema changes ever land.
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let manifest = match state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), Some(&expected_schema_hash))
        .await
    {
        Ok(Some(m)) => m,
        Ok(None) => {
            return Ok(DistributedDslSearchResult {
                all_hits: Vec::new(),
                total_hits: 0,
                successful_shards: 0,
                failed_shards: 0,
                aggregations: HashMap::new(),
                partial_aggs: Vec::new(),
            });
        }
        Err(e) => {
            tracing::error!(
                "remote_store: failed to load current manifest for {} ({}): {}",
                index_name,
                metadata.uuid.as_str(),
                e
            );
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load manifest: {}", e),
            ));
        }
    };

    let storage_root = state.storage_manager.root().to_path_buf();
    // Wrap mappings in an Arc so per-split spawn_search tasks share the
    // allocation instead of cloning the full HashMap for every split.
    let mappings = Arc::new(metadata.mappings.clone());

    let mut all_hits: Vec<Value> = Vec::new();
    let mut total_hits: usize = 0;
    let mut successful: u32 = 0;
    let mut failed: u32 = 0;
    let mut all_partial_aggs: Vec<HashMap<String, PartialAggResult>> = Vec::new();

    // `published_splits()` already filters to `RemoteSplitState::Published`, so
    // staged and marked-for-deletion splits are never opened.
    let splits: Vec<_> = manifest.published_splits().cloned().collect();

    for split in splits {
        let split_dir = match resolve_split_dir(&storage_root, &split.bundle_path) {
            Ok(p) => p,
            Err(reason) => {
                tracing::error!(
                    "remote_store: rejecting split {} for {}: {}",
                    split.split_id,
                    index_name,
                    reason
                );
                failed += 1;
                continue;
            }
        };

        if !split_dir.join("index").join("meta.json").exists() {
            tracing::warn!(
                "remote_store: split {} bundle_path {} missing index/meta.json at {:?}",
                split.split_id,
                split.bundle_path,
                split_dir
            );
            failed += 1;
            continue;
        }

        let split_id = split.split_id.clone();
        let split_dir_cloned = split_dir.clone();
        let mappings_arc = Arc::clone(&mappings);
        let search_req_cloned = search_req.clone();

        // Opening a Tantivy index + running a query is blocking work — run it
        // on the search worker pool so we don't stall the Tokio reactor.
        let result = state
            .worker_pools
            .spawn_search(move || -> anyhow::Result<(Vec<Value>, usize, HashMap<String, PartialAggResult>)> {
                let column_cache = Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                let engine = crate::engine::tantivy::HotEngine::new_with_mappings(
                    &split_dir_cloned,
                    Duration::from_secs(60),
                    mappings_arc.as_ref(),
                    crate::wal::TranslogDurability::Request,
                    column_cache,
                )?;
                use crate::engine::SearchEngine;
                engine.search_query(&search_req_cloned)
            })
            .await;

        match result {
            Ok(Ok((hits, shard_total, partial_aggs))) => {
                successful += 1;
                total_hits += shard_total;
                if !partial_aggs.is_empty() {
                    all_partial_aggs.push(partial_aggs);
                }
                for hit in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name,
                        "_split": split_id,
                        "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                        "_score": hit.get("_score"),
                        "_source": hit.get("_source").unwrap_or(&hit),
                    }));
                }
            }
            Ok(Err(e)) | Err(e) => {
                tracing::error!(
                    "remote_store: search failed on split {} at {:?}: {}",
                    split_id,
                    split_dir,
                    e
                );
                failed += 1;
            }
        }
    }

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits,
        successful_shards: successful,
        failed_shards: failed,
        aggregations: HashMap::new(),
        partial_aggs: all_partial_aggs,
    })
}

/// Publish a batch of JSON documents as a new split on this index.
///
/// Flow (matches Quickwit's build-then-publish pattern):
/// 1. Generate a fresh `split_id` (UUID v4).
/// 2. Build a local Tantivy index under `<storage_root>/<uuid>/.staging/<split_id>/`
///    using `HotEngine`, index every doc, flush.
/// 3. Rename the staging dir to `<storage_root>/<uuid>/splits/<split_id>/` so the
///    split directory only ever appears atomically.
/// 4. Append a new `Published` `RemoteSplitManifest` to the current manifest
///    via `StorageManager::append_split_and_publish` (serialized by the
///    per-storage publish lock).
///
/// Failure handling: on any failure before rename, the staging dir is
/// removed. After rename, the split dir is left in place — a future janitor
/// PR will reap splits not referenced by any live manifest generation.
pub(crate) async fn publish_docs(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    docs: Vec<serde_json::Value>,
) -> Result<serde_json::Value, (StatusCode, Json<Value>)> {
    if docs.is_empty() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "publish body must contain at least one document",
        ));
    }

    // Hard cap per publish to avoid building a giant split in one RPC. A
    // client that wants more should send multiple publishes; each publish
    // produces one split which matches the Quickwit granularity model.
    const MAX_DOCS_PER_PUBLISH: usize = 100_000;
    if docs.len() > MAX_DOCS_PER_PUBLISH {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "too many documents in one publish: got {}, max is {}",
                docs.len(),
                MAX_DOCS_PER_PUBLISH
            ),
        ));
    }

    let storage_root = state.storage_manager.root().to_path_buf();
    let index_uuid = metadata.uuid.as_str().to_string();
    let split_id = uuid::Uuid::new_v4().to_string();
    let split_rel = format!("{}/splits/{}", index_uuid, split_id);
    let staging_rel = format!("{}/.staging/{}", index_uuid, split_id);
    let staging_dir = storage_root.join(&staging_rel);
    let split_dir = storage_root.join(&split_rel);

    // Extract or generate _id per doc, then strip _id from payload so stored
    // source stays clean (mirrors single-doc index path).
    let mut prepared: Vec<(String, serde_json::Value)> = Vec::with_capacity(docs.len());
    for mut doc in docs {
        let doc_id = if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
            id.to_string()
        } else {
            uuid::Uuid::new_v4().to_string()
        };
        if let Some(obj) = doc.as_object_mut() {
            obj.remove("_id");
        }
        prepared.push((doc_id, doc));
    }

    let mappings_arc = Arc::new(metadata.mappings.clone());
    let staging_for_build = staging_dir.clone();
    let mappings_for_build = Arc::clone(&mappings_arc);
    let doc_count = prepared.len() as u64;

    // Build the split on the write worker pool — it's blocking I/O.
    let build_result = state
        .worker_pools
        .spawn_write(move || -> anyhow::Result<()> {
            std::fs::create_dir_all(&staging_for_build)?;
            let column_cache = Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
            let engine = crate::engine::tantivy::HotEngine::new_with_mappings(
                &staging_for_build,
                Duration::from_secs(60),
                mappings_for_build.as_ref(),
                crate::wal::TranslogDurability::Request,
                column_cache,
            )?;
            use crate::engine::SearchEngine;
            for (doc_id, payload) in prepared {
                engine.add_document(&doc_id, payload)?;
            }
            engine.flush()?;
            Ok(())
        })
        .await;

    match build_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) | Err(e) => {
            let _ = std::fs::remove_dir_all(&staging_dir);
            tracing::error!(
                "remote_store: split build failed for {} split_id {}: {}",
                index_name,
                split_id,
                e
            );
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_build_exception",
                format!("split build failed: {}", e),
            ));
        }
    }

    // Atomic rename so readers never observe a half-built split directory.
    // Ensure the parent (`splits/`) exists first — it may not on the first
    // publish for this index.
    if let Some(parent) = split_dir.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        let _ = std::fs::remove_dir_all(&staging_dir);
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_build_exception",
            format!("failed to create splits/ parent: {}", e),
        ));
    }
    if let Err(e) = std::fs::rename(&staging_dir, &split_dir) {
        let _ = std::fs::remove_dir_all(&staging_dir);
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_build_exception",
            format!("failed to promote staging split into splits/: {}", e),
        ));
    }

    let size_bytes = directory_size_bytes(&split_dir).unwrap_or(0);

    let schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);

    // Compute a real content-addressable checksum over the bundle. This gives
    // the S3/MinIO follow-up (issue #123) something to verify against when it
    // downloads a split into the local cache. It's intentionally computed
    // AFTER the atomic rename so we hash the final on-disk bytes, and
    // intentionally on the write worker pool since it's blocking I/O over
    // potentially hundreds of MB.
    let split_dir_for_hash = split_dir.clone();
    let checksum = match state
        .worker_pools
        .spawn_write(move || compute_bundle_checksum(&split_dir_for_hash))
        .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) | Err(e) => {
            // The split bytes are already in their final location. Leave them
            // there (next publish attempt can reuse the dir if desired) and
            // return an error so the caller knows not to trust this split.
            tracing::error!(
                "remote_store: bundle checksum failed for {} split_id {}: {}",
                index_name,
                split_id,
                e
            );
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_build_exception",
                format!("bundle checksum failed: {}", e),
            ));
        }
    };

    let new_split = crate::storage::RemoteSplitManifest {
        split_id: split_id.clone(),
        state: crate::storage::RemoteSplitState::Published,
        bundle_path: split_rel.clone(),
        bundle_etag: None,
        hotcache_path: None,
        checksum,
        doc_count,
        size_bytes,
        uncompressed_bytes: size_bytes,
        hotcache_bytes: None,
        time_range: None,
        tags: std::collections::BTreeMap::new(),
    };

    let settings_clone = metadata.settings.remote_store.clone();
    let manifest = match state
        .storage_manager
        .append_split_and_publish(
            &index_uuid,
            index_name,
            &schema_hash,
            settings_clone.as_ref(),
            new_split,
        )
        .await
    {
        Ok(m) => m,
        Err(e) => {
            // Leave the split dir in place — a janitor will reap it after
            // a future PR lands. Returning an error on manifest failure
            // matches Quickwit's indexer behavior.
            tracing::error!(
                "remote_store: manifest publish failed for {} split_id {}: {}",
                index_name,
                split_id,
                e
            );
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!("manifest publish failed: {}", e),
            ));
        }
    };

    Ok(serde_json::json!({
        "index": index_name,
        "split_id": split_id,
        "generation": manifest.generation,
        "doc_count": doc_count,
        "size_bytes": size_bytes,
    }))
}

/// Walk every `Published` split in the current manifest, recompute its
/// bundle checksum, and compare against the value stored in the manifest.
///
/// Returns an error response when the request itself is invalid (index
/// missing, wrong engine, manifest load failure). Per-split problems are
/// returned as individual entries in the `splits` JSON array so a single
/// corrupt split does not make the whole request fail.
///
/// This runs on the node that receives the request; the storage root is
/// local-filesystem today. A future PR (issue #123) that adds an S3 backend
/// will need to hydrate splits into a local cache first, at which point
/// verify can reuse the same hashing function over the cached bytes.
pub(crate) async fn verify_splits(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !matches!(
        metadata.settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    ) {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] uses engine [{}] which does not support remote_store verify",
                index_name, metadata.settings.engine
            ),
        ));
    }

    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let manifest = match state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), Some(&expected_schema_hash))
        .await
    {
        Ok(Some(m)) => m,
        Ok(None) => {
            return Ok(serde_json::json!({
                "index": index_name,
                "generation": 0,
                "splits": [],
                "ok_count": 0,
                "mismatch_count": 0,
                "missing_count": 0,
                "unsupported_count": 0,
            }));
        }
        Err(e) => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load manifest: {}", e),
            ));
        }
    };

    let storage_root = state.storage_manager.root().to_path_buf();
    let splits: Vec<_> = manifest.published_splits().cloned().collect();

    let mut ok_count = 0u32;
    let mut mismatch_count = 0u32;
    let mut missing_count = 0u32;
    let mut unsupported_count = 0u32;
    let mut split_reports: Vec<Value> = Vec::with_capacity(splits.len());

    for split in splits {
        let split_id = split.split_id.clone();
        let expected = split.checksum.clone();

        // Legacy placeholder from an earlier build of this feature. Surface
        // it so operators know which splits cannot be verified until they
        // are republished, without failing the whole request.
        if expected.starts_with("sha256:pending:") {
            unsupported_count += 1;
            split_reports.push(serde_json::json!({
                "split_id": split_id,
                "status": "unsupported",
                "reason": "legacy placeholder checksum; republish to get a real hash",
                "expected": expected,
            }));
            continue;
        }

        let split_dir = match resolve_split_dir(&storage_root, &split.bundle_path) {
            Ok(p) => p,
            Err(reason) => {
                missing_count += 1;
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "missing",
                    "reason": reason,
                    "expected": expected,
                }));
                continue;
            }
        };

        if !split_dir.exists() {
            missing_count += 1;
            split_reports.push(serde_json::json!({
                "split_id": split_id,
                "status": "missing",
                "reason": "bundle directory does not exist on this node",
                "bundle_path": split.bundle_path,
                "expected": expected,
            }));
            continue;
        }

        // Hashing is CPU + IO heavy — run it on the search worker pool so
        // we don't stall the Tokio reactor and don't compete with ingest.
        let dir_for_hash = split_dir.clone();
        let hash_result = state
            .worker_pools
            .spawn_search(move || compute_bundle_checksum(&dir_for_hash))
            .await;

        match hash_result {
            Ok(Ok(actual)) if actual == expected => {
                ok_count += 1;
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "ok",
                }));
            }
            Ok(Ok(actual)) => {
                mismatch_count += 1;
                tracing::error!(
                    "remote_store: checksum mismatch for index {} split {} — expected {} got {}",
                    index_name,
                    split_id,
                    expected,
                    actual
                );
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "mismatch",
                    "expected": expected,
                    "actual": actual,
                }));
            }
            Ok(Err(e)) | Err(e) => {
                missing_count += 1;
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "missing",
                    "reason": format!("checksum computation failed: {}", e),
                    "expected": expected,
                }));
            }
        }
    }

    Ok(serde_json::json!({
        "index": index_name,
        "generation": manifest.generation,
        "splits": split_reports,
        "ok_count": ok_count,
        "mismatch_count": mismatch_count,
        "missing_count": missing_count,
        "unsupported_count": unsupported_count,
    }))
}

/// Recursively sum the size of every regular file under `dir`. Returns 0 on
/// error because the size is informational (used only for manifest stats).
fn directory_size_bytes(dir: &std::path::Path) -> std::io::Result<u64> {
    let mut total: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries = match std::fs::read_dir(&next) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file()
                && let Ok(meta) = entry.metadata()
            {
                total = total.saturating_add(meta.len());
            }
        }
    }
    Ok(total)
}

/// Compute a deterministic SHA-256 checksum over every regular file under
/// `dir`. Files are hashed in POSIX-path sorted order so the result is stable
/// regardless of directory iteration order. Each file contributes:
///
///   u64_le(rel_path_bytes.len()) || rel_path_bytes
///   || u64_le(content.len())     || content_bytes
///
/// Returns a `"sha256:<64-hex>"` string. Any I/O failure (missing dir,
/// unreadable file) surfaces as an error so the caller can reject the publish
/// instead of storing a bogus hash.
fn compute_bundle_checksum(dir: &std::path::Path) -> anyhow::Result<String> {
    use anyhow::Context;

    // 1. Collect every regular file under `dir` with its relative POSIX path.
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries =
            std::fs::read_dir(&next).with_context(|| format!("failed to read_dir {:?}", next))?;
        for entry in entries {
            let entry = entry.with_context(|| format!("dir entry in {:?}", next))?;
            let ft = entry
                .file_type()
                .with_context(|| format!("file_type of {:?}", entry.path()))?;
            let path = entry.path();
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let rel = path
                    .strip_prefix(dir)
                    .with_context(|| format!("path {:?} not under bundle root {:?}", path, dir))?;
                // Canonicalize to POSIX separators so the hash is identical
                // on every platform (relevant for tests and future S3
                // replicas built on different hosts).
                let rel_posix = rel
                    .components()
                    .filter_map(|c| match c {
                        Component::Normal(s) => s.to_str(),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                files.push((rel_posix, path));
            }
            // Skip symlinks and other non-regular entries — we never create
            // them when building a split, so their presence indicates
            // tampering and we must not silently include them.
        }
    }

    // 2. Sort by relative path so the hash is order-stable.
    files.sort_by(|a, b| a.0.cmp(&b.0));

    // 3. Hash each file's path + content with explicit length framing so
    //    different paths / different splits cannot collide by chunking the
    //    same bytes differently.
    let mut hasher = Sha256::new();
    for (rel_posix, abs_path) in &files {
        let rel_bytes = rel_posix.as_bytes();
        hasher.update((rel_bytes.len() as u64).to_le_bytes());
        hasher.update(rel_bytes);

        let content = std::fs::read(abs_path)
            .with_context(|| format!("failed to read bundle file {:?}", abs_path))?;
        hasher.update((content.len() as u64).to_le_bytes());
        hasher.update(&content);
    }

    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_split_dir_accepts_simple_relative_path() {
        let root = Path::new("/var/data/_remote_store");
        let resolved = resolve_split_dir(root, "uuid-1/splits/split-a").unwrap();
        assert_eq!(resolved, root.join("uuid-1/splits/split-a"));
    }

    #[test]
    fn resolve_split_dir_accepts_current_dir_components() {
        let root = Path::new("/var/data/_remote_store");
        // `./splits/split-a` should be permitted (Component::CurDir is a no-op).
        let resolved = resolve_split_dir(root, "./splits/split-a").unwrap();
        assert_eq!(resolved, root.join("./splits/split-a"));
    }

    #[test]
    fn resolve_split_dir_rejects_empty() {
        let root = Path::new("/var/data/_remote_store");
        let err = resolve_split_dir(root, "").unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn resolve_split_dir_rejects_absolute_unix() {
        let root = Path::new("/var/data/_remote_store");
        let err = resolve_split_dir(root, "/etc/passwd").unwrap_err();
        assert!(err.contains("must be relative"));
    }

    #[test]
    fn resolve_split_dir_rejects_parent_dir_traversal() {
        let root = Path::new("/var/data/_remote_store");
        let err = resolve_split_dir(root, "../../etc/passwd").unwrap_err();
        assert!(err.contains("disallowed component"));
    }

    #[test]
    fn resolve_split_dir_rejects_embedded_parent_dir() {
        let root = Path::new("/var/data/_remote_store");
        let err = resolve_split_dir(root, "uuid-1/../../etc/passwd").unwrap_err();
        assert!(err.contains("disallowed component"));
    }

    #[test]
    fn resolve_split_dir_rejects_trailing_parent_dir() {
        let root = Path::new("/var/data/_remote_store");
        let err = resolve_split_dir(root, "uuid-1/splits/..").unwrap_err();
        assert!(err.contains("disallowed component"));
    }

    #[cfg(windows)]
    #[test]
    fn resolve_split_dir_rejects_windows_drive_prefix() {
        let root = Path::new(r"C:\var\data\_remote_store");
        let err = resolve_split_dir(root, r"C:\Windows\System32").unwrap_err();
        assert!(err.contains("must be relative"));
    }

    // ── compute_bundle_checksum ────────────────────────────────────────

    fn write_file(root: &std::path::Path, rel: &str, content: &[u8]) {
        let abs = root.join(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(abs, content).unwrap();
    }

    #[test]
    fn compute_bundle_checksum_has_sha256_prefix_and_64_hex_digest() {
        let dir = tempfile::tempdir().unwrap();
        write_file(dir.path(), "a.txt", b"hello");
        let checksum = compute_bundle_checksum(dir.path()).unwrap();
        assert!(
            checksum.starts_with("sha256:"),
            "checksum must start with sha256:, got {checksum}"
        );
        let hex = &checksum["sha256:".len()..];
        assert_eq!(hex.len(), 64, "sha256 hex digest must be 64 chars");
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn compute_bundle_checksum_is_deterministic_across_iteration_orders() {
        // Two bundles with identical file content should hash the same even
        // though std::fs::read_dir does not guarantee any particular order.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        // Create the files in opposite orders in the two bundles.
        write_file(a.path(), "index/meta.json", b"{\"v\":1}");
        write_file(a.path(), "index/000.store", b"\x00\x01\x02");
        write_file(a.path(), "aaa.bin", b"top-level");

        write_file(b.path(), "aaa.bin", b"top-level");
        write_file(b.path(), "index/000.store", b"\x00\x01\x02");
        write_file(b.path(), "index/meta.json", b"{\"v\":1}");

        let ha = compute_bundle_checksum(a.path()).unwrap();
        let hb = compute_bundle_checksum(b.path()).unwrap();
        assert_eq!(ha, hb, "bundles with identical content must hash equal");
    }

    #[test]
    fn compute_bundle_checksum_changes_on_single_byte_flip() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        write_file(a.path(), "data.bin", b"hello world");
        write_file(b.path(), "data.bin", b"hello worlD");
        let ha = compute_bundle_checksum(a.path()).unwrap();
        let hb = compute_bundle_checksum(b.path()).unwrap();
        assert_ne!(ha, hb);
    }

    #[test]
    fn compute_bundle_checksum_changes_on_rename_only() {
        // Length framing means renaming a file without changing content must
        // still change the hash — a hash scheme without path framing (just
        // concatenated bytes) would wrongly produce the same digest here.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        write_file(a.path(), "foo.bin", b"same bytes");
        write_file(b.path(), "bar.bin", b"same bytes");
        let ha = compute_bundle_checksum(a.path()).unwrap();
        let hb = compute_bundle_checksum(b.path()).unwrap();
        assert_ne!(
            ha, hb,
            "renaming a file must change the bundle checksum: {ha} vs {hb}"
        );
    }

    #[test]
    fn compute_bundle_checksum_different_chunking_is_not_collision() {
        // Length framing also prevents the "boundary shift" collision:
        // ("ab","c") vs ("a","bc") have identical concatenated bytes but
        // different framing, so their bundle hashes must differ.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        write_file(a.path(), "f1", b"ab");
        write_file(a.path(), "f2", b"c");
        write_file(b.path(), "f1", b"a");
        write_file(b.path(), "f2", b"bc");
        let ha = compute_bundle_checksum(a.path()).unwrap();
        let hb = compute_bundle_checksum(b.path()).unwrap();
        assert_ne!(ha, hb);
    }

    #[test]
    fn compute_bundle_checksum_empty_dir_is_stable() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let ha = compute_bundle_checksum(a.path()).unwrap();
        let hb = compute_bundle_checksum(b.path()).unwrap();
        assert_eq!(ha, hb);
        // sha256("") for reference — we hash nothing for an empty bundle, so
        // the digest matches the SHA-256 of the empty string.
        assert_eq!(
            ha,
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn compute_bundle_checksum_errors_on_missing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        assert!(compute_bundle_checksum(&missing).is_err());
    }
}
