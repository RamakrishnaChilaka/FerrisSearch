//! Read-path for the `remote_store` engine.
//!
//! Loads the current published manifest for an index, fetches each published
//! split bundle from the object store into a node-local cache, unpacks each
//! bundle into a standard `HotEngine` data layout, runs the DSL search
//! against every split, and merges the per-split results into a
//! `DistributedDslSearchResult` that the existing coordinator post-processing
//! can consume.
//!
//! Limitations (intentional for this slice):
//! - Runs on the coordinator only; no leaf gRPC fan-out.
//! - No hotcache, no LRU eviction of the local cache, no rendezvous hashing.
//! - k-NN vector search is not supported yet (remote_store is for
//!   inverted-index splits).
//! - `schema_hash` from the manifest is validated against the live index
//!   mappings on load (`remote_store` rejects writes so mappings cannot
//!   change after creation).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::StatusCode;
use serde_json::Value;

use crate::api::AppState;
use crate::api::index::DistributedDslSearchResult;
use crate::cluster::state::IndexMetadata;
use crate::search::{PartialAggResult, SearchRequest};

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

    // Wrap mappings in an Arc so per-split spawn_search tasks share the
    // allocation instead of cloning the full HashMap for every split.
    let mappings = Arc::new(metadata.mappings.clone());
    let index_uuid = metadata.uuid.as_str().to_string();

    let mut all_hits: Vec<Value> = Vec::new();
    let mut total_hits: usize = 0;
    let mut successful: u32 = 0;
    let mut failed: u32 = 0;
    let mut all_partial_aggs: Vec<HashMap<String, PartialAggResult>> = Vec::new();

    // `published_splits()` already filters to `RemoteSplitState::Published`, so
    // staged and marked-for-deletion splits are never opened.
    let splits: Vec<_> = manifest.published_splits().cloned().collect();

    for split in splits {
        // Fetch the bundle from the object store into the node-local cache.
        // Subsequent searches for the same split short-circuit on the cache's
        // `.done` marker.
        let split_dir = match state
            .storage_manager
            .fetch_split_into_cache(&index_uuid, &split)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    "remote_store: failed to fetch split {} for {}: {}",
                    split.split_id,
                    index_name,
                    e
                );
                failed += 1;
                continue;
            }
        };

        if !split_dir.join("index").join("meta.json").exists() {
            tracing::warn!(
                "remote_store: split {} cache dir {:?} missing index/meta.json after extract",
                split.split_id,
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
/// 2. Build a local Tantivy index in a node-local staging directory under
///    `<data_dir>/_remote_store_cache/staging/<uuid>/<split_id>/` using
///    `HotEngine`, index every doc, flush.
/// 3. Pack the staging tree into a single deterministic bundle byte stream
///    and upload it to the object store at
///    `<index_uuid>/splits/<split_id>/bundle`. The checksum stored in the
///    manifest is the sha256 of the bundle bytes.
/// 4. Append a new `Published` `RemoteSplitManifest` to the current manifest
///    via `StorageManager::append_split_and_publish` (serialized by the
///    per-storage publish lock).
///
/// Failure handling: on any failure before successful upload, the staging
/// dir is removed. After upload, the object-store bundle is left in place —
/// a future janitor PR will reap splits not referenced by any live manifest
/// generation.
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

    let index_uuid = metadata.uuid.as_str().to_string();
    let split_id = uuid::Uuid::new_v4().to_string();
    let staging_dir = state.storage_manager.staging_dir(&index_uuid, &split_id);

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
            if staging_for_build.exists() {
                std::fs::remove_dir_all(&staging_for_build)?;
            }
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

    if let Err(e) = match build_result {
        Ok(inner) => inner,
        Err(join_err) => Err(anyhow::anyhow!(join_err)),
    } {
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

    // Strip out the translog generation files from the staging tree before
    // packing: they're WAL state for crash recovery on a live engine, not
    // queryable data. `HotEngine::flush` empties the translog but the file
    // itself stays on disk; we want the bundle contents to be exactly the
    // Tantivy `index/` subtree plus the vector index payload.
    let staging_for_strip = staging_dir.clone();
    let strip_result = state
        .worker_pools
        .spawn_write(move || -> anyhow::Result<()> {
            let translog = staging_for_strip.join("translog.manifest");
            if translog.exists() {
                std::fs::remove_file(&translog)?;
            }
            for entry in std::fs::read_dir(&staging_for_strip)? {
                let entry = entry?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("translog-") && name_str.ends_with(".bin") {
                    std::fs::remove_file(entry.path())?;
                }
            }
            Ok(())
        })
        .await;
    if let Err(e) = match strip_result {
        Ok(inner) => inner,
        Err(join_err) => Err(anyhow::anyhow!(join_err)),
    } {
        tracing::warn!(
            "remote_store: failed to strip translog from staging for split {} of {}: {} \
             (continuing with upload; translog files will be bundled)",
            split_id,
            index_name,
            e
        );
    }

    // Pack + upload the bundle. `upload_split_bundle` is uniform across local
    // and remote backends: on a local backend the bundle becomes a file under
    // `<data_dir>/_remote_store/<uuid>/splits/<split_id>/bundle`; on s3:// it
    // becomes an object at the same relative key.
    let (bundle_key, checksum, size_bytes) = match state
        .storage_manager
        .upload_split_bundle(&index_uuid, &split_id, &staging_dir)
        .await
    {
        Ok(triple) => triple,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging_dir);
            tracing::error!(
                "remote_store: bundle upload failed for {} split_id {}: {}",
                index_name,
                split_id,
                e
            );
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_build_exception",
                format!("bundle upload failed: {}", e),
            ));
        }
    };

    // Staging bytes are no longer needed after a successful upload.
    let _ = std::fs::remove_dir_all(&staging_dir);

    let schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);

    let new_split = crate::storage::RemoteSplitManifest {
        split_id: split_id.clone(),
        state: crate::storage::RemoteSplitState::Published,
        bundle_path: bundle_key,
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

    let index_uuid = metadata.uuid.as_str();
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

        // Stream-download the bundle and hash it without materializing to disk.
        // This works uniformly against both the local and S3 backends.
        let hash_result = state
            .storage_manager
            .hash_split_bundle(index_uuid, &split)
            .await;

        match hash_result {
            Ok(actual) if actual == expected => {
                ok_count += 1;
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "ok",
                }));
            }
            Ok(actual) => {
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
            Err(e) => {
                missing_count += 1;
                split_reports.push(serde_json::json!({
                    "split_id": split_id,
                    "status": "missing",
                    "reason": format!("bundle fetch or hash failed: {}", e),
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
