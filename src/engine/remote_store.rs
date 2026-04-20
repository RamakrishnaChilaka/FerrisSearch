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
    let manifest = match state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), None)
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
}
