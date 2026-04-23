//! Read-path for the `remote_store` engine.
//!
//! Loads the current published manifest for an index, asks candidate leaves
//! for cache and load status, assigns published splits in batches via
//! rendezvous ranking, executes the DSL search on local or remote leaves, and
//! merges the per-split results into a `DistributedDslSearchResult` that the
//! existing coordinator post-processing can consume.
//!
//! Limitations (intentional for this slice):
//! - No hotcache tier yet; split readers are reopened from full split bundles.
//! - No object-store janitor yet; only node-local split cache reaping exists.
//! - k-NN vector search is not supported yet (remote_store is for
//!   inverted-index splits).
//! - `schema_hash` from the manifest is validated against the live index
//!   mappings on load (`remote_store` rejects writes so mappings cannot
//!   change after creation).

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::Json;
use axum::http::StatusCode;
use futures::FutureExt;
use murmur3::murmur3_32;
use serde_json::Value;

use crate::api::AppState;
use crate::api::index::DistributedDslSearchResult;
use crate::cluster::state::IndexMetadata;
use crate::engine::SearchEngine;
use crate::search::{PartialAggResult, SearchRequest};

/// Process-local cache for open remote_store split readers.
pub struct RemoteSplitReaderCache {
    readers: RwLock<HashMap<String, Arc<CachedRemoteSplitReader>>>,
}

impl Default for RemoteSplitReaderCache {
    fn default() -> Self {
        Self {
            readers: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssignedRemoteSplit {
    pub split_id: String,
    pub bundle_path: String,
    pub checksum: String,
    pub size_bytes: u64,
}

impl AssignedRemoteSplit {
    pub(crate) fn from_manifest(split: &crate::storage::RemoteSplitManifest) -> Self {
        Self {
            split_id: split.split_id.clone(),
            bundle_path: split.bundle_path.clone(),
            checksum: split.checksum.clone(),
            size_bytes: split.size_bytes,
        }
    }

    fn as_manifest(&self) -> crate::storage::RemoteSplitManifest {
        crate::storage::RemoteSplitManifest {
            split_id: self.split_id.clone(),
            state: crate::storage::RemoteSplitState::Published,
            bundle_path: self.bundle_path.clone(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: self.checksum.clone(),
            doc_count: 0,
            size_bytes: self.size_bytes,
            uncompressed_bytes: self.size_bytes,
            hotcache_bytes: None,
            time_range: None,
            tags: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SplitWarmth {
    pub artifact_cached: bool,
    pub reader_cached: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct LeafStatusSnapshot {
    pub root_capable: bool,
    pub leaf_capable: bool,
    pub inflight_bytes: u64,
    pub queue_depth: usize,
    pub split_statuses: HashMap<String, SplitWarmth>,
}

#[derive(Debug, Clone)]
pub(crate) struct LeafSplitSearchOutcome {
    pub split_id: String,
    pub hits: Vec<Value>,
    pub total_hits: usize,
    pub partial_aggs: HashMap<String, PartialAggResult>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct LeafExecutionContext {
    pub worker_pools: crate::worker::WorkerPools,
    pub storage_manager: Arc<crate::storage::StorageManager>,
    pub reader_cache: Arc<RemoteSplitReaderCache>,
}

struct CachedRemoteSplitReader {
    index_uuid: String,
    split_id: String,
    size_bytes: u64,
    last_access_epoch_ms: AtomicU64,
    engine: Arc<crate::engine::tantivy::HotEngine>,
    _pin: crate::storage::SplitCachePin,
}

impl CachedRemoteSplitReader {
    fn touch(&self, storage_manager: &crate::storage::StorageManager) {
        let now = now_epoch_ms();
        self.last_access_epoch_ms.store(now, Ordering::Relaxed);
        storage_manager.record_cached_split_access(
            &self.index_uuid,
            &self.split_id,
            self.size_bytes,
        );
    }
}

const REMOTE_STORE_RENDEZVOUS_WINDOW: usize = 2;

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn reader_cache_key(index_uuid: &str, split_id: &str, checksum: &str) -> String {
    format!("{index_uuid}/{split_id}@{checksum}")
}

fn node_is_remote_store_root(node: &crate::cluster::state::NodeInfo) -> bool {
    node.roles.iter().any(|role| {
        matches!(
            role,
            crate::cluster::state::NodeRole::Master | crate::cluster::state::NodeRole::Data
        )
    })
}

fn node_is_remote_store_leaf(node: &crate::cluster::state::NodeInfo) -> bool {
    node.roles
        .iter()
        .any(|role| matches!(role, crate::cluster::state::NodeRole::Data))
}

fn rendezvous_score(index_uuid: &str, generation: u64, split_id: &str, node_id: &str) -> u32 {
    let mut cursor = Cursor::new(format!("{index_uuid}:{generation}:{split_id}:{node_id}"));
    murmur3_32(&mut cursor, 0).unwrap_or(0)
}

fn split_warmth_rank(status: &LeafStatusSnapshot, split_id: &str) -> u8 {
    match status.split_statuses.get(split_id) {
        Some(warmth) if warmth.reader_cached => 2,
        Some(warmth) if warmth.artifact_cached => 1,
        _ => 0,
    }
}

fn dedupe_pending_splits(splits: Vec<AssignedRemoteSplit>) -> Vec<AssignedRemoteSplit> {
    let mut deduped = HashMap::new();
    for split in splits {
        deduped.insert(split.split_id.clone(), split);
    }
    deduped.into_values().collect()
}

impl RemoteSplitReaderCache {
    pub(crate) fn has_reader(&self, index_uuid: &str, split_id: &str, checksum: &str) -> bool {
        let key = reader_cache_key(index_uuid, split_id, checksum);
        self.readers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&key)
    }

    pub(crate) fn evict_for_index(
        &self,
        index_uuid: &str,
        live_split_ids: &HashSet<String>,
        budget_bytes: Option<u64>,
    ) {
        let mut readers = self.readers.write().unwrap_or_else(|e| e.into_inner());
        let mut entries: Vec<_> = readers
            .iter()
            .filter(|(_, reader)| reader.index_uuid == index_uuid)
            .map(|(key, reader)| (key.clone(), Arc::clone(reader)))
            .collect();

        let mut total_bytes: u64 = entries.iter().map(|(_, reader)| reader.size_bytes).sum();
        entries.sort_by_key(|(_, reader)| {
            (
                live_split_ids.contains(&reader.split_id),
                reader.last_access_epoch_ms.load(Ordering::Relaxed),
            )
        });

        for (key, reader) in entries {
            let is_stale = !live_split_ids.contains(&reader.split_id);
            let over_budget = budget_bytes.is_some_and(|budget| total_bytes > budget);
            if !is_stale && !over_budget {
                continue;
            }

            if readers.remove(&key).is_some() {
                total_bytes = total_bytes.saturating_sub(reader.size_bytes);
            }
        }
    }

    async fn get_or_open(
        &self,
        context: &LeafExecutionContext,
        metadata: &IndexMetadata,
        split: &AssignedRemoteSplit,
    ) -> anyhow::Result<Arc<CachedRemoteSplitReader>> {
        let key = reader_cache_key(metadata.uuid.as_str(), &split.split_id, &split.checksum);
        if let Some(reader) = self
            .readers
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .cloned()
        {
            reader.touch(context.storage_manager.as_ref());
            return Ok(reader);
        }

        let split_dir = context
            .storage_manager
            .fetch_split_into_cache(metadata.uuid.as_str(), &split.as_manifest())
            .await?;
        let index_uuid = metadata.uuid.to_string();
        let split_id = split.split_id.clone();
        let size_bytes = context
            .storage_manager
            .cached_split_size_bytes(metadata.uuid.as_str(), &split_id);
        let mappings = metadata.mappings.clone();
        let pin = context
            .storage_manager
            .pin_cached_split(metadata.uuid.as_str(), &split_id);

        let engine = context
            .worker_pools
            .spawn_search(
                move || -> anyhow::Result<Arc<crate::engine::tantivy::HotEngine>> {
                    let column_cache =
                        Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let engine = crate::engine::tantivy::HotEngine::new_with_mappings(
                        &split_dir,
                        Duration::from_secs(60),
                        &mappings,
                        crate::wal::TranslogDurability::Request,
                        column_cache,
                    )?;
                    Ok(Arc::new(engine))
                },
            )
            .await??;

        let entry = Arc::new(CachedRemoteSplitReader {
            index_uuid: index_uuid.clone(),
            split_id: split_id.clone(),
            size_bytes,
            last_access_epoch_ms: AtomicU64::new(now_epoch_ms()),
            engine,
            _pin: pin,
        });
        entry.touch(context.storage_manager.as_ref());

        let mut readers = self.readers.write().unwrap_or_else(|e| e.into_inner());
        let reader = readers
            .entry(key)
            .or_insert_with(|| Arc::clone(&entry))
            .clone();
        reader.touch(context.storage_manager.as_ref());
        Ok(reader)
    }
}

pub(crate) fn local_leaf_status_snapshot(
    cluster_state: &crate::cluster::state::ClusterState,
    local_node_id: &str,
    storage_manager: &crate::storage::StorageManager,
    reader_cache: &RemoteSplitReaderCache,
    index_uuid: &str,
    splits: &[AssignedRemoteSplit],
) -> LeafStatusSnapshot {
    let node_info = cluster_state.nodes.get(local_node_id);
    let load = storage_manager.remote_store_load_snapshot();
    let split_statuses = splits
        .iter()
        .map(|split| {
            let artifact_cached = storage_manager
                .cached_split_status(index_uuid, &split.split_id, &split.checksum)
                .artifact_cached;
            let reader_cached =
                reader_cache.has_reader(index_uuid, &split.split_id, &split.checksum);
            (
                split.split_id.clone(),
                SplitWarmth {
                    artifact_cached,
                    reader_cached,
                },
            )
        })
        .collect();

    LeafStatusSnapshot {
        root_capable: node_info.map(node_is_remote_store_root).unwrap_or(true),
        leaf_capable: node_info.map(node_is_remote_store_leaf).unwrap_or(true),
        inflight_bytes: load.inflight_bytes,
        queue_depth: load.queue_depth,
        split_statuses,
    }
}

pub(crate) async fn execute_leaf_search_batch(
    context: &LeafExecutionContext,
    metadata: &IndexMetadata,
    search_req: &SearchRequest,
    assigned_splits: &[AssignedRemoteSplit],
    live_split_ids: &HashSet<String>,
) -> anyhow::Result<Vec<LeafSplitSearchOutcome>> {
    let budget_bytes = metadata
        .settings
        .remote_store
        .as_ref()
        .and_then(|settings| settings.split_cache_bytes);
    context
        .reader_cache
        .evict_for_index(metadata.uuid.as_str(), live_split_ids, budget_bytes);
    context.storage_manager.reap_split_cache(
        metadata.uuid.as_str(),
        live_split_ids,
        budget_bytes,
    )?;

    let _load_guard = context
        .storage_manager
        .begin_remote_store_batch(assigned_splits.iter().map(|split| split.size_bytes).sum());

    let mut outcomes = Vec::with_capacity(assigned_splits.len());
    for split in assigned_splits {
        let outcome = match context
            .reader_cache
            .get_or_open(context, metadata, split)
            .await
        {
            Ok(reader) => {
                let engine = Arc::clone(&reader.engine);
                let search_req = search_req.clone();
                match context
                    .worker_pools
                    .spawn_search(move || engine.search_query(&search_req))
                    .await
                {
                    Ok(Ok((hits, total_hits, partial_aggs))) => LeafSplitSearchOutcome {
                        split_id: split.split_id.clone(),
                        hits,
                        total_hits,
                        partial_aggs,
                        error: None,
                    },
                    Ok(Err(error)) | Err(error) => LeafSplitSearchOutcome {
                        split_id: split.split_id.clone(),
                        hits: Vec::new(),
                        total_hits: 0,
                        partial_aggs: HashMap::new(),
                        error: Some(error.to_string()),
                    },
                }
            }
            Err(error) => LeafSplitSearchOutcome {
                split_id: split.split_id.clone(),
                hits: Vec::new(),
                total_hits: 0,
                partial_aggs: HashMap::new(),
                error: Some(error.to_string()),
            },
        };
        outcomes.push(outcome);
    }

    context
        .reader_cache
        .evict_for_index(metadata.uuid.as_str(), live_split_ids, budget_bytes);
    context.storage_manager.reap_split_cache(
        metadata.uuid.as_str(),
        live_split_ids,
        budget_bytes,
    )?;

    Ok(outcomes)
}

async fn collect_leaf_statuses(
    state: &AppState,
    index_name: &str,
    index_uuid: &str,
    splits: &[AssignedRemoteSplit],
    leaf_nodes: &[crate::cluster::state::NodeInfo],
) -> HashMap<String, LeafStatusSnapshot> {
    let cluster_state = state.cluster_manager.get_state();
    let local_leaf_context = local_leaf_status_snapshot(
        &cluster_state,
        &state.local_node_id,
        state.storage_manager.as_ref(),
        state.remote_store_reader_cache.as_ref(),
        index_uuid,
        splits,
    );

    let mut statuses = HashMap::new();
    statuses.insert(state.local_node_id.clone(), local_leaf_context);

    let mut futures = Vec::new();
    for node in leaf_nodes {
        if node.id == state.local_node_id {
            continue;
        }
        let client = state.transport_client.clone();
        let node = node.clone();
        let index_name = index_name.to_string();
        let index_uuid = index_uuid.to_string();
        let splits = splits.to_vec();
        futures.push(tokio::spawn(async move {
            let status = client
                .get_remote_store_leaf_status(&node, &index_name, &index_uuid, &splits)
                .await;
            (node.id.clone(), status)
        }));
    }

    for result in futures::future::join_all(futures).await {
        if let Ok((node_id, Ok(status))) = result {
            statuses.insert(node_id, status);
        }
    }

    statuses
}

fn assign_remote_store_splits(
    index_uuid: &str,
    generation: u64,
    pending_splits: &[AssignedRemoteSplit],
    leaf_nodes: &[crate::cluster::state::NodeInfo],
    statuses: &HashMap<String, LeafStatusSnapshot>,
    unhealthy_nodes: &HashSet<String>,
) -> HashMap<String, Vec<AssignedRemoteSplit>> {
    let mut assignments: HashMap<String, Vec<AssignedRemoteSplit>> = HashMap::new();

    for split in pending_splits {
        let mut candidates: Vec<_> = leaf_nodes
            .iter()
            .filter(|node| !unhealthy_nodes.contains(&node.id))
            .filter(|node| {
                statuses
                    .get(&node.id)
                    .is_some_and(|status| status.leaf_capable)
            })
            .map(|node| {
                (
                    node.id.clone(),
                    rendezvous_score(index_uuid, generation, &split.split_id, &node.id),
                )
            })
            .collect();
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        let window_len = candidates.len().clamp(1, REMOTE_STORE_RENDEZVOUS_WINDOW);

        let mut best: Option<(String, u8, u64, usize, u32)> = None;
        for (node_id, score) in candidates.iter().take(window_len) {
            let Some(status) = statuses.get(node_id) else {
                continue;
            };
            let candidate = (
                node_id.clone(),
                split_warmth_rank(status, &split.split_id),
                status.inflight_bytes,
                status.queue_depth,
                *score,
            );
            let replace = match &best {
                None => true,
                Some(current) => {
                    candidate.1 > current.1
                        || (candidate.1 == current.1 && candidate.2 < current.2)
                        || (candidate.1 == current.1
                            && candidate.2 == current.2
                            && candidate.3 < current.3)
                        || (candidate.1 == current.1
                            && candidate.2 == current.2
                            && candidate.3 == current.3
                            && candidate.4 > current.4)
                }
            };
            if replace {
                best = Some(candidate);
            }
        }

        if let Some((node_id, ..)) = best {
            assignments.entry(node_id).or_default().push(split.clone());
        }
    }

    assignments
}

#[derive(Default)]
struct RemoteStoreSearchAccumulator {
    split_hit_lists: Vec<Vec<Value>>,
    total_hits: usize,
    successful: u32,
    partial_aggs: Vec<HashMap<String, PartialAggResult>>,
}

struct RemoteStoreRoundOutcome {
    next_pending: Vec<AssignedRemoteSplit>,
    newly_unhealthy_nodes: HashSet<String>,
    made_progress: bool,
}

type RemoteStoreBatchResult = (
    String,
    Vec<AssignedRemoteSplit>,
    std::result::Result<Vec<LeafSplitSearchOutcome>, anyhow::Error>,
);

fn apply_remote_store_batch_results(
    index_name: &str,
    batch_results: Vec<RemoteStoreBatchResult>,
    split_lookup: &HashMap<String, AssignedRemoteSplit>,
    accumulator: &mut RemoteStoreSearchAccumulator,
) -> RemoteStoreRoundOutcome {
    let mut next_pending = Vec::new();
    let mut newly_unhealthy_nodes = HashSet::new();
    let mut made_progress = false;

    for (node_id, assigned_splits, result) in batch_results {
        match result {
            Ok(outcomes) => {
                let mut node_failed = false;
                for outcome in outcomes {
                    let LeafSplitSearchOutcome {
                        split_id,
                        hits,
                        total_hits,
                        partial_aggs,
                        error,
                    } = outcome;

                    if let Some(error) = error {
                        tracing::error!(
                            "remote_store: split {} failed on leaf {} for {}: {}",
                            split_id,
                            node_id,
                            index_name,
                            error
                        );
                        if let Some(split) = split_lookup.get(&split_id) {
                            next_pending.push(split.clone());
                        }
                        node_failed = true;
                        continue;
                    }

                    made_progress = true;
                    accumulator.successful += 1;
                    accumulator.total_hits += total_hits;
                    if !partial_aggs.is_empty() {
                        accumulator.partial_aggs.push(partial_aggs);
                    }

                    let hit_list = hits
                        .into_iter()
                        .map(|hit| {
                            serde_json::json!({
                                "_index": index_name,
                                "_split": split_id,
                                "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                                "_score": hit.get("_score"),
                                "_source": hit.get("_source").unwrap_or(&hit),
                            })
                        })
                        .collect::<Vec<_>>();
                    accumulator.split_hit_lists.push(hit_list);
                }

                if node_failed {
                    newly_unhealthy_nodes.insert(node_id);
                }
            }
            Err(error) => {
                tracing::error!(
                    "remote_store: leaf batch failed on {} for {}: {}",
                    node_id,
                    index_name,
                    error
                );
                newly_unhealthy_nodes.insert(node_id);
                next_pending.extend(assigned_splits);
            }
        }
    }

    RemoteStoreRoundOutcome {
        next_pending,
        newly_unhealthy_nodes,
        made_progress,
    }
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

    let split_plans: Vec<_> = manifest
        .published_splits()
        .map(AssignedRemoteSplit::from_manifest)
        .collect();
    if split_plans.is_empty() {
        return Ok(DistributedDslSearchResult {
            all_hits: Vec::new(),
            total_hits: 0,
            successful_shards: 0,
            failed_shards: 0,
            aggregations: HashMap::new(),
            partial_aggs: Vec::new(),
        });
    }

    let live_split_ids: HashSet<String> = split_plans
        .iter()
        .map(|split| split.split_id.clone())
        .collect();
    let cluster_state = state.cluster_manager.get_state();
    let leaf_nodes: Vec<_> = cluster_state
        .nodes
        .values()
        .filter(|node| node_is_remote_store_leaf(node))
        .cloned()
        .collect();
    if leaf_nodes.is_empty() {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "remote_store_leaf_unavailable_exception",
            format!(
                "no remote_store-capable leaf nodes available for index [{}]",
                index_name
            ),
        ));
    }

    let leaf_context = LeafExecutionContext {
        worker_pools: state.worker_pools.clone(),
        storage_manager: state.storage_manager.clone(),
        reader_cache: state.remote_store_reader_cache.clone(),
    };
    let statuses = collect_leaf_statuses(
        state,
        index_name,
        metadata.uuid.as_str(),
        &split_plans,
        &leaf_nodes,
    )
    .await;

    let split_lookup: HashMap<_, _> = split_plans
        .iter()
        .cloned()
        .map(|split| (split.split_id.clone(), split))
        .collect();
    let mut pending_splits = split_plans.clone();
    let mut unhealthy_nodes = HashSet::new();
    let mut accumulator = RemoteStoreSearchAccumulator::default();
    let mut failed = 0u32;

    while !pending_splits.is_empty() {
        let assignments = assign_remote_store_splits(
            metadata.uuid.as_str(),
            manifest.generation,
            &pending_splits,
            &leaf_nodes,
            &statuses,
            &unhealthy_nodes,
        );
        if assignments.is_empty() {
            failed += pending_splits.len() as u32;
            break;
        }

        let mut work = Vec::new();
        for (node_id, splits) in assignments {
            let node_id_for_result = node_id.clone();
            if node_id == state.local_node_id {
                let context = leaf_context.clone();
                let metadata = metadata.clone();
                let search_req = search_req.clone();
                let live_split_ids = live_split_ids.clone();
                work.push(
                    async move {
                        (
                            node_id_for_result,
                            splits.clone(),
                            execute_leaf_search_batch(
                                &context,
                                &metadata,
                                &search_req,
                                &splits,
                                &live_split_ids,
                            )
                            .await,
                        )
                    }
                    .boxed(),
                );
            } else {
                let Some(node) = leaf_nodes.iter().find(|node| node.id == node_id).cloned() else {
                    continue;
                };
                let client = state.transport_client.clone();
                let index_name = index_name.to_string();
                let index_uuid = metadata.uuid.to_string();
                let search_req = search_req.clone();
                let live_split_ids: Vec<_> = live_split_ids.iter().cloned().collect();
                work.push(
                    async move {
                        (
                            node_id_for_result,
                            splits.clone(),
                            client
                                .forward_remote_store_search(
                                    &node,
                                    &index_name,
                                    &index_uuid,
                                    &search_req,
                                    &splits,
                                    &live_split_ids,
                                )
                                .await,
                        )
                    }
                    .boxed(),
                );
            }
        }

        let round = apply_remote_store_batch_results(
            index_name,
            futures::future::join_all(work).await,
            &split_lookup,
            &mut accumulator,
        );
        unhealthy_nodes.extend(round.newly_unhealthy_nodes);

        if round.next_pending.is_empty() {
            break;
        }
        if !round.made_progress && round.next_pending.len() == pending_splits.len() {
            failed += round.next_pending.len() as u32;
            break;
        }
        pending_splits = dedupe_pending_splits(round.next_pending);
    }

    let all_hits =
        crate::search::merge_sorted_hit_lists(accumulator.split_hit_lists, &search_req.sort);
    let aggregations = if accumulator.partial_aggs.is_empty() {
        HashMap::new()
    } else {
        crate::search::merge_aggregations(accumulator.partial_aggs.clone(), &search_req.aggs)
    };

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits: accumulator.total_hits,
        successful_shards: accumulator.successful,
        failed_shards: failed,
        aggregations,
        partial_aggs: accumulator.partial_aggs,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::state::{
        DynamicMapping, FieldMapping, FieldType, IndexEngine, IndexMetadata, IndexSettings,
        IndexUuid, NodeInfo, NodeRole, RemoteStoreSettings,
    };
    use crate::engine::tantivy::HotEngine;
    use crate::storage::{RemoteSplitManifest, RemoteSplitState, StorageManager};
    use crate::wal::TranslogDurability;
    use std::collections::BTreeMap;
    use std::sync::mpsc;
    use tempfile::TempDir;

    fn test_leaf(node_id: &str) -> NodeInfo {
        NodeInfo {
            id: node_id.to_string(),
            name: node_id.to_string(),
            host: "127.0.0.1".to_string(),
            transport_port: 9300,
            http_port: 9200,
            roles: vec![NodeRole::Data],
            raft_node_id: 1,
        }
    }

    fn test_split(split_id: &str) -> AssignedRemoteSplit {
        AssignedRemoteSplit {
            split_id: split_id.to_string(),
            bundle_path: format!("idx-1/splits/{split_id}/bundle"),
            checksum: format!("sha256:{split_id}"),
            size_bytes: 128,
        }
    }

    fn test_status(entries: &[(&str, SplitWarmth)]) -> LeafStatusSnapshot {
        LeafStatusSnapshot {
            root_capable: true,
            leaf_capable: true,
            inflight_bytes: 0,
            queue_depth: 0,
            split_statuses: entries
                .iter()
                .map(|(split_id, warmth)| ((*split_id).to_string(), warmth.clone()))
                .collect(),
        }
    }

    fn test_remote_store_metadata(
        mappings: HashMap<String, FieldMapping>,
        split_cache_bytes: Option<u64>,
    ) -> IndexMetadata {
        IndexMetadata {
            name: "remotehits".into(),
            uuid: IndexUuid::new("idx-1"),
            number_of_shards: 0,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings,
            dynamic: DynamicMapping::False,
            settings: IndexSettings {
                engine: IndexEngine::RemoteStore,
                refresh_interval_ms: None,
                flush_threshold_bytes: None,
                remote_store: Some(RemoteStoreSettings {
                    split_cache_bytes,
                    ..Default::default()
                }),
            },
        }
    }

    async fn build_cached_remote_split(
        manager: &StorageManager,
    ) -> (IndexMetadata, AssignedRemoteSplit, HashSet<String>) {
        let mappings = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let metadata = test_remote_store_metadata(mappings.clone(), Some(0));

        let stage = manager.staging_dir(metadata.uuid.as_str(), "split-a");
        std::fs::create_dir_all(&stage).unwrap();
        let column_cache = Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
        let engine = HotEngine::new_with_mappings(
            &stage,
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            column_cache,
        )
        .unwrap();
        engine
            .add_document(
                "doc-1",
                serde_json::json!({ "title": "distributed remote hit" }),
            )
            .unwrap();
        engine.refresh().unwrap();

        let (bundle_path, checksum, size_bytes) = manager
            .upload_split_bundle(metadata.uuid.as_str(), "split-a", &stage)
            .await
            .unwrap();
        let manifest = RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path,
            bundle_etag: None,
            hotcache_path: None,
            checksum,
            doc_count: 1,
            size_bytes,
            uncompressed_bytes: size_bytes,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
        };
        let split = AssignedRemoteSplit::from_manifest(&manifest);
        let live_split_ids = HashSet::from([split.split_id.clone()]);
        (metadata, split, live_split_ids)
    }

    #[test]
    fn assign_remote_store_splits_prefers_reader_cached_leaf_over_artifact_cached_leaf() {
        let split = test_split("split-a");
        let leaf_nodes = vec![test_leaf("artifact"), test_leaf("reader")];
        let statuses = HashMap::from([
            (
                "artifact".to_string(),
                test_status(&[(
                    "split-a",
                    SplitWarmth {
                        artifact_cached: true,
                        reader_cached: false,
                    },
                )]),
            ),
            (
                "reader".to_string(),
                test_status(&[(
                    "split-a",
                    SplitWarmth {
                        artifact_cached: true,
                        reader_cached: true,
                    },
                )]),
            ),
        ]);

        let assignments = assign_remote_store_splits(
            "idx-1",
            7,
            std::slice::from_ref(&split),
            &leaf_nodes,
            &statuses,
            &HashSet::new(),
        );

        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments["reader"], vec![split]);
    }

    #[test]
    fn assign_remote_store_splits_prefers_artifact_cached_leaf_over_cold_leaf() {
        let split = test_split("split-a");
        let leaf_nodes = vec![test_leaf("warm"), test_leaf("cold")];
        let statuses = HashMap::from([
            (
                "warm".to_string(),
                test_status(&[(
                    "split-a",
                    SplitWarmth {
                        artifact_cached: true,
                        reader_cached: false,
                    },
                )]),
            ),
            ("cold".to_string(), test_status(&[])),
        ]);

        let assignments = assign_remote_store_splits(
            "idx-1",
            7,
            std::slice::from_ref(&split),
            &leaf_nodes,
            &statuses,
            &HashSet::new(),
        );

        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments["warm"], vec![split]);
    }

    #[test]
    fn retry_flow_reassigns_failed_split_to_remaining_healthy_leaf() {
        let split_a = test_split("split-a");
        let split_b = test_split("split-b");
        let leaf_nodes = vec![test_leaf("node-2"), test_leaf("node-3")];
        let statuses = HashMap::from([
            (
                "node-2".to_string(),
                test_status(&[(
                    "split-a",
                    SplitWarmth {
                        artifact_cached: true,
                        reader_cached: true,
                    },
                )]),
            ),
            (
                "node-3".to_string(),
                test_status(&[(
                    "split-b",
                    SplitWarmth {
                        artifact_cached: true,
                        reader_cached: true,
                    },
                )]),
            ),
        ]);

        let pending_splits = vec![split_a.clone(), split_b.clone()];
        let first_round = assign_remote_store_splits(
            "idx-1",
            7,
            &pending_splits,
            &leaf_nodes,
            &statuses,
            &HashSet::new(),
        );
        assert_eq!(first_round["node-2"], vec![split_a.clone()]);
        assert_eq!(first_round["node-3"], vec![split_b.clone()]);

        let split_lookup = HashMap::from([
            (split_a.split_id.clone(), split_a.clone()),
            (split_b.split_id.clone(), split_b.clone()),
        ]);
        let mut accumulator = RemoteStoreSearchAccumulator::default();
        let round = apply_remote_store_batch_results(
            "remotehits",
            vec![
                (
                    "node-2".to_string(),
                    first_round["node-2"].clone(),
                    Err(anyhow::anyhow!("leaf batch failed")),
                ),
                (
                    "node-3".to_string(),
                    first_round["node-3"].clone(),
                    Ok(vec![LeafSplitSearchOutcome {
                        split_id: split_b.split_id.clone(),
                        hits: vec![serde_json::json!({
                            "_id": "doc-2",
                            "_score": 1.0,
                            "_source": { "title": "split-b" },
                        })],
                        total_hits: 1,
                        partial_aggs: HashMap::new(),
                        error: None,
                    }]),
                ),
            ],
            &split_lookup,
            &mut accumulator,
        );

        assert!(round.made_progress);
        assert_eq!(round.next_pending, vec![split_a.clone()]);
        assert_eq!(
            round.newly_unhealthy_nodes,
            HashSet::from(["node-2".to_string()])
        );
        assert_eq!(accumulator.successful, 1);
        assert_eq!(accumulator.total_hits, 1);
        assert_eq!(accumulator.split_hit_lists.len(), 1);
        assert_eq!(accumulator.split_hit_lists[0][0]["_id"], "doc-2");

        let second_round = assign_remote_store_splits(
            "idx-1",
            7,
            &round.next_pending,
            &leaf_nodes,
            &statuses,
            &round.newly_unhealthy_nodes,
        );

        assert_eq!(second_round.len(), 1);
        assert_eq!(second_round["node-3"], vec![split_a]);
    }

    #[tokio::test]
    async fn get_or_open_pins_cached_split_before_reader_open_finishes() {
        let temp_dir = TempDir::new().unwrap();
        let storage_manager = Arc::new(StorageManager::new_in_path(temp_dir.path()).unwrap());
        let reader_cache = Arc::new(RemoteSplitReaderCache::default());
        let worker_pools = crate::worker::WorkerPools::new(1, 1);
        let (metadata, split, live_split_ids) =
            build_cached_remote_split(storage_manager.as_ref()).await;
        let context = LeafExecutionContext {
            worker_pools: worker_pools.clone(),
            storage_manager: storage_manager.clone(),
            reader_cache: reader_cache.clone(),
        };

        let (block_tx, block_rx) = mpsc::channel::<()>();
        let blocker_pools = worker_pools.clone();
        let blocker = tokio::spawn(async move {
            blocker_pools
                .spawn_search(move || {
                    block_rx.recv().unwrap();
                })
                .await
                .unwrap();
        });

        let open_context = context.clone();
        let open_metadata = metadata.clone();
        let open_split = split.clone();
        let open_reader_cache = reader_cache.clone();
        let open_handle = tokio::spawn(async move {
            open_reader_cache
                .get_or_open(&open_context, &open_metadata, &open_split)
                .await
        });

        let cache_dir = storage_manager
            .local_workdir()
            .join("splits")
            .join(metadata.uuid.as_str())
            .join(&split.split_id);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while (!cache_dir.exists())
            || storage_manager.cached_split_pin_count(metadata.uuid.as_str(), &split.split_id) == 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "split cache dir was never pinned during reader open"
            );
            tokio::task::yield_now().await;
        }

        storage_manager
            .reap_split_cache(metadata.uuid.as_str(), &live_split_ids, Some(0))
            .unwrap();
        assert!(
            cache_dir.exists(),
            "pinned split cache dir was reaped while reader open was pending"
        );

        block_tx.send(()).unwrap();
        blocker.await.unwrap();
        let reader = open_handle.await.unwrap().unwrap();
        assert!(reader.engine.doc_count() >= 1);
        assert!(
            cache_dir.exists(),
            "successful reader open should leave cache dir intact"
        );
    }
}
