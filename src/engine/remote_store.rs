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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
use crate::api::index::{DistributedDslSearchResult, RemoteStoreSearchStats};
use crate::api::search::failures::{
    ShardFailure, all_shards_failed_response, query_error_response,
};
use crate::cluster::state::{FieldMapping, FieldType, IndexMetadata};
use crate::engine::tantivy::{canonical_keyword_scalar, visit_indexed_keyword_values};
use crate::engine::{DocumentValidationError, SearchEngine};
use crate::search::{PartialAggResult, QueryClause, RangeCondition, SearchRequest};

const REMOTE_STORE_TERM_SUMMARY_CAP: usize = 64;

type SplitFieldSummaries = (
    BTreeMap<String, crate::storage::SplitFieldRange>,
    BTreeMap<String, crate::storage::SplitFieldTerms>,
);

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
            field_ranges: std::collections::BTreeMap::new(),
            field_terms: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct SplitPruningPredicate {
    terms: Vec<TermPruningClause>,
    ranges: Vec<RangePruningClause>,
}

#[derive(Debug)]
struct TermPruningClause {
    field: String,
    value: String,
}

#[derive(Debug, Clone, Copy)]
enum RangeFieldKind {
    Integer,
    Float,
    Date,
}

#[derive(Debug)]
struct RangePruningClause {
    field: String,
    kind: RangeFieldKind,
    lower: Option<RangePruningBound>,
    upper: Option<RangePruningBound>,
}

#[derive(Debug)]
struct RangePruningBound {
    value: RangePruningValue,
    inclusive: bool,
}

#[derive(Debug, Clone, Copy)]
enum RangePruningValue {
    I64(i64),
    F64(f64),
}

#[derive(Debug)]
enum RangeSummaryAccumulator {
    I64 { min: i64, max: i64 },
    F64 { min: f64, max: f64 },
}

#[derive(Debug, Default)]
struct TermSummaryAccumulator {
    values: BTreeSet<String>,
    exceeded_cap: bool,
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

fn build_split_pruning_predicate(
    query: &QueryClause,
    mappings: &HashMap<String, FieldMapping>,
) -> SplitPruningPredicate {
    let mut predicate = SplitPruningPredicate::default();
    collect_split_pruning_clauses(query, mappings, &mut predicate);
    predicate
}

fn collect_split_pruning_clauses(
    query: &QueryClause,
    mappings: &HashMap<String, FieldMapping>,
    predicate: &mut SplitPruningPredicate,
) {
    match query {
        QueryClause::Term(fields) => {
            if let Some((field, value)) = fields.iter().next()
                && let Some(clause) = build_term_pruning_clause(field, value, mappings)
            {
                predicate.terms.push(clause);
            }
        }
        QueryClause::Range(fields) => {
            if let Some((field, condition)) = fields.iter().next()
                && let Some(clause) = build_range_pruning_clause(field, condition, mappings)
            {
                predicate.ranges.push(clause);
            }
        }
        QueryClause::Bool(bool_query) => {
            for child in bool_query.must.iter().chain(bool_query.filter.iter()) {
                collect_split_pruning_clauses(child, mappings, predicate);
            }
        }
        _ => {}
    }
}

fn build_term_pruning_clause(
    field: &str,
    value: &Value,
    mappings: &HashMap<String, FieldMapping>,
) -> Option<TermPruningClause> {
    let mapping = mappings.get(field)?;
    let value = canonical_term_summary_value(&mapping.field_type, value)?;
    Some(TermPruningClause {
        field: field.to_string(),
        value,
    })
}

fn build_range_pruning_clause(
    field: &str,
    condition: &RangeCondition,
    mappings: &HashMap<String, FieldMapping>,
) -> Option<RangePruningClause> {
    let mapping = mappings.get(field)?;
    let kind = range_field_kind(&mapping.field_type)?;
    let lower = condition
        .gt
        .as_ref()
        .and_then(|value| canonical_range_summary_value(kind, value))
        .map(|value| RangePruningBound {
            value,
            inclusive: false,
        })
        .or_else(|| {
            condition
                .gte
                .as_ref()
                .and_then(|value| canonical_range_summary_value(kind, value))
                .map(|value| RangePruningBound {
                    value,
                    inclusive: true,
                })
        });
    let upper = condition
        .lt
        .as_ref()
        .and_then(|value| canonical_range_summary_value(kind, value))
        .map(|value| RangePruningBound {
            value,
            inclusive: false,
        })
        .or_else(|| {
            condition
                .lte
                .as_ref()
                .and_then(|value| canonical_range_summary_value(kind, value))
                .map(|value| RangePruningBound {
                    value,
                    inclusive: true,
                })
        });
    if lower.is_none() && upper.is_none() {
        return None;
    }
    Some(RangePruningClause {
        field: field.to_string(),
        kind,
        lower,
        upper,
    })
}

fn range_field_kind(field_type: &FieldType) -> Option<RangeFieldKind> {
    match field_type {
        FieldType::Integer => Some(RangeFieldKind::Integer),
        FieldType::Float => Some(RangeFieldKind::Float),
        FieldType::Date => Some(RangeFieldKind::Date),
        _ => None,
    }
}

fn canonical_term_summary_value(field_type: &FieldType, value: &Value) -> Option<String> {
    match field_type {
        FieldType::Keyword => canonical_keyword_scalar(value).map(|value| value.into_owned()),
        FieldType::Boolean => match value {
            Value::Bool(boolean) => Some(boolean.to_string()),
            Value::String(text) if text.eq_ignore_ascii_case("true") => Some("true".into()),
            Value::String(text) if text.eq_ignore_ascii_case("false") => Some("false".into()),
            _ => None,
        },
        _ => None,
    }
}

fn canonical_range_summary_value(kind: RangeFieldKind, value: &Value) -> Option<RangePruningValue> {
    match kind {
        RangeFieldKind::Integer => match value {
            Value::Number(number) => number.as_i64().map(RangePruningValue::I64),
            _ => None,
        },
        RangeFieldKind::Date => match value {
            Value::Number(number) => number.as_i64().map(RangePruningValue::I64),
            Value::String(text) => {
                crate::common::date::parse_iso8601_to_epoch_millis(text).map(RangePruningValue::I64)
            }
            _ => None,
        },
        RangeFieldKind::Float => match value {
            Value::Number(number) => number
                .as_f64()
                .filter(|value| value.is_finite())
                .map(RangePruningValue::F64),
            _ => None,
        },
    }
}

fn split_matches_pruning_predicate(
    split: &crate::storage::RemoteSplitManifest,
    predicate: &SplitPruningPredicate,
) -> bool {
    predicate
        .terms
        .iter()
        .all(|clause| split_matches_term_pruning_clause(split, clause))
        && predicate
            .ranges
            .iter()
            .all(|clause| split_matches_range_pruning_clause(split, clause))
}

fn split_matches_term_pruning_clause(
    split: &crate::storage::RemoteSplitManifest,
    clause: &TermPruningClause,
) -> bool {
    let Some(summary) = split.field_terms.get(&clause.field) else {
        return true;
    };
    summary.values.iter().any(|value| value == &clause.value)
}

fn split_matches_range_pruning_clause(
    split: &crate::storage::RemoteSplitManifest,
    clause: &RangePruningClause,
) -> bool {
    let summary = split
        .field_ranges
        .get(&clause.field)
        .map(|range| (range.min.as_str(), range.max.as_str()))
        .or_else(|| {
            split.time_range.as_ref().and_then(|range| {
                (range.field == clause.field).then_some((range.min.as_str(), range.max.as_str()))
            })
        });
    let Some((min, max)) = summary else {
        return true;
    };

    match clause.kind {
        RangeFieldKind::Integer | RangeFieldKind::Date => {
            let Some(split_min) = min.parse::<i64>().ok() else {
                return true;
            };
            let Some(split_max) = max.parse::<i64>().ok() else {
                return true;
            };
            if let Some(bound) = &clause.lower {
                let RangePruningValue::I64(bound_value) = bound.value else {
                    return true;
                };
                if split_max < bound_value || (!bound.inclusive && split_max == bound_value) {
                    return false;
                }
            }
            if let Some(bound) = &clause.upper {
                let RangePruningValue::I64(bound_value) = bound.value else {
                    return true;
                };
                if split_min > bound_value || (!bound.inclusive && split_min == bound_value) {
                    return false;
                }
            }
            true
        }
        RangeFieldKind::Float => {
            let Some(split_min) = min.parse::<f64>().ok().filter(|value| value.is_finite()) else {
                return true;
            };
            let Some(split_max) = max.parse::<f64>().ok().filter(|value| value.is_finite()) else {
                return true;
            };
            if let Some(bound) = &clause.lower {
                let RangePruningValue::F64(bound_value) = bound.value else {
                    return true;
                };
                if split_max < bound_value || (!bound.inclusive && split_max == bound_value) {
                    return false;
                }
            }
            if let Some(bound) = &clause.upper {
                let RangePruningValue::F64(bound_value) = bound.value else {
                    return true;
                };
                if split_min > bound_value || (!bound.inclusive && split_min == bound_value) {
                    return false;
                }
            }
            true
        }
    }
}

impl RangeSummaryAccumulator {
    fn new(value: RangePruningValue) -> Self {
        match value {
            RangePruningValue::I64(value) => Self::I64 {
                min: value,
                max: value,
            },
            RangePruningValue::F64(value) => Self::F64 {
                min: value,
                max: value,
            },
        }
    }

    fn observe(&mut self, value: RangePruningValue) {
        match (self, value) {
            (Self::I64 { min, max }, RangePruningValue::I64(value)) => {
                *min = (*min).min(value);
                *max = (*max).max(value);
            }
            (Self::F64 { min, max }, RangePruningValue::F64(value)) => {
                *min = (*min).min(value);
                *max = (*max).max(value);
            }
            _ => {}
        }
    }

    fn into_manifest(self) -> crate::storage::SplitFieldRange {
        match self {
            Self::I64 { min, max } => crate::storage::SplitFieldRange {
                min: min.to_string(),
                max: max.to_string(),
            },
            Self::F64 { min, max } => crate::storage::SplitFieldRange {
                min: min.to_string(),
                max: max.to_string(),
            },
        }
    }
}

impl TermSummaryAccumulator {
    fn observe(&mut self, value: String) {
        if self.exceeded_cap {
            return;
        }
        self.values.insert(value);
        if self.values.len() > REMOTE_STORE_TERM_SUMMARY_CAP {
            self.values.clear();
            self.exceeded_cap = true;
        }
    }

    fn into_manifest(self) -> Option<crate::storage::SplitFieldTerms> {
        if self.exceeded_cap || self.values.is_empty() {
            return None;
        }
        Some(crate::storage::SplitFieldTerms {
            values: self.values.into_iter().collect(),
        })
    }
}

fn build_split_field_summaries(
    docs: &[Value],
    mappings: &HashMap<String, FieldMapping>,
) -> Result<SplitFieldSummaries, DocumentValidationError> {
    let mut range_summaries: HashMap<String, RangeSummaryAccumulator> = HashMap::new();
    let mut term_summaries: HashMap<String, TermSummaryAccumulator> = HashMap::new();

    for doc in docs {
        let Some(object) = doc.as_object() else {
            continue;
        };
        for (field, value) in object {
            let Some(mapping) = mappings.get(field) else {
                continue;
            };
            if matches!(mapping.field_type, FieldType::Keyword) {
                let summary = term_summaries.entry(field.clone()).or_default();
                visit_indexed_keyword_values(field, value, &mut |value| {
                    summary.observe(value.into_owned());
                })?;
                continue;
            }
            if let Some(kind) = range_field_kind(&mapping.field_type)
                && let Some(summary_value) = canonical_range_summary_value(kind, value)
            {
                range_summaries
                    .entry(field.clone())
                    .and_modify(|summary| summary.observe(summary_value))
                    .or_insert_with(|| RangeSummaryAccumulator::new(summary_value));
                continue;
            }
            if let Some(summary_value) = canonical_term_summary_value(&mapping.field_type, value) {
                term_summaries
                    .entry(field.clone())
                    .or_default()
                    .observe(summary_value);
            }
        }
    }

    let field_ranges = range_summaries
        .into_iter()
        .map(|(field, summary)| (field, summary.into_manifest()))
        .collect();
    let field_terms = term_summaries
        .into_iter()
        .filter_map(|(field, summary)| summary.into_manifest().map(|summary| (field, summary)))
        .collect();
    Ok((field_ranges, field_terms))
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
        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;
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
        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;
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
                    crate::worker::check_current_request_active(
                        crate::worker::ExecutionStage::Hydration,
                    )?;
                    let column_cache =
                        Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let engine = crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                        &split_dir,
                        Duration::from_secs(60),
                        &mappings,
                        column_cache,
                    )?;
                    Ok(Arc::new(engine))
                },
            )
            .await??;
        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Hydration)?;

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
        crate::worker::check_current_request_active(crate::worker::ExecutionStage::Search)?;
        let estimated_query_bytes =
            ((search_req.size.saturating_add(search_req.from)).max(1) as u64).saturating_mul(256);
        let _query_memory_reservation = context.worker_pools.resource_governor().try_reserve(
            crate::worker::ResourcePoolKind::Memory,
            estimated_query_bytes,
        )?;
        let outcome = match context
            .reader_cache
            .get_or_open(context, metadata, split)
            .await
        {
            Ok(reader) => {
                let engine = Arc::clone(&reader.engine);
                let search_req = search_req.for_remote_split(&split.split_id);
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
                    Ok(Err(error))
                        if crate::search::query_string::query_error_is_client(&error)
                            || crate::worker::is_cancellation_or_deadline_error(&error) =>
                    {
                        return Err(error);
                    }
                    Err(error) if crate::worker::is_cancellation_or_deadline_error(&error) => {
                        return Err(error);
                    }
                    Ok(Err(error)) | Err(error) => LeafSplitSearchOutcome {
                        split_id: split.split_id.clone(),
                        hits: Vec::new(),
                        total_hits: 0,
                        partial_aggs: HashMap::new(),
                        error: Some(format!("{error:#}")),
                    },
                }
            }
            Err(error)
                if crate::worker::is_cancellation_or_deadline_error(&error)
                    || error.is::<crate::worker::ResourceExhaustedError>()
                    || error.is::<crate::worker::AdmissionError>() =>
            {
                return Err(error);
            }
            Err(error) => LeafSplitSearchOutcome {
                split_id: split.split_id.clone(),
                hits: Vec::new(),
                total_hits: 0,
                partial_aggs: HashMap::new(),
                error: Some(format!("{error:#}")),
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
        let child_ctx = crate::worker::current_request_context().map(|ctx| ctx.child());
        futures.push(tokio::spawn(async move {
            let fut = client.get_remote_store_leaf_status(&node, &index_name, &index_uuid, &splits);
            let status = match child_ctx {
                Some(ctx) => crate::worker::with_task_request_context(ctx, fut).await,
                None => fut.await,
            };
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
    failures: BTreeMap<String, ShardFailure>,
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
                        accumulator.failures.insert(
                            split_id.clone(),
                            ShardFailure::new(
                                index_name,
                                split_id,
                                &node_id,
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "search_exception",
                                error,
                            ),
                        );
                        node_failed = true;
                        continue;
                    }

                    accumulator.failures.remove(&split_id);
                    made_progress = true;
                    accumulator.successful += 1;
                    accumulator.total_hits += total_hits;
                    if !partial_aggs.is_empty() {
                        accumulator.partial_aggs.push(partial_aggs);
                    }

                    let hit_list = hits
                        .into_iter()
                        .map(|hit| {
                            let mut enriched = serde_json::json!({
                                "_index": index_name,
                                "_split": split_id,
                                "_id": hit.get("_id").and_then(|v| v.as_str()).unwrap_or(""),
                                "_score": hit.get("_score"),
                                "_source": hit.get("_source").unwrap_or(&hit),
                            });
                            if let Some(sort_vals) = hit.get("sort") {
                                enriched["sort"] = sort_vals.clone();
                            }
                            enriched
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
                for split in &assigned_splits {
                    accumulator.failures.insert(
                        split.split_id.clone(),
                        ShardFailure::from_error(
                            index_name,
                            split.split_id.clone(),
                            &node_id,
                            &error,
                        ),
                    );
                }
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

async fn validate_empty_search_request(
    state: &AppState,
    metadata: &IndexMetadata,
    search_req: &SearchRequest,
) -> Result<(), (StatusCode, Json<Value>)> {
    let request = search_req.clone();
    let mappings = metadata.mappings.clone();
    state
        .worker_pools
        .spawn_search(move || {
            crate::engine::tantivy::validate_search_request_with_mappings(&request, &mappings)
        })
        .await
        .map_err(query_error_response)?
        .map_err(query_error_response)
}

pub(crate) struct ResolvedIndexSnapshot {
    pub snapshot: crate::storage::ReadSnapshot,
    pub manifest: Option<crate::storage::RemoteStoreManifest>,
    pub split_excluded_doc_ids: BTreeMap<String, Vec<String>>,
    pub shard_filters: HashMap<u32, crate::search::SnapshotFilterParams>,
    pub _lease_guard: Option<crate::storage::SnapshotLeaseGuard>,
}

/// Resolve and pin one `ReadSnapshot` across immutable splits and mutable hot-delta
/// shards (`FS-037`, `FS-038`, ADR 0003 section 3.2).
pub(crate) async fn resolve_index_read_snapshot(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    requested_generation: Option<u64>,
    local_shards: &[(u32, Arc<dyn crate::engine::SearchEngine>)],
) -> Result<ResolvedIndexSnapshot, (StatusCode, Json<Value>)> {
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let loaded = state
        .storage_manager
        .load_manifest_for_snapshot(
            metadata.uuid.as_str(),
            requested_generation,
            Some(&expected_schema_hash),
        )
        .await
        .map_err(|error| {
            let msg = format!("{error:#}");
            if requested_generation.is_some()
                && (msg.contains("does not exist")
                    || msg.contains("exceeds current committed generation")
                    || msg.contains("is not available"))
            {
                crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    format!(
                        "cannot resolve pinned snapshot_generation for index [{index_name}]: {msg}"
                    ),
                )
            } else {
                tracing::error!(
                    "remote_store: failed to resolve manifest snapshot for {} ({}): {}",
                    index_name,
                    metadata.uuid.as_str(),
                    msg
                );
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_manifest_exception",
                    format!("failed to load manifest: {msg}"),
                )
            }
        })?;

    let now_ms = now_epoch_ms();
    let expires_at_epoch_ms = now_ms.saturating_add(300_000);
    let (
        manifest_opt,
        manifest_generation,
        manifest_path,
        manifest_checksum,
        writer_epoch,
        pointer_revision,
        published_watermarks,
    ) = match loaded {
        Some((manifest, path, checksum, ptr_rev)) => {
            let generation = manifest.generation;
            let epoch = manifest.writer_epoch;
            let wms = manifest.published_watermarks.clone();
            (
                Some(manifest),
                generation,
                Some(path),
                Some(checksum),
                epoch,
                ptr_rev,
                wms,
            )
        }
        None => (None, 0, None, None, 0, 0, BTreeMap::new()),
    };

    let snapshot_id = format!(
        "snap-{}-g{}-e{}-r{}",
        metadata.uuid, manifest_generation, writer_epoch, pointer_revision
    );
    crate::worker::set_current_trace_snapshot_id(&snapshot_id);
    if let Some(ref m) = manifest_opt
        && !m.operation_id.is_empty()
    {
        crate::worker::set_current_trace_operation_id(&m.operation_id);
    }

    let lease_guard = manifest_opt.as_ref().map(|m| {
        let pinned_split_ids: HashSet<String> =
            m.published_splits().map(|s| s.split_id.clone()).collect();
        state.storage_manager.acquire_snapshot_retention_lease(
            crate::storage::SnapshotRetentionLease {
                snapshot_id: snapshot_id.clone(),
                index_uuid: metadata.uuid.to_string(),
                manifest_generation: m.generation,
                pinned_split_ids,
                acquired_at_epoch_ms: now_ms,
                expires_at_epoch_ms,
            },
        )
    });

    // Compute per-document version and tombstone winners across published splits
    // and (when not pinned to a historical manifest generation) hot-delta shards.
    // Winner tuple: (seq_no, primary_term, version, split_order, is_tombstone, owner_tag)
    let mut doc_winners: HashMap<String, (u64, u64, u64, usize, bool, String)> = HashMap::new();
    let mut split_doc_membership: HashMap<String, HashSet<String>> = HashMap::new();

    if let Some(ref manifest) = manifest_opt {
        for (split_idx, split) in manifest.published_splits().enumerate() {
            let entry = split_doc_membership
                .entry(split.split_id.clone())
                .or_default();
            if let Ok(dvs) = split.doc_versions() {
                for dv in dvs {
                    entry.insert(dv.doc_id.clone());
                    let cand = (
                        dv.seq_no,
                        dv.primary_term,
                        dv.version,
                        split_idx,
                        false,
                        split.split_id.clone(),
                    );
                    let update = doc_winners
                        .get(&dv.doc_id)
                        .is_none_or(|(s, t, v, idx, _, _)| {
                            (cand.0, cand.1, cand.2, cand.3) >= (*s, *t, *v, *idx)
                        });
                    if update {
                        doc_winners.insert(dv.doc_id, cand);
                    }
                }
            }
            if let Ok(tss) = split.tombstones() {
                for ts in tss {
                    let cand = (
                        ts.seq_no,
                        ts.primary_term,
                        ts.version,
                        split_idx,
                        true,
                        split.split_id.clone(),
                    );
                    let update = doc_winners
                        .get(&ts.doc_id)
                        .is_none_or(|(s, t, v, idx, _, _)| {
                            (cand.0, cand.1, cand.2, cand.3) >= (*s, *t, *v, *idx)
                        });
                    if update {
                        doc_winners.insert(ts.doc_id, cand);
                    }
                }
            }
        }
    }

    let pinned_historical_generation = requested_generation.is_some();
    let mut shard_max_visible_seq_no = BTreeMap::new();
    let mut shard_filters: HashMap<u32, crate::search::SnapshotFilterParams> = HashMap::new();

    if metadata.is_tiered() && !pinned_historical_generation {
        for (shard_id, engine) in local_shards {
            let wm_seq = published_watermarks
                .get(shard_id)
                .map(|w| w.sealed_through_seq_no);
            let mut max_vis: Option<u64> = wm_seq;
            let mut shard_excluded: BTreeSet<String> = BTreeSet::new();
            let hot_owner = format!("hot:{shard_id}");

            let engine_for_versions = engine.clone();
            let (live_res, del_res) = state
                .worker_pools
                .spawn_search(move || {
                    (
                        engine_for_versions.live_documents_with_versions(),
                        engine_for_versions.deleted_documents_with_versions(),
                    )
                })
                .await
                .unwrap_or_else(|e| (Err(anyhow::anyhow!(e)), Ok(Vec::new())));

            if let Ok(live_docs) = live_res {
                for (doc_id, _src, seq_no, term, ver) in live_docs {
                    max_vis = Some(max_vis.map_or(seq_no, |m| m.max(seq_no)));
                    if wm_seq.is_none_or(|w| seq_no > w) {
                        shard_excluded.remove(&doc_id);
                        let cand = (seq_no, term, ver, usize::MAX, false, hot_owner.clone());
                        let update = doc_winners.get(&doc_id).is_none_or(|(s, t, v, idx, _, _)| {
                            (cand.0, cand.1, cand.2, cand.3) >= (*s, *t, *v, *idx)
                        });
                        if update {
                            doc_winners.insert(doc_id, cand);
                        }
                    } else {
                        shard_excluded.insert(doc_id);
                    }
                }
            }
            if let Ok(del_docs) = del_res {
                for (doc_id, seq_no, term, ver) in del_docs {
                    max_vis = Some(max_vis.map_or(seq_no, |m| m.max(seq_no)));
                    if wm_seq.is_none_or(|w| seq_no > w) {
                        let cand = (seq_no, term, ver, usize::MAX, true, hot_owner.clone());
                        let update = doc_winners.get(&doc_id).is_none_or(|(s, t, v, idx, _, _)| {
                            (cand.0, cand.1, cand.2, cand.3) >= (*s, *t, *v, *idx)
                        });
                        if update {
                            doc_winners.insert(doc_id.clone(), cand);
                        }
                        shard_excluded.insert(doc_id);
                    }
                }
            }

            shard_max_visible_seq_no.insert(*shard_id, max_vis);
            shard_filters.insert(
                *shard_id,
                crate::search::SnapshotFilterParams {
                    snapshot_id: Some(snapshot_id.clone()),
                    snapshot_generation: Some(manifest_generation),
                    excluded_doc_ids: shard_excluded.into_iter().collect(),
                    split_excluded_doc_ids: BTreeMap::new(),
                    min_exclusive_seq_no: wm_seq,
                    max_inclusive_seq_no: max_vis,
                },
            );
        }
        // Also populate min_exclusive_seq_no for any remote shard not in local_shards.
        for shard_id in metadata.shard_routing.keys() {
            shard_filters.entry(*shard_id).or_insert_with(|| {
                let wm_seq = published_watermarks
                    .get(shard_id)
                    .map(|w| w.sealed_through_seq_no);
                crate::search::SnapshotFilterParams {
                    snapshot_id: Some(snapshot_id.clone()),
                    snapshot_generation: Some(manifest_generation),
                    excluded_doc_ids: Vec::new(),
                    split_excluded_doc_ids: BTreeMap::new(),
                    min_exclusive_seq_no: wm_seq,
                    max_inclusive_seq_no: None,
                }
            });
        }
    }

    let mut split_excluded_doc_ids: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if let Some(ref manifest) = manifest_opt
        && !doc_winners.is_empty()
    {
        for split in manifest.published_splits() {
            let mut excluded = BTreeSet::new();
            if let Some(members) = split_doc_membership.get(&split.split_id)
                && !members.is_empty()
            {
                for doc_id in members {
                    let is_winner_here =
                        doc_winners
                            .get(doc_id)
                            .is_none_or(|(_, _, _, _, is_ts, owner)| {
                                !*is_ts && owner == &split.split_id
                            });
                    if !is_winner_here {
                        excluded.insert(doc_id.clone());
                    }
                }
            } else {
                for (doc_id, (_, _, _, _, is_ts, owner)) in &doc_winners {
                    if *is_ts || owner != &split.split_id {
                        excluded.insert(doc_id.clone());
                    }
                }
            }
            if !excluded.is_empty() {
                split_excluded_doc_ids
                    .insert(split.split_id.clone(), excluded.into_iter().collect());
            }
        }
    }

    let snapshot = crate::storage::ReadSnapshot {
        snapshot_id,
        index_uuid: metadata.uuid.to_string(),
        index_name: index_name.to_string(),
        schema_hash: expected_schema_hash,
        resolved_at_epoch_ms: now_ms,
        expires_at_epoch_ms,
        pinned_historical_generation,
        manifest_generation,
        manifest_path,
        manifest_checksum,
        writer_epoch,
        pointer_revision,
        published_watermarks,
        shard_max_visible_seq_no,
    };

    Ok(ResolvedIndexSnapshot {
        snapshot,
        manifest: manifest_opt,
        split_excluded_doc_ids,
        shard_filters,
        _lease_guard: lease_guard,
    })
}

/// Entry point invoked from `execute_distributed_dsl_search` when an index's
/// engine is `IndexEngine::RemoteStore`.
pub(crate) async fn search(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    search_req: &SearchRequest,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    let resolved = resolve_index_read_snapshot(
        state,
        index_name,
        metadata,
        search_req.snapshot_generation(),
        &[],
    )
    .await?;
    search_with_resolved_snapshot(state, index_name, metadata, search_req, &resolved).await
}

pub(crate) async fn search_with_resolved_snapshot(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    search_req: &SearchRequest,
    resolved: &ResolvedIndexSnapshot,
) -> Result<DistributedDslSearchResult, (StatusCode, Json<Value>)> {
    if search_req.knn.is_some() {
        return Err(crate::api::error_response(
            StatusCode::NOT_IMPLEMENTED,
            "illegal_argument_exception",
            "k-NN search is not supported on remote_store indices yet",
        ));
    }

    let clean_req = search_req.without_snapshot_filters();
    let Some(ref manifest) = resolved.manifest else {
        validate_empty_search_request(state, metadata, &clean_req).await?;
        return Ok(DistributedDslSearchResult {
            all_hits: Vec::new(),
            total_hits: 0,
            successful_shards: 0,
            failed_shards: 0,
            shard_failures: Vec::new(),
            aggregations: HashMap::new(),
            partial_aggs: Vec::new(),
            remote_store_stats: Some(RemoteStoreSearchStats::default()),
        });
    };

    let pruning_predicate = build_split_pruning_predicate(&clean_req.query, &metadata.mappings);
    let published_split_count = manifest.published_splits().count();
    let split_plans: Vec<_> = manifest
        .published_splits()
        .filter(|split| split_matches_pruning_predicate(split, &pruning_predicate))
        .map(AssignedRemoteSplit::from_manifest)
        .collect();
    let mut remote_store_stats = RemoteStoreSearchStats {
        published_splits: published_split_count,
        candidate_splits: split_plans.len(),
        pruned_splits: published_split_count.saturating_sub(split_plans.len()),
        assigned_splits: 0,
    };
    if split_plans.is_empty() {
        validate_empty_search_request(state, metadata, &clean_req).await?;
        return Ok(DistributedDslSearchResult {
            all_hits: Vec::new(),
            total_hits: 0,
            successful_shards: 0,
            failed_shards: 0,
            shard_failures: Vec::new(),
            aggregations: HashMap::new(),
            partial_aggs: Vec::new(),
            remote_store_stats: Some(remote_store_stats),
        });
    }

    let search_req_with_exclusions =
        clean_req.with_snapshot_filter(crate::search::SnapshotFilterParams {
            snapshot_id: Some(resolved.snapshot.snapshot_id.clone()),
            snapshot_generation: Some(manifest.generation),
            excluded_doc_ids: Vec::new(),
            split_excluded_doc_ids: resolved.split_excluded_doc_ids.clone(),
            min_exclusive_seq_no: None,
            max_inclusive_seq_no: None,
        });
    let search_req = &search_req_with_exclusions;

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
        let failures: Vec<_> = split_plans
            .iter()
            .map(|split| {
                ShardFailure::new(
                    index_name,
                    split.split_id.clone(),
                    "unassigned",
                    StatusCode::SERVICE_UNAVAILABLE,
                    "remote_store_leaf_unavailable_exception",
                    format!(
                        "no remote_store-capable leaf nodes available for index [{index_name}]"
                    ),
                )
            })
            .collect();
        return Err(all_shards_failed_response(0, &failures).expect("non-empty split plans"));
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
    let mut assigned_split_ids = HashSet::new();

    let mut round_index = 0usize;
    while !pending_splits.is_empty() {
        let round_started = std::time::Instant::now();
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
            for split in &pending_splits {
                accumulator
                    .failures
                    .entry(split.split_id.clone())
                    .or_insert_with(|| {
                        ShardFailure::new(
                            index_name,
                            split.split_id.clone(),
                            "unassigned",
                            StatusCode::SERVICE_UNAVAILABLE,
                            "remote_store_leaf_unavailable_exception",
                            format!("no healthy leaf available for split [{}]", split.split_id),
                        )
                    });
            }
            break;
        }
        for split in assignments.values().flatten() {
            assigned_split_ids.insert(split.split_id.clone());
        }

        let mut work = Vec::new();
        for (node_id, splits) in assignments {
            let assignment_id = format!(
                "assign-g{}-r{}-{}",
                manifest.generation, round_index, node_id
            );
            crate::worker::set_current_trace_assignment_id(&assignment_id);
            let node_id_for_result = node_id.clone();
            if node_id == state.local_node_id {
                let context = leaf_context.clone();
                let metadata = metadata.clone();
                let search_req = search_req.clone();
                let live_split_ids = live_split_ids.clone();
                let child_ctx = crate::worker::current_request_context().map(|ctx| {
                    let c = ctx.child();
                    let _ = c.set_assignment_id(&assignment_id);
                    c
                });
                work.push(
                    async move {
                        let fut = execute_leaf_search_batch(
                            &context,
                            &metadata,
                            &search_req,
                            &splits,
                            &live_split_ids,
                        );
                        let res = match child_ctx {
                            Some(ctx) => crate::worker::with_task_request_context(ctx, fut).await,
                            None => fut.await,
                        };
                        (node_id_for_result, splits.clone(), res)
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
                let child_ctx = crate::worker::current_request_context().map(|ctx| {
                    let c = ctx.child();
                    let _ = c.set_assignment_id(&assignment_id);
                    c
                });
                work.push(
                    async move {
                        let fut = client.forward_remote_store_search(
                            &node,
                            &index_name,
                            &index_uuid,
                            &search_req,
                            &splits,
                            &live_split_ids,
                        );
                        let res = match child_ctx {
                            Some(ctx) => crate::worker::with_task_request_context(ctx, fut).await,
                            None => fut.await,
                        };
                        (node_id_for_result, splits.clone(), res)
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
        if round_index > 0 {
            crate::worker::record_current_stage_duration(
                crate::worker::TraceStage::Retry,
                round_started.elapsed(),
            );
        }
        round_index += 1;
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
    remote_store_stats.assigned_splits = assigned_split_ids.len();
    let shard_failures: Vec<_> = accumulator.failures.into_values().collect();
    if let Some(error) = all_shards_failed_response(accumulator.successful, &shard_failures) {
        return Err(error);
    }

    let merge_started = std::time::Instant::now();
    let all_hits =
        crate::search::merge_sorted_hit_lists(accumulator.split_hit_lists, &search_req.sort);
    let aggregations = if accumulator.partial_aggs.is_empty() {
        HashMap::new()
    } else {
        crate::search::merge_aggregations(accumulator.partial_aggs.clone(), &search_req.aggs)
    };
    crate::worker::record_current_stage_duration(
        crate::worker::TraceStage::PartialMerge,
        merge_started.elapsed(),
    );

    Ok(DistributedDslSearchResult {
        all_hits,
        total_hits: accumulator.total_hits,
        successful_shards: accumulator.successful,
        failed_shards: failed,
        shard_failures,
        aggregations,
        partial_aggs: accumulator.partial_aggs,
        remote_store_stats: Some(remote_store_stats),
    })
}

fn strip_split_wal(staging_dir: &std::path::Path) -> anyhow::Result<()> {
    for name in [
        "translog.manifest",
        "translog.committed",
        crate::engine::SEAL_WATERMARK_FILE,
    ] {
        let path = staging_dir.join(name);
        if path.try_exists()? {
            std::fs::remove_file(path)?;
        }
    }
    for entry in std::fs::read_dir(staging_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("translog-") && name.ends_with(".bin") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
/// Failures never publish a partial split. Build cleanup is best-effort; later
/// failures may leave staging files or an uploaded unreferenced bundle for a
/// future janitor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ForwardedSequencerCommitRequest {
    pub operation_id: String,
    pub request_fingerprint: String,
    pub schema_generation: u64,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<crate::cluster::state::RemoteStoreSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub known_authority: Option<crate::cluster::state::ManifestWriterAuthority>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum ForwardedSequencerCommitResponse {
    Committed(crate::storage::PublicationCommitOutcome),
    Conflict {
        operation_id: String,
        reason: String,
    },
    Indeterminate {
        operation_id: String,
        reason: String,
    },
    RetryableAuthority {
        reason: String,
    },
    Fatal {
        reason: String,
    },
}

fn current_raft_leader_term(
    raft: &crate::consensus::types::RaftInstance,
    authority_term: u64,
) -> u64 {
    openraft::async_runtime::WatchReceiver::borrow_watched(&raft.metrics())
        .current_term
        .max(authority_term)
        .max(1)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_local_sequencer_commit(
    cluster_manager: &crate::cluster::ClusterManager,
    transport_client: &crate::transport::TransportClient,
    raft: &crate::consensus::types::RaftInstance,
    storage_manager: &crate::storage::StorageManager,
    local_node_id: &str,
    index_name: &str,
    index_uuid: &str,
    payload: &ForwardedSequencerCommitRequest,
) -> ForwardedSequencerCommitResponse {
    use crate::cluster::state::{
        DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS, ManifestLeaseAcquireReason,
        ManifestWriterIdentity, ManifestWriterMutation, ManifestWriterPhase,
    };

    let lock_arc = storage_manager.sequencer_lock(index_uuid).await;
    let _sequencer_guard = lock_arc.lock().await;
    if let Some(ref auth) = payload.known_authority {
        storage_manager.observe_writer_authority(auth);
    }

    for _ in 0..4 {
        let cluster_state = cluster_manager.get_state();
        let Some(metadata) = cluster_state.indices.get(index_name) else {
            return ForwardedSequencerCommitResponse::Fatal {
                reason: format!("no such index [{index_name}]"),
            };
        };
        if metadata.uuid.as_str() != index_uuid {
            return ForwardedSequencerCommitResponse::Fatal {
                reason: format!(
                    "index [{index_name}] UUID mismatch: expected {index_uuid}, found {}",
                    metadata.uuid
                ),
            };
        }

        let now_ms = now_epoch_ms().max(1);
        if let Some(state_auth) = cluster_state.manifest_writer_authorities.get(index_name) {
            storage_manager.observe_writer_authority(state_auth);
        }
        let authority = storage_manager
            .cached_writer_authority(index_uuid)
            .unwrap_or_else(|| {
                crate::cluster::state::ManifestWriterAuthority::unassigned(index_name, index_uuid)
            });

        // If the cluster state in a unit test has no registered nodes at all,
        // fence and commit directly on the local storage manager.
        if cluster_state.nodes.is_empty() {
            let (fenced_ptr, _) = match storage_manager.fence_manifest_pointer(index_uuid, 1).await
            {
                Ok(res) => res,
                Err(error) => {
                    return classify_commit_error(&payload.operation_id, error);
                }
            };
            return match storage_manager
                .commit_prepared_publication_locked(&crate::storage::SequencerCommitRequest {
                    index_uuid: index_uuid.to_string(),
                    index_name: index_name.to_string(),
                    operation_id: payload.operation_id.clone(),
                    request_fingerprint: payload.request_fingerprint.clone(),
                    schema_generation: payload.schema_generation,
                    schema_hash: payload.schema_hash.clone(),
                    writer_epoch: fenced_ptr.writer_epoch,
                    lease_id: 1,
                    sequencer_node_id: local_node_id.to_string(),
                    fenced_pointer_revision: fenced_ptr.pointer_revision,
                    settings: payload.settings.clone(),
                })
                .await
            {
                Ok(outcome) => ForwardedSequencerCommitResponse::Committed(outcome),
                Err(error) => classify_commit_error(&payload.operation_id, error),
            };
        }
        if !cluster_state.nodes.contains_key(local_node_id) {
            return ForwardedSequencerCommitResponse::RetryableAuthority {
                reason: format!("node '{local_node_id}' is not a registered cluster member"),
            };
        }

        match &authority.phase {
            ManifestWriterPhase::Active {
                holder_node,
                writer_epoch,
                lease_id,
                fenced_pointer_revision,
                fenced_pointer_token,
            } if holder_node == local_node_id && now_ms < authority.lease_expires_at_millis => {
                let mut active_epoch = *writer_epoch;
                let mut active_lease_id = *lease_id;
                let mut active_fenced_rev = *fenced_pointer_revision;
                if now_ms.saturating_add(DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS / 3)
                    >= authority.lease_expires_at_millis
                {
                    let renew_mutation = ManifestWriterMutation::Renew {
                        identity: ManifestWriterIdentity {
                            index_uuid: index_uuid.to_string(),
                            holder_node: local_node_id.to_string(),
                            writer_epoch: active_epoch,
                            lease_id: active_lease_id,
                        },
                        expected_fenced_pointer_revision: *fenced_pointer_revision,
                        expected_fenced_pointer_token: fenced_pointer_token.clone(),
                        now_millis: now_ms,
                        lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                        leader_term: current_raft_leader_term(raft, authority.leader_term),
                    };
                    match transport_client
                        .propose_manifest_writer_authority_mutation(
                            raft,
                            cluster_manager,
                            index_name,
                            renew_mutation,
                        )
                        .await
                    {
                        Ok(renewed) => {
                            storage_manager.observe_writer_authority(&renewed);
                            if let ManifestWriterPhase::Active {
                                writer_epoch,
                                lease_id,
                                fenced_pointer_revision,
                                ..
                            } = renewed.phase
                            {
                                active_epoch = writer_epoch;
                                active_lease_id = lease_id;
                                active_fenced_rev = fenced_pointer_revision;
                            }
                        }
                        Err(error) => {
                            return ForwardedSequencerCommitResponse::RetryableAuthority {
                                reason: format!("failed to renew manifest writer lease: {error:#}"),
                            };
                        }
                    }
                }

                return match storage_manager
                    .commit_prepared_publication_locked(&crate::storage::SequencerCommitRequest {
                        index_uuid: index_uuid.to_string(),
                        index_name: index_name.to_string(),
                        operation_id: payload.operation_id.clone(),
                        request_fingerprint: payload.request_fingerprint.clone(),
                        schema_generation: payload.schema_generation,
                        schema_hash: payload.schema_hash.clone(),
                        writer_epoch: active_epoch,
                        lease_id: active_lease_id,
                        sequencer_node_id: local_node_id.to_string(),
                        fenced_pointer_revision: active_fenced_rev,
                        settings: payload.settings.clone(),
                    })
                    .await
                {
                    Ok(outcome) => ForwardedSequencerCommitResponse::Committed(outcome),
                    Err(error) => classify_commit_error(&payload.operation_id, error),
                };
            }
            ManifestWriterPhase::Fencing {
                holder_node,
                writer_epoch,
                lease_id,
            } if holder_node == local_node_id && now_ms < authority.lease_expires_at_millis => {
                let (fenced_ptr, fenced_token) = match storage_manager
                    .fence_manifest_pointer(index_uuid, *writer_epoch)
                    .await
                {
                    Ok(res) => res,
                    Err(error) => return classify_commit_error(&payload.operation_id, error),
                };
                let activate_now = now_epoch_ms().max(now_ms);
                let activate_mutation = ManifestWriterMutation::Activate {
                    identity: ManifestWriterIdentity {
                        index_uuid: index_uuid.to_string(),
                        holder_node: local_node_id.to_string(),
                        writer_epoch: *writer_epoch,
                        lease_id: *lease_id,
                    },
                    fenced_pointer_revision: fenced_ptr.pointer_revision,
                    fenced_pointer_token: fenced_token.as_str().to_string(),
                    now_millis: activate_now,
                    lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                    leader_term: current_raft_leader_term(raft, authority.leader_term),
                };
                match transport_client
                    .propose_manifest_writer_authority_mutation(
                        raft,
                        cluster_manager,
                        index_name,
                        activate_mutation,
                    )
                    .await
                {
                    Ok(activated) => {
                        storage_manager.observe_writer_authority(&activated);
                        continue;
                    }
                    Err(error) => {
                        return ForwardedSequencerCommitResponse::RetryableAuthority {
                            reason: format!("failed to activate manifest writer lease: {error:#}"),
                        };
                    }
                }
            }
            _ => {
                let reason = if authority.phase.is_unassigned() {
                    ManifestLeaseAcquireReason::Unassigned
                } else if authority
                    .phase
                    .holder_node()
                    .is_some_and(|holder| !cluster_state.nodes.contains_key(holder))
                {
                    ManifestLeaseAcquireReason::HolderUnavailable
                } else if now_ms >= authority.lease_expires_at_millis {
                    ManifestLeaseAcquireReason::Expired
                } else {
                    return ForwardedSequencerCommitResponse::RetryableAuthority {
                        reason: format!(
                            "manifest writer lease for '{index_name}' is held by {:?}",
                            authority.phase.holder_node()
                        ),
                    };
                };

                let acquire_mutation = ManifestWriterMutation::Acquire {
                    expected: authority.expected_snapshot(),
                    candidate_node: local_node_id.to_string(),
                    reason,
                    now_millis: now_ms,
                    lease_duration_ms: DEFAULT_MANIFEST_WRITER_LEASE_DURATION_MS,
                    leader_term: current_raft_leader_term(raft, authority.leader_term),
                };
                match transport_client
                    .propose_manifest_writer_authority_mutation(
                        raft,
                        cluster_manager,
                        index_name,
                        acquire_mutation,
                    )
                    .await
                {
                    Ok(acquired) => {
                        storage_manager.observe_writer_authority(&acquired);
                        continue;
                    }
                    Err(error) => {
                        return ForwardedSequencerCommitResponse::RetryableAuthority {
                            reason: format!("failed to acquire manifest writer lease: {error:#}"),
                        };
                    }
                }
            }
        }
    }

    ForwardedSequencerCommitResponse::RetryableAuthority {
        reason: format!(
            "exceeded local sequencer authority stabilization attempts for index '{index_name}'"
        ),
    }
}

fn classify_commit_error(
    operation_id: &str,
    error: anyhow::Error,
) -> ForwardedSequencerCommitResponse {
    if let Some(pub_err) = error.downcast_ref::<crate::storage::PublicationError>() {
        return match pub_err {
            crate::storage::PublicationError::OperationConflict {
                operation_id,
                reason,
            } => ForwardedSequencerCommitResponse::Conflict {
                operation_id: operation_id.clone(),
                reason: reason.clone(),
            },
            crate::storage::PublicationError::Indeterminate {
                operation_id,
                reason,
            } => ForwardedSequencerCommitResponse::Indeterminate {
                operation_id: operation_id.clone(),
                reason: reason.clone(),
            },
            crate::storage::PublicationError::StaleWriterEpoch { .. }
            | crate::storage::PublicationError::UnfencedPointer { .. }
            | crate::storage::PublicationError::PointerConflict { .. } => {
                ForwardedSequencerCommitResponse::RetryableAuthority {
                    reason: pub_err.to_string(),
                }
            }
            crate::storage::PublicationError::NotCommitted { .. }
            | crate::storage::PublicationError::InjectedCrash { .. } => {
                ForwardedSequencerCommitResponse::Fatal {
                    reason: pub_err.to_string(),
                }
            }
        };
    }
    if let Some(cond_err) = error.downcast_ref::<crate::storage::ConditionalStorageError>() {
        return match cond_err {
            crate::storage::ConditionalStorageError::Indeterminate { reason, .. } => {
                ForwardedSequencerCommitResponse::Indeterminate {
                    operation_id: operation_id.to_string(),
                    reason: reason.clone(),
                }
            }
            crate::storage::ConditionalStorageError::Conflict { reason, .. } => {
                ForwardedSequencerCommitResponse::RetryableAuthority {
                    reason: reason.clone(),
                }
            }
            other => ForwardedSequencerCommitResponse::Fatal {
                reason: other.to_string(),
            },
        };
    }
    ForwardedSequencerCommitResponse::Fatal {
        reason: format!("{error:#}"),
    }
}

async fn coordinate_sequencer_commit(
    state: &AppState,
    index_name: &str,
    index_uuid: &str,
    payload: &ForwardedSequencerCommitRequest,
) -> ForwardedSequencerCommitResponse {
    let mut last_retry_reason = String::new();
    for attempt in 0..8 {
        let cluster_state = state.cluster_manager.get_state();
        let now_ms = now_epoch_ms().max(1);
        let state_authority = cluster_state
            .manifest_writer_authorities
            .get(index_name)
            .cloned();
        if let Some(ref auth) = state_authority {
            state.storage_manager.observe_writer_authority(auth);
        }
        let authority = state.storage_manager.cached_writer_authority(index_uuid);

        let remote_holder = authority
            .as_ref()
            .and_then(|auth| {
                if now_ms < auth.lease_expires_at_millis {
                    auth.phase.holder_node()
                } else {
                    None
                }
            })
            .or_else(|| {
                // When unassigned and this node is a follower, route sequencing to the
                // Raft master node so concurrent initial publishes do not race Acquire across followers.
                if !state.raft.is_leader() {
                    cluster_state.master_node.as_deref()
                } else {
                    None
                }
            })
            .filter(|holder| *holder != state.local_node_id)
            .and_then(|holder| cluster_state.nodes.get(holder).cloned());

        let mut req_with_auth = payload.clone();
        req_with_auth.known_authority = authority;

        let response = if let Some(target_node) = remote_holder {
            match state
                .transport_client
                .forward_remote_store_publish_commit(
                    &target_node,
                    index_name,
                    index_uuid,
                    &req_with_auth,
                )
                .await
            {
                Ok(resp) => resp,
                Err(error) => ForwardedSequencerCommitResponse::RetryableAuthority {
                    reason: format!(
                        "forwarding publish commit to sequencer node '{}' failed: {error:#}",
                        target_node.id
                    ),
                },
            }
        } else {
            execute_local_sequencer_commit(
                &state.cluster_manager,
                &state.transport_client,
                &state.raft,
                &state.storage_manager,
                &state.local_node_id,
                index_name,
                index_uuid,
                &req_with_auth,
            )
            .await
        };

        match response {
            ForwardedSequencerCommitResponse::RetryableAuthority { reason } => {
                last_retry_reason = reason;
                if attempt + 1 < 8 {
                    tokio::time::sleep(Duration::from_millis(25 * (attempt as u64 + 1))).await;
                }
            }
            other => return other,
        }
    }

    ForwardedSequencerCommitResponse::RetryableAuthority {
        reason: last_retry_reason,
    }
}

fn outcome_to_publish_json(
    index_name: &str,
    outcome: &crate::storage::PublicationCommitOutcome,
) -> serde_json::Value {
    let mut json = serde_json::json!({
        "index": index_name,
        "index_uuid": outcome.index_uuid,
        "operation_id": outcome.operation_id,
        "split_id": outcome.split_id,
        "generation": outcome.generation,
        "parent_generation": outcome.parent_generation,
        "parent_manifest_path": outcome.parent_manifest_path,
        "manifest_path": outcome.manifest_path,
        "manifest_checksum": outcome.manifest_checksum,
        "writer_epoch": outcome.writer_epoch,
        "pointer_revision": outcome.pointer_revision,
        "replayed": outcome.replayed,
        "doc_count": outcome.doc_count,
        "size_bytes": outcome.size_bytes,
    });
    if !outcome.published_watermarks.is_empty() {
        json["published_watermarks"] =
            serde_json::to_value(&outcome.published_watermarks).unwrap_or(serde_json::Value::Null);
    }
    json
}

#[cfg(test)]
pub(crate) async fn publish_docs(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    docs: Vec<serde_json::Value>,
) -> Result<serde_json::Value, (StatusCode, Json<Value>)> {
    publish_docs_with_operation(state, index_name, metadata, docs, None).await
}

/// Publish a batch of JSON documents as a new split on this index using the
/// ADR 0002 fenced single-sequencer commit protocol.
#[cfg(test)]
pub(crate) async fn publish_docs_with_operation(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    docs: Vec<serde_json::Value>,
    requested_operation_id: Option<String>,
) -> Result<serde_json::Value, (StatusCode, Json<Value>)> {
    publish_docs_with_tombstones(
        state,
        index_name,
        metadata,
        docs,
        Vec::new(),
        requested_operation_id,
    )
    .await
}

pub(crate) async fn publish_docs_with_tombstones(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    docs: Vec<serde_json::Value>,
    tombstone_doc_ids: Vec<String>,
    requested_operation_id: Option<String>,
) -> Result<serde_json::Value, (StatusCode, Json<Value>)> {
    if docs.is_empty() && tombstone_doc_ids.is_empty() {
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
    if docs.len() + tombstone_doc_ids.len() > MAX_DOCS_PER_PUBLISH {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "too many documents in one publish: got {}, max is {}",
                docs.len() + tombstone_doc_ids.len(),
                MAX_DOCS_PER_PUBLISH
            ),
        ));
    }

    if let Some(ref op_id) = requested_operation_id
        && let Err(error) = crate::storage::validate_publication_operation_id(op_id)
    {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            error,
        ));
    }

    for doc in &docs {
        let Some(object) = doc.as_object() else {
            continue;
        };
        if let Err(error) = crate::common::validate_mapping_field_names(
            object
                .keys()
                .map(String::as_str)
                .filter(|field| *field != "_id"),
        ) {
            return Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "mapper_parsing_exception",
                error,
            ));
        }
    }

    let (field_ranges, field_terms) = match build_split_field_summaries(&docs, &metadata.mappings) {
        Ok(summaries) => summaries,
        Err(error) => {
            return Err(crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "mapper_parsing_exception",
                error,
            ));
        }
    };

    if let Err(error) = state.storage_manager.ensure_publication_supported().await {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "remote_store_publish_exception",
            error,
        ));
    }

    let index_uuid = metadata.uuid.as_str().to_string();
    let operation_id = requested_operation_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let split_id = crate::storage::deterministic_publication_split_id(&index_uuid, &operation_id);
    let staging_dir = state.storage_manager.staging_dir(&index_uuid, &split_id);

    // Extract or deterministically derive _id per doc from (index_uuid, operation_id, ordinal),
    // then strip _id from payload so stored source stays clean and retries produce identical splits.
    let mut prepared: Vec<(String, serde_json::Value)> = Vec::with_capacity(docs.len());
    let mut doc_versions: Vec<crate::storage::SplitDocVersionRecord> =
        Vec::with_capacity(docs.len());
    for (ordinal, mut doc) in docs.into_iter().enumerate() {
        let doc_id = if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
            id.to_string()
        } else {
            crate::storage::deterministic_publication_doc_id(&index_uuid, &operation_id, ordinal)
        };
        if let Some(obj) = doc.as_object_mut() {
            obj.remove("_id");
        }
        doc_versions.push(crate::storage::SplitDocVersionRecord {
            doc_id: doc_id.clone(),
            seq_no: 0,
            primary_term: 1,
            version: 1,
        });
        prepared.push((doc_id, doc));
    }
    let tombstones: Vec<crate::storage::SplitTombstoneRecord> = tombstone_doc_ids
        .into_iter()
        .filter(|id| !id.is_empty())
        .map(|doc_id| crate::storage::SplitTombstoneRecord {
            doc_id,
            seq_no: 0,
            primary_term: 1,
            version: 1,
        })
        .collect();

    let schema_generation = 1u64;
    let schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let request_fingerprint = if tombstones.is_empty() {
        match crate::storage::compute_publication_request_fingerprint(
            &index_uuid,
            schema_generation,
            &schema_hash,
            &prepared,
        ) {
            Ok(fp) => fp,
            Err(error) => {
                return Err(crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    error,
                ));
            }
        }
    } else {
        let canonical = serde_json::json!({
            "kind": "remote_store_publish_v1",
            "index_uuid": index_uuid,
            "schema_generation": schema_generation,
            "schema_hash": schema_hash,
            "docs": prepared,
            "tombstones": tombstones,
        });
        match crate::engine::ClientOperationIntent::fingerprint_json(&canonical) {
            Ok(raw) if raw.starts_with("sha256:") => raw,
            Ok(raw) => format!("sha256:{raw}"),
            Err(error) => {
                return Err(crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    error,
                ));
            }
        }
    };

    // Fast path: check if operation_id is already committed or conflicting.
    let (existing_intent, existing_prepared) = match state
        .storage_manager
        .resolve_publication_operation(&index_uuid, &operation_id, &request_fingerprint)
        .await
    {
        Ok(crate::storage::PublicationResolution::Committed(outcome)) => {
            crate::metrics::record_remote_store_publication_attempt("replayed");
            return Ok(outcome_to_publish_json(index_name, &outcome));
        }
        Ok(crate::storage::PublicationResolution::Conflict {
            operation_id,
            reason,
        }) => {
            crate::metrics::record_remote_store_publication_attempt("conflict");
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "operation_id_conflict",
                format!("operation_id [{operation_id}] conflict: {reason}"),
            ));
        }
        Ok(crate::storage::PublicationResolution::NotCommitted { intent, prepared }) => {
            (intent, prepared)
        }
        Err(error) => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!("failed to resolve publication operation '{operation_id}': {error:#}"),
            ));
        }
    };

    let doc_count = prepared.len() as u64;
    let intent_doc_count = (doc_count + tombstones.len() as u64).max(1);
    let planned_bundle_path =
        crate::storage::StorageManager::split_bundle_key(&index_uuid, &split_id);

    // Stage immutable intent.json before building or uploading the split bundle.
    if existing_intent.is_none() {
        let now_ms = now_epoch_ms();
        let intent = crate::storage::PublicationIntentRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: request_fingerprint.clone(),
            schema_generation,
            schema_hash: schema_hash.clone(),
            split_id: split_id.clone(),
            planned_bundle_path: planned_bundle_path.clone(),
            doc_count: intent_doc_count,
            producer_node_id: state.local_node_id.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            created_at_epoch_ms: now_ms,
        };
        if let Err(error) = state
            .storage_manager
            .stage_publication_intent(&intent)
            .await
        {
            if let Some(crate::storage::PublicationError::OperationConflict {
                operation_id,
                reason,
            }) = error.downcast_ref::<crate::storage::PublicationError>()
            {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!("operation_id [{operation_id}] conflict: {reason}"),
                ));
            }
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!("failed to stage publication intent: {error:#}"),
            ));
        }
    }

    if state
        .storage_manager
        .take_publication_crash_stage_if_matches(
            crate::storage::PublicationCrashStage::AfterIntentBeforeBundleUpload,
        )
    {
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_publish_exception",
            format!(
                "injected crash for operation_id [{operation_id}] after intent creation before bundle upload"
            ),
        ));
    }

    let prepared_already_valid = match existing_prepared.as_ref() {
        Some(prep) => state
            .storage_manager
            .verify_prepared_split_bundle(&index_uuid, prep)
            .await
            .is_ok(),
        None => false,
    };

    if !prepared_already_valid {
        let resumed_bundle = if existing_intent.is_some() {
            state
                .storage_manager
                .inspect_existing_split_bundle(&index_uuid, &split_id)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let (bundle_key, checksum, size_bytes) = if let Some(triple) = resumed_bundle {
            triple
        } else {
            let mappings_arc = Arc::new(metadata.mappings.clone());
            let staging_for_build = staging_dir.clone();
            let mappings_for_build = Arc::clone(&mappings_arc);

            // Build the split on the write worker pool — it's blocking I/O.
            let build_result = state
                .worker_pools
                .spawn_write(move || -> anyhow::Result<()> {
                    if staging_for_build.exists() {
                        std::fs::remove_dir_all(&staging_for_build)?;
                    }
                    std::fs::create_dir_all(&staging_for_build)?;
                    let column_cache =
                        Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let engine = crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                        &staging_for_build,
                        Duration::from_secs(60),
                        mappings_for_build.as_ref(),
                        column_cache,
                    )?;
                    use crate::engine::SearchEngine;
                    if !prepared.is_empty() {
                        engine.bulk_add_documents(prepared)?;
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
                if e.is::<DocumentValidationError>()
                    || e.is::<crate::common::ReservedDocumentFieldError>()
                {
                    return Err(crate::api::error_response(
                        StatusCode::BAD_REQUEST,
                        "mapper_parsing_exception",
                        e,
                    ));
                }
                tracing::error!(
                    "remote_store: split build failed for {} split_id {}: {}",
                    index_name,
                    split_id,
                    e
                );
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!("split build failed: {e}"),
                ));
            }

            // Immutable bundles cannot retain mutable WAL checkpoints without their WAL.
            let staging_for_strip = staging_dir.clone();
            let strip_result = state
                .worker_pools
                .spawn_write(move || strip_split_wal(&staging_for_strip))
                .await;
            if let Err(e) = match strip_result {
                Ok(inner) => inner,
                Err(join_err) => Err(anyhow::anyhow!(join_err)),
            } {
                tracing::error!(
                    "remote_store: failed to strip mutable WAL state from split {} of {}: {:#}",
                    split_id,
                    index_name,
                    e
                );
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!("failed to remove mutable WAL state before split publication: {e:#}"),
                ));
            }

            let triple = match state
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
                        format!("bundle upload failed: {e}"),
                    ));
                }
            };

            let _ = std::fs::remove_dir_all(&staging_dir);
            triple
        };

        if state
            .storage_manager
            .take_publication_crash_stage_if_matches(
                crate::storage::PublicationCrashStage::AfterBundleUploadBeforePreparedRecord,
            )
        {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!(
                    "injected crash for operation_id [{operation_id}] after bundle upload before prepared record"
                ),
            ));
        }

        let mut tags = std::collections::BTreeMap::new();
        tags.insert("operation_id".to_string(), operation_id.clone());
        let mut new_split = crate::storage::RemoteSplitManifest {
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
            tags,
            field_ranges,
            field_terms,
        };
        new_split.set_doc_versions_and_tombstones(&doc_versions, &tombstones);
        let now_ms = now_epoch_ms();
        let prepared_record = crate::storage::PublicationPreparedRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: request_fingerprint.clone(),
            schema_generation,
            schema_hash: schema_hash.clone(),
            producer_node_id: state.local_node_id.clone(),
            prepared_at: chrono::Utc::now().to_rfc3339(),
            prepared_at_epoch_ms: now_ms,
            split: new_split,
        };
        if let Err(error) = state
            .storage_manager
            .write_publication_prepared(&prepared_record)
            .await
        {
            if let Some(crate::storage::PublicationError::OperationConflict {
                operation_id,
                reason,
            }) = error.downcast_ref::<crate::storage::PublicationError>()
            {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!("operation_id [{operation_id}] conflict: {reason}"),
                ));
            }
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!("failed to write prepared publication record: {error:#}"),
            ));
        }
    }

    let sequencer_req = ForwardedSequencerCommitRequest {
        operation_id: operation_id.clone(),
        request_fingerprint,
        schema_generation,
        schema_hash,
        settings: metadata.settings.remote_store.clone(),
        known_authority: None,
    };

    match coordinate_sequencer_commit(state, index_name, &index_uuid, &sequencer_req).await {
        ForwardedSequencerCommitResponse::Committed(outcome) => {
            Ok(outcome_to_publish_json(index_name, &outcome))
        }
        ForwardedSequencerCommitResponse::Conflict {
            operation_id,
            reason,
        } => Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "operation_id_conflict",
            format!("operation_id [{operation_id}] conflict: {reason}"),
        )),
        ForwardedSequencerCommitResponse::Indeterminate {
            operation_id,
            reason,
        } => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": {
                    "type": "remote_store_publish_indeterminate_exception",
                    "reason": format!("publication outcome for operation_id [{operation_id}] is indeterminate; retry with the same operation_id: {reason}"),
                    "operation_id": operation_id,
                },
                "status": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            })),
        )),
        ForwardedSequencerCommitResponse::RetryableAuthority { reason } => {
            Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "remote_store_publish_exception",
                format!("manifest publish failed after sequencer authority contention: {reason}"),
            ))
        }
        ForwardedSequencerCommitResponse::Fatal { reason } => Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_publish_exception",
            format!("manifest publish failed: {reason}"),
        )),
    }
}

pub(crate) async fn inspect_inventory(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !matches!(
        metadata.settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    ) && !metadata.is_tiered()
    {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] uses engine [{}] which does not support remote_store inventory",
                index_name, metadata.settings.engine
            ),
        ));
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut options = crate::storage::ReachabilityInventoryOptions::for_index(
        metadata.uuid.as_str(),
        Some(index_name.to_string()),
    );
    options.active_writer_authority = cluster_state
        .manifest_writer_authorities
        .get(index_name)
        .cloned();

    if let Some(ref obj) = body.and_then(|v| v.as_object().cloned()) {
        if let Some(gen_cap) = obj.get("retention_generations").and_then(Value::as_u64) {
            options.retention_generations = gen_cap as usize;
        }
        if let Some(horizon_ms) = obj.get("retention_horizon_ms").and_then(Value::as_u64) {
            options.retention_horizon_ms = horizon_ms;
        }
        if let Some(grace_ms) = obj.get("active_grace_ms").and_then(Value::as_u64) {
            options.active_grace_ms = grace_ms;
        }
        if let Some(now_ms) = obj.get("now_epoch_ms").and_then(Value::as_u64) {
            options.now_epoch_ms = now_ms;
        }
        if let Some(max_objs) = obj.get("max_reported_objects").and_then(Value::as_u64) {
            options.max_reported_objects = max_objs as usize;
        }
        if let Some(pinned_splits) = obj.get("pinned_split_ids").and_then(Value::as_array) {
            for item in pinned_splits {
                if let Some(s) = item.as_str() {
                    options.pinned_split_ids.insert(s.to_string());
                }
            }
        }
        if let Some(pinned_gens) = obj.get("pinned_generations").and_then(Value::as_array) {
            for item in pinned_gens {
                if let Some(g) = item.as_u64() {
                    options.pinned_generations.insert(g);
                }
            }
        }
    }

    if let Ok(Some(current_manifest)) = state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), None)
        .await
    {
        let mut keep_split_ids: HashSet<String> = current_manifest
            .published_splits()
            .map(|s| s.split_id.clone())
            .collect();
        keep_split_ids.extend(options.pinned_split_ids.iter().cloned());
        for lease in state
            .storage_manager
            .active_snapshot_leases_for_index(metadata.uuid.as_str(), options.now_epoch_ms)
        {
            keep_split_ids.extend(lease.pinned_split_ids);
        }
        if options.pinned_generations.is_empty() {
            let budget_bytes = metadata
                .settings
                .remote_store
                .as_ref()
                .and_then(|s| s.split_cache_bytes);
            state.remote_store_reader_cache.evict_for_index(
                metadata.uuid.as_str(),
                &keep_split_ids,
                budget_bytes,
            );
        }
    }

    let report = state
        .storage_manager
        .build_reachability_inventory(&options)
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_inventory_exception",
                format!("failed to build reachability inventory: {error:#}"),
            )
        })?;
    serde_json::to_value(report).map_err(|error| {
        crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_exception",
            error,
        )
    })
}

pub(crate) async fn run_janitor(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !matches!(
        metadata.settings.engine,
        crate::cluster::state::IndexEngine::RemoteStore
    ) && !metadata.is_tiered()
    {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] uses engine [{}] which does not support remote_store janitor",
                index_name, metadata.settings.engine
            ),
        ));
    }

    let cluster_state = state.cluster_manager.get_state();
    let mut janitor_opts = crate::storage::JanitorOptions::dry_run_for_index(
        metadata.uuid.as_str(),
        Some(index_name.to_string()),
    );
    janitor_opts.inventory_options.active_writer_authority = cluster_state
        .manifest_writer_authorities
        .get(index_name)
        .cloned();

    if let Some(ref obj) = body.and_then(|v| v.as_object().cloned()) {
        if let Some(dry_run) = obj.get("dry_run").and_then(Value::as_bool) {
            janitor_opts.dry_run = dry_run;
        }
        if let Some(max_del) = obj.get("max_deletes_per_run").and_then(Value::as_u64) {
            janitor_opts.max_deletes_per_run = max_del as usize;
        }
        if let Some(max_bytes) = obj.get("max_delete_bytes_per_run").and_then(Value::as_u64) {
            janitor_opts.max_delete_bytes_per_run = max_bytes;
        }
        if let Some(sweep_grace) = obj.get("sweep_grace_ms").and_then(Value::as_u64) {
            janitor_opts.sweep_grace_ms = sweep_grace;
        }
        if let Some(gen_cap) = obj.get("retention_generations").and_then(Value::as_u64) {
            janitor_opts.inventory_options.retention_generations = gen_cap as usize;
        }
        if let Some(horizon_ms) = obj.get("retention_horizon_ms").and_then(Value::as_u64) {
            janitor_opts.inventory_options.retention_horizon_ms = horizon_ms;
        }
        if let Some(grace_ms) = obj.get("active_grace_ms").and_then(Value::as_u64) {
            janitor_opts.inventory_options.active_grace_ms = grace_ms;
        }
        if let Some(now_ms) = obj.get("now_epoch_ms").and_then(Value::as_u64) {
            janitor_opts.inventory_options.now_epoch_ms = now_ms;
        }
        if let Some(pinned_splits) = obj.get("pinned_split_ids").and_then(Value::as_array) {
            for item in pinned_splits {
                if let Some(s) = item.as_str() {
                    janitor_opts
                        .inventory_options
                        .pinned_split_ids
                        .insert(s.to_string());
                }
            }
        }
        if let Some(pinned_gens) = obj.get("pinned_generations").and_then(Value::as_array) {
            for item in pinned_gens {
                if let Some(g) = item.as_u64() {
                    janitor_opts.inventory_options.pinned_generations.insert(g);
                }
            }
        }
    }

    if let Ok(Some(current_manifest)) = state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), None)
        .await
    {
        let mut keep_split_ids: HashSet<String> = current_manifest
            .published_splits()
            .map(|s| s.split_id.clone())
            .collect();
        keep_split_ids.extend(
            janitor_opts
                .inventory_options
                .pinned_split_ids
                .iter()
                .cloned(),
        );
        for lease in state.storage_manager.active_snapshot_leases_for_index(
            metadata.uuid.as_str(),
            janitor_opts.inventory_options.now_epoch_ms,
        ) {
            keep_split_ids.extend(lease.pinned_split_ids);
        }
        if janitor_opts.inventory_options.pinned_generations.is_empty() {
            let budget_bytes = metadata
                .settings
                .remote_store
                .as_ref()
                .and_then(|s| s.split_cache_bytes);
            state.remote_store_reader_cache.evict_for_index(
                metadata.uuid.as_str(),
                &keep_split_ids,
                budget_bytes,
            );
        }
    }

    let report = state
        .storage_manager
        .run_janitor(&janitor_opts)
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_janitor_exception",
                format!("failed to run object-store janitor: {error:#}"),
            )
        })?;
    serde_json::to_value(report).map_err(|error| {
        crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_exception",
            error,
        )
    })
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
    ) && !metadata.is_tiered()
    {
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
                format!("failed to load manifest: {e}"),
            ));
        }
    };

    let index_uuid = metadata.uuid.as_str();
    let splits: Vec<_> = manifest.published_splits().cloned().collect();

    let mut ok_count = 0u32;
    let mut mismatch_count = 0u32;
    let mut missing_count = 0u32;
    let mut split_reports: Vec<Value> = Vec::with_capacity(splits.len());

    for split in splits {
        let split_id = split.split_id.clone();
        let expected = split.checksum.clone();

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
        "unsupported_count": 0,
    }))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HotDeltaSealOptions {
    #[serde(default = "default_seal_force")]
    pub force: bool,
    #[serde(default = "default_seal_retire")]
    pub retire_after_commit: bool,
    #[serde(default)]
    pub max_docs: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub elapsed_age_ms: Option<u64>,
}

const fn default_seal_force() -> bool {
    true
}

const fn default_seal_retire() -> bool {
    true
}

impl Default for HotDeltaSealOptions {
    fn default() -> Self {
        Self {
            force: true,
            retire_after_commit: true,
            max_docs: None,
            max_bytes: None,
            elapsed_age_ms: None,
        }
    }
}

pub fn deterministic_hot_delta_seal_operation_id(
    index_uuid: &str,
    shard_id: u32,
    start_seq_no: u64,
    through_seq_no: u64,
) -> String {
    let raw = format!("seal-{index_uuid}-s{shard_id}-{start_seq_no}-{through_seq_no}");
    if raw.len() <= 120 {
        raw
    } else {
        let mut hasher = sha2::Sha256::new();
        use sha2::Digest;
        hasher.update(raw.as_bytes());
        let hex: String = hasher
            .finalize()
            .iter()
            .take(16)
            .map(|b| format!("{b:02x}"))
            .collect();
        format!("seal-s{shard_id}-{start_seq_no}-{through_seq_no}-{hex}")
    }
}

/// Reconcile local hot-delta retirement against the committed manifest's
/// `published_watermarks` (`FS-036`). Idempotent across retries and restarts.
pub(crate) async fn reconcile_published_watermark_retirement(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
) -> anyhow::Result<BTreeMap<u32, usize>> {
    let mut retired_by_shard = BTreeMap::new();
    if !metadata.is_tiered() {
        return Ok(retired_by_shard);
    }
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let Some(manifest) = state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), Some(&expected_schema_hash))
        .await?
    else {
        return Ok(retired_by_shard);
    };
    if manifest.published_watermarks.is_empty() {
        return Ok(retired_by_shard);
    }
    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        metadata,
        "reconcile published watermark retirement",
    )
    .await;
    for (shard_id, engine) in local_shards {
        let Some(wm) = manifest.published_watermarks.get(&shard_id) else {
            continue;
        };
        let target_seq = wm.sealed_through_seq_no;
        let generation = manifest.generation;
        let retired = state
            .worker_pools
            .spawn_maintenance("retire_sealed_delta", move || {
                engine.retire_sealed_delta_through(target_seq, generation)
            })
            .await?;
        retired_by_shard.insert(shard_id, retired);
    }
    Ok(retired_by_shard)
}

/// Seal eligible hot-delta ranges on local primary shards into immutable splits
/// and commit the updated publication visibility watermark (`FS-034`, `FS-035`, `FS-036`).
pub(crate) async fn seal_hot_delta(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if !metadata.is_tiered() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] has lifecycle_mode [{}] which does not support hot-delta sealing (requires a tiered index)",
                index_name,
                metadata.lifecycle_mode()
            ),
        ));
    }

    let options: HotDeltaSealOptions = match body {
        Some(v) if !v.is_null() => serde_json::from_value(v).map_err(|error| {
            crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("invalid seal request body: {error}"),
            )
        })?,
        _ => HotDeltaSealOptions::default(),
    };

    if let Err(error) = state.storage_manager.ensure_publication_supported().await {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "remote_store_publish_exception",
            error,
        ));
    }

    let policy = metadata
        .effective_hot_delta_seal_policy()
        .unwrap_or_default();
    let max_seal_docs = options
        .max_docs
        .unwrap_or_else(|| policy.effective_max_seal_docs())
        .max(1);
    let max_seal_bytes = options
        .max_bytes
        .unwrap_or_else(|| policy.effective_max_seal_bytes())
        .max(1);

    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let current_manifest = state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), Some(&expected_schema_hash))
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load current manifest before sealing: {error:#}"),
            )
        })?;

    let local_shards = crate::api::index::ensure_local_index_shards_open(
        state,
        index_name,
        metadata,
        "hot-delta seal",
    )
    .await;

    let mut shard_reports = Vec::new();
    let mut splits_sealed = 0usize;
    let mut total_docs_sealed = 0u64;
    let mut total_tombstones_sealed = 0u64;

    for (shard_id, routing) in &metadata.shard_routing {
        if routing.primary != state.local_node_id {
            continue;
        }
        let Some((_, engine)) = local_shards.iter().find(|(sid, _)| sid == shard_id) else {
            return Err(crate::api::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_not_available_exception",
                format!("primary shard [{index_name}][{shard_id}] is not open on local node"),
            ));
        };

        // First, if a previous manifest commit already advanced this shard's
        // published watermark beyond `retired_through_seq_no`, reconcile retirement.
        let mut wm_state = engine.seal_watermark_state().map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_seal_exception",
                format!("failed to load seal watermark state: {error:#}"),
            )
        })?;
        let committed_wm = current_manifest
            .as_ref()
            .and_then(|m| m.published_watermarks.get(shard_id))
            .cloned();
        if let Some(ref cwm) = committed_wm
            && options.retire_after_commit
            && wm_state
                .retired_through_seq_no
                .is_none_or(|r| r < cwm.sealed_through_seq_no)
        {
            let engine_for_retire = engine.clone();
            let target_seq = cwm.sealed_through_seq_no;
            let generation = cwm.updated_at_generation;
            state
                .worker_pools
                .spawn_maintenance("reconcile_retire_sealed_delta", move || {
                    engine_for_retire.retire_sealed_delta_through(target_seq, generation)
                })
                .await
                .map_err(|error| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_seal_exception",
                        format!("failed to reconcile post-commit delta retirement: {error:#}"),
                    )
                })?;
            wm_state = engine.seal_watermark_state().map_err(|error| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    format!("failed to reload seal watermark state: {error:#}"),
                )
            })?;
        }

        let after_seq_no = match (
            committed_wm.as_ref().map(|w| w.sealed_through_seq_no),
            wm_state.sealed_through_seq_no,
        ) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        // Check if there is an interrupted ActiveSealBarrier to resume for the next range.
        let resumed_barrier = wm_state.active_seal.clone().filter(|active| {
            let expected_start = after_seq_no.map_or(0, |s| s.saturating_add(1));
            active.start_seq_no == expected_start && active.through_seq_no >= active.start_seq_no
        });

        let delta_stats = engine.unsealed_delta_stats().map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_seal_exception",
                format!("failed to inspect unsealed delta stats: {error:#}"),
            )
        })?;

        let candidate_opt = if let Some(ref barrier) = resumed_barrier {
            let max_ops_for_resume =
                usize::try_from(barrier.through_seq_no - barrier.start_seq_no + 1)
                    .unwrap_or(usize::MAX);
            engine
                .enumerate_seal_boundary(after_seq_no, max_ops_for_resume, u64::MAX)
                .map_err(|error| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_seal_exception",
                        format!("failed to re-enumerate active seal barrier: {error:#}"),
                    )
                })?
        } else {
            engine
                .enumerate_seal_boundary(after_seq_no, max_seal_docs, max_seal_bytes)
                .map_err(|error| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_seal_exception",
                        format!("failed to enumerate hot-delta seal boundary: {error:#}"),
                    )
                })?
        };

        let Some(candidate) = candidate_opt else {
            let trigger_label = if options.force { "manual" } else { "auto_docs" };
            crate::metrics::HOT_DELTA_SEALS_TOTAL
                .with_label_values(&[trigger_label, "skipped_empty"])
                .inc();
            shard_reports.push(serde_json::json!({
                "shard_id": shard_id,
                "status": "skipped_empty",
                "sealed_through_seq_no": after_seq_no,
                "retired_through_seq_no": wm_state.retired_through_seq_no,
            }));
            continue;
        };

        let trigger_label = if options.force {
            "manual"
        } else if candidate.operation_count >= policy.effective_min_seal_docs() {
            "auto_docs"
        } else if candidate.estimated_bytes >= policy.effective_min_seal_bytes() {
            "auto_bytes"
        } else {
            let age_ms = options
                .elapsed_age_ms
                .unwrap_or(delta_stats.oldest_unsealed_age_ms);
            if policy.effective_max_seal_age_ms() > 0
                && age_ms >= policy.effective_max_seal_age_ms()
            {
                "auto_age"
            } else {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&["auto_docs", "skipped_policy"])
                    .inc();
                shard_reports.push(serde_json::json!({
                    "shard_id": shard_id,
                    "status": "skipped_policy",
                    "unsealed_ops": candidate.operation_count,
                    "unsealed_bytes": candidate.estimated_bytes,
                    "min_seal_docs": policy.effective_min_seal_docs(),
                    "min_seal_bytes": policy.effective_min_seal_bytes(),
                    "max_seal_age_ms": policy.effective_max_seal_age_ms(),
                    "sealed_through_seq_no": after_seq_no,
                    "retired_through_seq_no": wm_state.retired_through_seq_no,
                }));
                continue;
            }
        };

        let index_uuid = metadata.uuid.as_str().to_string();
        let operation_id = resumed_barrier
            .as_ref()
            .map(|b| b.operation_id.clone())
            .unwrap_or_else(|| {
                deterministic_hot_delta_seal_operation_id(
                    &index_uuid,
                    *shard_id,
                    candidate.start_seq_no,
                    candidate.through_seq_no,
                )
            });
        crate::worker::set_current_trace_operation_id(&operation_id);

        let barrier = crate::engine::ActiveSealBarrier {
            seal_id: operation_id.clone(),
            operation_id: operation_id.clone(),
            start_seq_no: candidate.start_seq_no,
            through_seq_no: candidate.through_seq_no,
            primary_term: candidate.primary_term,
            live_doc_count: candidate.live_doc_count(),
            tombstone_count: candidate.tombstone_count(),
            estimated_bytes: candidate.estimated_bytes,
            created_at_epoch_ms: now_epoch_ms(),
        };
        engine
            .record_active_seal_barrier(Some(barrier))
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    format!("failed to persist active seal barrier: {error:#}"),
                )
            })?;

        // Reserve Memory and TempDisk from ResourceGovernor (FS-031) and yield under foreground pressure (FS-032).
        let current_ctx = crate::worker::current_request_context();
        state
            .worker_pools
            .admission()
            .cooperative_background_yield(
                crate::worker::WorkClass::Maintenance,
                current_ctx.as_ref(),
            )
            .await
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rejected_execution_exception",
                    error,
                )
            })?;
        let mut mem_reservation = state
            .worker_pools
            .resource_governor()
            .try_reserve(
                crate::worker::ResourcePoolKind::Memory,
                candidate.estimated_bytes.max(1024),
            )
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rejected_execution_exception",
                    error,
                )
            })?;
        let mut temp_disk_reservation = state
            .worker_pools
            .resource_governor()
            .try_reserve(
                crate::worker::ResourcePoolKind::TempDisk,
                candidate.estimated_bytes.max(1024),
            )
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rejected_execution_exception",
                    error,
                )
            })?;

        let mut live_docs: Vec<(String, serde_json::Value)> = Vec::new();
        let mut raw_docs_for_summaries: Vec<serde_json::Value> = Vec::new();
        let mut doc_versions: Vec<crate::storage::SplitDocVersionRecord> = Vec::new();
        let mut tombstones: Vec<crate::storage::SplitTombstoneRecord> = Vec::new();
        for rec in &candidate.records {
            match rec {
                crate::engine::SealedDocumentRecord::Live {
                    doc_id,
                    source,
                    seq_no,
                    primary_term,
                    version,
                } => {
                    live_docs.push((doc_id.clone(), source.clone()));
                    raw_docs_for_summaries.push(source.clone());
                    doc_versions.push(crate::storage::SplitDocVersionRecord {
                        doc_id: doc_id.clone(),
                        seq_no: *seq_no,
                        primary_term: *primary_term,
                        version: *version,
                    });
                }
                crate::engine::SealedDocumentRecord::Tombstone {
                    doc_id,
                    seq_no,
                    primary_term,
                    version,
                } => {
                    tombstones.push(crate::storage::SplitTombstoneRecord {
                        doc_id: doc_id.clone(),
                        seq_no: *seq_no,
                        primary_term: *primary_term,
                        version: *version,
                    });
                }
            }
        }

        let seal_range_meta = crate::storage::SplitSealRangeMetadata {
            shard_id: *shard_id,
            start_seq_no: candidate.start_seq_no,
            end_seq_no: candidate.through_seq_no,
            primary_term: candidate.primary_term,
            doc_versions,
            tombstones,
        };

        let (field_ranges, field_terms) = build_split_field_summaries(
            &raw_docs_for_summaries,
            &metadata.mappings,
        )
        .map_err(|error| {
            crate::api::error_response(StatusCode::BAD_REQUEST, "mapper_parsing_exception", error)
        })?;

        let schema_generation = 1u64;
        let schema_hash = expected_schema_hash.clone();
        let fingerprint_payload = serde_json::json!({
            "kind": "hot_delta_seal_v1",
            "index_uuid": index_uuid,
            "shard_id": shard_id,
            "start_seq_no": candidate.start_seq_no,
            "through_seq_no": candidate.through_seq_no,
            "primary_term": candidate.primary_term,
            "schema_generation": schema_generation,
            "schema_hash": schema_hash,
            "live_docs": live_docs,
            "tombstones": seal_range_meta.tombstones,
        });
        let raw_fp = crate::engine::ClientOperationIntent::fingerprint_json(&fingerprint_payload)
            .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_seal_exception",
                error,
            )
        })?;
        let request_fingerprint = if raw_fp.starts_with("sha256:") {
            raw_fp
        } else {
            format!("sha256:{raw_fp}")
        };

        let split_id =
            crate::storage::deterministic_publication_split_id(&index_uuid, &operation_id);
        let staging_dir = state.storage_manager.staging_dir(&index_uuid, &split_id);

        let (existing_intent, existing_prepared) = match state
            .storage_manager
            .resolve_publication_operation(&index_uuid, &operation_id, &request_fingerprint)
            .await
        {
            Ok(crate::storage::PublicationResolution::Committed(outcome)) => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "replayed"])
                    .inc();
                if options.retire_after_commit {
                    let engine_for_retire = engine.clone();
                    let target_seq = candidate.through_seq_no;
                    let generation = outcome.generation;
                    let _ = state
                        .worker_pools
                        .spawn_maintenance("retire_replayed_seal", move || {
                            engine_for_retire.retire_sealed_delta_through(target_seq, generation)
                        })
                        .await;
                }
                let updated_wm = engine.seal_watermark_state().unwrap_or_default();
                splits_sealed += 1;
                shard_reports.push(serde_json::json!({
                    "shard_id": shard_id,
                    "status": "replayed",
                    "operation_id": operation_id,
                    "split_id": outcome.split_id,
                    "generation": outcome.generation,
                    "start_seq_no": candidate.start_seq_no,
                    "through_seq_no": candidate.through_seq_no,
                    "sealed_through_seq_no": updated_wm.sealed_through_seq_no,
                    "retired_through_seq_no": updated_wm.retired_through_seq_no,
                }));
                continue;
            }
            Ok(crate::storage::PublicationResolution::Conflict {
                operation_id,
                reason,
            }) => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!("seal operation_id [{operation_id}] conflict: {reason}"),
                ));
            }
            Ok(crate::storage::PublicationResolution::NotCommitted { intent, prepared }) => {
                (intent, prepared)
            }
            Err(error) => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_seal_exception",
                    format!("failed to resolve seal operation '{operation_id}': {error:#}"),
                ));
            }
        };

        let live_doc_count = live_docs.len() as u64;
        let intent_doc_count = (live_doc_count + candidate.tombstone_count()).max(1);
        let planned_bundle_path =
            crate::storage::StorageManager::split_bundle_key(&index_uuid, &split_id);

        if existing_intent.is_none() {
            let now_ms = now_epoch_ms();
            let intent = crate::storage::PublicationIntentRecord {
                format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
                index_uuid: index_uuid.clone(),
                index_name: index_name.to_string(),
                operation_id: operation_id.clone(),
                request_fingerprint: request_fingerprint.clone(),
                schema_generation,
                schema_hash: schema_hash.clone(),
                split_id: split_id.clone(),
                planned_bundle_path: planned_bundle_path.clone(),
                doc_count: intent_doc_count,
                producer_node_id: state.local_node_id.clone(),
                created_at: chrono::Utc::now().to_rfc3339(),
                created_at_epoch_ms: now_ms,
            };
            state
                .storage_manager
                .stage_publication_intent(&intent)
                .await
                .map_err(|error| {
                    crate::metrics::HOT_DELTA_SEALS_TOTAL
                        .with_label_values(&[trigger_label, "failed"])
                        .inc();
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_seal_exception",
                        format!("failed to stage seal intent: {error:#}"),
                    )
                })?;
        }

        if state
            .storage_manager
            .take_publication_crash_stage_if_matches(
                crate::storage::PublicationCrashStage::AfterIntentBeforeBundleUpload,
            )
        {
            crate::metrics::HOT_DELTA_SEALS_TOTAL
                .with_label_values(&[trigger_label, "failed"])
                .inc();
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!(
                    "injected crash for operation_id [{operation_id}] after intent creation before bundle upload"
                ),
            ));
        }

        let prepared_already_valid = match existing_prepared.as_ref() {
            Some(prep) => state
                .storage_manager
                .verify_prepared_split_bundle(&index_uuid, prep)
                .await
                .is_ok(),
            None => false,
        };

        if !prepared_already_valid {
            let mappings_for_build = Arc::new(metadata.mappings.clone());
            let staging_for_build = staging_dir.clone();
            let seal_meta_for_build = seal_range_meta.clone();
            let build_res = state
                .worker_pools
                .spawn_maintenance("build_sealed_split", move || {
                    if staging_for_build.exists() {
                        std::fs::remove_dir_all(&staging_for_build)?;
                    }
                    std::fs::create_dir_all(&staging_for_build)?;
                    let column_cache =
                        Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let split_engine =
                        crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                            &staging_for_build,
                            Duration::from_secs(60),
                            mappings_for_build.as_ref(),
                            column_cache,
                        )?;
                    if !live_docs.is_empty() {
                        split_engine.bulk_add_documents(live_docs)?;
                    }
                    split_engine.flush()?;
                    strip_split_wal(&staging_for_build)?;
                    let versions_bytes = serde_json::to_vec_pretty(&seal_meta_for_build)?;
                    std::fs::write(staging_for_build.join("seal.versions.json"), versions_bytes)?;
                    Ok(())
                })
                .await;

            if let Err(error) = build_res {
                let _ = std::fs::remove_dir_all(&staging_dir);
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!("sealed split build failed: {error:#}"),
                ));
            }

            let (bundle_key, checksum, size_bytes) = state
                .storage_manager
                .upload_split_bundle(&index_uuid, &split_id, &staging_dir)
                .await
                .map_err(|error| {
                    let _ = std::fs::remove_dir_all(&staging_dir);
                    crate::metrics::HOT_DELTA_SEALS_TOTAL
                        .with_label_values(&[trigger_label, "failed"])
                        .inc();
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_build_exception",
                        format!("sealed bundle upload failed: {error:#}"),
                    )
                })?;
            let _ = std::fs::remove_dir_all(&staging_dir);
            let _ = mem_reservation.reconcile(size_bytes.max(1));
            let _ = temp_disk_reservation.reconcile(size_bytes.max(1));

            if state
                .storage_manager
                .take_publication_crash_stage_if_matches(
                    crate::storage::PublicationCrashStage::AfterBundleUploadBeforePreparedRecord,
                )
            {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_publish_exception",
                    format!(
                        "injected crash for operation_id [{operation_id}] after bundle upload before prepared record"
                    ),
                ));
            }

            let mut tags = std::collections::BTreeMap::new();
            tags.insert("operation_id".to_string(), operation_id.clone());
            let mut new_split = crate::storage::RemoteSplitManifest {
                split_id: split_id.clone(),
                state: crate::storage::RemoteSplitState::Published,
                bundle_path: bundle_key,
                bundle_etag: None,
                hotcache_path: None,
                checksum,
                doc_count: live_doc_count,
                size_bytes,
                uncompressed_bytes: size_bytes,
                hotcache_bytes: None,
                time_range: None,
                tags,
                field_ranges,
                field_terms,
            };
            new_split.set_seal_range(&seal_range_meta);

            let now_ms = now_epoch_ms();
            let prepared_record = crate::storage::PublicationPreparedRecord {
                format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
                index_uuid: index_uuid.clone(),
                index_name: index_name.to_string(),
                operation_id: operation_id.clone(),
                request_fingerprint: request_fingerprint.clone(),
                schema_generation,
                schema_hash: schema_hash.clone(),
                producer_node_id: state.local_node_id.clone(),
                prepared_at: chrono::Utc::now().to_rfc3339(),
                prepared_at_epoch_ms: now_ms,
                split: new_split,
            };
            state
                .storage_manager
                .write_publication_prepared(&prepared_record)
                .await
                .map_err(|error| {
                    crate::metrics::HOT_DELTA_SEALS_TOTAL
                        .with_label_values(&[trigger_label, "failed"])
                        .inc();
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_publish_exception",
                        format!("failed to write prepared seal record: {error:#}"),
                    )
                })?;
        }
        drop(mem_reservation);
        drop(temp_disk_reservation);

        let sequencer_req = ForwardedSequencerCommitRequest {
            operation_id: operation_id.clone(),
            request_fingerprint,
            schema_generation,
            schema_hash: schema_hash.clone(),
            settings: metadata.settings.remote_store.clone(),
            known_authority: None,
        };

        let outcome = match coordinate_sequencer_commit(
            state,
            index_name,
            &index_uuid,
            &sequencer_req,
        )
        .await
        {
            ForwardedSequencerCommitResponse::Committed(outcome) => outcome,
            ForwardedSequencerCommitResponse::Conflict {
                operation_id,
                reason,
            } => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!("operation_id [{operation_id}] conflict: {reason}"),
                ));
            }
            ForwardedSequencerCommitResponse::Indeterminate {
                operation_id,
                reason,
            } => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": {
                            "type": "remote_store_publish_indeterminate_exception",
                            "reason": format!("publication outcome for operation_id [{operation_id}] is indeterminate; retry with the same operation_id: {reason}"),
                            "error_class": "retryable",
                            "operation_id": operation_id,
                        },
                        "status": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    })),
                ));
            }
            ForwardedSequencerCommitResponse::RetryableAuthority { reason } => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "remote_store_publish_exception",
                    format!(
                        "manifest seal publish failed after sequencer authority contention: {reason}"
                    ),
                ));
            }
            ForwardedSequencerCommitResponse::Fatal { reason } => {
                crate::metrics::HOT_DELTA_SEALS_TOTAL
                    .with_label_values(&[trigger_label, "failed"])
                    .inc();
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_publish_exception",
                    format!("manifest seal publish failed: {reason}"),
                ));
            }
        };

        crate::metrics::HOT_DELTA_SEALS_TOTAL
            .with_label_values(&[trigger_label, "committed"])
            .inc();
        crate::metrics::HOT_DELTA_SEALED_DOCS_TOTAL.inc_by(live_doc_count);
        crate::metrics::HOT_DELTA_SEALED_BYTES_TOTAL.inc_by(outcome.size_bytes);

        let mut retired_docs = 0usize;
        if options.retire_after_commit {
            let committed_manifest = state
                .storage_manager
                .load_current_manifest(&index_uuid, Some(&schema_hash))
                .await
                .map_err(|error| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_manifest_exception",
                        format!("failed to verify committed manifest after seal: {error:#}"),
                    )
                })?
                .ok_or_else(|| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_manifest_exception",
                        "committed manifest disappeared before delta retirement",
                    )
                })?;
            let verified_wm = committed_manifest
                .published_watermarks
                .get(shard_id)
                .filter(|w| w.sealed_through_seq_no >= candidate.through_seq_no)
                .ok_or_else(|| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_manifest_exception",
                        format!(
                            "committed manifest generation {} does not yet cover shard {} through seq_no {}",
                            committed_manifest.generation, shard_id, candidate.through_seq_no
                        ),
                    )
                })?;
            let engine_for_retire = engine.clone();
            let target_seq = verified_wm.sealed_through_seq_no;
            let generation = committed_manifest.generation;
            retired_docs = state
                .worker_pools
                .spawn_maintenance("retire_sealed_delta", move || {
                    engine_for_retire.retire_sealed_delta_through(target_seq, generation)
                })
                .await
                .map_err(|error| {
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_seal_exception",
                        format!("failed to retire sealed hot-delta range: {error:#}"),
                    )
                })?;
        }

        let updated_wm = engine.seal_watermark_state().unwrap_or_default();
        splits_sealed += 1;
        total_docs_sealed += live_doc_count;
        total_tombstones_sealed += candidate.tombstone_count();
        shard_reports.push(serde_json::json!({
            "shard_id": shard_id,
            "status": if outcome.replayed { "replayed" } else { "committed" },
            "trigger": trigger_label,
            "operation_id": outcome.operation_id,
            "split_id": outcome.split_id,
            "generation": outcome.generation,
            "start_seq_no": candidate.start_seq_no,
            "through_seq_no": candidate.through_seq_no,
            "live_doc_count": live_doc_count,
            "tombstone_count": candidate.tombstone_count(),
            "retired_doc_count": retired_docs,
            "sealed_through_seq_no": updated_wm.sealed_through_seq_no.or(Some(candidate.through_seq_no)),
            "retired_through_seq_no": updated_wm.retired_through_seq_no,
            "published_watermarks": outcome.published_watermarks,
        }));
    }

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": metadata.uuid,
        "splits_sealed": splits_sealed,
        "docs_sealed": total_docs_sealed,
        "tombstones_sealed": total_tombstones_sealed,
        "shards": shard_reports,
    }))
}

/// Resolve a document by `_id` from the committed manifest's published splits,
/// respecting split-recorded operation versions and tombstones (`FS-034`, `FS-036`, `FS-038`).
pub(crate) async fn get_document_from_published_splits(
    state: &AppState,
    metadata: &IndexMetadata,
    shard_id: u32,
    doc_id: &str,
) -> anyhow::Result<Option<crate::engine::DocumentRead>> {
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let Some(manifest) = state
        .storage_manager
        .load_current_manifest(metadata.uuid.as_str(), Some(&expected_schema_hash))
        .await?
    else {
        return Ok(None);
    };

    // Find the winning version across published splits for this shard.
    // Tuple: (seq_no, primary_term, version, split_idx, is_tombstone, split_manifest)
    let mut winner: Option<(
        u64,
        u64,
        u64,
        usize,
        bool,
        crate::storage::RemoteSplitManifest,
    )> = None;
    for (split_idx, split) in manifest.published_splits().enumerate() {
        if let Some(range) = split.seal_range()?
            && range.shard_id != shard_id
        {
            continue;
        }
        for dv in split.doc_versions()? {
            if dv.doc_id == doc_id {
                let replace = winner.as_ref().is_none_or(|(s, t, v, idx, _, _)| {
                    (dv.seq_no, dv.primary_term, dv.version, split_idx) >= (*s, *t, *v, *idx)
                });
                if replace {
                    winner = Some((
                        dv.seq_no,
                        dv.primary_term,
                        dv.version,
                        split_idx,
                        false,
                        split.clone(),
                    ));
                }
            }
        }
        for ts in split.tombstones()? {
            if ts.doc_id == doc_id {
                let replace = winner.as_ref().is_none_or(|(s, t, v, idx, _, _)| {
                    (ts.seq_no, ts.primary_term, ts.version, split_idx) >= (*s, *t, *v, *idx)
                });
                if replace {
                    winner = Some((
                        ts.seq_no,
                        ts.primary_term,
                        ts.version,
                        split_idx,
                        true,
                        split.clone(),
                    ));
                }
            }
        }
    }

    let Some((seq_no, primary_term, version, _, is_tombstone, winning_split)) = winner else {
        return Ok(None);
    };
    if is_tombstone {
        return Ok(None);
    }

    let assigned = AssignedRemoteSplit::from_manifest(&winning_split);
    let leaf_ctx = LeafExecutionContext {
        worker_pools: state.worker_pools.clone(),
        storage_manager: state.storage_manager.clone(),
        reader_cache: state.remote_store_reader_cache.clone(),
    };
    let reader = leaf_ctx
        .reader_cache
        .get_or_open(&leaf_ctx, metadata, &assigned)
        .await?;
    let engine = reader.engine.clone();
    let doc_id_owned = doc_id.to_string();
    let source_opt: Option<serde_json::Value> = state
        .worker_pools
        .spawn_search(move || engine.get_document(&doc_id_owned))
        .await??;
    Ok(source_opt.map(|source| crate::engine::DocumentRead {
        source,
        seq_no,
        primary_term,
        version,
    }))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemoteStoreCompactionOptions {
    #[serde(default = "default_compact_force")]
    pub force: bool,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub split_ids: Vec<String>,
    #[serde(default)]
    pub shard_id: Option<u32>,
    #[serde(default)]
    pub min_input_splits: Option<usize>,
    #[serde(default)]
    pub max_input_splits: Option<usize>,
    #[serde(default)]
    pub max_output_bytes: Option<u64>,
    #[serde(default)]
    pub min_tombstone_ratio: Option<f64>,
    #[serde(default)]
    pub min_overlap_docs: Option<usize>,
    #[serde(default)]
    pub drop_tombstones: bool,
}

const fn default_compact_force() -> bool {
    true
}

impl Default for RemoteStoreCompactionOptions {
    fn default() -> Self {
        Self {
            force: true,
            operation_id: None,
            split_ids: Vec::new(),
            shard_id: None,
            min_input_splits: None,
            max_input_splits: None,
            max_output_bytes: None,
            min_tombstone_ratio: None,
            min_overlap_docs: None,
            drop_tombstones: false,
        }
    }
}

pub fn deterministic_compaction_operation_id(
    index_uuid: &str,
    input_split_ids: &[String],
    drop_tombstones: bool,
) -> String {
    use sha2::Digest;
    let mut sorted_ids = input_split_ids.to_vec();
    sorted_ids.sort();
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"ferris-compact-op-v1\0");
    hasher.update(index_uuid.as_bytes());
    hasher.update(b"\0");
    for id in &sorted_ids {
        hasher.update(id.as_bytes());
        hasher.update(b",");
    }
    hasher.update(if drop_tombstones {
        b":drop1"
    } else {
        b":drop0"
    });
    let hex: String = hasher
        .finalize()
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("compact-{hex}")
}

/// Fenced, idempotent split compactor (`FS-039`, `FS-040`).
///
/// Selects eligible splits by count, size, document overlap, and tombstone density;
/// merges live document winners and retained tombstones under `ResourceGovernor` +
/// `WorkClass::Maintenance` limits; and atomically commits replacement lineage
/// through the single-sequencer manifest protocol while keeping retained snapshots
/// readable.
pub(crate) async fn compact_splits(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Option<Value>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if metadata.uses_local_shards() && !metadata.is_tiered() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] has lifecycle_mode [{}] which does not support split compaction",
                index_name,
                metadata.lifecycle_mode()
            ),
        ));
    }

    let options: RemoteStoreCompactionOptions = match body {
        Some(v) if !v.is_null() => serde_json::from_value(v).map_err(|error| {
            crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!("invalid compaction request body: {error}"),
            )
        })?,
        _ => RemoteStoreCompactionOptions::default(),
    };

    if let Some(ref op_id) = options.operation_id
        && let Err(error) = crate::storage::validate_publication_operation_id(op_id)
    {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            error,
        ));
    }

    if let Err(error) = state.storage_manager.ensure_publication_supported().await {
        return Err(crate::api::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "remote_store_publish_exception",
            error,
        ));
    }

    let index_uuid = metadata.uuid.as_str().to_string();
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let Some(current_manifest) = state
        .storage_manager
        .load_current_manifest(&index_uuid, Some(&expected_schema_hash))
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load current manifest before compaction: {error:#}"),
            )
        })?
    else {
        return Ok(serde_json::json!({
            "index": index_name,
            "index_uuid": index_uuid,
            "status": "skipped_empty",
            "splits_compacted": 0,
        }));
    };

    let min_input_splits = options.min_input_splits.unwrap_or(2).max(1);
    let max_input_splits = options.max_input_splits.unwrap_or(16).max(min_input_splits);
    let max_output_bytes = options.max_output_bytes.unwrap_or(256 * 1024 * 1024).max(1);
    let min_tombstone_ratio = options.min_tombstone_ratio.unwrap_or(0.10);
    let min_overlap_docs = options.min_overlap_docs.unwrap_or(1);

    // Resolve candidate input splits.
    let selected_inputs: Vec<(usize, crate::storage::RemoteSplitManifest)> = if !options
        .split_ids
        .is_empty()
    {
        // Check if this exact operation_id was already committed (idempotent retry!).
        let op_id = options.operation_id.clone().unwrap_or_else(|| {
            deterministic_compaction_operation_id(
                &index_uuid,
                &options.split_ids,
                options.drop_tombstones,
            )
        });
        if let Some(lineage) = current_manifest
            .compaction_lineage
            .iter()
            .find(|l| l.operation_id == op_id)
        {
            if lineage.input_split_ids != options.split_ids {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!(
                        "compaction operation_id [{op_id}] was already committed with different input splits"
                    ),
                ));
            }
            let out_split_id = lineage
                .output_split_ids
                .first()
                .cloned()
                .unwrap_or_default();
            return Ok(serde_json::json!({
                "index": index_name,
                "index_uuid": index_uuid,
                "status": "replayed",
                "replayed": true,
                "operation_id": op_id,
                "generation": lineage.committed_at_generation,
                "input_split_ids": lineage.input_split_ids,
                "output_split_id": out_split_id,
                "splits_compacted": lineage.input_split_ids.len(),
                "retained_live_docs": lineage.retained_live_docs,
                "retained_tombstones": lineage.retained_tombstones,
                "dropped_superseded_versions": lineage.dropped_superseded_versions,
            }));
        }

        let mut list = Vec::with_capacity(options.split_ids.len());
        let mut seen = HashSet::new();
        for req_id in &options.split_ids {
            if !seen.insert(req_id.as_str()) {
                return Err(crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    format!("duplicate split_id [{req_id}] in compaction request"),
                ));
            }
            let Some((idx, split)) = current_manifest
                .splits
                .iter()
                .enumerate()
                .find(|(_, s)| &s.split_id == req_id)
            else {
                return Err(crate::api::error_response(
                    StatusCode::BAD_REQUEST,
                    "illegal_argument_exception",
                    format!("compaction input split [{req_id}] does not exist in manifest"),
                ));
            };
            if !matches!(split.state, crate::storage::RemoteSplitState::Published) {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "operation_id_conflict",
                    format!(
                        "compaction input split [{req_id}] was already replaced (state: {:?})",
                        split.state
                    ),
                ));
            }
            list.push((idx, split.clone()));
        }
        list.sort_by_key(|(idx, _)| *idx);
        list
    } else {
        // Group published splits by shard_id (or None for unsharded remote_store).
        let mut groups: BTreeMap<Option<u32>, Vec<(usize, crate::storage::RemoteSplitManifest)>> =
            BTreeMap::new();
        for (idx, split) in current_manifest.splits.iter().enumerate() {
            if !matches!(split.state, crate::storage::RemoteSplitState::Published) {
                continue;
            }
            let sid = split.seal_range().ok().flatten().map(|r| r.shard_id);
            if let Some(target_sid) = options.shard_id
                && sid != Some(target_sid)
            {
                continue;
            }
            groups.entry(sid).or_default().push((idx, split.clone()));
        }

        let mut chosen: Option<Vec<(usize, crate::storage::RemoteSplitManifest)>> = None;
        for (_sid, mut group) in groups {
            group.sort_by_key(|(idx, s)| {
                (
                    s.seal_range()
                        .ok()
                        .flatten()
                        .map(|r| r.start_seq_no)
                        .unwrap_or(*idx as u64),
                    *idx,
                )
            });
            let mut window = Vec::new();
            let mut acc_bytes = 0u64;
            for item in group {
                if !window.is_empty()
                    && (window.len() >= max_input_splits
                        || acc_bytes.saturating_add(item.1.size_bytes) > max_output_bytes)
                {
                    break;
                }
                acc_bytes = acc_bytes.saturating_add(item.1.size_bytes);
                window.push(item);
            }
            if window.is_empty() {
                continue;
            }
            // Compute overlap and tombstone density for policy check.
            let mut seen_docs = HashSet::new();
            let mut overlap_docs = 0usize;
            let mut total_live = 0usize;
            let mut total_tombstones = 0usize;
            for (_, sp) in &window {
                let dvs = sp.doc_versions().unwrap_or_default();
                let tss = sp.tombstones().unwrap_or_default();
                total_live += dvs.len();
                total_tombstones += tss.len();
                for dv in dvs {
                    if !seen_docs.insert(dv.doc_id) {
                        overlap_docs += 1;
                    }
                }
                for ts in tss {
                    if !seen_docs.insert(ts.doc_id) {
                        overlap_docs += 1;
                    }
                }
            }
            let total_records = (total_live + total_tombstones).max(1);
            let tombstone_ratio = (total_tombstones as f64) / (total_records as f64);
            let policy_eligible = options.force
                || window.len() >= min_input_splits
                || overlap_docs >= min_overlap_docs
                || tombstone_ratio >= min_tombstone_ratio;
            if policy_eligible
                && (window.len() >= min_input_splits || (options.force && !window.is_empty()))
            {
                chosen = Some(window);
                break;
            }
        }

        let Some(chosen) = chosen else {
            let status = if current_manifest.published_splits().count() == 0 {
                "skipped_empty"
            } else {
                "skipped_policy"
            };
            return Ok(serde_json::json!({
                "index": index_name,
                "index_uuid": index_uuid,
                "status": status,
                "splits_compacted": 0,
            }));
        };
        chosen
    };

    let input_split_ids: Vec<String> = selected_inputs
        .iter()
        .map(|(_, s)| s.split_id.clone())
        .collect();
    let operation_id = options.operation_id.unwrap_or_else(|| {
        deterministic_compaction_operation_id(
            &index_uuid,
            &input_split_ids,
            options.drop_tombstones,
        )
    });
    crate::worker::set_current_trace_operation_id(&operation_id);

    let estimated_bytes: u64 = selected_inputs
        .iter()
        .map(|(_, s)| s.size_bytes)
        .sum::<u64>()
        .max(1024);

    let current_ctx = crate::worker::current_request_context();
    state
        .worker_pools
        .admission()
        .cooperative_background_yield(crate::worker::WorkClass::Maintenance, current_ctx.as_ref())
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rejected_execution_exception",
                error,
            )
        })?;
    let mut mem_reservation = state
        .worker_pools
        .resource_governor()
        .try_reserve(crate::worker::ResourcePoolKind::Memory, estimated_bytes)
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rejected_execution_exception",
                error,
            )
        })?;
    let mut temp_disk_reservation = state
        .worker_pools
        .resource_governor()
        .try_reserve(crate::worker::ResourcePoolKind::TempDisk, estimated_bytes)
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "rejected_execution_exception",
                error,
            )
        })?;

    // Hydrate and read all input splits to compute exact live document winners and tombstones.
    let leaf_ctx = LeafExecutionContext {
        worker_pools: state.worker_pools.clone(),
        storage_manager: state.storage_manager.clone(),
        reader_cache: state.remote_store_reader_cache.clone(),
    };

    // Map doc_id -> (seq_no, primary_term, version, manifest_idx, Option<source_json>)
    let mut merged_state: BTreeMap<String, (u64, u64, u64, usize, Option<Value>)> = BTreeMap::new();
    let mut input_ranges = Vec::new();
    let mut total_input_records = 0u64;

    for (manifest_idx, split) in &selected_inputs {
        if let Some(range) = split.seal_range().map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                error,
            )
        })? {
            input_ranges.push(range);
        }
        let assigned = AssignedRemoteSplit::from_manifest(split);
        let reader = leaf_ctx
            .reader_cache
            .get_or_open(&leaf_ctx, metadata, &assigned)
            .await
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!(
                        "failed to open input split '{}' for compaction: {error:#}",
                        split.split_id
                    ),
                )
            })?;
        let engine = reader.engine.clone();
        let split_live_docs = state
            .worker_pools
            .spawn_maintenance("read_compaction_input_split", move || {
                engine.live_documents_with_versions()
            })
            .await
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!(
                        "failed to read documents from input split '{}': {error:#}",
                        split.split_id
                    ),
                )
            })?;

        let dvs_by_id: HashMap<String, crate::storage::SplitDocVersionRecord> = split
            .doc_versions()
            .unwrap_or_default()
            .into_iter()
            .map(|dv| (dv.doc_id.clone(), dv))
            .collect();

        for (doc_id, source, eng_seq, eng_term, eng_ver) in split_live_docs {
            total_input_records += 1;
            let (seq_no, primary_term, version) = match dvs_by_id.get(&doc_id) {
                Some(dv) => (dv.seq_no, dv.primary_term, dv.version),
                None => (eng_seq, eng_term, eng_ver),
            };
            let cand_key = (seq_no, primary_term, version, *manifest_idx);
            let update = merged_state
                .get(&doc_id)
                .is_none_or(|(s, t, v, idx, _)| cand_key >= (*s, *t, *v, *idx));
            if update {
                merged_state.insert(
                    doc_id,
                    (seq_no, primary_term, version, *manifest_idx, Some(source)),
                );
            }
        }

        for ts in split.tombstones().unwrap_or_default() {
            total_input_records += 1;
            let cand_key = (ts.seq_no, ts.primary_term, ts.version, *manifest_idx);
            let update = merged_state
                .get(&ts.doc_id)
                .is_none_or(|(s, t, v, idx, _)| cand_key >= (*s, *t, *v, *idx));
            if update {
                merged_state.insert(
                    ts.doc_id,
                    (ts.seq_no, ts.primary_term, ts.version, *manifest_idx, None),
                );
            }
        }
    }

    // Check if there are any earlier Published splits before the first compacted input split;
    // if so, tombstones cannot be dropped even when drop_tombstones is requested.
    let first_input_idx = selected_inputs.first().map(|(i, _)| *i).unwrap_or(0);
    let selected_id_set: HashSet<&str> = input_split_ids.iter().map(String::as_str).collect();
    let has_earlier_published_splits =
        current_manifest
            .splits
            .iter()
            .take(first_input_idx)
            .any(|s| {
                matches!(s.state, crate::storage::RemoteSplitState::Published)
                    && !selected_id_set.contains(s.split_id.as_str())
            });
    let allow_drop_tombstones = options.drop_tombstones && !has_earlier_published_splits;

    let mut live_docs: Vec<(String, Value)> = Vec::new();
    let mut raw_docs_for_summaries: Vec<Value> = Vec::new();
    let mut out_doc_versions: Vec<crate::storage::SplitDocVersionRecord> = Vec::new();
    let mut out_tombstones: Vec<crate::storage::SplitTombstoneRecord> = Vec::new();

    for (doc_id, (seq_no, primary_term, version, _, source_opt)) in merged_state {
        match source_opt {
            Some(source) => {
                live_docs.push((doc_id.clone(), source.clone()));
                raw_docs_for_summaries.push(source);
                out_doc_versions.push(crate::storage::SplitDocVersionRecord {
                    doc_id,
                    seq_no,
                    primary_term,
                    version,
                });
            }
            None => {
                if !allow_drop_tombstones {
                    out_tombstones.push(crate::storage::SplitTombstoneRecord {
                        doc_id,
                        seq_no,
                        primary_term,
                        version,
                    });
                }
            }
        }
    }

    let out_seal_range = if !input_ranges.is_empty() {
        input_ranges.sort_by_key(|r| r.start_seq_no);
        let shard_id = input_ranges[0].shard_id;
        let start_seq_no = input_ranges[0].start_seq_no;
        let end_seq_no = input_ranges.last().unwrap().end_seq_no;
        let primary_term = input_ranges
            .iter()
            .map(|r| r.primary_term)
            .max()
            .unwrap_or(1);
        Some(crate::storage::SplitSealRangeMetadata {
            shard_id,
            start_seq_no,
            end_seq_no,
            primary_term,
            doc_versions: out_doc_versions.clone(),
            tombstones: out_tombstones.clone(),
        })
    } else {
        None
    };

    let (field_ranges, field_terms) = build_split_field_summaries(
        &raw_docs_for_summaries,
        &metadata.mappings,
    )
    .map_err(|error| {
        crate::api::error_response(StatusCode::BAD_REQUEST, "mapper_parsing_exception", error)
    })?;

    let schema_generation = 1u64;
    let schema_hash = expected_schema_hash.clone();
    let fingerprint_payload = serde_json::json!({
        "kind": "remote_store_compact_v1",
        "index_uuid": index_uuid,
        "input_split_ids": input_split_ids,
        "drop_tombstones": allow_drop_tombstones,
        "schema_generation": schema_generation,
        "schema_hash": schema_hash,
        "live_docs": live_docs,
        "tombstones": out_tombstones,
    });
    let raw_fp = crate::engine::ClientOperationIntent::fingerprint_json(&fingerprint_payload)
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                error,
            )
        })?;
    let request_fingerprint = if raw_fp.starts_with("sha256:") {
        raw_fp
    } else {
        format!("sha256:{raw_fp}")
    };

    let split_id = crate::storage::deterministic_publication_split_id(&index_uuid, &operation_id);
    let staging_dir = state.storage_manager.staging_dir(&index_uuid, &split_id);

    let (existing_intent, existing_prepared) = match state
        .storage_manager
        .resolve_publication_operation(&index_uuid, &operation_id, &request_fingerprint)
        .await
    {
        Ok(crate::storage::PublicationResolution::Committed(outcome)) => {
            let live_doc_count = out_doc_versions.len() as u64;
            let tombstone_count = out_tombstones.len() as u64;
            let dropped = total_input_records.saturating_sub(live_doc_count + tombstone_count);
            return Ok(serde_json::json!({
                "index": index_name,
                "index_uuid": index_uuid,
                "status": "replayed",
                "replayed": true,
                "operation_id": operation_id,
                "generation": outcome.generation,
                "input_split_ids": input_split_ids,
                "output_split_id": outcome.split_id,
                "splits_compacted": input_split_ids.len(),
                "retained_live_docs": live_doc_count,
                "retained_tombstones": tombstone_count,
                "dropped_superseded_versions": dropped,
            }));
        }
        Ok(crate::storage::PublicationResolution::Conflict {
            operation_id,
            reason,
        }) => {
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "operation_id_conflict",
                format!("compaction operation_id [{operation_id}] conflict: {reason}"),
            ));
        }
        Ok(crate::storage::PublicationResolution::NotCommitted { intent, prepared }) => {
            (intent, prepared)
        }
        Err(error) => {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!("failed to resolve compaction operation '{operation_id}': {error:#}"),
            ));
        }
    };

    let live_doc_count = live_docs.len() as u64;
    let tombstone_count = out_tombstones.len() as u64;
    let intent_doc_count = (live_doc_count + tombstone_count).max(1);
    let planned_bundle_path =
        crate::storage::StorageManager::split_bundle_key(&index_uuid, &split_id);

    if existing_intent.is_none() {
        let now_ms = now_epoch_ms();
        let intent = crate::storage::PublicationIntentRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: request_fingerprint.clone(),
            schema_generation,
            schema_hash: schema_hash.clone(),
            split_id: split_id.clone(),
            planned_bundle_path: planned_bundle_path.clone(),
            doc_count: intent_doc_count,
            producer_node_id: state.local_node_id.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            created_at_epoch_ms: now_ms,
        };
        state
            .storage_manager
            .stage_publication_intent(&intent)
            .await
            .map_err(|error| {
                if let Some(crate::storage::PublicationError::OperationConflict {
                    operation_id,
                    reason,
                }) = error.downcast_ref::<crate::storage::PublicationError>()
                {
                    return crate::api::error_response(
                        StatusCode::CONFLICT,
                        "operation_id_conflict",
                        format!("compaction operation_id [{operation_id}] conflict: {reason}"),
                    );
                }
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_publish_exception",
                    format!("failed to stage compaction intent: {error:#}"),
                )
            })?;
    }

    if state
        .storage_manager
        .take_publication_crash_stage_if_matches(
            crate::storage::PublicationCrashStage::AfterIntentBeforeBundleUpload,
        )
    {
        return Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_publish_exception",
            format!(
                "injected crash for operation_id [{operation_id}] after intent creation before bundle upload"
            ),
        ));
    }

    let prepared_already_valid = match existing_prepared.as_ref() {
        Some(prep) => state
            .storage_manager
            .verify_prepared_split_bundle(&index_uuid, prep)
            .await
            .is_ok(),
        None => false,
    };

    if !prepared_already_valid {
        let resumed_bundle = if existing_intent.is_some() {
            state
                .storage_manager
                .inspect_existing_split_bundle(&index_uuid, &split_id)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let (bundle_key, checksum, size_bytes) = if let Some(triple) = resumed_bundle {
            triple
        } else {
            let mappings_for_build = Arc::new(metadata.mappings.clone());
            let staging_for_build = staging_dir.clone();
            let seal_meta_for_build = out_seal_range.clone();
            let build_res = state
                .worker_pools
                .spawn_maintenance("build_compacted_split", move || {
                    if staging_for_build.exists() {
                        std::fs::remove_dir_all(&staging_for_build)?;
                    }
                    std::fs::create_dir_all(&staging_for_build)?;
                    let column_cache =
                        Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0));
                    let split_engine =
                        crate::engine::tantivy::HotEngine::new_remote_split_with_mappings(
                            &staging_for_build,
                            Duration::from_secs(60),
                            mappings_for_build.as_ref(),
                            column_cache,
                        )?;
                    if !live_docs.is_empty() {
                        split_engine.bulk_add_documents(live_docs)?;
                    }
                    split_engine.flush()?;
                    strip_split_wal(&staging_for_build)?;
                    if let Some(ref seal_meta) = seal_meta_for_build {
                        let versions_bytes = serde_json::to_vec_pretty(seal_meta)?;
                        std::fs::write(
                            staging_for_build.join("seal.versions.json"),
                            versions_bytes,
                        )?;
                    }
                    Ok(())
                })
                .await;

            if let Err(error) = build_res {
                let _ = std::fs::remove_dir_all(&staging_dir);
                return Err(crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_build_exception",
                    format!("compacted split build failed: {error:#}"),
                ));
            }

            let triple = state
                .storage_manager
                .upload_split_bundle(&index_uuid, &split_id, &staging_dir)
                .await
                .map_err(|error| {
                    let _ = std::fs::remove_dir_all(&staging_dir);
                    crate::api::error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "remote_store_build_exception",
                        format!("compacted bundle upload failed: {error:#}"),
                    )
                })?;
            let _ = std::fs::remove_dir_all(&staging_dir);
            triple
        };
        let _ = mem_reservation.reconcile(size_bytes.max(1));
        let _ = temp_disk_reservation.reconcile(size_bytes.max(1));

        if state
            .storage_manager
            .take_publication_crash_stage_if_matches(
                crate::storage::PublicationCrashStage::AfterBundleUploadBeforePreparedRecord,
            )
        {
            return Err(crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_publish_exception",
                format!(
                    "injected crash for operation_id [{operation_id}] after bundle upload before prepared record"
                ),
            ));
        }

        let mut tags = BTreeMap::new();
        tags.insert("operation_id".to_string(), operation_id.clone());
        if allow_drop_tombstones {
            tags.insert("compaction_drop_tombstones".to_string(), "true".to_string());
        }
        let mut new_split = crate::storage::RemoteSplitManifest {
            split_id: split_id.clone(),
            state: crate::storage::RemoteSplitState::Published,
            bundle_path: bundle_key,
            bundle_etag: None,
            hotcache_path: None,
            checksum,
            doc_count: live_doc_count,
            size_bytes,
            uncompressed_bytes: size_bytes,
            hotcache_bytes: None,
            time_range: None,
            tags,
            field_ranges,
            field_terms,
        };
        if let Some(ref seal_meta) = out_seal_range {
            new_split.set_seal_range(seal_meta);
        } else {
            new_split.set_doc_versions_and_tombstones(&out_doc_versions, &out_tombstones);
        }
        new_split.set_parent_split_ids(&input_split_ids);

        let now_ms = now_epoch_ms();
        let prepared_record = crate::storage::PublicationPreparedRecord {
            format_version: crate::storage::PUBLICATION_RECORD_FORMAT_VERSION,
            index_uuid: index_uuid.clone(),
            index_name: index_name.to_string(),
            operation_id: operation_id.clone(),
            request_fingerprint: request_fingerprint.clone(),
            schema_generation,
            schema_hash: schema_hash.clone(),
            producer_node_id: state.local_node_id.clone(),
            prepared_at: chrono::Utc::now().to_rfc3339(),
            prepared_at_epoch_ms: now_ms,
            split: new_split,
        };
        state
            .storage_manager
            .write_publication_prepared(&prepared_record)
            .await
            .map_err(|error| {
                if let Some(crate::storage::PublicationError::OperationConflict {
                    operation_id,
                    reason,
                }) = error.downcast_ref::<crate::storage::PublicationError>()
                {
                    return crate::api::error_response(
                        StatusCode::CONFLICT,
                        "operation_id_conflict",
                        format!("compaction operation_id [{operation_id}] conflict: {reason}"),
                    );
                }
                crate::api::error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "remote_store_publish_exception",
                    format!("failed to write prepared compaction record: {error:#}"),
                )
            })?;
    }
    drop(mem_reservation);
    drop(temp_disk_reservation);

    let sequencer_req = ForwardedSequencerCommitRequest {
        operation_id: operation_id.clone(),
        request_fingerprint,
        schema_generation,
        schema_hash: schema_hash.clone(),
        settings: metadata.settings.remote_store.clone(),
        known_authority: None,
    };

    match coordinate_sequencer_commit(state, index_name, &index_uuid, &sequencer_req).await {
        ForwardedSequencerCommitResponse::Committed(outcome) => {
            let dropped = total_input_records.saturating_sub(live_doc_count + tombstone_count);
            Ok(serde_json::json!({
                "index": index_name,
                "index_uuid": index_uuid,
                "status": if outcome.replayed { "replayed" } else { "committed" },
                "replayed": outcome.replayed,
                "operation_id": outcome.operation_id,
                "generation": outcome.generation,
                "parent_generation": outcome.parent_generation,
                "input_split_ids": input_split_ids,
                "output_split_id": outcome.split_id,
                "splits_compacted": input_split_ids.len(),
                "retained_live_docs": live_doc_count,
                "retained_tombstones": tombstone_count,
                "dropped_superseded_versions": dropped,
                "size_bytes": outcome.size_bytes,
                "published_watermarks": outcome.published_watermarks,
            }))
        }
        ForwardedSequencerCommitResponse::Conflict {
            operation_id,
            reason,
        } => Err(crate::api::error_response(
            StatusCode::CONFLICT,
            "operation_id_conflict",
            format!("compaction operation_id [{operation_id}] conflict: {reason}"),
        )),
        ForwardedSequencerCommitResponse::Indeterminate {
            operation_id,
            reason,
        } => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": {
                    "type": "remote_store_publish_indeterminate_exception",
                    "reason": format!("compaction outcome for operation_id [{operation_id}] is indeterminate; retry with the same operation_id: {reason}"),
                    "error_class": "retryable",
                    "operation_id": operation_id,
                },
                "status": StatusCode::SERVICE_UNAVAILABLE.as_u16(),
            })),
        )),
        ForwardedSequencerCommitResponse::RetryableAuthority { reason } => {
            Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "remote_store_publish_exception",
                format!("compaction commit failed after sequencer authority contention: {reason}"),
            ))
        }
        ForwardedSequencerCommitResponse::Fatal { reason } => Err(crate::api::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote_store_publish_exception",
            format!("compaction commit failed: {reason}"),
        )),
    }
}

/// Forward-only manifest rollback to a retained generation (`FS-041`, ADR 0003 section 3.8).
pub(crate) async fn rollback_manifest(
    state: &AppState,
    index_name: &str,
    metadata: &IndexMetadata,
    body: Value,
) -> Result<Value, (StatusCode, Json<Value>)> {
    if metadata.uses_local_shards() && !metadata.is_tiered() {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "index [{}] has lifecycle_mode [{}] which does not support manifest rollback",
                index_name,
                metadata.lifecycle_mode()
            ),
        ));
    }

    let target_generation = body
        .get("target_generation")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                "rollback body must contain positive integer 'target_generation'",
            )
        })?;
    if target_generation == 0 {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "target_generation must be >= 1",
        ));
    }

    let index_uuid = metadata.uuid.as_str().to_string();
    let expected_schema_hash = crate::storage::compute_schema_hash(&metadata.mappings);
    let lock_arc = state.storage_manager.sequencer_lock(&index_uuid).await;
    let _sequencer_guard = lock_arc.lock().await;

    let Some((current_manifest, current_path, _, current_rev)) = state
        .storage_manager
        .load_manifest_for_snapshot(&index_uuid, None, Some(&expected_schema_hash))
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "remote_store_manifest_exception",
                format!("failed to load current manifest for rollback: {error:#}"),
            )
        })?
    else {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            "index has no committed manifest to roll back from",
        ));
    };

    if target_generation >= current_manifest.generation {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!(
                "target_generation [{target_generation}] must be strictly less than current generation [{}]",
                current_manifest.generation
            ),
        ));
    }

    let Some((target_manifest, _, _, _)) = state
        .storage_manager
        .load_manifest_for_snapshot(
            &index_uuid,
            Some(target_generation),
            Some(&expected_schema_hash),
        )
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
                format!(
                    "cannot load target_generation [{target_generation}] for rollback: {error:#}"
                ),
            )
        })?
    else {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            format!("target_generation [{target_generation}] does not exist"),
        ));
    };

    if metadata.is_tiered() {
        for (shard_id, current_wm) in &current_manifest.published_watermarks {
            let target_seq = target_manifest
                .published_watermarks
                .get(shard_id)
                .map(|w| w.sealed_through_seq_no);
            if target_seq != Some(current_wm.sealed_through_seq_no) {
                return Err(crate::api::error_response(
                    StatusCode::CONFLICT,
                    "remote_store_publish_exception",
                    format!(
                        "cannot roll back tiered index [{index_name}] to generation {target_generation}: shard {shard_id} published watermark would regress from {:?} to {:?}",
                        current_wm.sealed_through_seq_no, target_seq
                    ),
                ));
            }
        }
    }

    let now_ms = now_epoch_ms();
    let _rollback_lease = state.storage_manager.acquire_snapshot_retention_lease(
        crate::storage::SnapshotRetentionLease {
            snapshot_id: format!("rollback-{index_uuid}-g{target_generation}"),
            index_uuid: index_uuid.clone(),
            manifest_generation: target_generation,
            pinned_split_ids: target_manifest
                .published_splits()
                .map(|s| s.split_id.clone())
                .collect(),
            acquired_at_epoch_ms: now_ms,
            expires_at_epoch_ms: now_ms.saturating_add(300_000),
        },
    );

    // Verify all published split bundles in target_manifest exist and match checksums.
    for split in target_manifest.published_splits() {
        let actual_checksum = state
            .storage_manager
            .hash_split_bundle(&index_uuid, split)
            .await
            .map_err(|error| {
                crate::api::error_response(
                    StatusCode::CONFLICT,
                    "remote_store_manifest_exception",
                    format!(
                        "cannot roll back to generation {target_generation}: split '{}' bundle is missing or unreadable: {error:#}",
                        split.split_id
                    ),
                )
            })?;
        if actual_checksum != split.checksum {
            return Err(crate::api::error_response(
                StatusCode::CONFLICT,
                "remote_store_manifest_exception",
                format!(
                    "cannot roll back to generation {target_generation}: split '{}' checksum mismatch",
                    split.split_id
                ),
            ));
        }
    }

    let next_generation = current_manifest.generation + 1;
    let operation_id = body
        .get("operation_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            format!(
                "rollback-g{}-to-g{}",
                current_manifest.generation, target_generation
            )
        });
    if let Err(error) = crate::storage::validate_publication_operation_id(&operation_id) {
        return Err(crate::api::error_response(
            StatusCode::BAD_REQUEST,
            "illegal_argument_exception",
            error,
        ));
    }

    let new_splits = target_manifest.splits.clone();

    let writer_epoch = current_manifest.writer_epoch.max(1);
    let fp_payload = serde_json::json!({
        "kind": "remote_store_rollback_v1",
        "index_uuid": index_uuid,
        "from_generation": current_manifest.generation,
        "target_generation": target_generation,
        "operation_id": operation_id,
    });
    let raw_fp =
        crate::engine::ClientOperationIntent::fingerprint_json(&fp_payload).unwrap_or_default();
    let request_fingerprint = if raw_fp.starts_with("sha256:") {
        raw_fp
    } else {
        format!("sha256:{raw_fp}")
    };

    let rollback_manifest = crate::storage::RemoteStoreManifest {
        version: crate::storage::REMOTE_MANIFEST_FORMAT_VERSION,
        engine: crate::cluster::state::IndexEngine::RemoteStore,
        index_uuid: index_uuid.clone(),
        index_name: index_name.to_string(),
        schema_generation: current_manifest
            .schema_generation
            .max(target_manifest.schema_generation),
        schema_hash: expected_schema_hash,
        generation: next_generation,
        parent_generation: current_manifest.generation,
        parent_manifest_path: Some(current_path),
        writer_epoch,
        operation_id: operation_id.clone(),
        request_fingerprint,
        published_at: chrono::Utc::now().to_rfc3339(),
        settings: target_manifest.settings.clone(),
        published_watermarks: target_manifest.published_watermarks.clone(),
        compaction_lineage: target_manifest.compaction_lineage.clone(),
        splits: new_splits,
    };

    let pointer = state
        .storage_manager
        .publish_manifest(&rollback_manifest)
        .await
        .map_err(|error| {
            crate::api::error_response(
                StatusCode::CONFLICT,
                "remote_store_publish_exception",
                format!("manifest rollback publish failed: {error:#}"),
            )
        })?;

    Ok(serde_json::json!({
        "index": index_name,
        "index_uuid": index_uuid,
        "status": "committed",
        "operation_id": operation_id,
        "previous_generation": current_manifest.generation,
        "previous_pointer_revision": current_rev,
        "target_generation": target_generation,
        "generation": pointer.current_generation,
        "pointer_revision": pointer.pointer_revision,
        "manifest_path": pointer.manifest_path,
        "published_splits": rollback_manifest.published_splits().count(),
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
    use std::collections::BTreeMap;
    use std::sync::mpsc;
    use tempfile::TempDir;

    #[test]
    fn immutable_split_strips_mutable_wal_metadata_without_removing_index_files() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("index")).unwrap();
        let index = directory.path().join("index/meta.json");
        std::fs::write(&index, b"index files").unwrap();
        for name in [
            "translog.manifest",
            "translog.committed",
            "translog-00000000000000000000.bin",
        ] {
            std::fs::write(directory.path().join(name), b"mutable WAL state").unwrap();
        }
        strip_split_wal(directory.path()).unwrap();
        assert_eq!(std::fs::read(index).unwrap(), b"index files");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn immutable_split_wal_cleanup_failure_is_not_success_shaped() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("translog.committed")).unwrap();
        let error = strip_split_wal(directory.path()).unwrap_err();
        assert!(error.to_string().contains("Is a directory"), "{error:#}");
    }

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
                minimum_durable_copies: 1,
                replica_policy: None,
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
        let engine = HotEngine::new_remote_split_with_mappings(
            &stage,
            Duration::from_secs(60),
            &mappings,
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
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
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
    fn split_pruning_keeps_only_matching_summaries_and_missing_metadata() {
        let mappings = HashMap::from([
            (
                "status".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            ),
            (
                "ts".to_string(),
                FieldMapping {
                    field_type: FieldType::Date,
                    dimension: None,
                },
            ),
        ]);
        let query = QueryClause::Bool(crate::search::BoolQuery {
            filter: vec![
                QueryClause::Term(HashMap::from([(
                    "status".to_string(),
                    serde_json::json!("error"),
                )])),
                QueryClause::Range(HashMap::from([(
                    "ts".to_string(),
                    RangeCondition {
                        gte: Some(serde_json::json!(200)),
                        ..Default::default()
                    },
                )])),
            ],
            ..Default::default()
        });
        let predicate = build_split_pruning_predicate(&query, &mappings);

        let matching = crate::storage::RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a/bundle".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::from([(
                "ts".into(),
                crate::storage::SplitFieldRange {
                    min: "200".into(),
                    max: "300".into(),
                },
            )]),
            field_terms: BTreeMap::from([(
                "status".into(),
                crate::storage::SplitFieldTerms {
                    values: vec!["error".into()],
                },
            )]),
        };
        let wrong_term = crate::storage::RemoteSplitManifest {
            split_id: "split-b".into(),
            field_terms: BTreeMap::from([(
                "status".into(),
                crate::storage::SplitFieldTerms {
                    values: vec!["ok".into()],
                },
            )]),
            ..matching.clone()
        };
        let wrong_range = crate::storage::RemoteSplitManifest {
            split_id: "split-c".into(),
            field_ranges: BTreeMap::from([(
                "ts".into(),
                crate::storage::SplitFieldRange {
                    min: "0".into(),
                    max: "199".into(),
                },
            )]),
            ..matching.clone()
        };
        let missing_metadata = crate::storage::RemoteSplitManifest {
            split_id: "split-d".into(),
            field_ranges: BTreeMap::new(),
            field_terms: BTreeMap::new(),
            ..matching.clone()
        };

        assert!(split_matches_pruning_predicate(&matching, &predicate));
        assert!(!split_matches_pruning_predicate(&wrong_term, &predicate));
        assert!(!split_matches_pruning_predicate(&wrong_range, &predicate));
        assert!(split_matches_pruning_predicate(
            &missing_metadata,
            &predicate
        ));
    }

    #[test]
    fn split_pruning_ignores_unsupported_should_clause_but_uses_required_filters() {
        let mappings = HashMap::from([(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        )]);
        let query = QueryClause::Bool(crate::search::BoolQuery {
            should: vec![QueryClause::Match(HashMap::from([(
                "title".to_string(),
                serde_json::json!("rust"),
            )]))],
            filter: vec![QueryClause::Range(HashMap::from([(
                "price".to_string(),
                RangeCondition {
                    lt: Some(serde_json::json!(10.0)),
                    ..Default::default()
                },
            )]))],
            ..Default::default()
        });
        let predicate = build_split_pruning_predicate(&query, &mappings);

        let split = crate::storage::RemoteSplitManifest {
            split_id: "split-a".into(),
            state: RemoteSplitState::Published,
            bundle_path: "idx-1/splits/split-a/bundle".into(),
            bundle_etag: None,
            hotcache_path: None,
            checksum: "sha256:a".into(),
            doc_count: 1,
            size_bytes: 16,
            uncompressed_bytes: 16,
            hotcache_bytes: None,
            time_range: None,
            tags: BTreeMap::new(),
            field_ranges: BTreeMap::from([(
                "price".into(),
                crate::storage::SplitFieldRange {
                    min: "10".into(),
                    max: "20".into(),
                },
            )]),
            field_terms: BTreeMap::new(),
        };

        assert!(!split_matches_pruning_predicate(&split, &predicate));
    }

    #[test]
    fn build_split_field_summaries_tracks_supported_mapped_fields() {
        let mappings = HashMap::from([
            (
                "status".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            ),
            (
                "active".to_string(),
                FieldMapping {
                    field_type: FieldType::Boolean,
                    dimension: None,
                },
            ),
            (
                "count".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "price".to_string(),
                FieldMapping {
                    field_type: FieldType::Float,
                    dimension: None,
                },
            ),
            (
                "ts".to_string(),
                FieldMapping {
                    field_type: FieldType::Date,
                    dimension: None,
                },
            ),
            (
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            ),
        ]);
        let docs = vec![
            serde_json::json!({
                "status": "error",
                "active": true,
                "count": 5,
                "price": 1.5,
                "ts": "2026-04-01T00:00:00Z",
                "title": "ignored text field"
            }),
            serde_json::json!({
                "status": ["warn", ["queued", 7, true, null], "warn"],
                "active": false,
                "count": 9,
                "price": 2.5,
                "ts": "2026-04-02T00:00:00Z"
            }),
        ];

        let (field_ranges, field_terms) = build_split_field_summaries(&docs, &mappings).unwrap();

        assert_eq!(field_ranges["count"].min, "5");
        assert_eq!(field_ranges["count"].max, "9");
        assert_eq!(field_ranges["price"].min, "1.5");
        assert_eq!(field_ranges["price"].max, "2.5");
        assert_eq!(
            field_ranges["ts"].min,
            crate::common::date::parse_iso8601_to_epoch_millis("2026-04-01T00:00:00Z")
                .unwrap()
                .to_string()
        );
        assert_eq!(
            field_ranges["ts"].max,
            crate::common::date::parse_iso8601_to_epoch_millis("2026-04-02T00:00:00Z")
                .unwrap()
                .to_string()
        );
        assert_eq!(
            field_terms["status"].values,
            vec!["7", "error", "queued", "true", "warn"]
        );
        assert_eq!(field_terms["active"].values, vec!["false", "true"]);
        assert!(!field_terms.contains_key("title"));
    }

    #[test]
    fn build_split_field_summaries_omits_high_cardinality_term_sets() {
        let mappings = HashMap::from([(
            "status".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        let values: Vec<_> = (0..=REMOTE_STORE_TERM_SUMMARY_CAP)
            .map(|idx| serde_json::json!(format!("value-{idx}")))
            .collect();
        let docs = vec![
            serde_json::json!({ "status": "anchor" }),
            serde_json::json!({ "status": values }),
        ];

        let (_field_ranges, field_terms) = build_split_field_summaries(&docs, &mappings).unwrap();

        assert!(!field_terms.contains_key("status"));
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

    #[tokio::test]
    async fn remote_store_publish_fails_closed_before_staging_when_backend_is_unsupported_while_reads_and_health_work()
     {
        let temp_dir = TempDir::new().unwrap();
        let storage_manager = Arc::new(StorageManager::new_in_path(temp_dir.path()).unwrap());
        let (raft, shared_state) =
            crate::consensus::create_raft_instance_mem(1, "remote-capability-test".to_string())
                .await
                .unwrap();
        crate::consensus::bootstrap_single_node(&raft, 1, "127.0.0.1:19300".to_string())
            .await
            .unwrap();
        for _ in 0..50 {
            if raft.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let cluster_manager = Arc::new(crate::cluster::ClusterManager::with_shared_state(
            shared_state,
        ));
        let mut cs = crate::cluster::state::ClusterState::new("remote-capability-test".into());
        cs.nodes.insert(
            "node-1".into(),
            NodeInfo {
                id: "node-1".into(),
                name: "node-1".into(),
                host: "127.0.0.1".into(),
                transport_port: 19300,
                http_port: 19200,
                roles: vec![NodeRole::Master, NodeRole::Data],
                raft_node_id: 1,
            },
        );
        let index_uuid = IndexUuid::new("idx-cap-1");
        let metadata = IndexMetadata {
            name: "events".into(),
            uuid: index_uuid.clone(),
            number_of_shards: 0,
            number_of_replicas: 0,
            shard_routing: HashMap::new(),
            mappings: HashMap::from([(
                "title".into(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            )]),
            dynamic: DynamicMapping::False,
            settings: IndexSettings {
                engine: IndexEngine::RemoteStore,
                refresh_interval_ms: None,
                flush_threshold_bytes: None,
                remote_store: None,
                ..IndexSettings::default()
            },
        };
        cs.add_index(metadata.clone());
        cluster_manager.update_state(cs);

        let app_state = AppState {
            cluster_manager,
            shard_manager: Arc::new(crate::shard::ShardManager::new(
                temp_dir.path(),
                Duration::from_secs(60),
            )),
            transport_client: crate::transport::TransportClient::new(),
            local_node_id: "node-1".into(),
            raft,
            worker_pools: crate::worker::WorkerPools::new(2, 2),
            task_manager: Arc::new(crate::tasks::TaskManager::new()),
            storage_manager: storage_manager.clone(),
            security_manager: Arc::new(crate::security::SecurityManager::disabled()),
            remote_store_reader_cache: Arc::new(RemoteSplitReaderCache::default()),
            bulk_runtime: Arc::new(crate::api::index::BulkRuntime::default()),
            sql_group_by_scan_limit: 1_000_000,
            sql_approximate_top_k: false,
        };

        // Publish one valid split while the backend is supported.
        let published = publish_docs(
            &app_state,
            "events",
            &metadata,
            vec![serde_json::json!({"_id": "1", "title": "initial readable split"})],
        )
        .await
        .expect("initial publish should succeed");
        assert_eq!(published["generation"], 1);

        // Now mark the backend's publication capability as unsupported (e.g. uncertified shared mount or disabled S3 conditional put).
        storage_manager.set_unsupported_capability_for_test(
            "shared or network filesystem 'nfs4' is not certified for conditional manifest publication",
        );

        // Cluster health must remain 200/green and explicitly name the missing capability.
        let axum::Json(health) =
            crate::api::cluster::get_health(axum::extract::State(app_state.clone())).await;
        let pub_status = health
            .remote_store_publication
            .expect("health must include remote_store_publication diagnostics when unsupported");
        assert!(!pub_status.supported);
        assert_eq!(pub_status.capability, "conditional_put_and_cas");
        assert!(
            pub_status
                .reason
                .as_deref()
                .is_some_and(|r| r.contains("nfs4"))
        );

        // Publication endpoint must fail closed BEFORE creating any staging directory or new split bundle.
        let staging_index_root = storage_manager
            .local_workdir()
            .join("staging")
            .join(index_uuid.as_str());
        let (status, axum::Json(err_body)) = publish_docs(
            &app_state,
            "events",
            &metadata,
            vec![serde_json::json!({"_id": "2", "title": "must not stage or publish"})],
        )
        .await
        .expect_err("publish must fail closed when capability is unavailable");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            err_body["error"]["type"],
            serde_json::json!("remote_store_publish_exception")
        );
        let reason = err_body["error"]["reason"].as_str().unwrap_or("");
        assert!(
            reason.contains("conditional_put_and_cas") && reason.contains("nfs4"),
            "unexpected error reason: {reason}"
        );
        assert!(
            !staging_index_root.exists()
                || std::fs::read_dir(&staging_index_root).unwrap().count() == 0,
            "failed publication on unsupported backend must not leave staged artifacts"
        );

        // Read-only operations (verify_splits and load_current_manifest) must still succeed on generation 1.
        let verified = verify_splits(&app_state, "events", &metadata)
            .await
            .expect("verify_splits must still work on read-only remote data");
        assert_eq!(verified["generation"], 1);
        assert_eq!(verified["ok_count"], 1);
    }
}
