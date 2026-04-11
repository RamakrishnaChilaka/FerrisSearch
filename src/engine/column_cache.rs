//! Column cache — lazy-loaded, segment-aware cache for SQL analytics and
//! grouped-partials execution.
//!
//! Caches either pre-built Arrow arrays or grouped-partials decoded full-segment
//! columns, keyed by `(SegmentId, column_name, format)`. Tantivy segments are
//! immutable once committed, so cached data never goes stale — entries are
//! evicted only by size pressure.

use datafusion::arrow::array::ArrayRef;
use std::sync::Arc;
use tantivy::index::SegmentId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CacheFormat {
    Arrow,
    Grouped,
}

/// Cache key: (segment UUID, column name, cache format).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    segment_id: SegmentId,
    column: String,
    format: CacheFormat,
}

/// Cached full-segment values used by grouped-partials readers.
///
/// Numeric metrics want direct typed access by doc ID, while keyword/string
/// group keys want dictionary ordinals rather than expanded strings.
#[derive(Clone)]
pub(crate) enum GroupedColumnCache {
    F64(Arc<[Option<f64>]>),
    I64(Arc<[Option<i64>]>),
    StrOrds(Arc<[Option<u64>]>),
}

impl GroupedColumnCache {
    fn weight_bytes(&self) -> u64 {
        match self {
            Self::F64(values) => values.len() as u64 * std::mem::size_of::<Option<f64>>() as u64,
            Self::I64(values) => values.len() as u64 * std::mem::size_of::<Option<i64>>() as u64,
            Self::StrOrds(values) => {
                values.len() as u64 * std::mem::size_of::<Option<u64>>() as u64
            }
        }
    }
}

#[derive(Clone)]
enum CacheValue {
    Arrow(ArrayRef),
    Grouped(GroupedColumnCache),
}

/// A lazily-populated, size-bounded column cache backed by `moka`.
///
/// Entries are either:
/// - Arrow `ArrayRef`s covering all docs in a segment for one SQL column, or
/// - grouped-partials decoded full-segment values keyed by doc ID.
///
/// The same shared capacity budget covers both formats.
pub struct ColumnCache {
    inner: moka::sync::Cache<CacheKey, CacheValue>,
    /// Selectivity threshold (0.0–1.0). On a cache miss, the full-segment array
    /// is only built when `matched_docs / max_doc >= threshold`. Below this,
    /// only matching docs are read directly without populating the cache.
    populate_threshold: f64,
}

impl ColumnCache {
    /// Create a new column cache with the given maximum size in bytes
    /// and a populate threshold percentage (0–100).
    /// Pass 0 for max_bytes to create a no-op cache that never stores anything.
    /// Pass 0 for threshold_percent to always eagerly populate on miss.
    pub fn new(max_bytes: u64, populate_threshold_percent: u8) -> Self {
        let inner = moka::sync::Cache::builder()
            .weigher(|_key: &CacheKey, value: &CacheValue| {
                let bytes = match value {
                    CacheValue::Arrow(array) => array.get_array_memory_size() as u64,
                    CacheValue::Grouped(column) => column.weight_bytes(),
                };
                bytes.min(u32::MAX as u64) as u32
            })
            .max_capacity(max_bytes)
            .build();
        let threshold = (populate_threshold_percent.min(100) as f64) / 100.0;
        Self {
            inner,
            populate_threshold: threshold,
        }
    }

    /// Returns true if the matched doc ratio justifies building the full-segment
    /// array on a cache miss. When `matched / total < threshold`, the caller
    /// should use selective reads instead of populating the cache.
    pub fn should_populate(&self, matched_docs: usize, segment_max_doc: u32) -> bool {
        if segment_max_doc == 0 {
            return false;
        }
        let ratio = matched_docs as f64 / segment_max_doc as f64;
        ratio >= self.populate_threshold
    }

    /// Try to get a cached column array for the given segment + column.
    pub fn get(&self, segment_id: SegmentId, column: &str) -> Option<ArrayRef> {
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Arrow,
        };
        match self.inner.get(&key) {
            Some(CacheValue::Arrow(array)) => Some(array),
            _ => None,
        }
    }

    /// Insert a column array into the cache.
    /// Skips insertion if the array is larger than 25% of the cache capacity
    /// (avoids building a full-segment array that would be immediately evicted).
    pub fn insert(&self, segment_id: SegmentId, column: &str, array: ArrayRef) {
        let array_bytes = array.get_array_memory_size() as u64;
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Arrow,
        };
        self.insert_value(key, CacheValue::Arrow(array), array_bytes);
    }

    /// Try to get a grouped-partials decoded full-segment column.
    pub(crate) fn get_grouped(
        &self,
        segment_id: SegmentId,
        column: &str,
    ) -> Option<GroupedColumnCache> {
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Grouped,
        };
        match self.inner.get(&key) {
            Some(CacheValue::Grouped(column)) => Some(column),
            _ => None,
        }
    }

    /// Insert a grouped-partials decoded full-segment column into the cache.
    pub(crate) fn insert_grouped(
        &self,
        segment_id: SegmentId,
        column: &str,
        values: GroupedColumnCache,
    ) {
        let value_bytes = values.weight_bytes();
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
            format: CacheFormat::Grouped,
        };
        self.insert_value(key, CacheValue::Grouped(values), value_bytes);
    }

    fn insert_value(&self, key: CacheKey, value: CacheValue, value_bytes: u64) {
        let max = self.inner.policy().max_capacity().unwrap_or(0);
        if max > 0 && value_bytes > max / 4 {
            return; // too large — would thrash the cache
        }
        self.inner.insert(key, value);
    }

    /// Maximum cache capacity in bytes.
    pub fn max_capacity(&self) -> u64 {
        self.inner.policy().max_capacity().unwrap_or(0)
    }

    /// Number of entries currently in the cache.
    pub fn entry_count(&self) -> u64 {
        self.inner.run_pending_tasks();
        self.inner.entry_count()
    }

    /// Weighted size of all entries (approximate bytes).
    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }
}

/// Compute the column cache size in bytes from a percentage of system memory.
pub fn compute_cache_bytes(percent: u8) -> u64 {
    if percent == 0 {
        return 0;
    }
    let percent = percent.min(90) as u64; // cap at 90% to avoid starving the OS
    let system_memory = system_memory_bytes();
    system_memory * percent / 100
}

/// Read total system memory from /proc/meminfo (Linux).
/// Falls back to 1 GB if unavailable.
fn system_memory_bytes() -> u64 {
    if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
            if let Some(val) = line.strip_prefix("MemTotal:") {
                let trimmed = val.trim().trim_end_matches(" kB").trim();
                if let Ok(kb) = trimmed.parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    // Fallback: 1 GB
    1024 * 1024 * 1024
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};

    fn make_segment_id() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0001-0000-000000000001").unwrap()
    }

    fn make_segment_id_2() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0002-0000-000000000002").unwrap()
    }

    #[test]
    fn cache_miss_returns_none() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        assert!(cache.get(make_segment_id(), "price").is_none());
    }

    #[test]
    fn cache_hit_after_insert() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let seg_id = make_segment_id();

        cache.insert(seg_id, "price", array.clone());
        let cached = cache.get(seg_id, "price").unwrap();
        assert_eq!(cached.len(), 3);
    }

    #[test]
    fn different_segments_are_independent() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg1 = make_segment_id();
        let seg2 = make_segment_id_2();

        let arr1: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let arr2: ArrayRef = Arc::new(Int64Array::from(vec![30, 40, 50]));

        cache.insert(seg1, "count", arr1);
        cache.insert(seg2, "count", arr2);

        assert_eq!(cache.get(seg1, "count").unwrap().len(), 2);
        assert_eq!(cache.get(seg2, "count").unwrap().len(), 3);
    }

    #[test]
    fn different_columns_same_segment() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();

        let prices: ArrayRef = Arc::new(Float64Array::from(vec![9.99, 19.99]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));

        cache.insert(seg, "price", prices);
        cache.insert(seg, "name", names);
        cache.inner.run_pending_tasks();

        assert_eq!(cache.entry_count(), 2);
        assert!(cache.get(seg, "price").is_some());
        assert!(cache.get(seg, "name").is_some());
    }

    #[test]
    fn grouped_cache_hit_after_insert() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();
        let ords: Arc<[Option<u64>]> = vec![Some(3), None, Some(8)].into();

        cache.insert_grouped(seg, "brand", GroupedColumnCache::StrOrds(ords.clone()));

        match cache.get_grouped(seg, "brand") {
            Some(GroupedColumnCache::StrOrds(cached)) => {
                assert_eq!(cached.len(), 3);
                assert_eq!(cached[0], Some(3));
                assert_eq!(cached[1], None);
                assert_eq!(cached[2], Some(8));
            }
            Some(_) => panic!("expected cached string ordinals"),
            None => panic!("expected grouped cache entry"),
        }
    }

    #[test]
    fn arrow_and_grouped_entries_use_distinct_keys() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        let seg = make_segment_id();

        let prices: ArrayRef = Arc::new(Float64Array::from(vec![9.99, 19.99]));
        let numeric: Arc<[Option<f64>]> = vec![Some(9.99), Some(19.99)].into();

        cache.insert(seg, "price", prices);
        cache.insert_grouped(seg, "price", GroupedColumnCache::F64(numeric));
        cache.inner.run_pending_tasks();

        assert!(cache.get(seg, "price").is_some());
        assert!(cache.get_grouped(seg, "price").is_some());
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn zero_size_cache_stores_nothing() {
        let cache = ColumnCache::new(0, 0);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        cache.insert(make_segment_id(), "x", array);
        // moka with max_capacity=0 may still briefly hold entries before eviction
        // but get() should eventually return None
        cache.inner.run_pending_tasks();
        assert!(cache.get(make_segment_id(), "x").is_none());
    }

    #[test]
    fn compute_cache_bytes_zero_percent() {
        assert_eq!(compute_cache_bytes(0), 0);
    }

    #[test]
    fn compute_cache_bytes_caps_at_90() {
        let at_100 = compute_cache_bytes(100);
        let at_90 = compute_cache_bytes(90);
        assert_eq!(at_100, at_90); // 100% is capped to 90%
    }

    #[test]
    fn compute_cache_bytes_reasonable() {
        let bytes = compute_cache_bytes(10);
        // Should be > 0 on any system
        assert!(bytes > 0);
        // On a system with at least 1GB RAM, 10% > 100MB
        if system_memory_bytes() >= 1024 * 1024 * 1024 {
            assert!(bytes >= 100 * 1024 * 1024);
        }
    }

    #[test]
    fn system_memory_is_positive() {
        assert!(system_memory_bytes() > 0);
    }

    // ── Selectivity threshold tests ─────────────────────────────────────

    #[test]
    fn should_populate_zero_threshold_always_true() {
        let cache = ColumnCache::new(1024 * 1024, 0);
        // threshold=0 → always populate (even for 1 match in 1M docs)
        assert!(cache.should_populate(1, 1_000_000));
        assert!(cache.should_populate(0, 1_000_000)); // 0 matches → ratio 0.0 >= 0.0 → true
    }

    #[test]
    fn should_populate_100_threshold_never_populates() {
        let cache = ColumnCache::new(1024 * 1024, 100);
        // threshold=100% → only populate when ALL docs match
        assert!(!cache.should_populate(999, 1000));
        assert!(cache.should_populate(1000, 1000)); // exactly 100%
    }

    #[test]
    fn should_populate_5_percent_threshold() {
        let cache = ColumnCache::new(1024 * 1024, 5);
        // 5% of 100,000 = 5,000
        assert!(!cache.should_populate(4999, 100_000)); // below threshold
        assert!(cache.should_populate(5000, 100_000)); // at threshold
        assert!(cache.should_populate(10000, 100_000)); // above threshold
    }

    #[test]
    fn should_populate_zero_max_doc_returns_false() {
        let cache = ColumnCache::new(1024 * 1024, 5);
        assert!(!cache.should_populate(0, 0));
    }

    #[test]
    fn should_populate_default_threshold() {
        // Default threshold is 5%
        let cache = ColumnCache::new(1024 * 1024, 5);
        // 560K docs, 20 matches → 0.003% → below 5%
        assert!(!cache.should_populate(20, 560_000));
        // 560K docs, 28K matches → 5% → at threshold
        assert!(cache.should_populate(28_000, 560_000));
        // 560K docs, 560K matches → 100% → above threshold
        assert!(cache.should_populate(560_000, 560_000));
    }
}
