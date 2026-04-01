//! Column cache — lazy-loaded, segment-aware Arrow array cache for SQL analytics.
//!
//! Caches pre-built Arrow arrays from Tantivy fast-field columns, keyed by
//! `(SegmentId, column_name)`. Tantivy segments are immutable once committed,
//! so cached data never goes stale — entries are evicted only by size pressure.

use datafusion::arrow::array::ArrayRef;
use tantivy::index::SegmentId;

/// Cache key: (segment UUID, column name).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    segment_id: SegmentId,
    column: String,
}

/// A lazily-populated, size-bounded column cache backed by `moka`.
///
/// Each entry is an Arrow `ArrayRef` covering all docs in a segment for one column.
/// Queries extract matching rows via `arrow::compute::take()`.
pub struct ColumnCache {
    inner: moka::sync::Cache<CacheKey, ArrayRef>,
}

impl ColumnCache {
    /// Create a new column cache with the given maximum size in bytes.
    /// Pass 0 to create a no-op cache that never stores anything.
    pub fn new(max_bytes: u64) -> Self {
        let inner = moka::sync::Cache::builder()
            .weigher(|_key: &CacheKey, value: &ArrayRef| {
                // Weight = Arrow array memory size. Capped at u32::MAX per moka API.
                let bytes = value.get_array_memory_size();
                bytes.min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .build();
        Self { inner }
    }

    /// Try to get a cached column array for the given segment + column.
    pub fn get(&self, segment_id: SegmentId, column: &str) -> Option<ArrayRef> {
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
        };
        self.inner.get(&key)
    }

    /// Insert a column array into the cache.
    /// Skips insertion if the array is larger than 25% of the cache capacity
    /// (avoids building a full-segment array that would be immediately evicted).
    pub fn insert(&self, segment_id: SegmentId, column: &str, array: ArrayRef) {
        let array_bytes = array.get_array_memory_size() as u64;
        let max = self.inner.policy().max_capacity().unwrap_or(0);
        if max > 0 && array_bytes > max / 4 {
            return; // too large — would thrash the cache
        }
        let key = CacheKey {
            segment_id,
            column: column.to_string(),
        };
        self.inner.insert(key, array);
    }

    /// Maximum cache capacity in bytes.
    pub fn max_capacity(&self) -> u64 {
        self.inner.policy().max_capacity().unwrap_or(0)
    }

    /// Number of entries currently in the cache.
    pub fn entry_count(&self) -> u64 {
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
    use std::sync::Arc;

    fn make_segment_id() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0001-0000-000000000001").unwrap()
    }

    fn make_segment_id_2() -> SegmentId {
        SegmentId::from_uuid_string("00000000-0000-0002-0000-000000000002").unwrap()
    }

    #[test]
    fn cache_miss_returns_none() {
        let cache = ColumnCache::new(1024 * 1024);
        assert!(cache.get(make_segment_id(), "price").is_none());
    }

    #[test]
    fn cache_hit_after_insert() {
        let cache = ColumnCache::new(1024 * 1024);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let seg_id = make_segment_id();

        cache.insert(seg_id, "price", array.clone());
        let cached = cache.get(seg_id, "price").unwrap();
        assert_eq!(cached.len(), 3);
    }

    #[test]
    fn different_segments_are_independent() {
        let cache = ColumnCache::new(1024 * 1024);
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
        let cache = ColumnCache::new(1024 * 1024);
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
    fn zero_size_cache_stores_nothing() {
        let cache = ColumnCache::new(0);
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
}
