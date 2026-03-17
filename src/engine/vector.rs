//! Vector search index backed by USearch (HNSW algorithm).
//!
//! Each shard gets its own VectorIndex alongside its Tantivy index.
//! Vectors are indexed by the same document key used in Tantivy.

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};

/// Wrapper around a USearch HNSW index for approximate nearest neighbor search.
pub struct VectorIndex {
    index: usearch::Index,
    dimensions: usize,
    /// Maps numeric USearch key → string doc_id for reverse lookup.
    key_to_doc_id: RwLock<HashMap<u64, String>>,
}

impl VectorIndex {
    /// Create a new vector index with the given dimensions and metric.
    pub fn new(dimensions: usize, metric: MetricKind) -> Result<Self> {
        let options = IndexOptions {
            dimensions,
            metric,
            quantization: ScalarKind::F32,
            connectivity: 16,       // HNSW M parameter (edges per node)
            expansion_add: 128,     // ef_construction
            expansion_search: 64,   // ef_search
            multi: false,
        };
        let index = usearch::Index::new(&options)
            .map_err(|e| anyhow::anyhow!("Failed to create vector index: {}", e))?;
        Ok(Self { index, dimensions, key_to_doc_id: RwLock::new(HashMap::new()) })
    }

    /// Open or create a persistent vector index at the given path.
    pub fn open(path: impl AsRef<Path>, dimensions: usize, metric: MetricKind) -> Result<Self> {
        let vi = Self::new(dimensions, metric)?;
        let path = path.as_ref();
        if path.exists() {
            vi.index.load(path.to_str().unwrap_or(""))
                .map_err(|e| anyhow::anyhow!("Failed to load vector index: {}", e))?;
        }
        // Reserve capacity for growth
        vi.index.reserve(10_000)
            .map_err(|e| anyhow::anyhow!("Failed to reserve capacity: {}", e))?;
        Ok(vi)
    }

    /// Number of dimensions each vector must have.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Number of vectors currently indexed.
    pub fn len(&self) -> usize {
        self.index.size()
    }

    pub fn is_empty(&self) -> bool {
        self.index.size() == 0
    }

    /// Add a vector with the given key. Key should be a unique numeric ID.
    pub fn add(&self, key: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimensions {
            return Err(anyhow::anyhow!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            ));
        }
        self.index.add(key, vector)
            .map_err(|e| anyhow::anyhow!("Failed to add vector: {}", e))?;
        Ok(())
    }

    /// Add a vector with a string doc_id. Hashes the doc_id to a numeric key
    /// and stores the mapping for reverse lookup.
    pub fn add_with_doc_id(&self, doc_id: &str, vector: &[f32]) -> Result<u64> {
        let key = crate::engine::routing::hash_string(doc_id);
        self.add(key, vector)?;
        let mut map = self.key_to_doc_id.write().unwrap_or_else(|e| e.into_inner());
        map.insert(key, doc_id.to_string());
        Ok(key)
    }

    /// Look up the original doc_id for a numeric USearch key.
    pub fn doc_id_for_key(&self, key: u64) -> Option<String> {
        let map = self.key_to_doc_id.read().unwrap_or_else(|e| e.into_inner());
        map.get(&key).cloned()
    }

    /// Remove a vector by key.
    pub fn remove(&self, key: u64) -> Result<()> {
        self.index.remove(key)
            .map_err(|e| anyhow::anyhow!("Failed to remove vector: {}", e))?;
        Ok(())
    }

    /// Search for the k nearest neighbors to the given query vector.
    /// Returns (keys, distances) sorted by distance ascending.
    pub fn search(&self, query: &[f32], k: usize) -> Result<(Vec<u64>, Vec<f32>)> {
        if query.len() != self.dimensions {
            return Err(anyhow::anyhow!(
                "Query dimension mismatch: expected {}, got {}",
                self.dimensions,
                query.len()
            ));
        }
        let results = self.index.search(query, k)
            .map_err(|e| anyhow::anyhow!("Vector search failed: {}", e))?;
        Ok((results.keys.to_vec(), results.distances.to_vec()))
    }

    /// Save the index to disk.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        self.index.save(path.as_ref().to_str().unwrap_or(""))
            .map_err(|e| anyhow::anyhow!("Failed to save vector index: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_empty_index() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        assert_eq!(vi.dimensions(), 3);
        assert_eq!(vi.len(), 0);
        assert!(vi.is_empty());
    }

    #[test]
    fn add_and_search_vectors() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        vi.index.reserve(10).unwrap();

        // Add 3 vectors
        vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        vi.add(2, &[0.0, 1.0, 0.0]).unwrap();
        vi.add(3, &[0.9, 0.1, 0.0]).unwrap();

        assert_eq!(vi.len(), 3);

        // Search for nearest to [1.0, 0.0, 0.0] — should find key 1 first, then 3
        let (keys, distances) = vi.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], 1, "nearest neighbor should be exact match");
        assert_eq!(keys[1], 3, "second nearest should be [0.9, 0.1, 0.0]");
        assert!(distances[0] < distances[1], "distances should be ascending");
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        vi.index.reserve(10).unwrap();

        let result = vi.add(1, &[1.0, 0.0]); // 2D instead of 3D
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dimension mismatch"));

        let result = vi.search(&[1.0, 0.0], 1); // 2D query on 3D index
        assert!(result.is_err());
    }

    #[test]
    fn search_empty_index() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        vi.index.reserve(10).unwrap();
        let (keys, distances) = vi.search(&[1.0, 0.0, 0.0], 5).unwrap();
        assert!(keys.is_empty());
        assert!(distances.is_empty());
    }

    #[test]
    fn save_and_load_persists_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vectors.usearch");

        // Create, add, save
        {
            let vi = VectorIndex::new(4, MetricKind::L2sq).unwrap();
            vi.index.reserve(10).unwrap();
            vi.add(10, &[1.0, 2.0, 3.0, 4.0]).unwrap();
            vi.add(20, &[5.0, 6.0, 7.0, 8.0]).unwrap();
            vi.save(&path).unwrap();
        }

        // Reopen and verify
        {
            let vi = VectorIndex::open(&path, 4, MetricKind::L2sq).unwrap();
            assert_eq!(vi.len(), 2);

            // Search should find both vectors
            let (keys, _) = vi.search(&[1.0, 2.0, 3.0, 4.0], 2).unwrap();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], 10, "exact match should be nearest");
        }
    }

    #[test]
    fn different_metrics() {
        // Cosine
        let cos_vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        cos_vi.index.reserve(10).unwrap();
        cos_vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        let (keys, _) = cos_vi.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(keys[0], 1);

        // L2 (Euclidean)
        let l2_vi = VectorIndex::new(3, MetricKind::L2sq).unwrap();
        l2_vi.index.reserve(10).unwrap();
        l2_vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        let (keys, _) = l2_vi.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(keys[0], 1);

        // Inner Product
        let ip_vi = VectorIndex::new(3, MetricKind::IP).unwrap();
        ip_vi.index.reserve(10).unwrap();
        ip_vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        let (keys, _) = ip_vi.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(keys[0], 1);
    }

    #[test]
    fn remove_vector() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        vi.index.reserve(10).unwrap();
        vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        vi.add(2, &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(vi.len(), 2);

        vi.remove(1).unwrap();
        // After removal, searching for [1,0,0] should not return key 1
        let (keys, _) = vi.search(&[1.0, 0.0, 0.0], 2).unwrap();
        assert!(!keys.contains(&1), "removed key should not appear in results");
    }

    #[test]
    fn k_larger_than_index_size() {
        let vi = VectorIndex::new(3, MetricKind::Cos).unwrap();
        vi.index.reserve(10).unwrap();
        vi.add(1, &[1.0, 0.0, 0.0]).unwrap();
        vi.add(2, &[0.0, 1.0, 0.0]).unwrap();

        // Ask for k=10 but only 2 vectors exist
        let (keys, distances) = vi.search(&[1.0, 0.0, 0.0], 10).unwrap();
        assert_eq!(keys.len(), 2, "should return all available vectors");
        assert_eq!(distances.len(), 2);
    }
}
