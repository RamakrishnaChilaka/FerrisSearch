//! Composite search engine — combines Tantivy (text) + USearch (vector).
//!
//! This is the default engine backing each shard. It delegates text operations
//! to HotEngine and vector operations to VectorIndex. Future engine
//! implementations (e.g. shardless/split-based) can implement the SearchEngine
//! trait directly without using this composite.

use anyhow::Result;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use super::tantivy::HotEngine;
use super::vector::VectorIndex;
use super::SearchEngine;

/// A composite engine that owns both a text index (Tantivy) and an optional
/// vector index (USearch). All document operations go through here — vector
/// fields are auto-detected and indexed into USearch transparently.
pub struct CompositeEngine {
    text: HotEngine,
    vector: RwLock<Option<VectorIndex>>,
    data_dir: std::path::PathBuf,
}

impl CompositeEngine {
    /// Create a new composite engine at the given data directory.
    pub fn new(data_dir: impl AsRef<Path>, refresh_interval: Duration) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let text = HotEngine::new(&data_dir, refresh_interval)?;

        // Load existing vector index if present
        let vector_path = data_dir.join("vectors.usearch");
        let vector = if vector_path.exists() {
            // We don't know the dimensions yet — we'll discover on first vector field.
            // For now, skip loading; rebuild_vectors will handle it.
            None
        } else {
            None
        };

        Ok(Self {
            text,
            vector: RwLock::new(vector),
            data_dir,
        })
    }

    /// Get a reference to the underlying HotEngine (for refresh loop).
    pub fn text_engine(&self) -> &HotEngine {
        &self.text
    }

    /// Start the background refresh loop for the text engine.
    /// Must be called with an Arc to self.
    pub fn start_refresh_loop(engine: Arc<Self>) {
        let interval = engine.text.refresh_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = engine.text.refresh() {
                    tracing::error!("Background refresh failed: {}", e);
                }
            }
        });
    }

    /// Ensure a vector index exists with the given dimensions.
    /// Creates one if it doesn't exist, or returns the existing one.
    fn ensure_vector_index(&self, dimensions: usize) -> Result<()> {
        {
            let vi = self.vector.read().unwrap_or_else(|e| e.into_inner());
            if vi.is_some() {
                return Ok(());
            }
        }

        let vector_path = self.data_dir.join("vectors.usearch");
        let vi = VectorIndex::open(&vector_path, dimensions, usearch::ffi::MetricKind::Cos)?;

        let mut guard = self.vector.write().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(vi);
        }
        Ok(())
    }

    /// Extract and index vector fields from a document payload.
    fn index_vectors(&self, doc_id: &str, payload: &serde_json::Value) {
        if let Some(obj) = payload.as_object() {
            for (_field, value) in obj {
                if let Some(arr) = value.as_array() {
                    let floats: Option<Vec<f32>> = arr.iter()
                        .map(|v| v.as_f64().map(|f| f as f32))
                        .collect();
                    if let Some(vec) = floats {
                        if !vec.is_empty() {
                            if self.ensure_vector_index(vec.len()).is_ok() {
                                let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
                                if let Some(ref vi) = *guard {
                                    if let Err(e) = vi.add_with_doc_id(doc_id, &vec) {
                                        tracing::warn!("Failed to index vector for doc '{}': {}", doc_id, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Save the vector index to disk (called during flush).
    fn save_vectors(&self) -> Result<()> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref vi) = *guard {
            let vector_path = self.data_dir.join("vectors.usearch");
            vi.save(&vector_path)?;
        }
        Ok(())
    }

    /// Rebuild vector index from all documents in Tantivy.
    /// Called on startup to recover vectors from persisted text documents.
    pub fn rebuild_vectors(&self) -> Result<()> {
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(serde_json::Value::Object(Default::default())),
            size: 100_000,
            from: 0,
            knn: None,
        };
        let docs = self.text.search_query(&req).unwrap_or_default();
        if docs.is_empty() {
            return Ok(());
        }

        let mut count = 0;
        for doc in &docs {
            let doc_id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or("");
            if doc_id.is_empty() { continue; }
            if let Some(source) = doc.get("_source") {
                self.index_vectors(doc_id, source);
                count += 1;
            }
        }

        if count > 0 {
            tracing::info!("Rebuilt vector index from {} documents", count);
        }
        Ok(())
    }
}

impl SearchEngine for CompositeEngine {
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String> {
        let id = self.text.add_document(doc_id, payload.clone())?;
        self.index_vectors(&id, &payload);
        Ok(id)
    }

    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>> {
        let ids = self.text.bulk_add_documents(docs.clone())?;
        for (i, (_, payload)) in docs.iter().enumerate() {
            if let Some(id) = ids.get(i) {
                self.index_vectors(id, payload);
            }
        }
        Ok(ids)
    }

    fn delete_document(&self, doc_id: &str) -> Result<u64> {
        // Remove from vector index if present
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref vi) = *guard {
            let key = crate::engine::routing::hash_string(doc_id);
            let _ = vi.remove(key); // ignore errors on missing keys
        }
        drop(guard);
        self.text.delete_document(doc_id)
    }

    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>> {
        self.text.get_document(doc_id)
    }

    fn refresh(&self) -> Result<()> {
        self.text.refresh()
    }

    fn flush(&self) -> Result<()> {
        self.text.flush()?;
        self.save_vectors()?;
        Ok(())
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        self.text.search(query_str)
    }

    fn search_query(&self, req: &crate::search::SearchRequest) -> Result<Vec<serde_json::Value>> {
        self.text.search_query(req)
    }

    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<serde_json::Value>> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        let vi = match *guard {
            Some(ref vi) => vi,
            None => return Ok(vec![]),
        };

        let (keys, distances) = vi.search(vector, k)?;
        let mut hits = Vec::with_capacity(keys.len());

        for (key, distance) in keys.iter().zip(distances.iter()) {
            let doc_id = vi.doc_id_for_key(*key)
                .unwrap_or_else(|| key.to_string());
            let source = self.text.get_document(&doc_id).ok().flatten();
            hits.push(serde_json::json!({
                "_id": doc_id,
                "_score": 1.0 / (1.0 + distance),
                "_source": source,
                "_knn_field": field,
                "_knn_distance": distance,
            }));
        }

        Ok(hits)
    }

    fn doc_count(&self) -> u64 {
        self.text.doc_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_engine() -> (tempfile::TempDir, CompositeEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        (dir, engine)
    }

    // ── Basic operations ────────────────────────────────────────────────

    #[test]
    fn new_engine_is_empty() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.doc_count(), 0);
    }

    #[test]
    fn add_and_get_text_document() {
        let (_dir, engine) = create_engine();
        let id = engine.add_document("d1", json!({"title": "hello world"})).unwrap();
        assert_eq!(id, "d1");
        engine.refresh().unwrap();

        let doc = engine.get_document("d1").unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["title"], "hello world");
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn delete_document_removes_it() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "test"})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        engine.delete_document("d1").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 0);
    }

    #[test]
    fn text_search_works() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust search engine"})).unwrap();
        engine.add_document("d2", json!({"title": "python web framework"})).unwrap();
        engine.refresh().unwrap();

        let results = engine.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn dsl_search_works() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "hello world"})).unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
        };
        let results = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Vector operations ───────────────────────────────────────────────

    #[test]
    fn add_document_with_vector_auto_indexes() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]})).unwrap();
        engine.add_document("d2", json!({"title": "python", "embedding": [0.0, 1.0, 0.0]})).unwrap();
        engine.refresh().unwrap();

        // Vector index should have been created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_some(), "vector index should be auto-created");
        assert_eq!(guard.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn search_knn_returns_nearest_neighbors() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]})).unwrap();
        engine.add_document("d2", json!({"title": "python", "embedding": [0.0, 1.0, 0.0]})).unwrap();
        engine.add_document("d3", json!({"title": "go", "embedding": [0.9, 0.1, 0.0]})).unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "d1", "exact match should be first");
        assert_eq!(hits[1]["_id"], "d3", "close vector should be second");
        assert!(hits[0]["_knn_distance"].as_f64().unwrap() < hits[1]["_knn_distance"].as_f64().unwrap());
    }

    #[test]
    fn search_knn_returns_source() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]})).unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);
        let source = hits[0].get("_source").unwrap();
        assert_eq!(source["title"], "rust");
    }

    #[test]
    fn search_knn_no_vector_index_returns_empty() {
        let (_dir, engine) = create_engine();
        // Only text, no vector fields
        engine.add_document("d1", json!({"title": "hello"})).unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 5).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn bulk_add_with_vectors() {
        let (_dir, engine) = create_engine();
        let docs = vec![
            ("d1".into(), json!({"title": "a", "vec": [1.0, 0.0]})),
            ("d2".into(), json!({"title": "b", "vec": [0.0, 1.0]})),
        ];
        let ids = engine.bulk_add_documents(docs).unwrap();
        assert_eq!(ids.len(), 2);
        engine.refresh().unwrap();

        let guard = engine.vector.read().unwrap();
        assert!(guard.is_some());
        assert_eq!(guard.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn delete_removes_from_vector_index() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a", "embedding": [1.0, 0.0, 0.0]})).unwrap();
        engine.add_document("d2", json!({"title": "b", "embedding": [0.0, 1.0, 0.0]})).unwrap();
        engine.refresh().unwrap();

        engine.delete_document("d1").unwrap();

        // knn search should not find d1 anymore
        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 5).unwrap();
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(!ids.contains(&"d1"), "deleted doc should not appear in knn results");
    }

    // ── Flush & persistence ─────────────────────────────────────────────

    #[test]
    fn flush_saves_vector_index_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let vector_path = dir.path().join("vectors.usearch");

        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine.add_document("d1", json!({"emb": [1.0, 0.0, 0.0]})).unwrap();
            engine.flush().unwrap();
        }

        assert!(vector_path.exists(), "flush should save vectors.usearch to disk");
    }

    #[test]
    fn rebuild_vectors_recovers_from_tantivy() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: add docs with vectors, flush text only (not vectors)
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine.add_document("d1", json!({"title": "a", "emb": [1.0, 0.0, 0.0]})).unwrap();
            engine.add_document("d2", json!({"title": "b", "emb": [0.0, 1.0, 0.0]})).unwrap();
            // Only flush text — don't save vectors
            engine.text.flush().unwrap();
        }

        // Phase 2: reopen engine — vectors are gone but text is persisted
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            assert_eq!(engine.doc_count(), 2, "text docs should survive restart");

            // Before rebuild, no vector index
            assert!(engine.vector.read().unwrap().is_none());

            // Rebuild vectors from Tantivy's _source
            engine.rebuild_vectors().unwrap();

            // Now knn should work
            let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
            assert_eq!(hits.len(), 2, "rebuild should restore vector search");
            assert_eq!(hits[0]["_id"], "d1");
        }
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn document_without_vectors_skips_vector_indexing() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "no vectors here"})).unwrap();
        engine.refresh().unwrap();

        // Vector index should NOT be created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none(), "no vector index for text-only docs");
    }

    #[test]
    fn mixed_docs_with_and_without_vectors() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "text only"})).unwrap();
        engine.add_document("d2", json!({"title": "with vec", "emb": [1.0, 0.0]})).unwrap();
        engine.add_document("d3", json!({"title": "text only too"})).unwrap();
        engine.refresh().unwrap();

        assert_eq!(engine.doc_count(), 3);

        // knn should only find d2
        let hits = engine.search_knn("emb", &[1.0, 0.0], 5).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn non_numeric_array_not_treated_as_vector() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"tags": ["rust", "search"]})).unwrap();
        engine.refresh().unwrap();

        // String arrays should NOT create a vector index
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none());
    }

    #[test]
    fn search_knn_includes_score_and_distance() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"emb": [1.0, 0.0, 0.0]})).unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);

        let score = hits[0]["_score"].as_f64().unwrap();
        let distance = hits[0]["_knn_distance"].as_f64().unwrap();
        assert!(score > 0.99, "exact match should have score ~1.0");
        assert!(distance < 0.001, "exact match should have distance ~0.0");
        assert_eq!(hits[0]["_knn_field"], "emb");
    }
}
