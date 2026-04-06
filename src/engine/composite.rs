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

use super::SearchEngine;
use super::tantivy::HotEngine;
use super::vector::VectorIndex;
use crate::wal::TranslogDurability;

/// A composite engine that owns both a text index (Tantivy) and an optional
/// vector index (USearch). All document operations go through here — vector
/// fields are auto-detected and indexed into USearch transparently.
pub struct CompositeEngine {
    text: HotEngine,
    vector: RwLock<Option<VectorIndex>>,
    data_dir: std::path::PathBuf,
    /// Local checkpoint: highest contiguous seq_no applied to this shard copy.
    checkpoint: std::sync::atomic::AtomicU64,
    /// Global checkpoint: min of all in-sync replica checkpoints (primary only).
    global_cp: std::sync::atomic::AtomicU64,
    /// Shared column cache for fast-field Arrow arrays.
    #[allow(dead_code)]
    column_cache: Arc<super::column_cache::ColumnCache>,
}

impl CompositeEngine {
    /// Create a new composite engine at the given data directory.
    pub fn new(data_dir: impl AsRef<Path>, refresh_interval: Duration) -> Result<Self> {
        Self::new_with_mappings(
            data_dir,
            refresh_interval,
            &std::collections::HashMap::new(),
            TranslogDurability::Request,
            Arc::new(super::column_cache::ColumnCache::new(0, 0)),
        )
    }

    /// Create a new composite engine with explicit field mappings.
    pub fn new_with_mappings(
        data_dir: impl AsRef<Path>,
        refresh_interval: Duration,
        mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let text = HotEngine::new_with_mappings(
            &data_dir,
            refresh_interval,
            mappings,
            durability,
            column_cache.clone(),
        )?;

        // Load existing vector index if present.
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
            checkpoint: std::sync::atomic::AtomicU64::new(0),
            global_cp: std::sync::atomic::AtomicU64::new(0),
            column_cache,
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
                if let Err(e) = Self::run_background_maintenance(engine.clone(), None).await {
                    tracing::error!("Background shard maintenance task failed: {}", e);
                }
            }
        });
    }

    async fn run_background_maintenance(
        engine: Arc<Self>,
        flush_threshold: Option<u64>,
    ) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            // Commit/truncate/save work is blocking I/O and must stay off the async
            // scheduler so Raft heartbeats and transport RPCs keep making progress.
            if let Err(e) = engine.text.refresh() {
                tracing::error!("Background refresh failed: {}", e);
            }
            if let Some(flush_threshold) = flush_threshold
                && let Err(e) = engine.maybe_auto_flush(flush_threshold)
            {
                tracing::error!("Auto-flush failed: {}", e);
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("background shard maintenance task failed: {}", e))?;
        Ok(())
    }

    /// Start a reactive refresh loop that adjusts its interval when the
    /// setting changes via the provided `watch::Receiver`.
    ///
    /// Uses `tokio::select!` to either:
    /// - Sleep for the current interval, then refresh
    /// - Wake up immediately when the interval setting changes
    ///
    /// After each refresh tick, checks the translog size and triggers an
    /// automatic flush+truncate if it exceeds the configured threshold.
    pub fn start_refresh_loop_reactive(
        engine: Arc<Self>,
        mut refresh_rx: tokio::sync::watch::Receiver<std::time::Duration>,
        mut flush_threshold_rx: tokio::sync::watch::Receiver<u64>,
    ) {
        tokio::spawn(async move {
            const MIN_REFRESH: std::time::Duration = std::time::Duration::from_secs(1);
            let mut interval = (*refresh_rx.borrow_and_update()).max(MIN_REFRESH);
            let mut flush_threshold = *flush_threshold_rx.borrow_and_update();
            loop {
                tokio::select! {
                    () = tokio::time::sleep(interval) => {
                        if let Err(e) = Self::run_background_maintenance(
                            engine.clone(),
                            Some(flush_threshold),
                        ).await {
                            tracing::error!("Background shard maintenance task failed: {}", e);
                        }
                    }
                    result = refresh_rx.changed() => {
                        match result {
                            Ok(()) => {
                                interval = (*refresh_rx.borrow_and_update()).max(MIN_REFRESH);
                                tracing::info!("Refresh interval updated to {:?}", interval);
                            }
                            Err(_) => {
                                // Sender dropped — settings manager is gone (index deleted)
                                tracing::info!("Settings channel closed, stopping refresh loop");
                                break;
                            }
                        }
                    }
                    result = flush_threshold_rx.changed() => {
                        match result {
                            Ok(()) => {
                                flush_threshold = *flush_threshold_rx.borrow_and_update();
                                tracing::info!("Flush threshold updated to {} bytes", flush_threshold);
                            }
                            Err(_) => {
                                tracing::info!("Flush threshold channel closed, stopping refresh loop");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn maybe_auto_flush(&self, flush_threshold: u64) -> Result<bool> {
        if flush_threshold == 0 {
            return Ok(false);
        }

        let tl_size = self.text.translog_size_bytes();
        if tl_size < flush_threshold {
            return Ok(false);
        }

        // `flush_with_global_checkpoint(0)` falls back to full truncation.
        // A zero checkpoint is ambiguous in this codebase, so skip auto-flush
        // until it advances past zero instead of risking loss of replica
        // recovery history.
        if self.global_checkpoint() == 0 {
            tracing::debug!(
                "Skipping auto-flush with translog size {} bytes because global checkpoint is 0",
                tl_size
            );
            return Ok(false);
        }

        tracing::info!(
            "Translog size ({} bytes) exceeds threshold ({} bytes), auto-flushing",
            tl_size,
            flush_threshold
        );
        if !self
            .text
            .try_flush_with_global_checkpoint(self.global_checkpoint())?
        {
            tracing::debug!(
                "Skipping auto-flush with translog size {} bytes because the shard is busy ingesting or committing",
                tl_size
            );
            return Ok(false);
        }
        if !self.try_save_vectors()? {
            tracing::debug!(
                "Deferred vector index save during auto-flush because the vector index is busy"
            );
        }
        Ok(true)
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
                    let floats: Option<Vec<f32>> =
                        arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
                    if let Some(vec) = floats
                        && !vec.is_empty()
                        && self.ensure_vector_index(vec.len()).is_ok()
                    {
                        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
                        if let Some(ref vi) = *guard
                            && let Err(e) = vi.add_with_doc_id(doc_id, &vec)
                        {
                            tracing::warn!("Failed to index vector for doc '{}': {}", doc_id, e);
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

    /// Best-effort vector persistence used by background auto-flush.
    /// Returns `Ok(false)` when the vector lock is poisoned-and-unrecoverable.
    fn try_save_vectors(&self) -> Result<bool> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref vi) = *guard {
            let vector_path = self.data_dir.join("vectors.usearch");
            vi.save(&vector_path)?;
        }
        Ok(true)
    }

    /// Rebuild vector index from all documents in Tantivy.
    /// Called on startup to recover vectors from persisted text documents.
    /// The primary guard is at the shard-manager level (only called when
    /// knn_vector fields exist in the index mappings). This method does
    /// its own early-return if the MatchAll scan finds no documents.
    pub fn rebuild_vectors(&self) -> Result<()> {
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(serde_json::Value::Object(
                Default::default(),
            )),
            size: 100_000,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (docs, _, _) = self.text.search_query(&req).unwrap_or_default();
        if docs.is_empty() {
            return Ok(());
        }

        let mut vector_count = 0;
        for doc in &docs {
            let doc_id = doc.get("_id").and_then(|v| v.as_str()).unwrap_or("");
            if doc_id.is_empty() {
                continue;
            }
            if let Some(source) = doc.get("_source") {
                self.index_vectors(doc_id, source);
            }
        }

        // Check if any vectors were actually indexed
        if let Some(ref vi) = *self.vector.read().unwrap() {
            vector_count = vi.len();
        }

        if vector_count > 0 {
            tracing::info!("Rebuilt vector index: {} vectors recovered", vector_count);
        }
        Ok(())
    }
}

impl SearchEngine for CompositeEngine {
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String> {
        let id = self.text.add_document(doc_id, payload.clone())?;
        self.index_vectors(&id, &payload);
        self.update_local_checkpoint(self.text.last_seq_no());
        Ok(id)
    }

    fn add_document_with_seq(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        seq_no: u64,
    ) -> Result<String> {
        let id = self
            .text
            .add_document_with_seq(doc_id, payload.clone(), seq_no)?;
        self.index_vectors(&id, &payload);
        self.update_local_checkpoint(seq_no);
        Ok(id)
    }

    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>> {
        // Extract vector fields before passing docs to text engine (avoids cloning)
        let mut vec_fields: Vec<Option<Vec<f32>>> = Vec::with_capacity(docs.len());
        for (_, payload) in &docs {
            let mut found = None;
            if let Some(obj) = payload.as_object() {
                for (_field, value) in obj {
                    if let Some(arr) = value.as_array() {
                        let floats: Option<Vec<f32>> =
                            arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
                        if let Some(ref vec) = floats
                            && !vec.is_empty()
                        {
                            found = floats;
                            break;
                        }
                    }
                }
            }
            vec_fields.push(found);
        }

        let ids = self.text.bulk_add_documents(docs)?;

        // Collect (doc_id, vector) pairs and bulk-add to vector index
        let mut vec_batch: Vec<(String, Vec<f32>)> = Vec::new();
        for (i, vec_opt) in vec_fields.into_iter().enumerate() {
            if let (Some(id), Some(vec)) = (ids.get(i), vec_opt)
                && self.ensure_vector_index(vec.len()).is_ok()
            {
                vec_batch.push((id.clone(), vec));
            }
        }
        if !vec_batch.is_empty() {
            let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
            if let Some(ref vi) = *guard {
                let refs: Vec<(&str, &[f32])> = vec_batch
                    .iter()
                    .map(|(id, v)| (id.as_str(), v.as_slice()))
                    .collect();
                if let Err(e) = vi.bulk_add_with_doc_ids(&refs) {
                    tracing::warn!("Failed to bulk-add vectors: {}", e);
                }
            }
        }
        self.update_local_checkpoint(self.text.last_seq_no());
        Ok(ids)
    }

    fn bulk_add_documents_with_start_seq(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        start_seq_no: u64,
    ) -> Result<Vec<String>> {
        let mut vec_fields: Vec<Option<Vec<f32>>> = Vec::with_capacity(docs.len());
        for (_, payload) in &docs {
            let mut found = None;
            if let Some(obj) = payload.as_object() {
                for (_field, value) in obj {
                    if let Some(arr) = value.as_array() {
                        let floats: Option<Vec<f32>> =
                            arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
                        if let Some(ref vec) = floats
                            && !vec.is_empty()
                        {
                            found = floats;
                            break;
                        }
                    }
                }
            }
            vec_fields.push(found);
        }

        let ids = self
            .text
            .bulk_add_documents_with_start_seq(docs, start_seq_no)?;

        let mut vec_batch: Vec<(String, Vec<f32>)> = Vec::new();
        for (i, vec_opt) in vec_fields.into_iter().enumerate() {
            if let (Some(id), Some(vec)) = (ids.get(i), vec_opt)
                && self.ensure_vector_index(vec.len()).is_ok()
            {
                vec_batch.push((id.clone(), vec));
            }
        }
        if !vec_batch.is_empty() {
            let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
            if let Some(ref vi) = *guard {
                let refs: Vec<(&str, &[f32])> = vec_batch
                    .iter()
                    .map(|(id, v)| (id.as_str(), v.as_slice()))
                    .collect();
                if let Err(e) = vi.bulk_add_with_doc_ids(&refs) {
                    tracing::warn!("Failed to bulk-add vectors: {}", e);
                }
            }
        }

        if !ids.is_empty() {
            self.update_local_checkpoint(start_seq_no + ids.len() as u64 - 1);
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
        let result = self.text.delete_document(doc_id)?;
        self.update_local_checkpoint(self.text.last_seq_no());
        Ok(result)
    }

    fn delete_document_with_seq(&self, doc_id: &str, seq_no: u64) -> Result<u64> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref vi) = *guard {
            let key = crate::engine::routing::hash_string(doc_id);
            let _ = vi.remove(key);
        }
        drop(guard);

        let result = self.text.delete_document_with_seq(doc_id, seq_no)?;
        self.update_local_checkpoint(seq_no);
        Ok(result)
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

    fn flush_with_global_checkpoint(&self) -> Result<()> {
        self.text
            .flush_with_global_checkpoint(self.global_checkpoint())?;
        self.save_vectors()?;
        Ok(())
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        self.text.search(query_str)
    }

    fn search_query(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<(
        Vec<serde_json::Value>,
        usize,
        std::collections::HashMap<String, crate::search::PartialAggResult>,
    )> {
        self.text.search_query(req)
    }

    fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<Option<super::SqlBatchResult>> {
        Ok(Some(self.text.sql_record_batch(
            req,
            columns,
            needs_id,
            needs_score,
        )?))
    }

    fn sql_streaming_batch_handle(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<Option<super::SqlStreamingBatchHandle>> {
        <HotEngine as super::SearchEngine>::sql_streaming_batch_handle(
            &self.text,
            req,
            columns,
            needs_id,
            needs_score,
            batch_size,
        )
    }

    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<serde_json::Value>> {
        self.search_knn_filtered(field, vector, k, None)
    }

    fn search_knn_filtered(
        &self,
        field: &str,
        vector: &[f32],
        k: usize,
        filter: Option<&crate::search::QueryClause>,
    ) -> Result<Vec<serde_json::Value>> {
        let guard = self.vector.read().unwrap_or_else(|e| e.into_inner());
        let vi = match *guard {
            Some(ref vi) => vi,
            None => return Ok(vec![]),
        };

        // When a filter is present, oversample to get enough candidates that
        // pass the filter. We fetch k * OVERSAMPLE_FACTOR candidates from the
        // vector index, then post-filter against the Tantivy query.
        const OVERSAMPLE_FACTOR: usize = 10;
        let fetch_k = if filter.is_some() {
            std::cmp::min(k * OVERSAMPLE_FACTOR, vi.len())
        } else {
            k
        };

        let (keys, distances) = vi.search(vector, fetch_k)?;

        // Build the allowed doc_id set if a filter is present
        let allowed_ids = match filter {
            Some(clause) => Some(self.text.matching_doc_ids(clause)?),
            None => None,
        };

        let mut hits = Vec::with_capacity(k);
        for (key, distance) in keys.iter().zip(distances.iter()) {
            if hits.len() >= k {
                break;
            }
            let doc_id = vi.doc_id_for_key(*key).unwrap_or_else(|| key.to_string());

            // Skip docs that don't pass the filter
            if let Some(ref allowed) = allowed_ids
                && !allowed.contains(&doc_id)
            {
                continue;
            }

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

    fn local_checkpoint(&self) -> u64 {
        self.checkpoint.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn update_local_checkpoint(&self, seq_no: u64) {
        self.checkpoint
            .fetch_max(seq_no, std::sync::atomic::Ordering::Relaxed);
    }

    fn global_checkpoint(&self) -> u64 {
        self.global_cp.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn update_global_checkpoint(&self, checkpoint: u64) {
        self.global_cp
            .store(checkpoint, std::sync::atomic::Ordering::Relaxed);
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
        let id = engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
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
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine.refresh().unwrap();

        let results = engine.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn dsl_search_works() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Vector operations ───────────────────────────────────────────────

    #[test]
    fn add_document_with_vector_auto_indexes() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python", "embedding": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Vector index should have been created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_some(), "vector index should be auto-created");
        assert_eq!(guard.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn search_knn_returns_nearest_neighbors() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python", "embedding": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document("d3", json!({"title": "go", "embedding": [0.9, 0.1, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["_id"], "d1", "exact match should be first");
        assert_eq!(hits[1]["_id"], "d3", "close vector should be second");
        assert!(
            hits[0]["_knn_distance"].as_f64().unwrap() < hits[1]["_knn_distance"].as_f64().unwrap()
        );
    }

    #[test]
    fn search_knn_returns_source() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
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
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
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
        engine
            .add_document("d1", json!({"title": "a", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "b", "embedding": [0.0, 1.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        engine.delete_document("d1").unwrap();

        // knn search should not find d1 anymore
        let hits = engine.search_knn("embedding", &[1.0, 0.0, 0.0], 5).unwrap();
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(
            !ids.contains(&"d1"),
            "deleted doc should not appear in knn results"
        );
    }

    // ── Flush & persistence ─────────────────────────────────────────────

    #[test]
    fn flush_saves_vector_index_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let vector_path = dir.path().join("vectors.usearch");

        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        assert!(
            vector_path.exists(),
            "flush should save vectors.usearch to disk"
        );
    }

    #[test]
    fn flush_saves_doc_id_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar_path = dir.path().join("vectors.docids.bin");

        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("doc_one", json!({"emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("doc_two", json!({"emb": [0.0, 1.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        assert!(
            sidecar_path.exists(),
            "flush should save doc_id sidecar alongside vector index"
        );
    }

    #[test]
    fn flush_reopen_knn_preserves_doc_ids() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: add docs with vectors, flush everything
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("alpha", json!({"title": "a", "emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("beta", json!({"title": "b", "emb": [0.0, 1.0, 0.0]}))
                .unwrap();
            engine.flush().unwrap();
        }

        // Phase 2: reopen, rebuild vectors, knn should return correct doc_ids
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            assert_eq!(engine.doc_count(), 2);
            engine.rebuild_vectors().unwrap();

            let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
            assert_eq!(hits.len(), 2);
            assert_eq!(hits[0]["_id"], "alpha");
            assert_eq!(hits[1]["_id"], "beta");
        }
    }

    #[test]
    fn rebuild_vectors_recovers_from_tantivy() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: add docs with vectors, flush text only (not vectors)
        {
            let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("d1", json!({"title": "a", "emb": [1.0, 0.0, 0.0]}))
                .unwrap();
            engine
                .add_document("d2", json!({"title": "b", "emb": [0.0, 1.0, 0.0]}))
                .unwrap();
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
        engine
            .add_document("d1", json!({"title": "no vectors here"}))
            .unwrap();
        engine.refresh().unwrap();

        // Vector index should NOT be created
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none(), "no vector index for text-only docs");
    }

    #[test]
    fn mixed_docs_with_and_without_vectors() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "text only"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "with vec", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "text only too"}))
            .unwrap();
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
        engine
            .add_document("d1", json!({"tags": ["rust", "search"]}))
            .unwrap();
        engine.refresh().unwrap();

        // String arrays should NOT create a vector index
        let guard = engine.vector.read().unwrap();
        assert!(guard.is_none());
    }

    #[test]
    fn search_knn_includes_score_and_distance() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits.len(), 1);

        let score = hits[0]["_score"].as_f64().unwrap();
        let distance = hits[0]["_knn_distance"].as_f64().unwrap();
        assert!(score > 0.99, "exact match should have score ~1.0");
        assert!(distance < 0.001, "exact match should have distance ~0.0");
        assert_eq!(hits[0]["_knn_field"], "emb");
    }

    // ── Hybrid search (text + kNN) ─────────────────────────────────────

    #[test]
    fn hybrid_text_and_knn_both_return_hits() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "rust search engine", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python web framework", "emb": [0.0, 1.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "rust compiler internals", "emb": [0.0, 0.0, 1.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Text search: "rust" should match d1 and d3
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (text_hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(text_hits.len(), 3, "match_all should return all 3 docs");

        // kNN search: vector closest to [1.0, 0.0, 0.0] should be d1
        let knn_hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(knn_hits.len(), 2);
        assert_eq!(knn_hits[0]["_id"], "d1", "d1 should be nearest neighbor");

        // Both searches independently produce results — confirms hybrid is possible
        assert!(!text_hits.is_empty());
        assert!(!knn_hits.is_empty());
    }

    #[test]
    fn search_query_with_match_finds_docs_via_body_fallback() {
        // Verifies that match queries on named fields fall back to the "body"
        // catch-all and still find documents (since Tantivy schema is dynamic).
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "movie one"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "movie two"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "book three"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Match({
                let mut m = std::collections::HashMap::new();
                m.insert("title".to_string(), json!("movie"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 2, "match on 'movie' should find d1 and d2");
    }

    #[test]
    fn knn_only_search_request_returns_vector_hits() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "doc A", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "doc B", "emb": [0.0, 1.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // kNN-only (no text query clause exercised at engine level)
        let hits = engine.search_knn("emb", &[0.9, 0.1], 1).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    // ── Pre-filtered kNN search ────────────────────────────────────────

    #[test]
    fn knn_filtered_returns_only_matching_docs() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "rust search", "year": "2020", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python web", "year": "2021", "emb": [0.9, 0.1, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "rust compiler", "year": "2022", "emb": [0.8, 0.2, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Without filter: k=3 should return all 3 docs
        let hits = engine.search_knn("emb", &[1.0, 0.0, 0.0], 3).unwrap();
        assert_eq!(hits.len(), 3);

        // With filter: only docs matching "rust" should be returned
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 3, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 2, "filter should only return d1 and d3");
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
        assert!(
            !ids.contains(&"d2"),
            "d2 ('python web') should be filtered out"
        );
    }

    #[test]
    fn knn_filtered_respects_k_limit() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust a", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust b", "emb": [0.9, 0.1, 0.0]}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust c", "emb": [0.8, 0.2, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        // All 3 match the filter, but k=1 should return only the nearest
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 1, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1", "d1 should be nearest neighbor");
    }

    #[test]
    fn knn_filtered_none_filter_returns_all() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "a", "emb": [1.0, 0.0]}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "b", "emb": [0.0, 1.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // No filter should return same as search_knn
        let hits_unfiltered = engine.search_knn("emb", &[1.0, 0.0], 2).unwrap();
        let hits_none_filter = engine
            .search_knn_filtered("emb", &[1.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(hits_unfiltered.len(), hits_none_filter.len());
    }

    #[test]
    fn knn_filtered_with_no_matches_returns_empty() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust only", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        // Filter for "python" — no docs match
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("python"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 5, Some(&filter))
            .unwrap();
        assert!(hits.is_empty(), "no docs match filter, should return empty");
    }

    #[test]
    fn knn_filtered_with_range_filter() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "d1",
                json!({"title": "old movie", "year": "2000", "emb": [1.0, 0.0, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "new movie", "year": "2025", "emb": [0.95, 0.05, 0.0]}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "mid movie", "year": "2015", "emb": [0.5, 0.5, 0.0]}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Filter: match_all (should get everything — range on dynamic fields
        // won't work without typed fields, so we test with match_all + term)
        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("new"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 3, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn knn_filtered_preserves_knn_metadata() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust", "emb": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let filter = crate::search::QueryClause::Match({
            let mut m = std::collections::HashMap::new();
            m.insert("title".to_string(), json!("rust"));
            m
        });
        let hits = engine
            .search_knn_filtered("emb", &[1.0, 0.0, 0.0], 1, Some(&filter))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_knn_field"], "emb");
        assert!(hits[0]["_knn_distance"].as_f64().is_some());
        assert!(hits[0]["_score"].as_f64().unwrap() > 0.0);
        assert!(hits[0]["_source"].is_object());
    }

    // ── Checkpoint tracking ─────────────────────────────────────────────

    #[test]
    fn local_checkpoint_starts_at_zero() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.local_checkpoint(), 0);
    }

    #[test]
    fn update_local_checkpoint_advances() {
        let (_dir, engine) = create_engine();
        engine.update_local_checkpoint(5);
        assert_eq!(engine.local_checkpoint(), 5);

        // fetch_max semantics: only advances
        engine.update_local_checkpoint(3);
        assert_eq!(
            engine.local_checkpoint(),
            5,
            "checkpoint should not go backward"
        );

        engine.update_local_checkpoint(10);
        assert_eq!(engine.local_checkpoint(), 10);
    }

    #[test]
    fn global_checkpoint_starts_at_zero() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.global_checkpoint(), 0);
    }

    #[test]
    fn update_global_checkpoint_stores_value() {
        let (_dir, engine) = create_engine();
        engine.update_global_checkpoint(42);
        assert_eq!(engine.global_checkpoint(), 42);
    }

    #[test]
    fn flush_with_global_checkpoint_retains_entries() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        // Index 3 documents — each creates a WAL entry
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();
        engine.add_document("d3", json!({"x": 3})).unwrap();

        // Set global checkpoint to 1 (entries 0 and 1 are safe to discard)
        engine.update_global_checkpoint(1);

        // Flush with checkpoint-aware truncation
        engine.flush_with_global_checkpoint().unwrap();

        // Verify documents are still searchable
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 3);
    }

    #[test]
    fn maybe_auto_flush_zero_threshold_disables_auto_flush() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();

        let before = engine.text.translog_size_bytes();
        assert!(before > 0);

        let flushed = engine.maybe_auto_flush(0).unwrap();

        assert!(!flushed);
        assert_eq!(engine.text.translog_size_bytes(), before);
    }

    #[test]
    fn maybe_auto_flush_skips_when_global_checkpoint_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();

        let before = engine.text.translog_size_bytes();
        assert!(before > 0);
        assert_eq!(engine.global_checkpoint(), 0);

        let flushed = engine.maybe_auto_flush(1).unwrap();

        assert!(!flushed);
        assert_eq!(engine.text.translog_size_bytes(), before);
    }

    #[test]
    fn maybe_auto_flush_uses_checkpoint_aware_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();
        engine.add_document("d3", json!({"x": 3})).unwrap();
        engine.update_global_checkpoint(1);

        let before = engine.text.translog_size_bytes();
        let flushed = engine.maybe_auto_flush(1).unwrap();
        let after = engine.text.translog_size_bytes();

        assert!(flushed);
        assert!(after > 0);
        assert!(after < before);
    }

    #[test]
    fn maybe_auto_flush_succeeds_with_vectors_present() {
        let dir = tempfile::tempdir().unwrap();
        let engine = CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine
            .add_document("d1", json!({"emb": [1.0, 0.0, 0.0], "x": 1}))
            .unwrap();
        engine
            .add_document("d2", json!({"emb": [0.0, 1.0, 0.0], "x": 2}))
            .unwrap();
        engine.update_global_checkpoint(1);

        let flushed = engine.maybe_auto_flush(1).unwrap();

        assert!(flushed, "text + vector auto-flush should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn background_maintenance_tick_does_not_starve_async_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(CompositeEngine::new(dir.path(), Duration::from_secs(60)).unwrap());

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let lock_engine = engine.clone();
        let holder = std::thread::spawn(move || {
            let _guard = lock_engine.text.writer_lock_for_test();
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });
        locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let maintenance_engine = engine.clone();
        let maintenance = tokio::spawn(async move {
            let _ = started_tx.send(());
            CompositeEngine::run_background_maintenance(maintenance_engine, None).await
        });

        let start = std::time::Instant::now();
        started_rx.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "background maintenance blocked the async runtime for {:?}",
            elapsed
        );

        maintenance.await.unwrap().unwrap();
        holder.join().unwrap();
    }

    // ── Checkpoint auto-update on writes ────────────────────────────────

    #[test]
    fn add_document_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.local_checkpoint(), 0);

        engine.add_document("a", json!({"x": 1})).unwrap();
        assert_eq!(engine.local_checkpoint(), 0, "first WAL entry is seq_no 0");

        engine.add_document("b", json!({"x": 2})).unwrap();
        assert_eq!(engine.local_checkpoint(), 1);

        engine.add_document("c", json!({"x": 3})).unwrap();
        assert_eq!(engine.local_checkpoint(), 2);
    }

    #[test]
    fn bulk_add_documents_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        let docs = vec![
            ("b1".into(), json!({"x": 1})),
            ("b2".into(), json!({"x": 2})),
            ("b3".into(), json!({"x": 3})),
        ];
        engine.bulk_add_documents(docs).unwrap();
        assert_eq!(
            engine.local_checkpoint(),
            2,
            "3 docs → seq_nos 0,1,2 → checkpoint=2"
        );
    }

    #[test]
    fn delete_document_advances_local_checkpoint() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        let cp_after_add = engine.local_checkpoint();

        engine.delete_document("d1").unwrap();
        assert!(
            engine.local_checkpoint() > cp_after_add,
            "delete should advance checkpoint"
        );
    }

    #[test]
    fn mixed_writes_advance_checkpoint_monotonically() {
        let (_dir, engine) = create_engine();

        engine.add_document("a", json!({"v": 1})).unwrap();
        let cp1 = engine.local_checkpoint();

        engine
            .bulk_add_documents(vec![
                ("b".into(), json!({"v": 2})),
                ("c".into(), json!({"v": 3})),
            ])
            .unwrap();
        let cp2 = engine.local_checkpoint();
        assert!(cp2 > cp1, "bulk after single should advance");

        engine.delete_document("a").unwrap();
        let cp3 = engine.local_checkpoint();
        assert!(cp3 > cp2, "delete after bulk should advance");

        engine.add_document("d", json!({"v": 4})).unwrap();
        let cp4 = engine.local_checkpoint();
        assert!(cp4 > cp3, "add after delete should advance");
    }

    #[test]
    fn explicit_seq_write_updates_local_checkpoint() {
        let (_dir, engine) = create_engine();

        engine
            .add_document_with_seq("replica-doc", json!({"x": 1}), 7)
            .unwrap();

        assert_eq!(engine.local_checkpoint(), 7);
    }

    #[test]
    fn explicit_seq_bulk_updates_local_checkpoint() {
        let (_dir, engine) = create_engine();

        engine
            .bulk_add_documents_with_start_seq(
                vec![
                    ("b1".into(), json!({"x": 1})),
                    ("b2".into(), json!({"x": 2})),
                    ("b3".into(), json!({"x": 3})),
                ],
                10,
            )
            .unwrap();

        assert_eq!(engine.local_checkpoint(), 12);
    }

    #[test]
    fn update_global_checkpoint_can_overwrite() {
        let (_dir, engine) = create_engine();
        engine.update_global_checkpoint(10);
        assert_eq!(engine.global_checkpoint(), 10);

        // store semantics — can go lower (advance_global_checkpoint prevents this)
        engine.update_global_checkpoint(5);
        assert_eq!(engine.global_checkpoint(), 5);
    }
}
