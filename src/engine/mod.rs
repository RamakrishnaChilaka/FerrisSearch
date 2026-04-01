pub mod column_cache;
pub mod composite;
pub mod routing;
pub mod tantivy;
pub mod vector;

use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;

pub use self::composite::CompositeEngine;
pub use self::tantivy::HotEngine;

pub struct SqlBatchResult {
    pub batch: RecordBatch,
    pub total_hits: usize,
}

/// Trait abstracting a search engine backend.
/// Each shard/split is backed by one `SearchEngine` implementation.
/// Implementations handle both text and vector indexing/search.
///
/// Current implementations:
/// - `CompositeEngine` — HotEngine (Tantivy) + optional VectorIndex (USearch)
///
/// Future implementations could include:
/// - Shardless/split-based engines (Quickwit-style immutable segments)
/// - Remote storage backends
/// - Warm/cold tiered engines
pub trait SearchEngine: Send + Sync {
    /// Index a single document with a given ID. Returns the document ID.
    /// Implementations should handle both text and vector fields.
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String>;

    /// Index a single document using a caller-supplied sequence number.
    /// Replica/recovery paths use this so WAL entries preserve primary-assigned seq_nos.
    fn add_document_with_seq(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        _seq_no: u64,
    ) -> Result<String> {
        self.add_document(doc_id, payload)
    }

    /// Bulk-index documents. Each tuple is (doc_id, payload). Returns document IDs.
    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>>;

    /// Bulk-index documents using caller-supplied contiguous sequence numbers.
    fn bulk_add_documents_with_start_seq(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        _start_seq_no: u64,
    ) -> Result<Vec<String>> {
        self.bulk_add_documents(docs)
    }

    /// Delete a document by its `_id`. Returns the number of deleted documents.
    fn delete_document(&self, doc_id: &str) -> Result<u64>;

    /// Delete a document using a caller-supplied sequence number.
    fn delete_document_with_seq(&self, doc_id: &str, _seq_no: u64) -> Result<u64> {
        self.delete_document(doc_id)
    }

    /// Retrieve a document by its `_id`. Returns the `_source` JSON if found.
    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>>;

    /// Commit in-memory buffer and reload the reader so new docs become searchable.
    fn refresh(&self) -> Result<()>;

    /// Flush: commit to disk and truncate the write-ahead log.
    fn flush(&self) -> Result<()>;

    /// Flush with translog retention: commit to disk and truncate WAL entries
    /// only up to the global checkpoint. Entries above the checkpoint are retained
    /// for replica recovery.
    fn flush_with_global_checkpoint(&self) -> Result<()> {
        self.flush()
    }

    /// Search using a simple query string (e.g. `?q=...`).
    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>>;

    /// Search using the OpenSearch Query DSL body.
    /// Returns (hits, total_matching_docs, partial_aggregations).
    fn search_query(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<(
        Vec<serde_json::Value>,
        usize,
        std::collections::HashMap<String, crate::search::PartialAggResult>,
    )>;

    /// Build a RecordBatch directly from local Tantivy columns for SQL execution.
    fn sql_record_batch(
        &self,
        _req: &crate::search::SearchRequest,
        _columns: &[String],
        _needs_id: bool,
        _needs_score: bool,
    ) -> Result<Option<SqlBatchResult>> {
        Ok(None)
    }

    /// k-NN vector search. Returns hits with _id, _score, _source, _knn_distance.
    /// Default implementation returns empty (no vector support).
    fn search_knn(
        &self,
        _field: &str,
        _vector: &[f32],
        _k: usize,
    ) -> Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }

    /// k-NN vector search with an optional pre-filter query.
    /// When a filter is provided, candidates are oversampled from the vector index
    /// then post-filtered against the query, keeping the top k matches.
    /// Default implementation ignores the filter and delegates to search_knn.
    fn search_knn_filtered(
        &self,
        field: &str,
        vector: &[f32],
        k: usize,
        _filter: Option<&crate::search::QueryClause>,
    ) -> Result<Vec<serde_json::Value>> {
        self.search_knn(field, vector, k)
    }

    /// Returns the number of searchable documents.
    fn doc_count(&self) -> u64;

    /// Get the local checkpoint: highest contiguous seq_no applied to this shard copy.
    /// Returns 0 if no seq_no tracking is configured (backward compat).
    fn local_checkpoint(&self) -> u64 {
        0
    }

    /// Update the local checkpoint after applying a replicated operation.
    fn update_local_checkpoint(&self, _seq_no: u64) {}

    /// Get the global checkpoint: min of all in-sync replica checkpoints.
    /// Only meaningful on the primary shard.
    fn global_checkpoint(&self) -> u64 {
        0
    }

    /// Update the global checkpoint (called by primary after collecting replica checkpoints).
    fn update_global_checkpoint(&self, _checkpoint: u64) {}
}
