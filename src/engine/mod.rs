pub mod routing;
pub mod tantivy;

use anyhow::Result;

pub use self::tantivy::HotEngine;

/// Trait abstracting a search engine backend.
/// Each shard is backed by one `SearchEngine` implementation.
/// The current default is `HotEngine` (Tantivy-based, all data in-memory/mmap).
/// Future implementations could include warm/cold storage engines,
/// columnar engines, or remote storage backends.
pub trait SearchEngine: Send + Sync {
    /// Index a single document with a given ID. Returns the document ID.
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String>;

    /// Bulk-index documents. Each tuple is (doc_id, payload). Returns document IDs.
    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>>;

    /// Delete a document by its `_id`. Returns the number of deleted documents.
    fn delete_document(&self, doc_id: &str) -> Result<u64>;

    /// Retrieve a document by its `_id`. Returns the `_source` JSON if found.
    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>>;

    /// Commit in-memory buffer and reload the reader so new docs become searchable.
    fn refresh(&self) -> Result<()>;

    /// Flush: commit to disk and truncate the write-ahead log.
    fn flush(&self) -> Result<()>;

    /// Search using a simple query string (e.g. `?q=...`).
    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>>;

    /// Search using the OpenSearch Query DSL body.
    fn search_query(&self, req: &crate::search::SearchRequest) -> Result<Vec<serde_json::Value>>;

    /// Returns the number of searchable documents.
    fn doc_count(&self) -> u64;
}
