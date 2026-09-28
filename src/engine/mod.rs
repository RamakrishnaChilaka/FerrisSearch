pub mod column_cache;
pub mod composite;
pub mod remote_store;
pub mod routing;
pub(crate) mod sequence;
pub mod tantivy;
pub mod vector;
pub(crate) mod version_map;

use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub use self::composite::CompositeEngine;
pub use self::sequence::{SEQUENCE_FORMAT_VERSION, SequenceStats};
pub use self::tantivy::HotEngine;

#[derive(Debug, Clone, PartialEq)]
pub enum DocumentMutation {
    Index {
        doc_id: String,
        source: serde_json::Value,
    },
    Delete {
        doc_id: String,
    },
    NoOp {
        reason: String,
    },
}

impl DocumentMutation {
    pub fn doc_id(&self) -> Option<&str> {
        match self {
            Self::Index { doc_id, .. } | Self::Delete { doc_id } => Some(doc_id),
            Self::NoOp { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SequencedOperation {
    pub seq_no: u64,
    pub primary_term: u64,
    pub mutation: DocumentMutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Stale,
    Redelivery,
    NoOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaApplyReceipt {
    pub outcome: ApplyOutcome,
    pub operation_processed: bool,
    pub operation_persisted: bool,
    pub sequence: SequenceStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaBulkApplyReceipt {
    pub outcomes: Vec<ApplyOutcome>,
    pub all_operations_processed: bool,
    pub all_operations_persisted: bool,
    pub sequence: SequenceStats,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct DocumentValidationError(pub String);

pub(crate) fn is_write_validation_error(error: &anyhow::Error) -> bool {
    error.is::<DocumentValidationError>() || error.is::<crate::wal::WalFrameTooLargeError>()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexWriteReceipt {
    pub doc_id: String,
    pub seq_no: u64,
    pub primary_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkWriteReceipt {
    pub doc_ids: Vec<String>,
    pub start_seq_no: Option<u64>,
    pub primary_term: u64,
}

impl BulkWriteReceipt {
    pub fn last_seq_no(&self) -> Result<Option<u64>> {
        match (self.start_seq_no, self.doc_ids.len()) {
            (None, 0) => Ok(None),
            (Some(start), count) if count > 0 => start
                .checked_add((count - 1) as u64)
                .map(Some)
                .ok_or_else(|| anyhow::anyhow!("bulk write sequence range overflows")),
            _ => anyhow::bail!("bulk write receipt has inconsistent sequence metadata"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteWriteReceipt {
    pub deleted: u64,
    pub seq_no: u64,
    pub primary_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRecoveryFileMetadata {
    pub name: String,
    pub length: u64,
    pub sha256: String,
}

pub struct PeerRecoverySnapshot {
    pub snapshot_next_seq_no: u64,
    pub retention_pin_id: u64,
    pub files: Vec<PeerRecoveryFileMetadata>,
}

pub struct PeerRecoveryRetentionPin {
    translog: Arc<Mutex<dyn crate::wal::WriteAheadLog>>,
    pin_id: Option<u64>,
}

impl PeerRecoveryRetentionPin {
    pub(crate) fn new(translog: Arc<Mutex<dyn crate::wal::WriteAheadLog>>, pin_id: u64) -> Self {
        Self {
            translog,
            pin_id: Some(pin_id),
        }
    }

    pub fn release(mut self) -> Result<()> {
        if let Some(pin_id) = self.pin_id.take() {
            self.translog
                .lock()
                .map_err(|_| anyhow::anyhow!("peer recovery pin lock poisoned"))?
                .release_retention_pin(pin_id)?;
        }
        Ok(())
    }

    pub(crate) fn into_pin_id(mut self) -> u64 {
        self.pin_id
            .take()
            .expect("peer recovery pin has already been released")
    }
}

impl Drop for PeerRecoveryRetentionPin {
    fn drop(&mut self) {
        let Some(pin_id) = self.pin_id.take() else {
            return;
        };
        let translog = self.translog.clone();
        let release = move || {
            if let Ok(translog) = translog.lock() {
                let _ = translog.release_retention_pin(pin_id);
            }
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(release);
        } else {
            release();
        }
    }
}

pub struct PeerRecoverySnapshotPreparation {
    pub snapshot_next_seq_no: u64,
    pub retention_pin: PeerRecoveryRetentionPin,
    pub file_names: Vec<String>,
}

pub struct PreparedPeerRecoverySnapshot {
    pub snapshot_next_seq_no: u64,
    pub retention_pin: PeerRecoveryRetentionPin,
    pub files: Vec<PeerRecoveryFileMetadata>,
}

impl PeerRecoverySnapshotPreparation {
    pub fn hash_files(self, snapshot_dir: &Path) -> Result<PreparedPeerRecoverySnapshot> {
        let Self {
            snapshot_next_seq_no,
            retention_pin,
            file_names,
        } = self;
        let mut files = Vec::with_capacity(file_names.len());
        for name in file_names {
            let path = snapshot_dir.join(&name);
            let mut file = std::fs::File::open(&path)?;
            let length = file.metadata()?.len();
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 1024 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            let sha256 = hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            files.push(PeerRecoveryFileMetadata {
                name,
                length,
                sha256,
            });
        }
        Ok(PreparedPeerRecoverySnapshot {
            snapshot_next_seq_no,
            retention_pin,
            files,
        })
    }
}

pub struct PeerRecoveryOpsBatch {
    pub operations: Vec<crate::wal::TranslogEntry>,
    pub primary_next_seq_no: u64,
    pub complete: bool,
}

/// Per-segment metadata for diagnostics and monitoring.
pub struct SegmentInfo {
    pub segment_id: String,
    pub num_docs: u32,
    pub deleted_docs: u32,
}

pub struct SqlBatchResult {
    pub batch: RecordBatch,
    pub total_hits: usize,
}

pub struct SqlStreamingResult {
    pub batches: Vec<RecordBatch>,
    pub total_hits: usize,
    pub collected_rows: usize,
}

pub struct SqlStreamingBatchHandle {
    pub total_hits: usize,
    pub collected_rows: usize,
    next_batch: Box<dyn FnMut() -> Result<Option<RecordBatch>> + Send>,
}

impl SqlStreamingBatchHandle {
    pub fn new<F>(total_hits: usize, collected_rows: usize, next_batch: F) -> Self
    where
        F: FnMut() -> Result<Option<RecordBatch>> + Send + 'static,
    {
        Self {
            total_hits,
            collected_rows,
            next_batch: Box::new(next_batch),
        }
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        (self.next_batch)()
    }
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
    #[cfg(test)]
    fn inject_wal_write_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn inject_writer_replacement_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn inject_engine_apply_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn writer_is_failed_for_test(&self) -> bool {
        false
    }

    /// Index a single document with a given ID. Returns the document ID.
    /// Implementations should handle both text and vector fields.
    fn add_document(&self, doc_id: &str, payload: serde_json::Value) -> Result<String> {
        Ok(self.add_document_with_receipt(doc_id, payload)?.doc_id)
    }

    fn add_document_with_receipt(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
    ) -> Result<IndexWriteReceipt> {
        self.add_document_with_receipt_at_term(doc_id, payload, self.current_primary_term())
    }

    fn add_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
    ) -> Result<IndexWriteReceipt>;

    /// Bulk-index documents. Each tuple is (doc_id, payload). Returns document IDs.
    fn bulk_add_documents(&self, docs: Vec<(String, serde_json::Value)>) -> Result<Vec<String>> {
        Ok(self.bulk_add_documents_with_receipt(docs)?.doc_ids)
    }

    fn bulk_add_documents_with_receipt(
        &self,
        docs: Vec<(String, serde_json::Value)>,
    ) -> Result<BulkWriteReceipt> {
        self.bulk_add_documents_with_receipt_at_term(docs, self.current_primary_term())
    }

    fn bulk_add_documents_with_receipt_at_term(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        primary_term: u64,
    ) -> Result<BulkWriteReceipt>;

    /// Delete a document by its `_id`. Returns the number of deleted documents.
    fn delete_document(&self, doc_id: &str) -> Result<u64> {
        Ok(self.delete_document_with_receipt(doc_id)?.deleted)
    }

    fn delete_document_with_receipt(&self, doc_id: &str) -> Result<DeleteWriteReceipt> {
        self.delete_document_with_receipt_at_term(doc_id, self.current_primary_term())
    }

    fn delete_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
    ) -> Result<DeleteWriteReceipt>;

    fn apply_replica_operation(&self, operation: SequencedOperation)
    -> Result<ReplicaApplyReceipt>;

    fn apply_replica_batch(
        &self,
        operations: Vec<SequencedOperation>,
    ) -> Result<ReplicaBulkApplyReceipt>;

    /// Persist a sequence-numbered no-op without mutating document state.
    fn apply_noop_with_seq(&self, reason: &str, seq_no: u64, primary_term: u64) -> Result<()> {
        self.apply_replica_operation(SequencedOperation {
            seq_no,
            primary_term,
            mutation: DocumentMutation::NoOp {
                reason: reason.to_string(),
            },
        })?;
        Ok(())
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

    /// Force-merge segments down to at most `max_num_segments` (which must be at least 1).
    /// Commits first to ensure all buffered docs are on disk, then merges.
    fn force_merge(&self, max_num_segments: usize) -> Result<()>;

    /// Returns per-segment metadata for diagnostics/monitoring.
    fn segment_infos(&self) -> Vec<SegmentInfo> {
        vec![]
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

    /// Build multiple smaller RecordBatches from local Tantivy columns using bitset
    /// collection instead of TopDocs. Collects ALL matching docs (no scan limit),
    /// produces batches of `batch_size` rows for streaming DataFusion execution.
    /// Used for GROUP BY fallback queries where the TopDocs cap would produce wrong results.
    fn sql_streaming_batch_handle(
        &self,
        _req: &crate::search::SearchRequest,
        _columns: &[String],
        _needs_id: bool,
        _needs_score: bool,
        _batch_size: usize,
    ) -> Result<Option<SqlStreamingBatchHandle>> {
        Ok(None)
    }

    /// Build multiple smaller RecordBatches eagerly by draining the lazy streaming
    /// handle into memory. This keeps the old API surface for tests and buffered
    /// compatibility paths while the streamed coordinator path consumes batches lazily.
    fn sql_streaming_batches(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<Option<SqlStreamingResult>> {
        let Some(mut handle) =
            self.sql_streaming_batch_handle(req, columns, needs_id, needs_score, batch_size)?
        else {
            return Ok(None);
        };

        let total_hits = handle.total_hits;
        let collected_rows = handle.collected_rows;
        let mut batches = Vec::new();
        while let Some(batch) = handle.next_batch()? {
            batches.push(batch);
        }

        Ok(Some(SqlStreamingResult {
            batches,
            total_hits,
            collected_rows,
        }))
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

    /// Get the local checkpoint: highest observed seq_no applied to this shard copy.
    /// This is currently a high-water mark, not a contiguous-prefix proof.
    /// Returns 0 if no seq_no tracking is configured (backward compat).
    fn local_checkpoint(&self) -> u64 {
        self.sequence_stats().processed_checkpoint.unwrap_or(0)
    }

    /// Update the local checkpoint after applying a replicated operation.
    fn update_local_checkpoint(&self, _seq_no: u64) {}

    fn sequence_stats(&self) -> SequenceStats {
        SequenceStats {
            processed_checkpoint: None,
            persisted_checkpoint: None,
            max_seq_no: None,
        }
    }

    fn wal_max_seq_no(&self) -> Option<u64> {
        None
    }

    fn reconcile_term_sequence_state(
        &self,
        _identity_fence: u64,
        _identity_fence_max_seq_no: Option<u64>,
    ) -> Result<()> {
        Ok(())
    }

    fn current_primary_term(&self) -> u64 {
        1
    }

    /// Get the global checkpoint: min of all in-sync replica checkpoints.
    /// Only meaningful on the primary shard.
    fn global_checkpoint(&self) -> Option<u64> {
        None
    }

    /// Update the global checkpoint (called by primary after collecting replica checkpoints).
    fn update_global_checkpoint(&self, _checkpoint: u64) {}

    fn create_peer_recovery_snapshot(
        &self,
        _snapshot_dir: &std::path::Path,
    ) -> Result<PeerRecoverySnapshot> {
        anyhow::bail!("peer recovery snapshots are not supported by this engine")
    }

    fn prepare_peer_recovery_snapshot(
        &self,
        _snapshot_dir: &std::path::Path,
    ) -> Result<PeerRecoverySnapshotPreparation> {
        anyhow::bail!("peer recovery snapshot preparation is not supported by this engine")
    }

    fn release_peer_recovery_pin(&self, _pin_id: u64) -> Result<()> {
        anyhow::bail!("peer recovery retention pins are not supported by this engine")
    }

    fn peer_recovery_ops(
        &self,
        _min_seq_no: u64,
        _max_ops: usize,
        _max_bytes: usize,
    ) -> Result<PeerRecoveryOpsBatch> {
        anyhow::bail!("peer recovery operation streaming is not supported by this engine")
    }

    fn peer_recovery_commit_files(&self) -> Result<Vec<String>> {
        anyhow::bail!("peer recovery commit inspection is not supported by this engine")
    }
}

#[cfg(test)]
mod tests {
    use super::BulkWriteReceipt;

    #[test]
    fn bulk_receipts_distinguish_zero_from_missing_sequences() {
        let receipt = BulkWriteReceipt {
            doc_ids: vec!["a".into(), "b".into()],
            start_seq_no: Some(0),
            primary_term: 1,
        };
        assert_eq!(receipt.last_seq_no().unwrap(), Some(1));
        assert!(
            BulkWriteReceipt {
                doc_ids: vec!["a".into()],
                start_seq_no: None,
                primary_term: 1,
            }
            .last_seq_no()
            .is_err()
        );
        assert!(
            BulkWriteReceipt {
                doc_ids: vec!["a".into(), "b".into()],
                start_seq_no: Some(u64::MAX),
                primary_term: 1,
            }
            .last_seq_no()
            .is_err()
        );
        assert_eq!(
            BulkWriteReceipt {
                doc_ids: vec![],
                start_seq_no: None,
                primary_term: 1,
            }
            .last_seq_no()
            .unwrap(),
            None
        );
    }
}
