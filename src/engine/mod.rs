pub mod canonical;
pub mod client_operation;
pub mod column_cache;
pub mod composite;
pub mod remote_store;
pub mod routing;
pub(crate) mod sequence;
pub mod snapshot;
pub mod tantivy;
pub mod vector;
pub(crate) mod version_map;
pub mod write_authority;
pub(crate) mod write_failure;

use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub use self::client_operation::{
    CLIENT_OPERATION_ID_MAX_BYTES, CLIENT_OPERATION_RETENTION_OPERATIONS,
    ClientOperationConflictError, ClientOperationIntent, ClientOperationRecord,
    ClientOperationResult,
};
pub use self::composite::{CompositeEngine, VectorRecoveryMode, VectorRecoveryProgress};
pub use self::sequence::{SEQUENCE_FORMAT_VERSION, SequenceStats};
pub use self::snapshot::{
    PERSISTED_SHARD_SNAPSHOT_DIR, SHARD_SNAPSHOT_COMPLETE_FILE, SHARD_SNAPSHOT_FORMAT_CAPABILITY,
    SHARD_SNAPSHOT_FORMAT_VERSION, SHARD_SNAPSHOT_MANIFEST_FILE, ShardSnapshotCompletionMarker,
    ShardSnapshotManifest, ShardSnapshotVectorMetadata, compute_shard_schema_hash,
};
pub use self::tantivy::HotEngine;

pub(crate) const PEER_RECOVERY_VERSION_SNAPSHOT_FILE: &str = "retained-delete-versions.json";
pub(crate) const PEER_RECOVERY_CANONICAL_HISTORY_FILE: &str = "retained-canonical-history.bin";
pub(crate) const PEER_RECOVERY_VERSION_SNAPSHOT_FORMAT: u32 = 3;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerRecoveryVersionSnapshot {
    pub format_version: u32,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub canonical_history: Option<crate::cluster::state::CanonicalHistory>,
    pub history_start_seq_no: u64,
    pub history_end_seq_no: u64,
    pub deletes: Vec<PeerRecoveryDeleteVersion>,
    pub client_operations: Vec<PeerRecoveryClientOperation>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerRecoveryDeleteVersion {
    pub doc_id: String,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerRecoveryClientOperation {
    pub seq_no: u64,
    pub primary_term: u64,
    pub op: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SequencedOperation {
    pub seq_no: u64,
    pub primary_term: u64,
    /// Primary-assigned per-document version. NoOp operations use zero.
    pub document_version: u64,
    #[serde(deserialize_with = "serde::Deserialize::deserialize")]
    pub client_operation: Option<ClientOperationRecord>,
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
    error.is::<DocumentValidationError>()
        || error.is::<crate::common::ReservedDocumentFieldError>()
        || error.is::<crate::wal::WalFrameTooLargeError>()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WriteCondition {
    #[default]
    Unconditional,
    Create,
    IfMatch {
        seq_no: u64,
        primary_term: u64,
    },
}

impl WriteCondition {
    pub fn expected_version(self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::IfMatch {
                seq_no,
                primary_term,
            } => (Some(seq_no), Some(primary_term)),
            _ => (None, None),
        }
    }

    pub fn from_optional_values(seq_no: Option<u64>, primary_term: Option<u64>) -> Result<Self> {
        match (seq_no, primary_term) {
            (None, None) => Ok(Self::Unconditional),
            (Some(seq_no), Some(primary_term)) if primary_term > 0 => Ok(Self::IfMatch {
                seq_no,
                primary_term,
            }),
            (Some(_), Some(_)) => Err(DocumentValidationError(
                "if_primary_term must be greater than zero".to_string(),
            )
            .into()),
            _ => Err(DocumentValidationError(
                "if_seq_no and if_primary_term must be specified together".to_string(),
            )
            .into()),
        }
    }

    pub(crate) fn check(self, doc_id: &str, current: Option<(u64, u64)>) -> Result<()> {
        let reason = match self {
            Self::Unconditional => return Ok(()),
            Self::Create if current.is_none() => return Ok(()),
            Self::Create => format!("[{doc_id}]: version conflict, document already exists"),
            Self::IfMatch {
                seq_no,
                primary_term,
            } if current == Some((seq_no, primary_term)) => return Ok(()),
            Self::IfMatch {
                seq_no,
                primary_term,
            } => {
                let actual = match current {
                    Some((seq, term)) => {
                        format!("current document has seqNo [{seq}] and primary term [{term}]")
                    }
                    None => "but no document was found".to_string(),
                };
                format!(
                    "[{doc_id}]: version conflict, required seqNo [{seq_no}], primary term [{primary_term}]. {actual}"
                )
            }
        };
        Err(VersionConflictError(reason).into())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VersionConflictError(pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct DocumentRead {
    pub source: serde_json::Value,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexWriteReceipt {
    pub doc_id: String,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
    pub created: bool,
    pub client_operation: Option<ClientOperationRecord>,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkWriteReceipt {
    pub doc_ids: Vec<String>,
    pub start_seq_no: Option<u64>,
    pub primary_term: u64,
    pub versions: Vec<u64>,
    pub created: Vec<bool>,
}

impl BulkWriteReceipt {
    pub fn last_seq_no(&self) -> Result<Option<u64>> {
        if self.created.len() != self.doc_ids.len()
            || self.versions.len() != self.doc_ids.len()
            || self.versions.contains(&0)
        {
            anyhow::bail!("bulk write receipt has inconsistent item results");
        }
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
    pub version: u64,
    pub client_operation: Option<ClientOperationRecord>,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientNoOpWriteReceipt {
    pub operation_seq_no: u64,
    pub operation_primary_term: u64,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
    pub client_operation: ClientOperationRecord,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerRecoveryFileMetadata {
    pub name: String,
    pub length: u64,
    pub sha256: String,
}

pub struct PeerRecoverySnapshot {
    pub snapshot_cursor: crate::wal::WalCursor,
    #[cfg(test)]
    pub snapshot_next_seq_no: u64,
    #[cfg(feature = "protocol-trace")]
    pub trace_processed_seqs: Vec<u64>,
    #[cfg(feature = "protocol-trace")]
    pub trace_documents: Vec<(String, serde_json::Value, u64, u64)>,
    #[allow(dead_code)]
    pub(crate) committed_boundary: sequence::CommittedBoundaryRecord,
    pub retention_pin_id: u64,
    pub files: Vec<PeerRecoveryFileMetadata>,
    pub manifest: snapshot::ShardSnapshotManifest,
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
        let _ = std::thread::Builder::new()
            .name("peer-recovery-pin-release".into())
            .spawn(release);
    }
}

pub struct PeerRecoverySnapshotPreparation {
    pub snapshot_cursor: crate::wal::WalCursor,
    #[cfg(test)]
    pub snapshot_next_seq_no: u64,
    #[cfg(feature = "protocol-trace")]
    pub trace_processed_seqs: Vec<u64>,
    #[cfg(feature = "protocol-trace")]
    pub trace_documents: Vec<(String, serde_json::Value, u64, u64)>,
    pub(crate) committed_boundary: sequence::CommittedBoundaryRecord,
    pub retention_pin: PeerRecoveryRetentionPin,
    pub file_names: Vec<String>,
    pub schema_hash: String,
    pub vector_state: snapshot::ShardSnapshotVectorMetadata,
    pub index_uuid: Option<String>,
    pub shard_id: Option<u32>,
    pub allocation_id: Option<u64>,
}

pub struct PreparedPeerRecoverySnapshot {
    pub snapshot_cursor: crate::wal::WalCursor,
    #[cfg(test)]
    pub snapshot_next_seq_no: u64,
    #[cfg(feature = "protocol-trace")]
    pub trace_processed_seqs: Vec<u64>,
    #[cfg(feature = "protocol-trace")]
    pub trace_documents: Vec<(String, serde_json::Value, u64, u64)>,
    pub(crate) committed_boundary: sequence::CommittedBoundaryRecord,
    pub retention_pin: PeerRecoveryRetentionPin,
    pub files: Vec<PeerRecoveryFileMetadata>,
    pub manifest: snapshot::ShardSnapshotManifest,
}

impl PeerRecoverySnapshotPreparation {
    pub fn hash_files(self, snapshot_dir: &Path) -> Result<PreparedPeerRecoverySnapshot> {
        let Self {
            snapshot_cursor,
            #[cfg(test)]
            snapshot_next_seq_no,
            #[cfg(feature = "protocol-trace")]
            trace_processed_seqs,
            #[cfg(feature = "protocol-trace")]
            trace_documents,
            committed_boundary,
            retention_pin,
            file_names,
            schema_hash,
            vector_state,
            index_uuid,
            shard_id,
            allocation_id,
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
        let snapshot_id = snapshot_dir
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let manifest = snapshot::ShardSnapshotManifest::new(
            snapshot_id,
            index_uuid,
            shard_id,
            allocation_id,
            schema_hash,
            committed_boundary.clone(),
            snapshot_cursor,
            vector_state,
            files.clone(),
        )?;
        manifest.persist_atomically(snapshot_dir)?;
        Ok(PreparedPeerRecoverySnapshot {
            snapshot_cursor,
            #[cfg(test)]
            snapshot_next_seq_no,
            #[cfg(feature = "protocol-trace")]
            trace_processed_seqs,
            #[cfg(feature = "protocol-trace")]
            trace_documents,
            committed_boundary,
            retention_pin,
            files,
            manifest,
        })
    }
}

pub struct PeerRecoveryOpsBatch {
    pub operations: Vec<crate::wal::TranslogEntry>,
    pub next_cursor: crate::wal::WalCursor,
    pub source_max_seq_no: Option<u64>,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerRecoveryBarrier {
    pub wal_end: crate::wal::WalCursor,
    pub sequence: SequenceStats,
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

#[cfg(feature = "protocol-trace")]
pub type ProtocolTraceCopyEvidence = (
    Vec<(String, serde_json::Value, u64, u64)>,
    Vec<crate::protocol_trace::TraceActualDocument>,
    Vec<crate::protocol_trace::TraceWalEntry>,
);

pub const SEAL_WATERMARK_FILE: &str = "seal.watermark.json";
pub const SEAL_WATERMARK_FORMAT_VERSION: u32 = 1;

/// Durable in-flight seal barrier persisted in `seal.watermark.json` (`FS-035`, `FS-036`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveSealBarrier {
    pub seal_id: String,
    pub operation_id: String,
    pub start_seq_no: u64,
    pub through_seq_no: u64,
    pub primary_term: u64,
    pub live_doc_count: u64,
    pub tombstone_count: u64,
    pub estimated_bytes: u64,
    pub created_at_epoch_ms: u64,
}

/// Sealed document version entry retained across hot-delta retirement so OCC,
/// `_update`, `DELETE`, and version numbers remain accurate (`FS-034`, `FS-036`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedDocVersionEntry {
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
    pub deleted: bool,
}

/// Durable shard-local seal and visibility watermark state (`FS-034`, `FS-035`, `FS-036`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealWatermarkState {
    pub format_version: u32,
    pub sealed_through_seq_no: Option<u64>,
    pub retired_through_seq_no: Option<u64>,
    pub last_sealed_primary_term: Option<u64>,
    pub last_sealed_generation: Option<u64>,
    pub last_seal_timestamp_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_seal: Option<ActiveSealBarrier>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub sealed_doc_versions: std::collections::BTreeMap<String, SealedDocVersionEntry>,
}

impl Default for SealWatermarkState {
    fn default() -> Self {
        Self {
            format_version: SEAL_WATERMARK_FORMAT_VERSION,
            sealed_through_seq_no: None,
            retired_through_seq_no: None,
            last_sealed_primary_term: None,
            last_sealed_generation: None,
            last_seal_timestamp_ms: 0,
            active_seal: None,
            sealed_doc_versions: std::collections::BTreeMap::new(),
        }
    }
}

impl SealWatermarkState {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != SEAL_WATERMARK_FORMAT_VERSION {
            return Err(crate::common::unsupported_index_format(
                "shard seal watermark",
                format!(
                    "format_version {} is not supported; expected {}",
                    self.format_version, SEAL_WATERMARK_FORMAT_VERSION
                ),
            ));
        }
        if let (Some(retired), Some(sealed)) =
            (self.retired_through_seq_no, self.sealed_through_seq_no)
            && retired > sealed
        {
            anyhow::bail!(
                "invalid seal watermark state: retired_through_seq_no ({retired}) > sealed_through_seq_no ({sealed})"
            );
        }
        if self.retired_through_seq_no.is_some() && self.sealed_through_seq_no.is_none() {
            anyhow::bail!(
                "invalid seal watermark state: retired_through_seq_no is set while sealed_through_seq_no is None"
            );
        }
        if let Some(ref active) = self.active_seal {
            if active.through_seq_no < active.start_seq_no || active.primary_term == 0 {
                anyhow::bail!(
                    "invalid active seal barrier: start_seq_no={} through_seq_no={} primary_term={}",
                    active.start_seq_no,
                    active.through_seq_no,
                    active.primary_term
                );
            }
            if active.operation_id.is_empty() || active.seal_id.is_empty() {
                anyhow::bail!("invalid active seal barrier: empty seal_id or operation_id");
            }
        }
        Ok(())
    }
}

/// Coalesced logical document state within a contiguous hot-delta seal range (`FS-034`, `FS-035`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SealedDocumentRecord {
    Live {
        doc_id: String,
        source: serde_json::Value,
        seq_no: u64,
        primary_term: u64,
        version: u64,
    },
    Tombstone {
        doc_id: String,
        seq_no: u64,
        primary_term: u64,
        version: u64,
    },
}

impl SealedDocumentRecord {
    pub fn doc_id(&self) -> &str {
        match self {
            Self::Live { doc_id, .. } | Self::Tombstone { doc_id, .. } => doc_id,
        }
    }

    pub fn seq_no(&self) -> u64 {
        match self {
            Self::Live { seq_no, .. } | Self::Tombstone { seq_no, .. } => *seq_no,
        }
    }

    pub fn primary_term(&self) -> u64 {
        match self {
            Self::Live { primary_term, .. } | Self::Tombstone { primary_term, .. } => *primary_term,
        }
    }

    pub fn version(&self) -> u64 {
        match self {
            Self::Live { version, .. } | Self::Tombstone { version, .. } => *version,
        }
    }
}

/// Contiguous, gap-free hot-delta batch selected for immutable split sealing (`FS-034`, `FS-035`).
#[derive(Debug, Clone, PartialEq)]
pub struct SealBatchCandidate {
    pub start_seq_no: u64,
    pub through_seq_no: u64,
    pub primary_term: u64,
    pub operation_count: usize,
    pub estimated_bytes: u64,
    pub records: Vec<SealedDocumentRecord>,
}

impl SealBatchCandidate {
    pub fn live_doc_count(&self) -> u64 {
        self.records
            .iter()
            .filter(|r| matches!(r, SealedDocumentRecord::Live { .. }))
            .count() as u64
    }

    pub fn tombstone_count(&self) -> u64 {
        self.records
            .iter()
            .filter(|r| matches!(r, SealedDocumentRecord::Tombstone { .. }))
            .count() as u64
    }
}

/// Snapshot of unsealed hot-delta backlog and seal watermarks on a shard (`FS-034`, `FS-035`, `FS-036`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsealedDeltaStats {
    pub sealed_through_seq_no: Option<u64>,
    pub retired_through_seq_no: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
    pub unsealed_ops: u64,
    pub unsealed_bytes: u64,
    pub oldest_unsealed_age_ms: u64,
    pub active_seal: Option<ActiveSealBarrier>,
}

pub type LiveDocumentVersionTuple = (String, serde_json::Value, u64, u64, u64);
pub type DeletedDocumentVersionTuple = (String, u64, u64, u64);

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
    fn copy_write_state(&self) -> Arc<write_authority::CopyWriteState>;

    fn write_failure_reason(&self) -> Option<String>;

    fn fail_stop_writes(&self, reason: &str);

    fn has_all_mapped_fields(
        &self,
        _mappings: &std::collections::HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> bool {
        true
    }

    fn ensure_write_health(&self) -> Result<()> {
        match self.write_failure_reason() {
            Some(reason) => Err(crate::wal::WalFailStoppedError { reason }.into()),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    fn inject_wal_write_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn inject_writer_replacement_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn inject_engine_apply_failures_for_test(&self, _raw_os_error: i32, _attempts: usize) {}

    #[cfg(test)]
    fn inject_refresh_commit_failures_for_test(&self, _attempts: usize) {
        panic!("refresh commit fault injection requires a local shard engine");
    }

    #[cfg(test)]
    fn inject_replay_commit_failure_for_test(
        &self,
        _raw_os_error: i32,
        _successful_commits: usize,
    ) {
        panic!("replay commit fault injection requires a local shard engine");
    }

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

    fn add_document_with_condition_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        condition: WriteCondition,
    ) -> Result<IndexWriteReceipt>;

    fn add_document_with_operation_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        condition: WriteCondition,
        client_operation: Option<ClientOperationIntent>,
    ) -> Result<IndexWriteReceipt> {
        if client_operation.is_some() {
            anyhow::bail!("client operation IDs are not supported by this search engine");
        }
        self.add_document_with_condition_at_term(doc_id, payload, primary_term, condition)
    }

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

    fn delete_document_with_condition_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
        condition: WriteCondition,
    ) -> Result<DeleteWriteReceipt>;

    fn delete_document_with_operation_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
        condition: WriteCondition,
        client_operation: Option<ClientOperationIntent>,
    ) -> Result<DeleteWriteReceipt> {
        if client_operation.is_some() {
            anyhow::bail!("client operation IDs are not supported by this search engine");
        }
        self.delete_document_with_condition_at_term(doc_id, primary_term, condition)
    }

    fn record_client_noop_at_term(
        &self,
        _primary_term: u64,
        _client_operation: ClientOperationIntent,
        _seq_no: u64,
        _result_primary_term: u64,
        _version: u64,
    ) -> Result<ClientNoOpWriteReceipt> {
        anyhow::bail!("client operation no-ops are not supported by this search engine")
    }

    fn retained_client_operation(
        &self,
        _seq_no: u64,
        _primary_term: u64,
        _record: &ClientOperationRecord,
    ) -> Result<SequencedOperation> {
        anyhow::bail!("retained client operations are not supported by this search engine")
    }

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
            document_version: 0,
            client_operation: None,
            mutation: DocumentMutation::NoOp {
                reason: reason.to_string(),
            },
        })?;
        Ok(())
    }

    /// Retrieve a document by its `_id`. Returns the `_source` JSON if found.
    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>>;

    fn get_document_with_metadata(
        &self,
        doc_id: &str,
        realtime: bool,
    ) -> Result<Option<DocumentRead>>;

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_documents(&self) -> Result<Vec<(String, serde_json::Value, u64, u64)>> {
        anyhow::bail!("protocol trace document enumeration is not supported by this engine")
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_processed_sequences(&self) -> Result<Vec<u64>> {
        anyhow::bail!(
            "protocol trace processed-sequence enumeration is not supported by this engine"
        )
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_copy_evidence(&self) -> Result<ProtocolTraceCopyEvidence> {
        anyhow::bail!("protocol trace copy evidence is not supported by this engine")
    }

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

    /// Get the highest contiguous processed sequence on this shard copy.
    /// `None` means no operation has been processed and differs from `Some(0)`.
    fn local_checkpoint(&self) -> Option<u64> {
        self.sequence_stats().processed_checkpoint
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

    fn seal_watermark_state(&self) -> Result<SealWatermarkState> {
        Ok(SealWatermarkState::default())
    }

    fn record_active_seal_barrier(&self, _barrier: Option<ActiveSealBarrier>) -> Result<()> {
        anyhow::bail!("seal barriers are not supported by this engine")
    }

    fn enumerate_seal_boundary(
        &self,
        _after_seq_no: Option<u64>,
        _max_docs: usize,
        _max_bytes: u64,
    ) -> Result<Option<SealBatchCandidate>> {
        Ok(None)
    }

    fn retire_sealed_delta_through(
        &self,
        _committed_watermark_seq_no: u64,
        _manifest_generation: u64,
    ) -> Result<usize> {
        Ok(0)
    }

    fn unsealed_delta_stats(&self) -> Result<UnsealedDeltaStats> {
        Ok(UnsealedDeltaStats {
            sealed_through_seq_no: None,
            retired_through_seq_no: None,
            persisted_checkpoint: None,
            unsealed_ops: 0,
            unsealed_bytes: 0,
            oldest_unsealed_age_ms: 0,
            active_seal: None,
        })
    }

    fn live_documents_with_versions(&self) -> Result<Vec<LiveDocumentVersionTuple>> {
        Ok(Vec::new())
    }

    fn deleted_documents_with_versions(&self) -> Result<Vec<DeletedDocumentVersionTuple>> {
        Ok(Vec::new())
    }

    fn restore_documents_with_versions(
        &self,
        _live_docs: &[LiveDocumentVersionTuple],
        _tombstones: &[DeletedDocumentVersionTuple],
        _watermark_state: Option<SealWatermarkState>,
    ) -> Result<usize> {
        anyhow::bail!("document version restoration is not supported by this engine")
    }

    /// Get the monotonic minimum persisted checkpoint across authoritative copies.
    /// Only meaningful on the primary shard; `None` means it is not yet available.
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

    #[cfg(test)]
    fn notify_peer_recovery_snapshot_ready_for_test(
        &self,
        _prepared: &PeerRecoverySnapshotPreparation,
    ) {
    }

    fn create_persisted_shard_snapshot(
        &self,
        snapshot_dir: &std::path::Path,
    ) -> Result<snapshot::ShardSnapshotManifest> {
        let prepared = self
            .prepare_peer_recovery_snapshot(snapshot_dir)?
            .hash_files(snapshot_dir)?;
        prepared.retention_pin.release()?;
        Ok(prepared.manifest)
    }

    fn latest_persisted_shard_snapshot(&self) -> Result<Option<snapshot::ShardSnapshotManifest>> {
        Ok(None)
    }

    fn recover_vectors(&self) -> Result<VectorRecoveryProgress> {
        Ok(VectorRecoveryProgress::not_applicable())
    }

    fn vector_recovery_progress(&self) -> Option<VectorRecoveryProgress> {
        None
    }

    fn release_peer_recovery_pin(&self, _pin_id: u64) -> Result<()> {
        anyhow::bail!("peer recovery retention pins are not supported by this engine")
    }

    fn peer_recovery_ops(
        &self,
        _cursor: crate::wal::WalCursor,
        _end_cursor: Option<crate::wal::WalCursor>,
        _max_ops: usize,
        _max_bytes: usize,
    ) -> Result<PeerRecoveryOpsBatch> {
        anyhow::bail!("peer recovery operation streaming is not supported by this engine")
    }

    fn retained_recovery_ops(
        &self,
        _min_seq_no: u64,
        _max_ops: usize,
        _max_bytes: usize,
    ) -> Result<PeerRecoveryOpsBatch> {
        anyhow::bail!("retained recovery operation streaming is not supported by this engine")
    }

    fn peer_recovery_barrier(&self) -> Result<PeerRecoveryBarrier> {
        anyhow::bail!("peer recovery barriers are not supported by this engine")
    }

    fn freeze_canonical_history(&self) -> Result<canonical::FrozenEngineHistory> {
        anyhow::bail!("canonical history freezing is not supported by this engine")
    }

    fn canonical_inventory(
        &self,
        _end_cursor: crate::wal::WalCursor,
        _start_seq_no: u64,
        _end_seq_no: u64,
    ) -> Result<canonical::HistoryInventory> {
        anyhow::bail!("canonical history inventory is not supported by this engine")
    }

    fn canonical_operation_at(
        &self,
        _cursor: crate::wal::WalCursor,
        _end_cursor: crate::wal::WalCursor,
    ) -> Result<SequencedOperation> {
        anyhow::bail!("canonical history reads are not supported by this engine")
    }

    fn append_canonical_operations(
        &self,
        _operations: &[SequencedOperation],
    ) -> Result<ReplicaBulkApplyReceipt> {
        anyhow::bail!("canonical history resynchronization is not supported by this engine")
    }

    fn prepare_primary_activation(&self, _primary_term: u64) -> Result<Vec<SequencedOperation>> {
        anyhow::bail!("primary activation gap filling is not supported by this engine")
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
            versions: vec![1, 1],
            created: vec![true, true],
        };
        assert_eq!(receipt.last_seq_no().unwrap(), Some(1));
        assert!(
            BulkWriteReceipt {
                doc_ids: vec!["a".into()],
                start_seq_no: None,
                primary_term: 1,
                versions: vec![1],
                created: vec![true],
            }
            .last_seq_no()
            .is_err()
        );
        assert!(
            BulkWriteReceipt {
                doc_ids: vec!["a".into(), "b".into()],
                start_seq_no: Some(u64::MAX),
                primary_term: 1,
                versions: vec![1, 1],
                created: vec![true, true],
            }
            .last_seq_no()
            .is_err()
        );
        assert_eq!(
            BulkWriteReceipt {
                doc_ids: vec![],
                start_seq_no: None,
                primary_term: 1,
                versions: vec![],
                created: vec![],
            }
            .last_seq_no()
            .unwrap(),
            None
        );
    }
}
