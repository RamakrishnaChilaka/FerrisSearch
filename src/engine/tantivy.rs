use anyhow::{Context, Result};
use datafusion::arrow::record_batch::RecordBatch;
use std::any::Any;
use std::borrow::Cow;
#[cfg(feature = "protocol-trace")]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tantivy::collector::{Count, TopDocs};
use tantivy::merge_policy::{MergeCandidate, MergePolicy, NoMergePolicy};
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, Field, STORED, STRING, Schema, TEXT, Value};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, SegmentMeta, TantivyDocument, Term};

use super::SearchEngine;
use super::sequence::{
    CommittedBoundaryRecord, LocalCheckpointTracker, PrimaryTermSequenceState, SequenceStats,
    initialize_term_sequence_state,
};
use super::version_map::VersionValue;
use super::version_map::{DEFAULT_VERSION_MAP_MAX_BYTES, LiveVersionMap, PrunedTombstone};
use crate::wal::{
    HotTranslog, TranslogDurability, WalDocumentOperation, WriteAheadLog, document_operation,
};

#[derive(Debug, thiserror::Error)]
#[error("authoritative shard schema validation failed: {message}")]
pub(crate) struct AuthoritativeSchemaError {
    message: String,
}

fn authoritative_schema_error(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(AuthoritativeSchemaError {
        message: message.into(),
    })
}

#[derive(Debug, thiserror::Error)]
#[error("Tantivy writer is unavailable during {context}: {reason}")]
pub(crate) struct TantivyWriterUnavailableError {
    context: String,
    reason: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Tantivy commit failed during {context}: {source}")]
pub(crate) struct TantivyCommitFailureError {
    context: String,
    #[source]
    source: tantivy::TantivyError,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "sequence operation collision at ({primary_term}, {seq_no}): existing operation differs from the incoming operation"
)]
pub(crate) struct SequenceOperationCollisionError {
    primary_term: u64,
    seq_no: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("internal sequence field validation failed: {message}")]
pub(crate) struct InternalSequenceFieldError {
    message: String,
}

struct ApplyState {
    checkpoints: LocalCheckpointTracker,
    term_sequences: PrimaryTermSequenceState,
    versions: LiveVersionMap,
    max_seq_no_of_updates_or_deletes: Option<u64>,
}

#[derive(Clone)]
struct SequencePlanningSnapshot {
    checkpoints: LocalCheckpointTracker,
    term_sequences: PrimaryTermSequenceState,
    max_seq_no_of_updates_or_deletes: Option<u64>,
}

impl ApplyState {
    fn new(committed: CommittedBoundaryRecord) -> Result<Self> {
        let term_sequences = initialize_term_sequence_state(
            committed.term_sequence_state.current_term,
            committed.term_sequence_state.max_seq_no_at_term_start,
            &committed,
        )?;
        Ok(Self {
            checkpoints: LocalCheckpointTracker::new(committed.clone())?,
            term_sequences,
            versions: LiveVersionMap::new(DEFAULT_VERSION_MAP_MAX_BYTES),
            max_seq_no_of_updates_or_deletes: committed.max_seq_no_of_updates_or_deletes,
        })
    }

    fn reset_to_commit(&mut self, committed: CommittedBoundaryRecord) -> Result<()> {
        self.checkpoints.reset_to_commit(committed.clone())?;
        self.term_sequences = initialize_term_sequence_state(
            committed.term_sequence_state.current_term,
            committed.term_sequence_state.max_seq_no_at_term_start,
            &committed,
        )?;
        self.versions.reset();
        self.max_seq_no_of_updates_or_deletes = committed.max_seq_no_of_updates_or_deletes;
        Ok(())
    }

    fn complete_operation(
        &mut self,
        primary_term: u64,
        seq_no: u64,
        durability: TranslogDurability,
    ) -> Result<()> {
        self.term_sequences.mark_processed(primary_term, seq_no)?;
        self.checkpoints.mark_processed(seq_no);
        if matches!(durability, TranslogDurability::Request) {
            self.checkpoints.mark_persisted(seq_no);
        }
        Ok(())
    }

    fn committed_boundary(&self) -> CommittedBoundaryRecord {
        CommittedBoundaryRecord {
            version: super::sequence::COMMITTED_BOUNDARY_FORMAT_VERSION,
            processed_checkpoint: self.checkpoints.processed_checkpoint(),
            persisted_checkpoint: self.checkpoints.persisted_checkpoint(),
            max_seq_no: self.checkpoints.max_seq_no(),
            max_seq_no_of_updates_or_deletes: self.max_seq_no_of_updates_or_deletes,
            term_sequence_state: self.term_sequences.to_record(),
        }
    }

    fn planning_snapshot(&self) -> SequencePlanningSnapshot {
        SequencePlanningSnapshot {
            checkpoints: self.checkpoints.clone(),
            term_sequences: self.term_sequences.clone(),
            max_seq_no_of_updates_or_deletes: self.max_seq_no_of_updates_or_deletes,
        }
    }

    fn restore_planning_snapshot(&mut self, snapshot: SequencePlanningSnapshot) {
        self.checkpoints = snapshot.checkpoints;
        self.term_sequences = snapshot.term_sequences;
        self.max_seq_no_of_updates_or_deletes = snapshot.max_seq_no_of_updates_or_deletes;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalDisposition<'a> {
    Append,
    AlreadyInLocalWal {
        persisted: bool,
        validate_redelivery: bool,
        start_cursor: Option<crate::wal::WalCursor>,
        replay_positions: Option<&'a WalPositions>,
        initial_versions: Option<&'a HashMap<&'a str, Option<CurrentVersion>>>,
    },
}

type WalPositions = HashMap<(u64, u64), crate::wal::WalCursor>;

impl WalDisposition<'_> {
    fn is_already_in_local_wal(self) -> bool {
        matches!(self, Self::AlreadyInLocalWal { .. })
    }

    fn is_persisted(self) -> bool {
        matches!(
            self,
            Self::AlreadyInLocalWal {
                persisted: true,
                ..
            }
        )
    }

    fn validates_redelivery(self) -> bool {
        matches!(
            self,
            Self::Append
                | Self::AlreadyInLocalWal {
                    validate_redelivery: true,
                    ..
                }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurrentVersion {
    Native(super::version_map::VersionValue),
}

struct PlannedOperation<'a> {
    operation: &'a super::SequencedOperation,
    outcome: super::ApplyOutcome,
    complete: bool,
}

/// Dynamic field registry — maps user-facing field names to Tantivy Field handles.
/// New fields are added on first encounter (dynamic mapping, like OpenSearch).
struct FieldRegistry {
    /// _id: unique document identifier (indexed, not tokenized)
    id_field: Field,
    /// _source: stores the raw JSON document (STORED only, not indexed)
    source_field: Field,
    seq_no_field: Option<Field>,
    primary_term_field: Option<Field>,
    /// Named text fields created dynamically from document keys
    fields: HashMap<String, Field>,
    /// Logical field mappings so Date remains distinct from Integer even
    /// though both are stored as Tantivy i64 fast fields.
    field_types: HashMap<String, crate::cluster::state::FieldType>,
    /// Mapped Date field names for targeted source normalization on ingest/read.
    date_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HotEnginePurpose {
    LocalShard,
    RemoteSplit,
}

impl HotEnginePurpose {
    fn requires_sequence_fields(self) -> bool {
        matches!(self, Self::LocalShard)
    }
}

const SEQ_NO_FIELD_NAME: &str = "_seq_no";
const PRIMARY_TERM_FIELD_NAME: &str = "_primary_term";
const VECTOR_REBUILD_BATCH_SIZE: usize = 1_024;

#[cfg(test)]
thread_local! {
    static VECTOR_REBUILD_BATCH_SIZE_OVERRIDE: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

fn vector_rebuild_batch_size() -> usize {
    #[cfg(test)]
    if let Some(batch_size) = VECTOR_REBUILD_BATCH_SIZE_OVERRIDE.with(|value| {
        let value = value.get();
        (value > 0).then_some(value)
    }) {
        return batch_size;
    }
    VECTOR_REBUILD_BATCH_SIZE
}

#[cfg(test)]
pub(crate) struct VectorRebuildBatchSizeGuard {
    previous: usize,
}

#[cfg(test)]
impl Drop for VectorRebuildBatchSizeGuard {
    fn drop(&mut self) {
        VECTOR_REBUILD_BATCH_SIZE_OVERRIDE.with(|value| value.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn override_vector_rebuild_batch_size_for_test(
    batch_size: usize,
) -> VectorRebuildBatchSizeGuard {
    assert!(batch_size > 0);
    let previous = VECTOR_REBUILD_BATCH_SIZE_OVERRIDE.with(|value| {
        let previous = value.get();
        value.set(batch_size);
        previous
    });
    VectorRebuildBatchSizeGuard { previous }
}

struct WriterState {
    writer: Option<IndexWriter>,
    failure: Option<String>,
}

impl WriterState {
    fn ready(writer: IndexWriter) -> Self {
        Self {
            writer: Some(writer),
            failure: None,
        }
    }

    fn writer_mut(&mut self, context: &str) -> Result<&mut IndexWriter> {
        self.writer.as_mut().ok_or_else(|| {
            anyhow::Error::new(TantivyWriterUnavailableError {
                context: context.to_string(),
                reason: self
                    .failure
                    .clone()
                    .unwrap_or_else(|| "writer reinitialization is incomplete".to_string()),
            })
        })
    }

    fn take(&mut self, context: &str) -> Result<IndexWriter> {
        self.writer.take().ok_or_else(|| {
            anyhow::Error::new(TantivyWriterUnavailableError {
                context: context.to_string(),
                reason: self
                    .failure
                    .clone()
                    .unwrap_or_else(|| "writer reinitialization is incomplete".to_string()),
            })
        })
    }

    fn replace(&mut self, writer: IndexWriter) {
        self.writer = Some(writer);
        self.failure = None;
    }

    fn fail(&mut self, error: impl Into<String>) {
        self.writer = None;
        self.failure = Some(error.into());
    }
}

#[derive(Clone)]
struct SharedMergePolicy(Arc<dyn MergePolicy>);

impl fmt::Debug for SharedMergePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SharedMergePolicy")
            .field(&self.0)
            .finish()
    }
}

impl MergePolicy for SharedMergePolicy {
    fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
        self.0.compute_merge_candidates(segments)
    }
}

/// Hot engine — Tantivy-backed search engine where all data lives in
/// memory-mapped segments for maximum query performance.
pub struct HotEngine {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<WriterState>>,
    maintenance_lock: Mutex<()>,
    automatic_merge_policy: RwLock<Arc<dyn MergePolicy>>,
    #[cfg(test)]
    force_merge_entry_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    force_merge_before_wait_sender: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    writer_replacement_failure: Mutex<Option<(i32, usize)>>,
    #[cfg(test)]
    engine_apply_failure: Mutex<Option<(i32, usize)>>,
    #[cfg(test)]
    refresh_commit_failures: Mutex<usize>,
    #[cfg(test)]
    post_apply_refresh_failures: Mutex<usize>,
    #[cfg(test)]
    refresh_before_writer_sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[cfg(test)]
    refresh_after_commit_sender: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    #[cfg(test)]
    refresh_after_commit_release_receiver: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    #[cfg(test)]
    peer_recovery_snapshot_ready_sender: Mutex<Option<std::sync::mpsc::Sender<u64>>>,
    #[cfg(test)]
    peer_recovery_snapshot_release_receiver: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    field_registry: RwLock<FieldRegistry>,
    /// The per-index refresh interval (e.g. 5s default, matches OpenSearch's index.refresh_interval)
    pub refresh_interval: Duration,
    /// Write-ahead log for crash durability
    translog: Arc<Mutex<dyn WriteAheadLog>>,
    apply_state: Mutex<ApplyState>,
    identity_term_state: Mutex<Option<(u64, Option<u64>)>>,
    committed_boundary_path: PathBuf,
    durability: TranslogDurability,
    delete_tombstone_retention: Duration,
    /// Shared column cache for fast-field Arrow arrays and grouped-partials
    /// full-segment decoded columns.
    column_cache: Arc<super::column_cache::ColumnCache>,
}

// Stay well below Tantivy's document-channel capacity so a replay against a
// persistently failing writer reaches a commit error instead of blocking while
// holding the translog lock.
const TRANSLOG_REPLAY_BATCH_SIZE: u64 = 1_000;
const TANTIVY_WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

struct AutomaticMergePolicyRestore<'a> {
    engine: &'a HotEngine,
    armed: bool,
}

impl AutomaticMergePolicyRestore<'_> {
    fn restore(mut self) -> Result<()> {
        let result = self.engine.restore_automatic_merge_policy();
        self.armed = false;
        result
    }
}

impl Drop for AutomaticMergePolicyRestore<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Err(error) = self.engine.restore_automatic_merge_policy()
        {
            tracing::error!(
                "Failed to restore Tantivy automatic merge policy after force merge: {error}"
            );
        }
    }
}

#[cfg(test)]
pub(crate) struct WriterLockForTest<'a> {
    guard: std::sync::RwLockWriteGuard<'a, WriterState>,
}

#[cfg(test)]
impl std::ops::Deref for WriterLockForTest<'_> {
    type Target = IndexWriter;

    fn deref(&self) -> &Self::Target {
        self.guard
            .writer
            .as_ref()
            .expect("test writer lock requires an available writer")
    }
}

#[cfg(test)]
impl std::ops::DerefMut for WriterLockForTest<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .writer
            .as_mut()
            .expect("test writer lock requires an available writer")
    }
}

pub(crate) fn canonical_keyword_scalar(value: &serde_json::Value) -> Option<Cow<'_, str>> {
    match value {
        serde_json::Value::String(text) => Some(Cow::Borrowed(text)),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => {
            Some(Cow::Owned(value.to_string()))
        }
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            None
        }
    }
}

pub(crate) fn visit_indexed_keyword_values<'a>(
    field_name: &str,
    value: &'a serde_json::Value,
    visitor: &mut impl FnMut(Cow<'a, str>),
) -> std::result::Result<(), super::DocumentValidationError> {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::Array(values) => {
            for value in values {
                visit_indexed_keyword_values(field_name, value, visitor)?;
            }
        }
        serde_json::Value::Object(_) => {
            return Err(super::DocumentValidationError(format!(
                "field [{field_name}] is a keyword field and cannot index object values"
            )));
        }
        value => visitor(
            canonical_keyword_scalar(value)
                .expect("non-null keyword scalar should have a canonical text value"),
        ),
    }
    Ok(())
}

/// Add a single mapped field to a Tantivy `SchemaBuilder`.
fn add_mapping_field_to_schema(
    builder: &mut tantivy::schema::SchemaBuilder,
    name: &str,
    mapping: &crate::cluster::state::FieldMapping,
) -> Result<Option<Field>> {
    use crate::cluster::state::FieldType;
    validate_authoritative_mapping_entry(name, mapping)?;
    if crate::common::is_builtin_body_field(name) {
        return Ok(None);
    }
    let field = match mapping.field_type {
        FieldType::Text => builder.add_text_field(name, TEXT | STORED),
        FieldType::Keyword => builder.add_text_field(name, (STRING | STORED).set_fast(None)),
        FieldType::Integer => builder.add_i64_field(name, tantivy::schema::INDEXED | STORED | FAST),
        FieldType::Float => builder.add_f64_field(name, tantivy::schema::INDEXED | STORED | FAST),
        FieldType::Boolean => builder.add_text_field(name, (STRING | STORED).set_fast(None)),
        FieldType::Date => builder.add_i64_field(name, tantivy::schema::INDEXED | STORED | FAST),
        FieldType::KnnVector => return Ok(None), // vectors in USearch, not Tantivy
    };
    Ok(Some(field))
}

fn validate_authoritative_mapping_entry(
    name: &str,
    mapping: &crate::cluster::state::FieldMapping,
) -> Result<()> {
    crate::common::validate_mapping_field_names([name]).map_err(|error| {
        crate::common::unsupported_index_format("index mapping metadata", error.to_string())
    })?;
    crate::common::validate_builtin_body_field_mapping(name, mapping).map_err(|error| {
        crate::common::unsupported_index_format("index mapping metadata", error.to_string())
    })
}

fn validate_authoritative_mappings(
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<()> {
    for (name, mapping) in mappings {
        validate_authoritative_mapping_entry(name, mapping)?;
    }
    Ok(())
}

/// Evolve an existing Tantivy meta.json to include new mapped fields.
///
/// Reads the current meta.json, collects the names already in the stored schema,
/// then appends entries for any mapped fields that are missing.  Existing fields
/// are never moved or reordered — new fields get strictly higher Field handles —
/// so segments written against the old schema remain valid.
fn evolve_meta_json_schema(
    meta_json_path: &Path,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<()> {
    validate_authoritative_mappings(mappings)?;
    let raw = std::fs::read_to_string(meta_json_path)?;
    let mut meta: serde_json::Value = serde_json::from_str(&raw)?;

    let schema_arr = meta
        .get_mut("schema")
        .and_then(|v| v.as_array_mut())
        .ok_or_else(|| authoritative_schema_error("meta.json missing 'schema' array"))?;

    // Collect names already in the stored schema.
    let existing_names: std::collections::HashSet<String> = schema_arr
        .iter()
        .filter_map(|entry| entry.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();

    // Build JSON entries for any new mapped fields by round-tripping through
    // SchemaBuilder so the serialized form exactly matches Tantivy's own
    // conventions (indexing options, fast-field flags, etc.).
    let mut new_names: Vec<_> = mappings
        .keys()
        .filter(|name| !existing_names.contains(*name))
        .cloned()
        .collect();
    if new_names.is_empty() {
        return Ok(()); // nothing to do
    }
    new_names.sort();

    for name in &new_names {
        let mapping = &mappings[name];
        let mut tmp_builder = Schema::builder();
        let Some(_) = add_mapping_field_to_schema(&mut tmp_builder, name, mapping)? else {
            continue; // knn_vector → skip
        };
        let tmp_schema = tmp_builder.build();
        let serialized = serde_json::to_value(&tmp_schema)?;
        if let Some(arr) = serialized.as_array() {
            // tmp_schema has exactly one field; take its serialized entry.
            if let Some(entry) = arr.first() {
                schema_arr.push(entry.clone());
            }
        }
    }

    // Write atomically: temp file + rename.
    let tmp_path = meta_json_path.with_extension("json.tmp");
    let mut buf = serde_json::to_vec_pretty(&meta)?;
    buf.push(b'\n');
    std::fs::write(&tmp_path, &buf)?;
    std::fs::rename(&tmp_path, meta_json_path)?;
    tracing::info!(
        "Evolved Tantivy schema: appended {} new field(s): {:?}",
        new_names.len(),
        new_names
    );
    Ok(())
}

fn validate_builtin_schema_fields(
    schema: &Schema,
    purpose: HotEnginePurpose,
) -> Result<(Field, Field)> {
    let component = match purpose {
        HotEnginePurpose::LocalShard => "local shard Tantivy schema",
        HotEnginePurpose::RemoteSplit => "remote split Tantivy schema",
    };
    let required_field = |name: &str| {
        schema.get_field(name).map_err(|_| {
            crate::common::unsupported_index_format(
                component,
                format!("required internal field {name} is missing"),
            )
        })
    };
    let id_field = required_field("_id")?;
    let source_field = required_field("_source")?;
    let body_field = required_field(crate::common::BUILTIN_BODY_FIELD)?;
    let valid_body = matches!(
        schema.get_field_entry(body_field).field_type(),
        tantivy::schema::FieldType::Str(options)
            if options.is_stored()
                && options
                    .get_indexing_options()
                    .is_some_and(|indexing| indexing.tokenizer() != "raw")
    );
    if !valid_body {
        return Err(crate::common::unsupported_index_format(
            component,
            "built-in body field is not a stored analyzed text field",
        ));
    }
    Ok((id_field, source_field))
}

fn validate_internal_sequence_fields(schema: &Schema, purpose: HotEnginePurpose) -> Result<()> {
    if !purpose.requires_sequence_fields() {
        return Ok(());
    }
    for name in [SEQ_NO_FIELD_NAME, PRIMARY_TERM_FIELD_NAME] {
        let field = schema.get_field(name).map_err(|_| {
            crate::common::unsupported_index_format(
                "local shard Tantivy schema",
                format!("required internal field {name} is missing"),
            )
        })?;
        let entry = schema.get_field_entry(field);
        let valid = matches!(
            entry.field_type(),
            tantivy::schema::FieldType::U64(options)
                if options.is_fast() && options.is_stored()
        );
        if !valid {
            return Err(crate::common::unsupported_index_format(
                "local shard Tantivy schema",
                format!("internal field {name} is not a stored u64 fast field"),
            ));
        }
    }
    Ok(())
}

fn validate_existing_schema_mappings(
    meta_json_path: &Path,
    mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
) -> Result<()> {
    let raw = std::fs::read_to_string(meta_json_path)?;
    let meta: serde_json::Value = serde_json::from_str(&raw)?;
    let stored_schema: Schema = serde_json::from_value(
        meta.get("schema")
            .cloned()
            .ok_or_else(|| authoritative_schema_error("meta.json missing 'schema' array"))?,
    )?;

    for (name, mapping) in mappings {
        if matches!(
            mapping.field_type,
            crate::cluster::state::FieldType::KnnVector
        ) {
            continue;
        }
        let Ok(field) = stored_schema.get_field(name) else {
            continue;
        };
        let field_type = stored_schema.get_field_entry(field).field_type();
        let matches_mapping = match (&mapping.field_type, field_type) {
            (crate::cluster::state::FieldType::Text, tantivy::schema::FieldType::Str(options)) => {
                options
                    .get_indexing_options()
                    .is_some_and(|indexing| indexing.tokenizer() != "raw")
            }
            (
                crate::cluster::state::FieldType::Keyword
                | crate::cluster::state::FieldType::Boolean,
                tantivy::schema::FieldType::Str(options),
            ) => options
                .get_indexing_options()
                .is_some_and(|indexing| indexing.tokenizer() == "raw"),
            (
                crate::cluster::state::FieldType::Integer | crate::cluster::state::FieldType::Date,
                tantivy::schema::FieldType::I64(_),
            ) => true,
            (crate::cluster::state::FieldType::Float, tantivy::schema::FieldType::F64(_)) => true,
            _ => false,
        };
        if !matches_mapping {
            return Err(authoritative_schema_error(format!(
                "schema does not match authoritative mappings for field '{name}'"
            )));
        }
    }
    Ok(())
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn sequenced_wal_entry(
    operation: &super::SequencedOperation,
) -> crate::wal::BorrowedSequencedWalEntry<'_> {
    let mutation = match &operation.mutation {
        super::DocumentMutation::Index { doc_id, source } => {
            WalDocumentOperation::Index { doc_id, source }
        }
        super::DocumentMutation::Delete { doc_id } => WalDocumentOperation::Delete { doc_id },
        super::DocumentMutation::NoOp { reason } => WalDocumentOperation::NoOp { reason },
    };
    crate::wal::BorrowedSequencedWalEntry {
        seq_no: operation.seq_no,
        primary_term: operation.primary_term,
        operation: mutation,
    }
}

fn wal_entry_matches_operation(
    entry: &crate::wal::TranslogEntry,
    operation: &super::SequencedOperation,
) -> Result<bool> {
    let decoded = document_operation(entry)?;
    Ok(match (decoded, &operation.mutation) {
        (
            WalDocumentOperation::Index { doc_id, source },
            super::DocumentMutation::Index {
                doc_id: incoming_id,
                source: incoming_source,
            },
        ) => doc_id == incoming_id && source == incoming_source,
        (
            WalDocumentOperation::Delete { doc_id },
            super::DocumentMutation::Delete {
                doc_id: incoming_id,
            },
        ) => doc_id == incoming_id,
        (
            WalDocumentOperation::NoOp { reason },
            super::DocumentMutation::NoOp {
                reason: incoming_reason,
            },
        ) => reason == incoming_reason,
        _ => false,
    })
}

fn sequenced_operation_from_entry(
    mut entry: crate::wal::TranslogEntry,
) -> Result<super::SequencedOperation> {
    document_operation(&entry)?;
    let mutation = match entry.op {
        crate::wal::WalOperation::Index | crate::wal::WalOperation::Delete => {
            let serde_json::Value::String(doc_id) = entry.payload["_doc_id"].take() else {
                unreachable!("validated WAL document ID is a string");
            };
            if entry.op == crate::wal::WalOperation::Index {
                super::DocumentMutation::Index {
                    doc_id,
                    source: entry.payload["_source"].take(),
                }
            } else {
                super::DocumentMutation::Delete { doc_id }
            }
        }
        crate::wal::WalOperation::NoOp => {
            let serde_json::Value::String(reason) = entry.payload["_reason"].take() else {
                unreachable!("validated WAL no-op reason is a string");
            };
            super::DocumentMutation::NoOp { reason }
        }
    };
    Ok(super::SequencedOperation {
        seq_no: entry.seq_no,
        primary_term: entry.primary_term,
        mutation,
    })
}

fn join_scoped_handles<'scope, T>(
    handles: Vec<std::thread::ScopedJoinHandle<'scope, T>>,
    context: &'static str,
) -> Result<Vec<T>> {
    handles
        .into_iter()
        .map(|handle| {
            handle.join().map_err(|panic| {
                let message = panic_payload_message(panic.as_ref());
                anyhow::anyhow!("{context} thread panicked: {message}")
            })
        })
        .collect()
}

impl HotEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, refresh_interval: Duration) -> Result<Self> {
        Self::new_with_mappings(
            data_dir,
            refresh_interval,
            &HashMap::new(),
            TranslogDurability::Request,
            Arc::new(super::column_cache::ColumnCache::new(0, 0)),
        )
    }

    /// Create a new HotEngine with explicit field mappings.
    /// When mappings are provided, named Tantivy fields are created for each mapped field.
    /// The "body" catch-all is always created for `?q=` queries.
    ///
    /// **Schema evolution**: If the on-disk index already exists but the provided
    /// mappings contain fields not yet in the stored schema, the meta.json is
    /// updated in place (new fields are appended, preserving existing field IDs)
    /// before the index is opened.
    pub fn new_with_mappings<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        Self::new_with_mappings_mode(
            data_dir,
            refresh_interval,
            mappings,
            durability,
            column_cache,
            false,
            HotEnginePurpose::LocalShard,
        )
    }

    pub(crate) fn new_remote_split_with_mappings<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        Self::new_with_mappings_mode(
            data_dir,
            refresh_interval,
            mappings,
            TranslogDurability::Request,
            column_cache,
            false,
            HotEnginePurpose::RemoteSplit,
        )
    }

    pub(crate) fn open_existing_with_mappings<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
    ) -> Result<Self> {
        Self::new_with_mappings_mode(
            data_dir,
            refresh_interval,
            mappings,
            durability,
            column_cache,
            true,
            HotEnginePurpose::LocalShard,
        )
    }

    fn new_with_mappings_mode<P: AsRef<Path>>(
        data_dir: P,
        refresh_interval: Duration,
        mappings: &HashMap<String, crate::cluster::state::FieldMapping>,
        durability: TranslogDurability,
        column_cache: Arc<super::column_cache::ColumnCache>,
        existing_only: bool,
        purpose: HotEnginePurpose,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        validate_authoritative_mappings(mappings)?;
        let index_path = data_dir.join("index");
        let meta_json_path = index_path.join("meta.json");
        if existing_only && !meta_json_path.is_file() {
            anyhow::bail!("existing Tantivy index metadata is missing at {meta_json_path:?}");
        }
        if !existing_only {
            std::fs::create_dir_all(&index_path)?;
        }
        let index_exists = meta_json_path.is_file();

        // If an existing index is on disk, evolve its schema to include any
        // new mapped fields before opening.  This preserves the existing field
        // order (and thus Field handle IDs) and only appends.
        if index_exists {
            validate_existing_schema_mappings(&meta_json_path, mappings)?;
            evolve_meta_json_schema(&meta_json_path, mappings)?;
        }

        // Build the schema from scratch only for brand-new indices.
        // For existing indices we open from disk (which now has any new fields).
        let index = if index_exists {
            let mmap_dir = tantivy::directory::MmapDirectory::open(&index_path)?;
            Index::open(mmap_dir)?
        } else {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("_id", (STRING | STORED).set_fast(None));
            schema_builder.add_text_field("_source", STORED);
            if purpose.requires_sequence_fields() {
                schema_builder.add_u64_field(SEQ_NO_FIELD_NAME, FAST | STORED);
                schema_builder.add_u64_field(PRIMARY_TERM_FIELD_NAME, FAST | STORED);
            }
            schema_builder.add_text_field("body", TEXT | STORED);

            let mut mapping_names: Vec<_> = mappings.keys().cloned().collect();
            mapping_names.sort();
            for name in mapping_names {
                let mapping = &mappings[&name];
                add_mapping_field_to_schema(&mut schema_builder, &name, mapping)?;
            }

            let schema = schema_builder.build();
            let mmap_dir = tantivy::directory::MmapDirectory::open(&index_path)?;
            Index::open_or_create(mmap_dir, schema)?
        };

        let schema = index.schema();
        let (id_field, source_field) = validate_builtin_schema_fields(&schema, purpose)?;
        validate_internal_sequence_fields(&schema, purpose)?;

        // Build the FieldRegistry from the opened schema (authoritative).
        let seq_no_field = schema.get_field(SEQ_NO_FIELD_NAME).ok();
        let primary_term_field = schema.get_field(PRIMARY_TERM_FIELD_NAME).ok();

        let mut fields = HashMap::new();
        let mut field_types = HashMap::new();
        let mut date_fields = Vec::new();
        for (field, entry) in schema.fields() {
            let name: String = entry.name().to_string();
            if crate::common::is_reserved_document_key(&name) {
                continue;
            }
            fields.insert(name.clone(), field);
            // Recover logical field types from mappings (Date vs Integer).
            if let Some(mapping) = mappings.get(&name) {
                if !matches!(
                    mapping.field_type,
                    crate::cluster::state::FieldType::KnnVector
                ) {
                    field_types.insert(name.clone(), mapping.field_type.clone());
                }
                if matches!(mapping.field_type, crate::cluster::state::FieldType::Date) {
                    date_fields.push(name);
                }
            }
        }

        // Keep the per-shard writer heap modest so nodes reopening many local shards
        // do not reserve multiple GiB before they can finish recovery.
        let writer = index.writer(TANTIVY_WRITER_HEAP_BYTES)?;
        let automatic_merge_policy = writer.get_merge_policy();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        // Open or create the translog in the data directory (not index_path)
        let translog = HotTranslog::open_with_durability(data_dir, durability)?;
        if matches!(durability, TranslogDurability::Async { .. }) {
            translog.start_sync_task();
        }
        let committed_boundary_path = data_dir.join("translog.committed");
        let committed_boundary = CommittedBoundaryRecord::load_or_initialize_empty(
            &committed_boundary_path,
            0,
            translog.max_seq_no(),
        )?;
        let apply_state = ApplyState::new(committed_boundary)?;

        let field_registry = FieldRegistry {
            id_field,
            source_field,
            seq_no_field,
            primary_term_field,
            fields,
            field_types,
            date_fields,
        };

        let engine = Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(WriterState::ready(writer))),
            maintenance_lock: Mutex::new(()),
            automatic_merge_policy: RwLock::new(automatic_merge_policy),
            #[cfg(test)]
            force_merge_entry_barrier: Mutex::new(None),
            #[cfg(test)]
            force_merge_before_wait_sender: Mutex::new(None),
            #[cfg(test)]
            writer_replacement_failure: Mutex::new(None),
            #[cfg(test)]
            engine_apply_failure: Mutex::new(None),
            #[cfg(test)]
            refresh_commit_failures: Mutex::new(0),
            #[cfg(test)]
            post_apply_refresh_failures: Mutex::new(0),
            #[cfg(test)]
            refresh_before_writer_sender: Mutex::new(None),
            #[cfg(test)]
            refresh_after_commit_sender: Mutex::new(None),
            #[cfg(test)]
            refresh_after_commit_release_receiver: Mutex::new(None),
            #[cfg(test)]
            peer_recovery_snapshot_ready_sender: Mutex::new(None),
            #[cfg(test)]
            peer_recovery_snapshot_release_receiver: Mutex::new(None),
            field_registry: RwLock::new(field_registry),
            refresh_interval,
            translog: Arc::new(Mutex::new(translog)),
            apply_state: Mutex::new(apply_state),
            identity_term_state: Mutex::new(None),
            committed_boundary_path,
            durability,
            delete_tombstone_retention: Duration::from_secs(60),
            column_cache,
        };

        #[cfg(feature = "protocol-trace")]
        if let Some(copy) = crate::protocol_trace::current_open_copy() {
            crate::protocol_trace::record_node_restarted(&copy, engine.sequence_stats());
        }

        // Replay any uncommitted translog entries from before a crash
        engine.replay_translog()?;

        Ok(engine)
    }

    fn with_translog<T>(
        &self,
        context: &'static str,
        f: impl FnOnce(&dyn WriteAheadLog) -> Result<T>,
    ) -> Result<T> {
        let tl = self
            .translog
            .lock()
            .map_err(|_| anyhow::anyhow!("translog lock poisoned during {context}"))?;
        f(&*tl)
    }

    fn with_translog_recover<T>(
        &self,
        context: &'static str,
        f: impl FnOnce(&dyn WriteAheadLog) -> T,
    ) -> T {
        let tl = match self.translog.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    operation = context,
                    "translog lock poisoned; continuing with inner state"
                );
                poisoned.into_inner()
            }
        };
        f(&*tl)
    }

    fn maintenance_guard(&self, context: &str) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.maintenance_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance lock poisoned during {context}"))
    }

    fn open_replacement_writer(
        &self,
        context: &str,
        merge_policy: Box<dyn MergePolicy>,
    ) -> Result<IndexWriter> {
        #[cfg(test)]
        if let Some(error) = self.maybe_fail_writer_replacement_for_test() {
            return Err(error).with_context(|| context.to_string());
        }
        let writer = self
            .index
            .writer(TANTIVY_WRITER_HEAP_BYTES)
            .with_context(|| context.to_string())?;
        writer.set_merge_policy(merge_policy);
        Ok(writer)
    }

    fn commit_writer_at_boundary(
        &self,
        writer_state: &mut WriterState,
        context: &str,
        _boundary: CommittedBoundaryRecord,
    ) -> Result<CommittedBoundaryRecord> {
        #[cfg(test)]
        if context == "refresh" {
            let mut remaining = self
                .refresh_commit_failures
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if *remaining > 0 {
                *remaining -= 1;
                writer_state.fail("injected refresh commit failure");
                anyhow::bail!("injected refresh commit failure");
            }
        }
        let commit_result = {
            let writer = writer_state.writer_mut(context)?;
            writer.commit()
        };
        match commit_result {
            Ok(_) => {
                let mut state = self
                    .apply_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
                let processed_checkpoint = state.checkpoints.processed_checkpoint();
                state
                    .checkpoints
                    .mark_persisted_through(processed_checkpoint);
                let boundary = state.committed_boundary();
                boundary.validate()?;
                #[cfg(feature = "protocol-trace")]
                if let Some(copy) = crate::protocol_trace::current_open_copy() {
                    crate::protocol_trace::record_commit_captured(
                        &copy,
                        super::SequenceStats {
                            processed_checkpoint: boundary.processed_checkpoint,
                            persisted_checkpoint: boundary.persisted_checkpoint,
                            max_seq_no: boundary.max_seq_no,
                        },
                        boundary.term_sequence_state.current_term,
                        boundary.term_sequence_state.max_seq_no_at_term_start,
                        boundary
                            .term_sequence_state
                            .processed_in_current_term_below_start_max
                            .iter()
                            .map(|range| (range.start, range.end))
                            .collect(),
                    );
                }
                Ok(boundary)
            }
            Err(source) => {
                let failure = TantivyCommitFailureError {
                    context: context.to_string(),
                    source,
                };
                writer_state.fail(failure.to_string());
                Err(failure.into())
            }
        }
    }

    fn validate_truncation_boundary(
        &self,
        translog: &dyn WriteAheadLog,
        boundary: &CommittedBoundaryRecord,
    ) -> Result<()> {
        let persisted = self.load_committed_boundary()?;
        if persisted != *boundary {
            anyhow::bail!(
                "refusing WAL truncation: persisted committed boundary does not match the successful Tantivy commit boundary"
            );
        }
        if boundary.max_seq_no != translog.max_seq_no() {
            anyhow::bail!(
                "refusing WAL truncation: successful Tantivy maximum sequence {:?} does not match WAL maximum sequence {:?}",
                boundary.max_seq_no,
                translog.max_seq_no()
            );
        }
        Ok(())
    }

    fn load_committed_boundary(&self) -> Result<CommittedBoundaryRecord> {
        CommittedBoundaryRecord::load(&self.committed_boundary_path)?.ok_or_else(|| {
            anyhow::anyhow!(
                "committed boundary {:?} disappeared after engine initialization",
                self.committed_boundary_path
            )
        })
    }

    fn current_committed_boundary(&self) -> Result<CommittedBoundaryRecord> {
        let state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        let boundary = state.committed_boundary();
        boundary.validate()?;
        Ok(boundary)
    }

    fn reset_apply_state_to_commit(&self, committed: CommittedBoundaryRecord) -> Result<()> {
        let identity_term_state = *self
            .identity_term_state
            .lock()
            .map_err(|_| anyhow::anyhow!("identity term state lock poisoned"))?;
        let mut state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        state.reset_to_commit(committed.clone())?;
        if let Some((identity_fence, identity_fence_max_seq_no)) = identity_term_state {
            state.term_sequences = initialize_term_sequence_state(
                identity_fence,
                identity_fence_max_seq_no,
                &committed,
            )?;
        }
        Ok(())
    }

    fn prepare_primary_term_before_wal(&self, primary_term: u64) -> Result<()> {
        let mut state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        state.term_sequences.ensure_not_stale(primary_term)?;
        if primary_term > state.term_sequences.current_term() {
            let max_seq_no = state.checkpoints.max_seq_no();
            state.term_sequences.raise_term(primary_term, max_seq_no)?;
        }
        Ok(())
    }

    fn current_version(&self, state: &ApplyState, doc_id: &str) -> Result<Option<CurrentVersion>> {
        if let Some(version) = state.versions.lookup(doc_id)? {
            return Ok(Some(CurrentVersion::Native(version)));
        }

        Ok(self
            .find_refreshed_document(doc_id)?
            .map(|(_, _, version)| CurrentVersion::Native(VersionValue::Index(version))))
    }

    fn find_refreshed_document(
        &self,
        doc_id: &str,
    ) -> Result<
        Option<(
            tantivy::Searcher,
            tantivy::DocAddress,
            super::version_map::IndexVersionValue,
        )>,
    > {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let searcher = self.reader.searcher();
        let query = tantivy::query::TermQuery::new(
            Term::from_field_text(registry.id_field, doc_id),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let Some((_, address)) = searcher
            .search(&query, &TopDocs::with_limit(1))?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let segment = &searcher.segment_readers()[address.segment_ord as usize];
        let seq_column = segment.fast_fields().u64(SEQ_NO_FIELD_NAME);
        let term_column = segment.fast_fields().u64(PRIMARY_TERM_FIELD_NAME);
        let (seq_column, term_column) = match (seq_column, term_column) {
            (Ok(seq_column), Ok(term_column)) => (seq_column, term_column),
            _ => {
                return Err(crate::common::unsupported_index_format(
                    "local shard Tantivy segment",
                    format!("document [{doc_id}] is missing _seq_no or _primary_term"),
                ));
            }
        };
        let mut seq_values = seq_column.values_for_doc(address.doc_id);
        let Some(seq_no) = seq_values.next() else {
            return Err(crate::common::unsupported_index_format(
                "local shard Tantivy segment",
                format!("document [{doc_id}] has no {SEQ_NO_FIELD_NAME} value"),
            ));
        };
        if seq_values.next().is_some() {
            return Err(InternalSequenceFieldError {
                message: format!("document [{doc_id}] has multiple {SEQ_NO_FIELD_NAME} values"),
            }
            .into());
        }
        let mut term_values = term_column.values_for_doc(address.doc_id);
        let Some(primary_term) = term_values.next() else {
            return Err(crate::common::unsupported_index_format(
                "local shard Tantivy segment",
                format!("document [{doc_id}] has no {PRIMARY_TERM_FIELD_NAME} value"),
            ));
        };
        if term_values.next().is_some() {
            return Err(InternalSequenceFieldError {
                message: format!(
                    "document [{doc_id}] has multiple {PRIMARY_TERM_FIELD_NAME} values"
                ),
            }
            .into());
        }
        if primary_term == 0 {
            return Err(InternalSequenceFieldError {
                message: format!("document [{doc_id}] has a zero _primary_term"),
            }
            .into());
        }
        Ok(Some((
            searcher,
            address,
            super::version_map::IndexVersionValue {
                seq_no,
                primary_term,
                wal_position: None,
            },
        )))
    }

    fn check_primary_condition(
        &self,
        doc_id: &str,
        condition: super::WriteCondition,
    ) -> Result<bool> {
        let state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        let current = match self.current_version(&state, doc_id)? {
            Some(CurrentVersion::Native(VersionValue::Index(value))) => {
                Some((value.seq_no, value.primary_term))
            }
            _ => None,
        };
        condition.check(doc_id, current)?;
        Ok(current.is_some())
    }

    fn read_refreshed_document(&self, doc_id: &str) -> Result<Option<super::DocumentRead>> {
        let Some((searcher, address, version)) = self.find_refreshed_document(doc_id)? else {
            return Ok(None);
        };
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let document = searcher.doc::<TantivyDocument>(address)?;
        let text = document
            .get_first(registry.source_field)
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow::anyhow!("document [{doc_id}] has no stored _source"))?;
        let mut source = serde_json::from_str(text)
            .with_context(|| format!("invalid stored _source for document [{doc_id}]"))?;
        Self::normalize_result_source_with_registry(&registry, &mut source);
        Ok(Some(super::DocumentRead {
            source,
            seq_no: version.seq_no,
            primary_term: version.primary_term,
        }))
    }

    fn validate_redelivery(
        &self,
        translog: &dyn WriteAheadLog,
        operation: &super::SequencedOperation,
    ) -> Result<bool> {
        let Some(entry) = translog.find_entry(operation.seq_no)? else {
            return Ok(false);
        };
        if entry.primary_term != operation.primary_term {
            return Ok(false);
        }
        if !wal_entry_matches_operation(&entry, operation)? {
            return Err(SequenceOperationCollisionError {
                primary_term: operation.primary_term,
                seq_no: operation.seq_no,
            }
            .into());
        }
        Ok(true)
    }

    fn apply_sequenced_batch_locked<F>(
        &self,
        translog: &dyn WriteAheadLog,
        operations: &[super::SequencedOperation],
        wal_disposition: WalDisposition<'_>,
        mut writer_override: Option<&mut WriterState>,
        inject_apply_failure: bool,
        mut side_effect: F,
    ) -> Result<super::ReplicaBulkApplyReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        #[cfg(not(test))]
        let _ = inject_apply_failure;
        if operations.is_empty() {
            return Ok(super::ReplicaBulkApplyReceipt {
                outcomes: Vec::new(),
                all_operations_processed: true,
                all_operations_persisted: true,
                sequence: self.sequence_stats(),
            });
        }

        if writer_override.is_none() {
            drop(self.writer_state_with_replay(translog, "sequenced operation apply")?);
        }
        let mut state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::current_open_copy();
        #[cfg(feature = "protocol-trace")]
        let arrival_order_apply = crate::protocol_trace::current_apply_origin()
            == Some(crate::protocol_trace::ApplyOrigin::LiveReplication)
            && crate::protocol_trace::mutation_enabled(
                crate::protocol_trace::MutationMode::ArrivalOrderApply,
            );
        #[cfg(not(feature = "protocol-trace"))]
        let arrival_order_apply = false;
        #[cfg(feature = "protocol-trace")]
        let seq_only_redelivery = crate::protocol_trace::current_apply_origin()
            == Some(crate::protocol_trace::ApplyOrigin::LiveReplication)
            && crate::protocol_trace::mutation_enabled(
                crate::protocol_trace::MutationMode::SeqOnlyRedelivery,
            );
        #[cfg(not(feature = "protocol-trace"))]
        let seq_only_redelivery = false;
        let planning_snapshot = state.planning_snapshot();
        let mut planned = Vec::with_capacity(operations.len());
        let mut shadow_versions = HashMap::<&str, CurrentVersion>::with_capacity(operations.len());
        #[cfg(feature = "protocol-trace")]
        let mut collision_operation = None;

        let planning_result = (|| {
            for operation in operations {
                if operation.primary_term > state.term_sequences.current_term() {
                    let max_seq_no = state.checkpoints.max_seq_no();
                    state
                        .term_sequences
                        .raise_term(operation.primary_term, max_seq_no)?;
                }
                let already_processed = state.checkpoints.has_processed(operation.seq_no);
                if !seq_only_redelivery
                    && let Err(error) = state.term_sequences.check_before_redelivery(
                        operation.primary_term,
                        operation.seq_no,
                        already_processed,
                    )
                {
                    #[cfg(feature = "protocol-trace")]
                    {
                        collision_operation = Some(operation);
                    }
                    return Err(error);
                }
                if already_processed {
                    if wal_disposition.validates_redelivery()
                        && !seq_only_redelivery
                        && let Err(error) = self.validate_redelivery(translog, operation)
                    {
                        #[cfg(feature = "protocol-trace")]
                        {
                            collision_operation = Some(operation);
                        }
                        return Err(error);
                    }
                    planned.push(PlannedOperation {
                        operation,
                        outcome: super::ApplyOutcome::Redelivery,
                        complete: false,
                    });
                    continue;
                }

                state.checkpoints.advance_max_seq_no(operation.seq_no);
                if !matches!(operation.mutation, super::DocumentMutation::NoOp { .. }) {
                    state.max_seq_no_of_updates_or_deletes = Some(
                        state
                            .max_seq_no_of_updates_or_deletes
                            .map_or(operation.seq_no, |current| current.max(operation.seq_no)),
                    );
                }

                let outcome = match &operation.mutation {
                    super::DocumentMutation::NoOp { .. } => super::ApplyOutcome::NoOp,
                    super::DocumentMutation::Index { doc_id, .. }
                    | super::DocumentMutation::Delete { doc_id } => {
                        let current =
                            if let Some(current) = shadow_versions.get(doc_id.as_str()).copied() {
                                Some(current)
                            } else if let WalDisposition::AlreadyInLocalWal {
                                initial_versions: Some(initial_versions),
                                ..
                            } = wal_disposition
                            {
                                initial_versions.get(doc_id.as_str()).copied().ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "primary bulk plan has no initial version for [{doc_id}]"
                                    )
                                })?
                            } else {
                                self.current_version(&state, doc_id)?
                            };
                        let outcome = match current {
                            Some(CurrentVersion::Native(version))
                                if version.seq_no() > operation.seq_no =>
                            {
                                if arrival_order_apply {
                                    super::ApplyOutcome::Applied
                                } else {
                                    super::ApplyOutcome::Stale
                                }
                            }
                            Some(CurrentVersion::Native(version))
                                if version.seq_no() == operation.seq_no =>
                            {
                                let kind_matches = matches!(
                                    (&operation.mutation, version),
                                    (
                                        super::DocumentMutation::Index { .. },
                                        super::version_map::VersionValue::Index(_)
                                    ) | (
                                        super::DocumentMutation::Delete { .. },
                                        super::version_map::VersionValue::Delete(_)
                                    )
                                );
                                if version.primary_term() != operation.primary_term || !kind_matches
                                {
                                    if seq_only_redelivery {
                                        super::ApplyOutcome::Redelivery
                                    } else {
                                        #[cfg(feature = "protocol-trace")]
                                        {
                                            collision_operation = Some(operation);
                                        }
                                        return Err(SequenceOperationCollisionError {
                                            primary_term: operation.primary_term,
                                            seq_no: operation.seq_no,
                                        }
                                        .into());
                                    }
                                } else {
                                    if wal_disposition.validates_redelivery()
                                        && !seq_only_redelivery
                                        && let Err(error) =
                                            self.validate_redelivery(translog, operation)
                                    {
                                        #[cfg(feature = "protocol-trace")]
                                        {
                                            collision_operation = Some(operation);
                                        }
                                        return Err(error);
                                    }
                                    super::ApplyOutcome::Redelivery
                                }
                            }
                            _ => super::ApplyOutcome::Applied,
                        };
                        if outcome == super::ApplyOutcome::Applied {
                            let version = match &operation.mutation {
                                super::DocumentMutation::Index { .. } => {
                                    CurrentVersion::Native(super::version_map::VersionValue::Index(
                                        super::version_map::IndexVersionValue {
                                            seq_no: operation.seq_no,
                                            primary_term: operation.primary_term,
                                            wal_position: None,
                                        },
                                    ))
                                }
                                super::DocumentMutation::Delete { .. } => CurrentVersion::Native(
                                    super::version_map::VersionValue::Delete(
                                        super::version_map::DeleteVersionValue {
                                            seq_no: operation.seq_no,
                                            primary_term: operation.primary_term,
                                            deleted_at: Instant::now(),
                                        },
                                    ),
                                ),
                                super::DocumentMutation::NoOp { .. } => unreachable!(),
                            };
                            shadow_versions.insert(doc_id.as_str(), version);
                        }
                        outcome
                    }
                };
                planned.push(PlannedOperation {
                    operation,
                    outcome,
                    complete: true,
                });
            }
            Ok::<(), anyhow::Error>(())
        })();
        if let Err(error) = planning_result {
            state.restore_planning_snapshot(planning_snapshot);
            #[cfg(feature = "protocol-trace")]
            let trace_collision_result = match (trace_copy.as_ref(), collision_operation.as_ref()) {
                (Some(copy), Some(operation)) => crate::protocol_trace::record_operation_collision(
                    copy,
                    operation,
                    state.checkpoints.stats(),
                ),
                _ => Ok(()),
            };
            #[cfg(feature = "protocol-trace")]
            trace_collision_result.context("record protocol trace operation collision")?;
            if wal_disposition.is_already_in_local_wal() {
                if let Some(writer_state) = writer_override.as_deref_mut() {
                    writer_state.fail(format!(
                        "sequenced operation failed after WAL persistence: {error:#}"
                    ));
                } else {
                    self.fail_writer_after_wal(&error);
                }
            }
            return Err(error);
        }

        let wal_entries = if wal_disposition == WalDisposition::Append {
            planned
                .iter()
                .filter(|planned| planned.outcome != super::ApplyOutcome::Redelivery)
                .map(|planned| sequenced_wal_entry(planned.operation))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let appended = wal_disposition == WalDisposition::Append && !wal_entries.is_empty();
        let start_cursor = match wal_disposition {
            WalDisposition::Append if appended => match translog.recovery_read_snapshot() {
                Ok(snapshot) => Some(snapshot.end_cursor()),
                Err(error) => {
                    state.restore_planning_snapshot(planning_snapshot);
                    return Err(error);
                }
            },
            WalDisposition::AlreadyInLocalWal { start_cursor, .. } => start_cursor,
            _ => None,
        };
        if appended && let Err(error) = translog.write_document_batch_with_seq(&wal_entries) {
            state.restore_planning_snapshot(planning_snapshot);
            return Err(error);
        }
        #[cfg(feature = "protocol-trace")]
        if appended && let Some(copy) = trace_copy.as_ref() {
            let durable = matches!(self.durability, TranslogDurability::Request);
            for planned_operation in planned
                .iter()
                .filter(|planned| planned.outcome != super::ApplyOutcome::Redelivery)
            {
                crate::protocol_trace::record_wal_appended(
                    copy,
                    planned_operation.operation,
                    durable,
                )?;
            }
        }

        let has_applied = planned
            .iter()
            .any(|planned| planned.outcome == super::ApplyOutcome::Applied);
        let mut owned_writer_state = if has_applied && writer_override.is_none() {
            Some(
                self.writer
                    .write()
                    .unwrap_or_else(|error| error.into_inner()),
            )
        } else {
            None
        };
        let mut writer_state = if has_applied {
            if let Some(writer_state) = &mut writer_override {
                Some(&mut **writer_state)
            } else {
                owned_writer_state.as_deref_mut()
            }
        } else {
            None
        };
        let execution_result = (|| {
            let positions = match wal_disposition {
                WalDisposition::AlreadyInLocalWal {
                    replay_positions: Some(positions),
                    ..
                } => positions.clone(),
                _ => match start_cursor {
                    Some(start) => {
                        let count = if appended {
                            wal_entries.len()
                        } else {
                            operations.len()
                        };
                        let cursors = if count == 1 {
                            vec![start]
                        } else {
                            translog.entry_positions(start, count)?
                        };
                        if appended {
                            wal_entries
                                .iter()
                                .zip(cursors)
                                .map(|(entry, position)| {
                                    ((entry.primary_term, entry.seq_no), position)
                                })
                                .collect::<HashMap<_, _>>()
                        } else {
                            operations
                                .iter()
                                .zip(cursors)
                                .map(|(operation, position)| {
                                    ((operation.primary_term, operation.seq_no), position)
                                })
                                .collect::<HashMap<_, _>>()
                        }
                    }
                    None => HashMap::new(),
                },
            };
            #[cfg(test)]
            if inject_apply_failure {
                self.maybe_fail_engine_apply_for_test()?;
            }
            for planned_operation in &planned {
                match (
                    &planned_operation.operation.mutation,
                    planned_operation.outcome,
                ) {
                    (
                        super::DocumentMutation::Index { doc_id, source },
                        super::ApplyOutcome::Applied,
                    ) => {
                        let writer = writer_state
                            .as_mut()
                            .expect("applied operation requires a writer")
                            .writer_mut("sequenced index apply")?;
                        let id_field = self
                            .field_registry
                            .read()
                            .unwrap_or_else(|error| error.into_inner())
                            .id_field;
                        writer.delete_term(Term::from_field_text(id_field, doc_id));
                        let doc = self.build_tantivy_doc(
                            doc_id,
                            source,
                            planned_operation.operation.seq_no,
                            planned_operation.operation.primary_term,
                        )?;
                        writer.add_document(doc)?;
                        side_effect(planned_operation.operation)?;
                        let wal_position = match positions.get(&(
                            planned_operation.operation.primary_term,
                            planned_operation.operation.seq_no,
                        )) {
                            Some(position) => *position,
                            None => translog
                                .find_entry_position(
                                    planned_operation.operation.seq_no,
                                    planned_operation.operation.primary_term,
                                )?
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "applied index operation ({}, {}) has no WAL position",
                                        planned_operation.operation.primary_term,
                                        planned_operation.operation.seq_no
                                    )
                                })?,
                        };
                        state.versions.apply_index_at(
                            doc_id,
                            planned_operation.operation.seq_no,
                            planned_operation.operation.primary_term,
                            wal_position,
                        );
                    }
                    (super::DocumentMutation::Delete { doc_id }, super::ApplyOutcome::Applied) => {
                        let writer = writer_state
                            .as_mut()
                            .expect("applied operation requires a writer")
                            .writer_mut("sequenced delete apply")?;
                        let id_field = self
                            .field_registry
                            .read()
                            .unwrap_or_else(|error| error.into_inner())
                            .id_field;
                        writer.delete_term(Term::from_field_text(id_field, doc_id));
                        side_effect(planned_operation.operation)?;
                        state.versions.apply_delete(
                            doc_id,
                            planned_operation.operation.seq_no,
                            planned_operation.operation.primary_term,
                        );
                    }
                    (_, super::ApplyOutcome::Applied) => unreachable!(),
                    _ => {}
                }

                if planned_operation.complete {
                    state.complete_operation(
                        planned_operation.operation.primary_term,
                        planned_operation.operation.seq_no,
                        self.durability,
                    )?;
                    if wal_disposition.is_persisted() {
                        state
                            .checkpoints
                            .mark_persisted(planned_operation.operation.seq_no);
                    }
                }
                #[cfg(feature = "protocol-trace")]
                if let Some(copy) = trace_copy.as_ref() {
                    crate::protocol_trace::record_operation_processed(
                        copy,
                        planned_operation.operation,
                        planned_operation.outcome,
                        state.checkpoints.stats(),
                    )?;
                }
            }
            Ok::<(), anyhow::Error>(())
        })();
        if let Err(error) = execution_result {
            if appended || wal_disposition.is_already_in_local_wal() {
                if let Some(writer_state) = writer_state.as_mut() {
                    (*writer_state).fail(format!(
                        "sequenced operation failed after WAL persistence: {error:#}"
                    ));
                } else {
                    self.fail_writer_after_wal(&error);
                }
            }
            return Err(error);
        }

        let outcomes = planned
            .iter()
            .map(|planned| planned.outcome)
            .collect::<Vec<_>>();
        let all_operations_processed = planned
            .iter()
            .all(|planned| state.checkpoints.has_processed(planned.operation.seq_no));
        let all_operations_persisted = planned
            .iter()
            .all(|planned| state.checkpoints.has_persisted(planned.operation.seq_no));
        Ok(super::ReplicaBulkApplyReceipt {
            outcomes,
            all_operations_processed,
            all_operations_persisted,
            sequence: state.checkpoints.stats(),
        })
    }

    pub(crate) fn apply_sequenced_operation_with_side_effect<F>(
        &self,
        operation: &super::SequencedOperation,
        side_effect: F,
    ) -> Result<super::ReplicaApplyReceipt>
    where
        F: FnOnce(&super::SequencedOperation) -> Result<()>,
    {
        let mut side_effect = Some(side_effect);
        let bulk = self.apply_sequenced_batch_with_side_effect(
            std::slice::from_ref(operation),
            false,
            |operation| {
                side_effect
                    .take()
                    .expect("single operation side effect runs at most once")(
                    operation
                )
            },
        )?;
        Ok(super::ReplicaApplyReceipt {
            outcome: bulk.outcomes[0],
            operation_processed: bulk.all_operations_processed,
            operation_persisted: bulk.all_operations_persisted,
            sequence: bulk.sequence,
        })
    }

    pub(crate) fn apply_sequenced_batch_with_side_effect<F>(
        &self,
        operations: &[super::SequencedOperation],
        allow_oversized_bulk: bool,
        side_effect: F,
    ) -> Result<super::ReplicaBulkApplyReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.with_version_map_capacity(
            "sequenced operation apply",
            operations
                .iter()
                .filter_map(|operation| operation.mutation.doc_id()),
            allow_oversized_bulk,
            |translog| {
                self.apply_sequenced_batch_locked(
                    translog,
                    operations,
                    WalDisposition::Append,
                    None,
                    true,
                    side_effect,
                )
            },
        )
    }

    pub(crate) fn add_primary_index_with_side_effect<F>(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        side_effect: F,
    ) -> Result<super::IndexWriteReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.add_primary_index_with_condition_and_side_effect(
            doc_id,
            payload,
            primary_term,
            super::WriteCondition::Unconditional,
            side_effect,
        )
    }

    pub(crate) fn add_primary_index_with_condition_and_side_effect<F>(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        condition: super::WriteCondition,
        mut side_effect: F,
    ) -> Result<super::IndexWriteReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.validate_keyword_documents(std::iter::once(&payload))?;
        self.prepare_primary_term_before_wal(primary_term)?;
        let doc_id_owned = doc_id.to_string();
        let (seq_no, created) =
            self.with_version_map_capacity("document indexing", [doc_id], false, |translog| {
                drop(self.writer_state_with_replay(translog, "document indexing")?);
                self.prepare_primary_term_before_wal(primary_term)?;
                let existed = self.check_primary_condition(doc_id, condition)?;
                let start_cursor = translog.recovery_read_snapshot()?.end_cursor();
                let seq_no = translog.write_document_with_receipt(
                    primary_term,
                    WalDocumentOperation::Index {
                        doc_id,
                        source: &payload,
                    },
                )?;
                let operation = super::SequencedOperation {
                    seq_no,
                    primary_term,
                    mutation: super::DocumentMutation::Index {
                        doc_id: doc_id_owned,
                        source: payload,
                    },
                };
                #[cfg(feature = "protocol-trace")]
                if let Some(copy) = crate::protocol_trace::current_open_copy() {
                    crate::protocol_trace::record_wal_appended(
                        &copy,
                        &operation,
                        matches!(self.durability, TranslogDurability::Request),
                    )?;
                }
                self.apply_sequenced_batch_locked(
                    translog,
                    std::slice::from_ref(&operation),
                    WalDisposition::AlreadyInLocalWal {
                        persisted: matches!(self.durability, TranslogDurability::Request),
                        validate_redelivery: true,
                        start_cursor: Some(start_cursor),
                        replay_positions: None,
                        initial_versions: None,
                    },
                    None,
                    true,
                    &mut side_effect,
                )?;
                Ok((seq_no, !existed))
            })?;
        Ok(super::IndexWriteReceipt {
            doc_id: doc_id.to_string(),
            seq_no,
            primary_term,
            created,
        })
    }

    pub(crate) fn add_primary_bulk_with_side_effect<F>(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        primary_term: u64,
        mut side_effect: F,
    ) -> Result<super::BulkWriteReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.validate_keyword_documents(docs.iter().map(|(_, payload)| payload))?;
        self.prepare_primary_term_before_wal(primary_term)?;
        let doc_ids = docs
            .iter()
            .map(|(doc_id, _)| doc_id.clone())
            .collect::<Vec<_>>();
        let (start_seq_no, created) = self.with_version_map_capacity(
            "bulk indexing",
            doc_ids.iter().map(String::as_str),
            true,
            |translog| {
                drop(self.writer_state_with_replay(translog, "bulk indexing")?);
                self.prepare_primary_term_before_wal(primary_term)?;
                let mut initial_versions = HashMap::with_capacity(docs.len());
                let mut created = Vec::with_capacity(docs.len());
                {
                    let state = self
                        .apply_state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
                    for doc_id in &doc_ids {
                        match initial_versions.entry(doc_id.as_str()) {
                            std::collections::hash_map::Entry::Occupied(_) => created.push(false),
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                let current = self.current_version(&state, doc_id)?;
                                created.push(!matches!(
                                    current,
                                    Some(CurrentVersion::Native(VersionValue::Index(_)))
                                ));
                                entry.insert(current);
                            }
                        }
                    }
                }
                let start_cursor = translog.recovery_read_snapshot()?.end_cursor();
                let start_seq_no = {
                    let wal_ops = docs
                        .iter()
                        .map(|(doc_id, source)| WalDocumentOperation::Index { doc_id, source })
                        .collect::<Vec<_>>();
                    translog.write_document_bulk_with_receipt(primary_term, &wal_ops)?
                };
                let Some(start_seq_no) = start_seq_no else {
                    return Ok((None, created));
                };
                let operations = docs
                    .into_iter()
                    .enumerate()
                    .map(|(offset, (doc_id, payload))| {
                        Ok(super::SequencedOperation {
                            seq_no: start_seq_no.checked_add(offset as u64).ok_or_else(|| {
                                anyhow::anyhow!("bulk write sequence range overflows")
                            })?,
                            primary_term,
                            mutation: super::DocumentMutation::Index {
                                doc_id,
                                source: payload,
                            },
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                #[cfg(feature = "protocol-trace")]
                if let Some(copy) = crate::protocol_trace::current_open_copy() {
                    for operation in &operations {
                        crate::protocol_trace::record_wal_appended(
                            &copy,
                            operation,
                            matches!(self.durability, TranslogDurability::Request),
                        )?;
                    }
                }
                self.apply_sequenced_batch_locked(
                    translog,
                    &operations,
                    WalDisposition::AlreadyInLocalWal {
                        persisted: matches!(self.durability, TranslogDurability::Request),
                        validate_redelivery: true,
                        start_cursor: Some(start_cursor),
                        replay_positions: None,
                        initial_versions: Some(&initial_versions),
                    },
                    None,
                    true,
                    &mut side_effect,
                )?;
                Ok((Some(start_seq_no), created))
            },
        )?;
        Ok(super::BulkWriteReceipt {
            doc_ids,
            start_seq_no,
            primary_term,
            created,
        })
    }

    pub(crate) fn delete_primary_with_side_effect<F>(
        &self,
        doc_id: &str,
        primary_term: u64,
        side_effect: F,
    ) -> Result<super::DeleteWriteReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.delete_primary_with_condition_and_side_effect(
            doc_id,
            primary_term,
            super::WriteCondition::Unconditional,
            side_effect,
        )
    }

    pub(crate) fn delete_primary_with_condition_and_side_effect<F>(
        &self,
        doc_id: &str,
        primary_term: u64,
        condition: super::WriteCondition,
        mut side_effect: F,
    ) -> Result<super::DeleteWriteReceipt>
    where
        F: FnMut(&super::SequencedOperation) -> Result<()>,
    {
        self.prepare_primary_term_before_wal(primary_term)?;
        let doc_id_owned = doc_id.to_string();
        let (seq_no, existed) =
            self.with_version_map_capacity("document delete", [doc_id], false, |translog| {
                drop(self.writer_state_with_replay(translog, "document delete")?);
                self.prepare_primary_term_before_wal(primary_term)?;
                let existed = self.check_primary_condition(doc_id, condition)?;
                let start_cursor = translog.recovery_read_snapshot()?.end_cursor();
                let seq_no = translog.write_document_with_receipt(
                    primary_term,
                    WalDocumentOperation::Delete { doc_id },
                )?;
                let operation = super::SequencedOperation {
                    seq_no,
                    primary_term,
                    mutation: super::DocumentMutation::Delete {
                        doc_id: doc_id_owned,
                    },
                };
                #[cfg(feature = "protocol-trace")]
                if let Some(copy) = crate::protocol_trace::current_open_copy() {
                    crate::protocol_trace::record_wal_appended(
                        &copy,
                        &operation,
                        matches!(self.durability, TranslogDurability::Request),
                    )?;
                }
                self.apply_sequenced_batch_locked(
                    translog,
                    std::slice::from_ref(&operation),
                    WalDisposition::AlreadyInLocalWal {
                        persisted: matches!(self.durability, TranslogDurability::Request),
                        validate_redelivery: true,
                        start_cursor: Some(start_cursor),
                        replay_positions: None,
                        initial_versions: None,
                    },
                    None,
                    true,
                    &mut side_effect,
                )?;
                Ok((seq_no, existed))
            })?;
        Ok(super::DeleteWriteReceipt {
            deleted: u64::from(existed),
            seq_no,
            primary_term,
        })
    }

    fn fail_writer_after_wal(&self, error: &anyhow::Error) {
        self.writer
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .fail(format!(
                "sequenced operation failed after WAL persistence: {error:#}"
            ));
    }

    fn with_version_map_capacity<'a, T, I, F>(
        &self,
        context: &'static str,
        doc_ids: I,
        allow_oversized_bulk: bool,
        operation: F,
    ) -> Result<T>
    where
        I: IntoIterator<Item = &'a str>,
        F: FnOnce(&dyn WriteAheadLog) -> Result<T>,
    {
        let reservation_bytes = LiveVersionMap::estimate_reservation(doc_ids);
        let mut refreshed = false;
        loop {
            let translog = self
                .translog
                .lock()
                .map_err(|_| anyhow::anyhow!("translog lock poisoned during {context}"))?;
            let (can_reserve, current_old_empty, estimated_bytes, max_bytes) = {
                let state = self
                    .apply_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
                (
                    state.versions.can_reserve(reservation_bytes),
                    state.versions.current_and_old_are_empty(),
                    state.versions.estimated_bytes(),
                    state.versions.max_bytes(),
                )
            };
            let oversized_bulk = reservation_bytes > max_bytes;
            if can_reserve
                || (allow_oversized_bulk && oversized_bulk && refreshed && current_old_empty)
            {
                let result = operation(&*translog);
                drop(translog);
                if result.is_ok()
                    && allow_oversized_bulk
                    && oversized_bulk
                    && let Err(error) = self.post_apply_refresh()
                {
                    tracing::warn!(
                        "post-apply refresh failed after an oversized version-map bulk: {error:#}"
                    );
                }
                return result;
            }
            drop(translog);

            if refreshed {
                return Err(super::version_map::VersionMapCapacityError {
                    estimated_bytes,
                    reservation_bytes,
                    max_bytes,
                }
                .into());
            }
            self.refresh()?;
            refreshed = true;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_version_map_max_bytes_for_test(&self, max_bytes: usize) {
        self.apply_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .versions
            .set_max_bytes_for_test(max_bytes);
    }

    pub(crate) fn refresh_with_pruned_tombstones(&self) -> Result<Vec<PrunedTombstone>> {
        let _maintenance = self.maintenance_guard("refresh")?;
        let committed_boundary = self.with_translog("refresh", |translog| {
            #[cfg(test)]
            if let Some(sender) = self
                .refresh_before_writer_sender
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = sender.send(());
            }
            let mut writer_state = self.writer_state_with_replay(translog, "refresh")?;
            {
                let mut state = self
                    .apply_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
                state.versions.rotate_current_into_old()?;
            }
            let boundary = self.current_committed_boundary()?;
            match self.commit_writer_at_boundary(&mut writer_state, "refresh", boundary) {
                Ok(boundary) => Ok(boundary),
                Err(error) => {
                    self.apply_state
                        .lock()
                        .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
                        .versions
                        .rollback_refresh()?;
                    Err(error)
                }
            }
        })?;
        self.persist_committed_boundary(&committed_boundary)?;

        #[cfg(test)]
        if let Some(sender) = self
            .refresh_after_commit_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = sender.send(());
            if let Some(release) = self
                .refresh_after_commit_release_receiver
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = release.recv();
            }
        }

        self.reader.reload()?;
        let pruned = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
            .versions
            .complete_reader_reload(
                committed_boundary.processed_checkpoint,
                self.delete_tombstone_retention,
            );
        Ok(pruned)
    }

    fn replay_translog_suffix_locked(
        &self,
        translog: &dyn WriteAheadLog,
        writer_state: &mut WriterState,
        context: &str,
    ) -> Result<u64> {
        let committed = self.load_committed_boundary()?;
        // Publish the committed state to the reader before clearing the live
        // version map. Otherwise a realtime GET or primary condition that
        // misses the map falls back to an older reader. Some paths commit
        // without reloading, such as the peer-recovery snapshot, and rely on
        // the delayed commit watcher.
        self.reader
            .reload()
            .with_context(|| format!("reader reload failed before {context} replay"))?;
        self.reset_apply_state_to_commit(committed.clone())?;
        #[cfg(feature = "protocol-trace")]
        let trace_copy = crate::protocol_trace::current_open_copy();
        #[cfg(feature = "protocol-trace")]
        let trace_replay = match trace_copy.as_ref() {
            Some(copy) => {
                crate::protocol_trace::record_replay_started(copy, self.sequence_stats())?.is_some()
            }
            None => false,
        };
        let committed_next_seq = committed
            .processed_checkpoint
            .and_then(|checkpoint| checkpoint.checked_add(1))
            .unwrap_or(0);
        let wal_next_seq = translog.next_seq_no();
        if committed_next_seq > wal_next_seq {
            anyhow::bail!(
                "committed translog checkpoint {committed_next_seq} exceeds WAL next sequence {wal_next_seq}"
            );
        }
        if committed_next_seq == wal_next_seq {
            #[cfg(feature = "protocol-trace")]
            if trace_replay && let Some(copy) = trace_copy.as_ref() {
                crate::protocol_trace::record_replay_finished(copy, "completed")?;
            }
            return Ok(0);
        }

        let mut replayed: u64 = 0;
        let mut last_committed_boundary = None;
        let mut batch = Vec::with_capacity(TRANSLOG_REPLAY_BATCH_SIZE as usize);
        let mut flush_batch =
            |batch: &mut Vec<(super::SequencedOperation, crate::wal::WalCursor)>| -> Result<()> {
                if batch.is_empty() {
                    return Ok(());
                }
                let (operations, cursors): (Vec<_>, Vec<_>) =
                    std::mem::take(batch).into_iter().unzip();
                let positions = operations
                    .iter()
                    .zip(cursors)
                    .map(|(operation, cursor)| ((operation.primary_term, operation.seq_no), cursor))
                    .collect::<WalPositions>();
                #[cfg(feature = "protocol-trace")]
                let result = if trace_replay {
                    crate::protocol_trace::with_apply_scope(
                        crate::protocol_trace::ApplyOrigin::Replay,
                        operations.clone(),
                        || {
                            self.apply_sequenced_batch_locked(
                                translog,
                                &operations,
                                WalDisposition::AlreadyInLocalWal {
                                    persisted: true,
                                    validate_redelivery: false,
                                    start_cursor: None,
                                    replay_positions: Some(&positions),
                                    initial_versions: None,
                                },
                                Some(writer_state),
                                false,
                                |_| Ok(()),
                            )
                        },
                    )
                } else {
                    self.apply_sequenced_batch_locked(
                        translog,
                        &operations,
                        WalDisposition::AlreadyInLocalWal {
                            persisted: true,
                            validate_redelivery: false,
                            start_cursor: None,
                            replay_positions: Some(&positions),
                            initial_versions: None,
                        },
                        Some(writer_state),
                        false,
                        |_| Ok(()),
                    )
                };
                #[cfg(not(feature = "protocol-trace"))]
                let result = self.apply_sequenced_batch_locked(
                    translog,
                    &operations,
                    WalDisposition::AlreadyInLocalWal {
                        persisted: true,
                        validate_redelivery: false,
                        start_cursor: None,
                        replay_positions: Some(&positions),
                        initial_versions: None,
                    },
                    Some(writer_state),
                    false,
                    |_| Ok(()),
                );
                result?;
                let boundary = self.current_committed_boundary()?;
                let boundary = self.commit_writer_at_boundary(writer_state, context, boundary)?;
                self.persist_committed_boundary(&boundary)?;
                last_committed_boundary = Some(boundary);
                Ok(())
            };
        let replay_result = translog.for_each_from_at(0, &mut |position, entry| {
            if committed
                .processed_checkpoint
                .is_some_and(|checkpoint| entry.seq_no <= checkpoint)
            {
                #[cfg(feature = "protocol-trace")]
                if trace_replay && let Some(copy) = trace_copy.as_ref() {
                    let operation = sequenced_operation_from_entry(entry)?;
                    crate::protocol_trace::record_replay_skip(
                        copy,
                        &operation,
                        self.sequence_stats(),
                    )?;
                }
                return Ok(());
            }
            if replayed == 0 {
                tracing::warn!("Replaying translog entries during {context}...");
            }
            batch.push((sequenced_operation_from_entry(entry)?, position));
            replayed += 1;
            if batch.len() >= TRANSLOG_REPLAY_BATCH_SIZE as usize {
                flush_batch(&mut batch)?;
            }
            Ok(())
        });
        if let Err(error) = replay_result {
            let message = format!("translog replay failed during {context}: {error}");
            writer_state.fail(format!("{message}: {error:#}"));
            #[cfg(feature = "protocol-trace")]
            if trace_replay && let Some(copy) = trace_copy.as_ref() {
                crate::protocol_trace::record_replay_finished(copy, "failed")?;
            }
            return Err(error).context(message);
        }
        if replayed == 0 {
            #[cfg(feature = "protocol-trace")]
            if trace_replay && let Some(copy) = trace_copy.as_ref() {
                crate::protocol_trace::record_replay_finished(copy, "completed")?;
            }
            return Ok(0);
        }
        flush_batch(&mut batch)?;
        if let Err(error) = self.reader.reload() {
            writer_state.fail(format!("reader reload failed after {context}: {error}"));
            #[cfg(feature = "protocol-trace")]
            if trace_replay && let Some(copy) = trace_copy.as_ref() {
                crate::protocol_trace::record_replay_finished(copy, "failed")?;
            }
            return Err(error).with_context(|| format!("reader reload failed after {context}"));
        }
        let committed_boundary = last_committed_boundary
            .expect("a non-empty successful replay has a committed boundary");
        if let Err(error) = self.persist_committed_boundary(&committed_boundary) {
            writer_state.fail(format!(
                "committed checkpoint persistence failed after {context}: {error:#}"
            ));
            #[cfg(feature = "protocol-trace")]
            if trace_replay && let Some(copy) = trace_copy.as_ref() {
                crate::protocol_trace::record_replay_finished(copy, "failed")?;
            }
            return Err(error).with_context(|| {
                format!("committed checkpoint persistence failed after {context}")
            });
        }
        self.apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
            .versions
            .complete_reader_reload(
                committed_boundary.processed_checkpoint,
                self.delete_tombstone_retention,
            );
        tracing::info!(
            "Translog replay during {} recovered {} operations.",
            context,
            replayed
        );
        #[cfg(feature = "protocol-trace")]
        if trace_replay && let Some(copy) = trace_copy.as_ref() {
            crate::protocol_trace::record_replay_finished(copy, "completed")?;
        }
        Ok(replayed)
    }

    fn writer_state_with_replay(
        &self,
        translog: &dyn WriteAheadLog,
        context: &str,
    ) -> Result<std::sync::RwLockWriteGuard<'_, WriterState>> {
        let automatic_policy = self
            .automatic_merge_policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut writer_state = self
            .writer
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if writer_state.writer.is_none() {
            let previous_failure = writer_state
                .failure
                .clone()
                .unwrap_or_else(|| "writer reinitialization is incomplete".to_string());
            let rebuild_context = format!("failed to rebuild Tantivy writer during {context}");
            let replacement = match self.open_replacement_writer(
                &rebuild_context,
                Box::new(SharedMergePolicy(automatic_policy)),
            ) {
                Ok(writer) => writer,
                Err(error) => {
                    writer_state.fail(format!(
                        "{rebuild_context}: {error:#}; previous writer failure: {previous_failure}"
                    ));
                    return Err(error).context(rebuild_context);
                }
            };
            writer_state.replace(replacement);
            if let Err(error) =
                self.replay_translog_suffix_locked(translog, &mut writer_state, context)
            {
                if writer_state.writer.is_some() {
                    writer_state.fail(format!(
                        "failed to replay WAL after rebuilding writer during {context}: {error:#}"
                    ));
                }
                return Err(error);
            }
        }
        Ok(writer_state)
    }

    fn pause_and_drain_automatic_merges(
        &self,
        translog: &dyn WriteAheadLog,
    ) -> Result<CommittedBoundaryRecord> {
        let automatic_policy = self
            .automatic_merge_policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut writer_state =
            self.writer_state_with_replay(translog, "force-merge preparation")?;

        writer_state
            .writer_mut("force-merge preparation")?
            .set_merge_policy(Box::new(NoMergePolicy));
        let boundary = self.current_committed_boundary()?;
        let committed_boundary =
            self.commit_writer_at_boundary(&mut writer_state, "force-merge preparation", boundary)?;

        let writer = writer_state.take("force-merge merge-thread drain")?;
        #[cfg(test)]
        if let Some(sender) = self
            .force_merge_before_wait_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = sender.send(());
        }
        let wait_result = writer.wait_merging_threads();
        let reopen_context = "failed to reopen Tantivy writer after draining merge threads";
        let replacement =
            match self.open_replacement_writer(reopen_context, Box::new(NoMergePolicy)) {
                Ok(writer) => writer,
                Err(error) => {
                    let message = format!("{reopen_context}: {error:#}");
                    writer_state.fail(message.clone());
                    return Err(error).context(message);
                }
            };
        writer_state.replace(replacement);

        if let Err(error) = wait_result {
            writer_state
                .writer_mut("automatic merge-policy restoration")?
                .set_merge_policy(Box::new(SharedMergePolicy(automatic_policy)));
            return Err(error).context("failed while draining Tantivy merge threads");
        }

        Ok(committed_boundary)
    }

    fn restore_automatic_merge_policy(&self) -> Result<()> {
        let automatic_policy = self
            .automatic_merge_policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mut writer_state = self
            .writer
            .write()
            .unwrap_or_else(|error| error.into_inner());
        writer_state
            .writer_mut("automatic merge-policy restoration")?
            .set_merge_policy(Box::new(SharedMergePolicy(automatic_policy)));
        Ok(())
    }

    #[cfg(test)]
    fn set_merge_policy_for_test(&self, policy: Box<dyn MergePolicy>) -> Result<()> {
        let policy: Arc<dyn MergePolicy> = Arc::from(policy);
        *self
            .automatic_merge_policy
            .write()
            .unwrap_or_else(|error| error.into_inner()) = policy.clone();
        let mut writer_state = self
            .writer
            .write()
            .unwrap_or_else(|error| error.into_inner());
        writer_state
            .writer_mut("test merge-policy update")?
            .set_merge_policy(Box::new(SharedMergePolicy(policy)));
        Ok(())
    }

    /// Get (or lazily register) a field by name.
    /// With Tantivy, once an index is created, the schema is fixed — so we look up
    /// pre-existing fields. If a field doesn't exist, we fall back to the "body" field.
    fn resolve_field(&self, field_name: &str) -> Field {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(f) = registry.fields.get(field_name) {
            return *f;
        }
        // Fall back to "body" for unknown fields
        *registry.fields.get("body").expect("body field must exist")
    }

    fn logical_field_type_for_field(
        &self,
        field: Field,
    ) -> Option<crate::cluster::state::FieldType> {
        let field_name = self
            .index
            .schema()
            .get_field_entry(field)
            .name()
            .to_string();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        registry.field_types.get(&field_name).cloned()
    }

    fn normalize_result_source_with_registry(
        registry: &FieldRegistry,
        value: &mut serde_json::Value,
    ) {
        if registry.date_fields.is_empty() {
            return;
        }

        let Some(object) = value.as_object_mut() else {
            return;
        };

        for field_name in &registry.date_fields {
            let Some(field_value) = object.get_mut(field_name) else {
                continue;
            };
            if let Some(normalized) = crate::common::date::normalize_json_date_value(field_value) {
                *field_value = normalized;
            }
        }
    }

    fn decode_stored_source_with_registry(
        registry: &FieldRegistry,
        text: &str,
    ) -> Option<serde_json::Value> {
        let mut json_val = serde_json::from_str::<serde_json::Value>(text).ok()?;
        // Keep stored date fields in the public JSON representation.
        Self::normalize_result_source_with_registry(registry, &mut json_val);
        Some(json_val)
    }

    /// Create a Tantivy Term that matches the schema type of the target field.
    /// This prevents type mismatches (e.g., i64 term on an f64 field) that cause
    /// silent 0-hit results.
    fn typed_term(&self, field: Field, value: &serde_json::Value) -> Term {
        use tantivy::schema::FieldType;
        let schema = self.index.schema();
        let field_type = schema.get_field_entry(field).field_type();
        let logical_field_type = self.logical_field_type_for_field(field);
        match value {
            serde_json::Value::String(s) => match logical_field_type {
                Some(crate::cluster::state::FieldType::Date)
                    if matches!(field_type, FieldType::I64(_)) =>
                {
                    crate::common::date::parse_iso8601_to_epoch_millis(s)
                        .or_else(|| s.parse::<i64>().ok())
                        .map(|millis| Term::from_field_i64(field, millis))
                        .unwrap_or_else(|| Term::from_field_text(field, s))
                }
                Some(crate::cluster::state::FieldType::Integer)
                    if matches!(field_type, FieldType::I64(_)) =>
                {
                    s.parse::<i64>()
                        .map(|value| Term::from_field_i64(field, value))
                        .unwrap_or_else(|_| Term::from_field_text(field, s))
                }
                Some(crate::cluster::state::FieldType::Float)
                    if matches!(field_type, FieldType::F64(_)) =>
                {
                    s.parse::<f64>()
                        .map(|value| Term::from_field_f64(field, value))
                        .unwrap_or_else(|_| Term::from_field_text(field, s))
                }
                _ => Term::from_field_text(field, s),
            },
            serde_json::Value::Number(n) => match field_type {
                FieldType::I64(_) => {
                    let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                    Term::from_field_i64(field, i)
                }
                FieldType::F64(_) => {
                    let f = n.as_f64().unwrap_or(n.as_i64().unwrap_or(0) as f64);
                    Term::from_field_f64(field, f)
                }
                FieldType::U64(_) => {
                    let u = n.as_u64().unwrap_or(n.as_f64().unwrap_or(0.0) as u64);
                    Term::from_field_u64(field, u)
                }
                _ => Term::from_field_text(field, &n.to_string()),
            },
            serde_json::Value::Bool(b) => {
                Term::from_field_text(field, if *b { "true" } else { "false" })
            }
            other => Term::from_field_text(field, &other.to_string()),
        }
    }

    /// Resolve a named field for sort/search_after use.
    /// Supports `_id` (returns the special id field) and any explicitly mapped
    /// field. Returns an error for unknown names so callers can surface a 400
    /// instead of silently treating it as the "body" field.
    fn resolve_named_field(&self, name: &str) -> Result<Field> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if name == "_id" {
            return Ok(registry.id_field);
        }
        registry
            .fields
            .get(name)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown field for sort/search_after: {name}"))
    }

    /// Build a Tantivy filter that admits only docs strictly past the cursor
    /// in tuple-lexicographic order over the configured sort fields.
    ///
    /// For sort `[(f1, dir1), (f2, dir2), ..., (fn, dirn)]` and cursor
    /// `[v1, v2, ..., vn]`, the filter is the disjunction:
    ///   (f1 strict_gt_dir1 v1)
    /// OR (f1 == v1 AND f2 strict_gt_dir2 v2)
    /// OR (f1 == v1 AND f2 == v2 AND f3 strict_gt_dir3 v3)
    /// OR ...
    ///
    /// Caller is responsible for validating that `sort.len() == cursor.len()`,
    /// `sort` is non-empty, and no clause sorts by `_score`.
    fn build_search_after_filter(
        &self,
        sort: &[crate::search::SortClause],
        cursor: &[serde_json::Value],
    ) -> Result<Box<dyn tantivy::query::Query>> {
        use std::ops::Bound;
        use tantivy::query::{BooleanQuery, Occur, RangeQuery, TermQuery};
        use tantivy::schema::IndexRecordOption;

        if sort.is_empty() {
            return Err(anyhow::anyhow!("search_after requires a non-empty sort"));
        }
        if sort.len() != cursor.len() {
            return Err(anyhow::anyhow!(
                "search_after length ({}) does not match sort length ({})",
                cursor.len(),
                sort.len()
            ));
        }

        // Pre-resolve each (field_name, direction, Field handle).
        let mut resolved: Vec<(&str, crate::search::SortDirection, Field)> =
            Vec::with_capacity(sort.len());
        for clause in sort {
            let Some((name, direction)) = crate::search::sort_clause_name_direction(clause) else {
                return Err(anyhow::anyhow!("malformed sort clause"));
            };
            if name == "_score" {
                return Err(anyhow::anyhow!(
                    "search_after does not support sorting by _score"
                ));
            }
            let field = self.resolve_named_field(name)?;
            resolved.push((name, direction, field));
        }

        let mut should_branches: Vec<(Occur, Box<dyn tantivy::query::Query>)> =
            Vec::with_capacity(resolved.len());

        for i in 0..resolved.len() {
            let mut prefix: Vec<(Occur, Box<dyn tantivy::query::Query>)> =
                Vec::with_capacity(i + 1);

            // Equality clauses for all preceding sort fields.
            for j in 0..i {
                let (_name_j, _dir_j, field_j) = &resolved[j];
                let term_j = self.typed_term(*field_j, &cursor[j]);
                let eq = TermQuery::new(term_j, IndexRecordOption::Basic);
                prefix.push((Occur::Must, Box::new(eq)));
            }

            // Strict inequality on the i-th sort field, oriented per direction.
            let (_name_i, dir_i, field_i) = &resolved[i];
            let term_i = self.typed_term(*field_i, &cursor[i]);
            let range: RangeQuery = match dir_i {
                crate::search::SortDirection::Asc => {
                    RangeQuery::new(Bound::Excluded(term_i), Bound::Unbounded)
                }
                crate::search::SortDirection::Desc => {
                    RangeQuery::new(Bound::Unbounded, Bound::Excluded(term_i))
                }
            };
            prefix.push((Occur::Must, Box::new(range)));

            // Bundle the prefix as a single AND-branch.
            let branch: Box<dyn tantivy::query::Query> = if prefix.len() == 1 {
                prefix.pop().unwrap().1
            } else {
                Box::new(BooleanQuery::new(prefix))
            };
            should_branches.push((Occur::Should, branch));
        }

        // A BooleanQuery with only Should branches requires at least one to match.
        Ok(Box::new(BooleanQuery::new(should_branches)))
    }

    pub fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<super::SqlBatchResult> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&req.query)?;
        let limit = std::cmp::max(req.size, 1);

        // Use fast-field sort when the SearchRequest includes a sortable field,
        // otherwise fall back to score-based collection.
        let (top_docs, total_hits) =
            if let Some((sort_field, order)) = self.extract_fast_field_sort(req) {
                let schema = self.index.schema();
                let field = self.resolve_field(&sort_field);
                match schema.get_field_entry(field).field_type() {
                    tantivy::schema::FieldType::F64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<f64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    tantivy::schema::FieldType::I64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<i64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    tantivy::schema::FieldType::U64(_) => {
                        let td = TopDocs::with_limit(limit)
                            .order_by_fast_field::<u64>(&sort_field, order);
                        let (sorted, count) = searcher.search(&*query, &(td, Count))?;
                        let docs: Vec<(f32, tantivy::DocAddress)> =
                            sorted.into_iter().map(|(_, addr)| (0.0f32, addr)).collect();
                        (docs, count)
                    }
                    _ => searcher.search(&*query, &(TopDocs::with_limit(limit), Count))?,
                }
            } else {
                searcher.search(&*query, &(TopDocs::with_limit(limit), Count))?
            };

        let schema = self.index.schema();
        let segment_readers = searcher.segment_readers();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        // Build per-segment field readers for requested columns
        let mut field_plans = Vec::with_capacity(segment_readers.len());
        // Also open fast-field reader for _id per segment (only if needed)
        let mut id_readers: Vec<Option<StringFastFieldReader>> =
            Vec::with_capacity(segment_readers.len());
        let mut needs_stored_doc = false;

        for segment_reader in segment_readers {
            let fast_fields = segment_reader.fast_fields();
            let mut segment_fields = Vec::with_capacity(columns.len());
            for column in columns {
                let reader = open_sql_field_reader(
                    &schema,
                    fast_fields,
                    column,
                    registry.field_types.get(column),
                );
                if matches!(reader, SqlFieldReader::SourceFallback) {
                    needs_stored_doc = true;
                }
                segment_fields.push(reader);
            }
            field_plans.push(segment_fields);

            // Only open _id fast-field reader if we actually need _id
            let id_reader = if needs_id {
                StringFastFieldReader::open(fast_fields, "_id")
            } else {
                None
            };
            id_readers.push(id_reader);
        }

        let mut ids = Vec::with_capacity(if needs_id { top_docs.len() } else { 0 });
        let mut scores = Vec::with_capacity(if needs_score { top_docs.len() } else { 0 });

        // Fast path: use column cache when no SourceFallback columns are needed.
        // This avoids per-doc serde_json::Value allocation by working directly with Arrow arrays,
        // including zero-column queries like `SELECT 1 ...` that still need one output row per hit.
        let use_cache = !needs_stored_doc;

        if use_cache {
            // Group matching doc IDs by segment ordinal
            let mut seg_docs: Vec<Vec<(u32, f32)>> = vec![Vec::new(); segment_readers.len()];
            for (score, doc_address) in &top_docs {
                let seg_ord = doc_address.segment_ord as usize;
                seg_docs[seg_ord].push((doc_address.doc_id, *score));
            }

            // Collect scores in doc order. _id now uses the same per-segment
            // projection path as other fast-field columns.
            for (score, _) in &top_docs {
                if needs_score {
                    scores.push(*score);
                }
            }

            use datafusion::arrow::array::UInt32Array;

            let id_array = if needs_id {
                let mut segment_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();

                for (seg_ord, docs) in seg_docs.iter().enumerate() {
                    if docs.is_empty() {
                        continue;
                    }

                    let taken = if let Some(id_reader) = id_readers[seg_ord].as_ref() {
                        let reader = SqlFieldReader::Str(id_reader.clone());
                        build_projected_fast_field_array(
                            self.column_cache.as_ref(),
                            segment_readers[seg_ord].segment_id(),
                            segment_readers[seg_ord].max_doc(),
                            "_id",
                            &reader,
                            docs,
                        )?
                    } else {
                        std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![
                            "";
                            docs.len()
                        ]))
                    };

                    segment_arrays.push(taken);
                }

                let refs: Vec<&dyn datafusion::arrow::array::Array> =
                    segment_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated: datafusion::arrow::array::ArrayRef = if refs.is_empty() {
                    std::sync::Arc::new(datafusion::arrow::array::StringArray::from(
                        Vec::<&str>::new(),
                    ))
                } else {
                    datafusion::arrow::compute::concat(&refs)?
                };
                Some(concatenated)
            } else {
                None
            };

            // For each column, build per-segment Arrow arrays via cache, then take() matching rows

            let mut result_columns: Vec<(String, datafusion::arrow::array::ArrayRef)> = Vec::new();

            for (col_idx, column) in columns.iter().enumerate() {
                let mut segment_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();

                for (seg_ord, docs) in seg_docs.iter().enumerate() {
                    if docs.is_empty() {
                        continue;
                    }
                    let seg_id = segment_readers[seg_ord].segment_id();
                    let max_doc = segment_readers[seg_ord].max_doc();
                    let reader = &field_plans[seg_ord][col_idx];

                    let taken = build_projected_fast_field_array(
                        self.column_cache.as_ref(),
                        seg_id,
                        max_doc,
                        column,
                        reader,
                        docs,
                    )?;
                    segment_arrays.push(taken);
                }

                // Concatenate across segments (preserving top_docs order within each segment)
                let refs: Vec<&dyn datafusion::arrow::array::Array> =
                    segment_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = if refs.is_empty() {
                    // Empty result — build typed empty array from schema
                    let kind =
                        column_kind_for_column(&schema, registry.field_types.get(column), column);
                    empty_typed_array(kind)
                } else {
                    datafusion::arrow::compute::concat(&refs)?
                };
                result_columns.push((column.clone(), concatenated));
            }

            // Build the RecordBatch from the cached/taken columns + ids + scores
            // We need to reorder rows to match the original top_docs order since we grouped by segment.
            // Build a mapping: for each (seg_ord, position_in_seg_docs) → position in top_docs
            let mut seg_positions: Vec<usize> = vec![0; segment_readers.len()];
            let mut reorder_indices: Vec<u32> = Vec::with_capacity(top_docs.len());

            // First pass: compute output offset per segment
            let mut seg_offsets: Vec<usize> = Vec::with_capacity(segment_readers.len());
            let mut offset = 0;
            for docs in &seg_docs {
                seg_offsets.push(offset);
                offset += docs.len();
            }

            // For each doc in original top_docs order, find its position in the concatenated output
            for (_, doc_address) in &top_docs {
                let seg_ord = doc_address.segment_ord as usize;
                let pos = seg_offsets[seg_ord] + seg_positions[seg_ord];
                reorder_indices.push(pos as u32);
                seg_positions[seg_ord] += 1;
            }
            let reorder_array = UInt32Array::from(reorder_indices);

            // Reorder all columns to match original top_docs order
            let mut schema_fields = Vec::new();
            let mut ordered_arrays: Vec<datafusion::arrow::array::ArrayRef> = Vec::new();
            let num_rows = top_docs.len();

            // _id column — empty strings if not needed
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                "_id",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            ));
            if needs_id {
                let reordered = if let Some(arr) = &id_array {
                    datafusion::arrow::compute::take(arr.as_ref(), &reorder_array, None)?
                } else {
                    let empty_ids: datafusion::arrow::array::ArrayRef = std::sync::Arc::new(
                        datafusion::arrow::array::StringArray::from(vec![""; num_rows]),
                    );
                    empty_ids
                };
                ordered_arrays.push(reordered);
            } else {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::StringArray::from(vec![""; num_rows]),
                ));
            }

            // score column — zeros if not needed
            schema_fields.push(datafusion::arrow::datatypes::Field::new(
                "_score",
                datafusion::arrow::datatypes::DataType::Float32,
                false,
            ));
            if needs_score {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::Float32Array::from(scores),
                ));
            } else {
                ordered_arrays.push(std::sync::Arc::new(
                    datafusion::arrow::array::Float32Array::from(vec![0.0f32; num_rows]),
                ));
            }

            // Data columns — reorder each to match top_docs order
            for (name, arr) in &result_columns {
                let reordered =
                    datafusion::arrow::compute::take(arr.as_ref(), &reorder_array, None)?;
                let dt = reordered.data_type().clone();
                schema_fields.push(datafusion::arrow::datatypes::Field::new(name, dt, true));
                ordered_arrays.push(reordered);
            }

            let schema =
                std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(schema_fields));
            let batch =
                datafusion::arrow::record_batch::RecordBatch::try_new(schema, ordered_arrays)?;
            return Ok(super::SqlBatchResult { batch, total_hits });
        }

        // Fallback: per-doc reading (used when SourceFallback columns are needed)
        let mut projected_columns = std::collections::BTreeMap::new();
        for column in columns {
            projected_columns.insert(column.clone(), Vec::with_capacity(top_docs.len()));
        }

        for (score, doc_address) in top_docs {
            let seg_ord = doc_address.segment_ord as usize;
            let doc_id = doc_address.doc_id;

            // Load stored doc only when needed for SourceFallback columns
            let retrieved_doc = if needs_stored_doc {
                Some(searcher.doc::<TantivyDocument>(doc_address)?)
            } else {
                None
            };

            // Read _id only if the SQL query references it
            if needs_id {
                let id_str = if needs_stored_doc {
                    retrieved_doc
                        .as_ref()
                        .unwrap()
                        .get_all(registry.id_field)
                        .next()
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else if let Some(ref id_reader) = id_readers[seg_ord] {
                    let mut text = String::new();
                    if id_reader.first_text(doc_id, &mut text) {
                        text
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                ids.push(id_str);
            }

            // Read score only if the SQL query references it
            if needs_score {
                scores.push(score);
            }

            let mut source_json = None;
            for (index, column) in columns.iter().enumerate() {
                let value = match &field_plans[seg_ord][index] {
                    SqlFieldReader::F64(reader) => reader
                        .first(doc_id)
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                    SqlFieldReader::I64(reader) => reader
                        .first(doc_id)
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                    SqlFieldReader::DateMillis(reader) => {
                        // Emits raw i64 epoch millis intentionally — the ColumnStore
                        // path uses build_timestamp_millis_array() which handles
                        // Value::Number correctly via the TimestampMillis type hint.
                        reader
                            .first(doc_id)
                            .map(serde_json::Value::from)
                            .unwrap_or(serde_json::Value::Null)
                    }
                    SqlFieldReader::Str(reader) => {
                        let mut text = String::new();
                        if reader.first_text(doc_id, &mut text) {
                            serde_json::Value::String(text)
                        } else {
                            serde_json::Value::Null
                        }
                    }
                    SqlFieldReader::SourceFallback => {
                        let source = source_json.get_or_insert_with(|| {
                            retrieved_doc
                                .as_ref()
                                .unwrap()
                                .get_all(registry.source_field)
                                .next()
                                .and_then(|value| value.as_str())
                                .and_then(|text| {
                                    serde_json::from_str::<serde_json::Value>(text).ok()
                                })
                                .and_then(|value| value.as_object().cloned())
                                .unwrap_or_default()
                        });
                        source
                            .get(column)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    }
                };
                projected_columns
                    .get_mut(column)
                    .expect("projected SQL column should exist")
                    .push(value);
            }
        }

        let column_store =
            crate::hybrid::column_store::ColumnStore::new(ids, scores, projected_columns);

        // Build type hints from the SqlFieldReader variants so that zero-result
        // queries still produce correctly-typed Arrow columns (e.g. Float64 for
        // price) instead of defaulting to Utf8.
        let mut type_hints = std::collections::HashMap::new();
        if let Some(first_segment) = field_plans.first() {
            for (i, column) in columns.iter().enumerate() {
                let kind = match &first_segment[i] {
                    SqlFieldReader::F64(_) => crate::hybrid::arrow_bridge::ColumnKind::Float64,
                    SqlFieldReader::I64(_) => crate::hybrid::arrow_bridge::ColumnKind::Int64,
                    SqlFieldReader::DateMillis(_) => {
                        crate::hybrid::arrow_bridge::ColumnKind::TimestampMillis
                    }
                    SqlFieldReader::Str(_) => crate::hybrid::arrow_bridge::ColumnKind::Utf8,
                    SqlFieldReader::SourceFallback => continue,
                };
                type_hints.insert(column.clone(), kind);
            }
        } else {
            // No segments — derive types from the Tantivy schema directly
            for column in columns {
                type_hints.insert(
                    column.clone(),
                    column_kind_for_column(&schema, registry.field_types.get(column), column),
                );
            }
        }
        let batch =
            crate::hybrid::arrow_bridge::build_record_batch_with_hints(&column_store, &type_hints)?;
        Ok(super::SqlBatchResult { batch, total_hits })
    }

    /// Build a Tantivy document from a JSON object.
    /// When typed fields exist in the registry, values are indexed into their
    /// proper field types. All text values also go into the "body" catch-all
    /// for backward-compatible `?q=` query string searches.
    pub(crate) fn is_keyword_field(&self, name: &str) -> bool {
        matches!(
            self.field_registry
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .field_types
                .get(name),
            Some(crate::cluster::state::FieldType::Keyword)
        )
    }

    fn validate_keyword_documents<'a>(
        &self,
        documents: impl IntoIterator<Item = &'a serde_json::Value>,
    ) -> Result<()> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        for document in documents {
            crate::common::validate_document_source(document)?;
            if let Some(object) = document.as_object() {
                for (field_name, value) in object {
                    if matches!(
                        value,
                        serde_json::Value::Array(_) | serde_json::Value::Object(_)
                    ) && matches!(
                        registry.field_types.get(field_name),
                        Some(crate::cluster::state::FieldType::Keyword)
                    ) {
                        visit_indexed_keyword_values(field_name, value, &mut |_| {})?;
                    }
                }
            }
        }
        Ok(())
    }

    fn build_tantivy_doc(
        &self,
        doc_id: &str,
        payload: &serde_json::Value,
        seq_no: u64,
        primary_term: u64,
    ) -> Result<TantivyDocument> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        Self::build_tantivy_doc_inner(
            &registry,
            &self.index.schema(),
            doc_id,
            payload,
            seq_no,
            primary_term,
        )
    }

    /// Build a Tantivy document using an already-acquired registry reference.
    /// Used by bulk paths to avoid per-doc RwLock acquisition.
    fn build_tantivy_doc_inner(
        registry: &FieldRegistry,
        schema: &Schema,
        doc_id: &str,
        payload: &serde_json::Value,
        seq_no: u64,
        primary_term: u64,
    ) -> Result<TantivyDocument> {
        crate::common::validate_document_source(payload)?;
        let mut doc = TantivyDocument::new();

        // Store the document ID
        doc.add_text(registry.id_field, doc_id);
        if let Some(field) = registry.seq_no_field {
            doc.add_u64(field, seq_no);
        }
        if let Some(field) = registry.primary_term_field {
            doc.add_u64(field, primary_term);
        }

        // Canonicalize mapped Date fields before persisting _source so every read path
        // sees the same UTC ISO 8601 representation.
        let normalized_source = payload.as_object().and_then(|object| {
            if registry
                .date_fields
                .iter()
                .any(|field_name| object.contains_key(field_name))
            {
                let mut value = payload.clone();
                Self::normalize_result_source_with_registry(registry, &mut value);
                Some(value)
            } else {
                None
            }
        });
        let source_value = normalized_source.as_ref().unwrap_or(payload);

        doc.add_text(registry.source_field, serde_json::to_string(source_value)?);

        let body_field = *registry.fields.get("body").expect("body field must exist");

        if let Some(obj) = payload.as_object() {
            // Build body catch-all with a single String buffer (avoids Vec<String> + join)
            let mut body_buf = String::new();

            for (key, value) in obj {
                // If this field has a named Tantivy field, index into it by type
                if let Some(&field) = registry.fields.get(key.as_str())
                    && field != body_field
                {
                    let logical_field_type = registry.field_types.get(key.as_str());
                    if matches!(
                        logical_field_type,
                        Some(crate::cluster::state::FieldType::Keyword)
                    ) {
                        let mut seen = value
                            .is_array()
                            .then(std::collections::HashSet::<String>::new);
                        visit_indexed_keyword_values(key, value, &mut |text| {
                            if let Some(seen) = &mut seen
                                && !seen.insert(text.to_string())
                            {
                                return;
                            }
                            doc.add_text(field, text.as_ref());
                            if !body_buf.is_empty() {
                                body_buf.push(' ');
                            }
                            body_buf.push_str(text.as_ref());
                        })?;
                        continue;
                    }
                    match value {
                        serde_json::Value::String(s) => match logical_field_type {
                            Some(crate::cluster::state::FieldType::Date) => {
                                if let Some(millis) =
                                    crate::common::date::parse_iso8601_to_epoch_millis(s)
                                        .or_else(|| s.parse::<i64>().ok())
                                {
                                    doc.add_i64(field, millis);
                                }
                            }
                            Some(crate::cluster::state::FieldType::Integer) => {
                                if let Ok(value) = s.parse::<i64>() {
                                    doc.add_i64(field, value);
                                }
                            }
                            Some(crate::cluster::state::FieldType::Float) => {
                                if let Ok(value) = s.parse::<f64>() {
                                    doc.add_f64(field, value);
                                }
                            }
                            _ => {
                                use tantivy::schema::FieldType;
                                match schema.get_field_entry(field).field_type() {
                                    FieldType::I64(_) | FieldType::F64(_) | FieldType::U64(_) => {}
                                    _ => doc.add_text(field, s),
                                }
                            }
                        },
                        serde_json::Value::Number(n) => {
                            use tantivy::schema::FieldType;
                            match schema.get_field_entry(field).field_type() {
                                FieldType::F64(_) => {
                                    let f = n.as_f64().unwrap_or(n.as_i64().unwrap_or(0) as f64);
                                    doc.add_f64(field, f);
                                }
                                FieldType::I64(_) => {
                                    let i = n.as_i64().unwrap_or(n.as_f64().unwrap_or(0.0) as i64);
                                    doc.add_i64(field, i);
                                }
                                FieldType::U64(_) => {
                                    let u = n.as_u64().unwrap_or(n.as_f64().unwrap_or(0.0) as u64);
                                    doc.add_u64(field, u);
                                }
                                _ => {}
                            }
                        }
                        serde_json::Value::Bool(b) => {
                            doc.add_text(field, if *b { "true" } else { "false" });
                        }
                        _ => {}
                    }
                }

                // Append text representation to body catch-all buffer
                match value {
                    serde_json::Value::String(s) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        body_buf.push_str(s);
                    }
                    serde_json::Value::Number(n) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        use std::fmt::Write;
                        let _ = write!(body_buf, "{n}");
                    }
                    serde_json::Value::Bool(b) => {
                        if !body_buf.is_empty() {
                            body_buf.push(' ');
                        }
                        body_buf.push_str(if *b { "true" } else { "false" });
                    }
                    _ => {}
                }
            }

            if !body_buf.is_empty() {
                doc.add_text(body_field, body_buf);
            }
        } else if let Ok(s) = serde_json::to_string(payload) {
            doc.add_text(body_field, s);
        }

        Ok(doc)
    }

    /// Replays pending translog entries into the Tantivy buffer in a streaming
    /// fashion — entries are never all held in memory at once.
    /// Called on startup to recover from an unclean shutdown.
    fn replay_translog(&self) -> Result<()> {
        self.with_translog("startup translog replay", |tl| {
            let mut writer_state = self.writer.write().unwrap_or_else(|e| e.into_inner());
            self.replay_translog_suffix_locked(tl, &mut writer_state, "startup translog replay")?;
            Ok(())
        })
    }

    #[cfg(test)]
    fn load_committed_next_seq_no(&self) -> Result<u64> {
        Ok(self
            .load_committed_boundary()?
            .processed_checkpoint
            .and_then(|checkpoint| checkpoint.checked_add(1))
            .unwrap_or(0))
    }

    fn persist_committed_boundary(&self, boundary: &CommittedBoundaryRecord) -> Result<()> {
        boundary.persist(&self.committed_boundary_path)?;
        #[cfg(feature = "protocol-trace")]
        if let Some(copy) = crate::protocol_trace::current_open_copy() {
            crate::protocol_trace::record_commit_persisted(&copy)?;
        }
        Ok(())
    }

    fn persist_committed_boundary_durable(&self, boundary: &CommittedBoundaryRecord) -> Result<()> {
        self.persist_committed_boundary(boundary)
    }

    fn peer_recovery_file_names(&self) -> Result<Vec<String>> {
        let index_path = self
            .committed_boundary_path
            .parent()
            .expect("committed checkpoint path has a parent")
            .join("index");
        let mut paths: std::collections::HashSet<PathBuf> = self
            .index
            .searchable_segment_metas()?
            .into_iter()
            .flat_map(|segment| segment.list_files())
            .filter(|path| index_path.join(path).is_file())
            .collect();
        paths.insert(PathBuf::from("meta.json"));

        if index_path.join(".managed.json").exists() {
            paths.insert(PathBuf::from(".managed.json"));
        }

        let mut names = Vec::with_capacity(paths.len());
        for path in paths {
            if path.components().count() != 1 {
                anyhow::bail!("Tantivy committed file path {path:?} is not a plain file name");
            }
            names.push(
                path.to_str()
                    .ok_or_else(|| anyhow::anyhow!("Tantivy file name is not UTF-8"))?
                    .to_string(),
            );
        }
        names.sort();
        Ok(names)
    }

    /// Starts the per-index background refresh loop.
    /// Called by the Node after wrapping the engine in an Arc.
    pub fn start_refresh_loop(engine: Arc<Self>) {
        let interval = engine.refresh_interval;
        tokio::spawn(async move {
            tracing::info!("Index refresh loop started (interval: {:?})", interval);
            loop {
                tokio::time::sleep(interval).await;
                let engine = engine.clone();
                match tokio::task::spawn_blocking(move || engine.refresh()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        tracing::error!("Background refresh failed: {}", e);
                    }
                    Err(e) => {
                        tracing::error!("Background refresh task failed: {}", e);
                    }
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn writer_lock_for_test(&self) -> WriterLockForTest<'_> {
        WriterLockForTest {
            guard: self.writer.write().unwrap_or_else(|e| e.into_inner()),
        }
    }

    #[cfg(test)]
    pub(crate) fn writer_is_failed_for_test(&self) -> bool {
        self.writer
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .writer
            .is_none()
    }

    pub(crate) fn writer_requires_rebuild(&self) -> bool {
        self.writer
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .writer
            .is_none()
    }

    #[cfg(test)]
    pub(crate) fn notify_before_refresh_writer_for_test(
        &self,
        sender: tokio::sync::oneshot::Sender<()>,
    ) {
        *self
            .refresh_before_writer_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
    }

    #[cfg(test)]
    pub(crate) fn pause_after_refresh_commit_for_test(
        &self,
        sender: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) {
        *self
            .refresh_after_commit_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(sender);
        *self
            .refresh_after_commit_release_receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(release);
    }

    #[cfg(test)]
    fn set_peer_recovery_scan_barrier_for_test(&self, barrier: Arc<std::sync::Barrier>) {
        self.with_translog("set recovery scan barrier", |translog| {
            translog.set_recovery_scan_barrier(Some(barrier));
            Ok(())
        })
        .expect("set recovery scan barrier");
    }

    #[cfg(test)]
    pub(crate) fn inject_wal_write_failures_for_test(&self, raw_os_error: i32, attempts: usize) {
        self.with_translog("inject WAL write failure", |translog| {
            translog.inject_write_io_failures_for_test(raw_os_error, attempts);
            Ok(())
        })
        .expect("inject WAL write failure");
    }

    #[cfg(test)]
    pub(crate) fn inject_writer_replacement_failures_for_test(
        &self,
        raw_os_error: i32,
        attempts: usize,
    ) {
        *self
            .writer_replacement_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((raw_os_error, attempts));
    }

    #[cfg(test)]
    pub(crate) fn inject_engine_apply_failures_for_test(&self, raw_os_error: i32, attempts: usize) {
        *self
            .engine_apply_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((raw_os_error, attempts));
    }

    #[cfg(test)]
    pub(crate) fn inject_refresh_commit_failures_for_test(&self, attempts: usize) {
        *self
            .refresh_commit_failures
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = attempts;
    }

    #[cfg(test)]
    pub(crate) fn inject_post_apply_refresh_failures_for_test(&self, attempts: usize) {
        *self
            .post_apply_refresh_failures
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = attempts;
    }

    fn post_apply_refresh(&self) -> Result<()> {
        #[cfg(test)]
        {
            let mut remaining = self
                .post_apply_refresh_failures
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if *remaining > 0 {
                *remaining -= 1;
                anyhow::bail!("injected post-apply refresh failure");
            }
        }
        self.refresh()
    }

    #[cfg(test)]
    fn maybe_fail_writer_replacement_for_test(&self) -> Option<anyhow::Error> {
        let mut failure = self
            .writer_replacement_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (raw_os_error, remaining) = failure.as_mut()?;
        if *remaining == 0 {
            *failure = None;
            return None;
        }
        *remaining -= 1;
        Some(std::io::Error::from_raw_os_error(*raw_os_error).into())
    }

    #[cfg(test)]
    fn maybe_fail_engine_apply_for_test(&self) -> Result<()> {
        let mut failure = self
            .engine_apply_failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some((raw_os_error, remaining)) = failure.as_mut() else {
            return Ok(());
        };
        if *remaining == 0 {
            *failure = None;
            return Ok(());
        }
        *remaining -= 1;
        Err(std::io::Error::from_raw_os_error(*raw_os_error).into())
    }

    /// Shared search execution helper — returns _id + _source from each hit.
    /// `limit` controls how many top docs Tantivy collects.
    fn execute_search(
        &self,
        searcher: tantivy::Searcher,
        query: &dyn tantivy::query::Query,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let effective_limit = if limit == 0 { 1 } else { limit };
        let top_docs = searcher.search(query, &TopDocs::with_limit(effective_limit))?;
        self.collect_hits(&searcher, top_docs)
    }

    /// Extract _id, _score, _source from pre-collected top docs.
    fn collect_hits(
        &self,
        searcher: &tantivy::Searcher,
        top_docs: Vec<(f32, tantivy::DocAddress)>,
    ) -> Result<Vec<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            // Get _id
            let doc_id = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str()
                    && let Some(json_val) =
                        Self::decode_stored_source_with_registry(&registry, text)
                {
                    results.push(serde_json::json!({
                        "_id": doc_id,
                        "_score": score,
                        "_source": json_val
                    }));
                }
            }
        }
        Ok(results)
    }

    /// Return the set of document IDs matching a query clause.
    /// Used by CompositeEngine for pre-filtering kNN results.
    pub fn matching_doc_ids(
        &self,
        clause: &crate::search::QueryClause,
    ) -> Result<std::collections::HashSet<String>> {
        let query = self.build_query(clause)?;
        let searcher = self.reader.searcher();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        // Collect up to 100k matching docs — a reasonable ceiling for filter sets
        let top_docs = searcher.search(&*query, &TopDocs::with_limit(100_000))?;
        let mut ids = std::collections::HashSet::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            if let Some(doc_id) = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
            {
                ids.insert(doc_id.to_string());
            }
        }
        Ok(ids)
    }

    /// Recursively convert a QueryClause into a Tantivy Query.
    fn build_query(
        &self,
        clause: &crate::search::QueryClause,
    ) -> Result<Box<dyn tantivy::query::Query>> {
        use crate::search::QueryClause;
        use tantivy::Term;
        use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Occur, TermQuery};
        use tantivy::schema::IndexRecordOption;

        match clause {
            QueryClause::MatchAll(_) => Ok(Box::new(AllQuery)),
            QueryClause::MatchNone(_) => Ok(Box::new(EmptyQuery)),
            QueryClause::Match(fields) => {
                if let Some((field_name, value)) = fields.iter().next() {
                    let query_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let target_field = self.resolve_field(field_name);
                    let query_parser = QueryParser::for_index(&self.index, vec![target_field]);
                    let query = query_parser.parse_query(&query_str)?;
                    Ok(query)
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Term(fields) => {
                if let Some((field_name, value)) = fields.iter().next() {
                    let target_field = self.resolve_field(field_name);
                    let term = self.typed_term(target_field, value);
                    Ok(Box::new(TermQuery::new(term, IndexRecordOption::Basic)))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Bool(bq) => {
                let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

                for clause in &bq.must {
                    subqueries.push((Occur::Must, self.build_query(clause)?));
                }
                for clause in &bq.should {
                    subqueries.push((Occur::Should, self.build_query(clause)?));
                }
                for clause in &bq.must_not {
                    subqueries.push((Occur::MustNot, self.build_query(clause)?));
                }
                // filter = must without scoring (Tantivy doesn't distinguish, so treat as Must)
                for clause in &bq.filter {
                    subqueries.push((Occur::Must, self.build_query(clause)?));
                }

                if subqueries.is_empty() {
                    // Empty bool matches all
                    Ok(Box::new(AllQuery))
                } else {
                    Ok(Box::new(BooleanQuery::new(subqueries)))
                }
            }
            QueryClause::Range(fields) => {
                use std::ops::Bound;
                use tantivy::query::RangeQuery;

                if let Some((field_name, condition)) = fields.iter().next() {
                    let target_field = self.resolve_field(field_name);

                    let to_term =
                        |v: &serde_json::Value| -> Term { self.typed_term(target_field, v) };

                    let lower = if let Some(ref v) = condition.gt {
                        Bound::Excluded(to_term(v))
                    } else if let Some(ref v) = condition.gte {
                        Bound::Included(to_term(v))
                    } else {
                        Bound::Unbounded
                    };

                    let upper = if let Some(ref v) = condition.lt {
                        Bound::Excluded(to_term(v))
                    } else if let Some(ref v) = condition.lte {
                        Bound::Included(to_term(v))
                    } else {
                        Bound::Unbounded
                    };

                    Ok(Box::new(RangeQuery::new(lower, upper)))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Wildcard(fields) => {
                use tantivy::query::RegexQuery;
                if let Some((field_name, value)) = fields.iter().next() {
                    let pattern = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // Convert OpenSearch wildcard syntax to regex:
                    // Escape regex special chars first, then convert * → .* and ? → .
                    let mut regex_pattern = String::new();
                    for ch in pattern.chars() {
                        match ch {
                            '*' => regex_pattern.push_str(".*"),
                            '?' => regex_pattern.push('.'),
                            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|'
                            | '\\' => {
                                regex_pattern.push('\\');
                                regex_pattern.push(ch);
                            }
                            _ => regex_pattern.push(ch),
                        }
                    }
                    let target_field = self.resolve_field(field_name);
                    let query = RegexQuery::from_pattern(&regex_pattern, target_field)
                        .map_err(|e| anyhow::anyhow!("Invalid wildcard pattern: {e}"))?;
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Prefix(fields) => {
                use tantivy::query::RegexQuery;
                if let Some((field_name, value)) = fields.iter().next() {
                    let prefix = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    // Escape the prefix for regex safety, then append .*
                    let mut escaped = String::new();
                    for ch in prefix.chars() {
                        match ch {
                            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^'
                            | '$' | '|' | '\\' => {
                                escaped.push('\\');
                                escaped.push(ch);
                            }
                            _ => escaped.push(ch),
                        }
                    }
                    let regex_pattern = format!("{escaped}.*");
                    let target_field = self.resolve_field(field_name);
                    let query = RegexQuery::from_pattern(&regex_pattern, target_field)
                        .map_err(|e| anyhow::anyhow!("Invalid prefix pattern: {e}"))?;
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
            QueryClause::Fuzzy(fields) => {
                use tantivy::query::FuzzyTermQuery;
                if let Some((field_name, params)) = fields.iter().next() {
                    let target_field = self.resolve_field(field_name);
                    let term = self.typed_term(
                        target_field,
                        &serde_json::Value::String(params.value.clone()),
                    );
                    let query = FuzzyTermQuery::new(term, params.fuzziness, true);
                    Ok(Box::new(query))
                } else {
                    Ok(Box::new(AllQuery))
                }
            }
        }
    }

    /// Flush with translog retention: commit to disk and truncate WAL entries
    /// only up to the given global checkpoint. Entries above the checkpoint
    /// are retained for replica recovery via translog replay.
    /// Returns the highest seq_no written to the WAL.
    pub fn sequence_stats(&self) -> SequenceStats {
        self.apply_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .checkpoints
            .stats()
    }

    #[cfg(feature = "protocol-trace")]
    pub(crate) fn protocol_trace_processed_sequences(&self) -> Result<Vec<u64>> {
        let state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        let Some(max_seq_no) = state.checkpoints.max_seq_no() else {
            return Ok(Vec::new());
        };
        Ok((0..=max_seq_no)
            .filter(|seq_no| state.checkpoints.has_processed(*seq_no))
            .collect())
    }

    #[cfg(feature = "protocol-trace")]
    pub(crate) fn protocol_trace_documents_snapshot(
        &self,
    ) -> Result<Vec<(String, serde_json::Value, u64, u64)>> {
        let mut documents = Vec::new();
        self.for_each_vector_rebuild_batch(|batch| {
            documents.extend(batch);
            Ok(())
        })?;
        Ok(documents)
    }

    #[cfg(feature = "protocol-trace")]
    pub(crate) fn protocol_trace_copy_evidence(&self) -> Result<super::ProtocolTraceCopyEvidence> {
        let live_documents = self.protocol_trace_documents_snapshot()?;
        let versions = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
            .versions
            .protocol_trace_versions()?;
        let mut actual = BTreeMap::new();
        for (doc, source, seq_no, term) in &live_documents {
            actual.insert(
                doc.clone(),
                crate::protocol_trace::TraceActualDocument {
                    doc: doc.clone(),
                    state: "live",
                    seq_no: Some(*seq_no),
                    term: Some(*term),
                    content_hash: Some(crate::protocol_trace::content_hash(
                        &super::DocumentMutation::Index {
                            doc_id: doc.clone(),
                            source: source.clone(),
                        },
                    )),
                },
            );
        }
        for (doc, version) in versions {
            match version {
                VersionValue::Index(version) => {
                    let observed = actual.get(&doc).with_context(|| {
                        format!(
                            "version map records live document [{doc}] that is absent from the refreshed reader"
                        )
                    })?;
                    if observed.seq_no != Some(version.seq_no)
                        || observed.term != Some(version.primary_term)
                    {
                        anyhow::bail!(
                            "version map identity for live document [{doc}] differs from the refreshed reader"
                        );
                    }
                }
                VersionValue::Delete(version) => {
                    if actual.contains_key(&doc) {
                        anyhow::bail!(
                            "version map records deleted document [{doc}] that remains in the refreshed reader"
                        );
                    }
                    actual.insert(
                        doc.clone(),
                        crate::protocol_trace::TraceActualDocument {
                            doc: doc.clone(),
                            state: "deleted",
                            seq_no: Some(version.seq_no),
                            term: Some(version.primary_term),
                            content_hash: Some(crate::protocol_trace::content_hash(
                                &super::DocumentMutation::Delete { doc_id: doc },
                            )),
                        },
                    );
                }
            }
        }
        let wal_entries = self.with_translog("protocol trace WAL evidence", |translog| {
            translog
                .read_all()?
                .into_iter()
                .map(|entry| {
                    let operation = sequenced_operation_from_entry(entry)?;
                    let (doc, op, content_hash) =
                        crate::protocol_trace::operation_parts(&operation);
                    Ok(crate::protocol_trace::TraceWalEntry {
                        seq_no: operation.seq_no,
                        term: operation.primary_term,
                        doc,
                        op,
                        content_hash,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok((live_documents, actual.into_values().collect(), wal_entries))
    }

    pub fn current_primary_term(&self) -> u64 {
        self.apply_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .term_sequences
            .current_term()
            .max(1)
    }

    pub(crate) fn for_each_vector_rebuild_batch(
        &self,
        mut consume: impl FnMut(Vec<(String, serde_json::Value, u64, u64)>) -> Result<()>,
    ) -> Result<()> {
        let searcher = self.reader.searcher();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let batch_size = vector_rebuild_batch_size();
        let mut batch = Vec::with_capacity(batch_size);
        for (segment_ord, segment) in searcher.segment_readers().iter().enumerate() {
            let seq_column = segment.fast_fields().u64(SEQ_NO_FIELD_NAME).ok();
            let term_column = segment.fast_fields().u64(PRIMARY_TERM_FIELD_NAME).ok();
            for doc_id in segment.doc_ids_alive() {
                let address = tantivy::DocAddress::new(segment_ord as u32, doc_id);
                let stored = searcher.doc::<TantivyDocument>(address)?;
                let document_id = stored
                    .get_all(registry.id_field)
                    .next()
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| anyhow::anyhow!("vector rebuild document has no _id"))?
                    .to_string();
                let source = stored
                    .get_all(registry.source_field)
                    .next()
                    .and_then(|value| value.as_str())
                    .and_then(|source| Self::decode_stored_source_with_registry(&registry, source))
                    .ok_or_else(|| {
                        anyhow::anyhow!("vector rebuild document has no valid _source")
                    })?;
                let (Some(seq_column), Some(term_column)) = (&seq_column, &term_column) else {
                    return Err(crate::common::unsupported_index_format(
                        "local shard Tantivy segment",
                        format!("document [{document_id}] is missing vector version fields"),
                    ));
                };
                let seq_no = seq_column.first(doc_id).ok_or_else(|| {
                    anyhow::Error::new(InternalSequenceFieldError {
                        message: format!("document [{document_id}] has no {SEQ_NO_FIELD_NAME}"),
                    })
                })?;
                let primary_term = term_column.first(doc_id).ok_or_else(|| {
                    anyhow::Error::new(InternalSequenceFieldError {
                        message: format!(
                            "document [{document_id}] has no {PRIMARY_TERM_FIELD_NAME}"
                        ),
                    })
                })?;
                batch.push((document_id, source, seq_no, primary_term));
                if batch.len() == batch_size {
                    consume(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(batch_size),
                    ))?;
                }
            }
        }
        if !batch.is_empty() {
            consume(batch)?;
        }
        Ok(())
    }

    pub fn missing_sequence_intervals_through(
        &self,
        end: u64,
    ) -> Vec<std::ops::RangeInclusive<u64>> {
        self.apply_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .checkpoints
            .missing_intervals_through(end)
    }

    pub(crate) fn update_local_checkpoint_compat(&self, seq_no: u64) {
        let mut state = self
            .apply_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.checkpoints.advance_max_seq_no(seq_no);
        state.checkpoints.mark_processed(seq_no);
    }

    pub fn wal_max_seq_no(&self) -> Option<u64> {
        self.with_translog_recover("wal_max_seq_no", |translog| translog.max_seq_no())
    }

    pub fn reconcile_term_sequence_state(
        &self,
        identity_fence: u64,
        identity_fence_max_seq_no: Option<u64>,
    ) -> Result<()> {
        let committed = self.load_committed_boundary()?;
        let mut state = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
        let current = state.term_sequences.to_record();
        if current.current_term != identity_fence
            || current.max_seq_no_at_term_start != identity_fence_max_seq_no
        {
            state.term_sequences = initialize_term_sequence_state(
                identity_fence,
                identity_fence_max_seq_no,
                &committed,
            )?;
        }
        *self
            .identity_term_state
            .lock()
            .map_err(|_| anyhow::anyhow!("identity term state lock poisoned"))? =
            Some((identity_fence, identity_fence_max_seq_no));
        Ok(())
    }

    pub fn last_seq_no(&self) -> u64 {
        self.with_translog_recover("last_seq_no", |tl| tl.last_seq_no())
    }

    /// Return the current on-disk size of the translog in bytes.
    pub fn translog_size_bytes(&self) -> u64 {
        self.with_translog_recover("translog_size_bytes", |tl| tl.size_bytes().unwrap_or(0))
    }

    /// Best-effort checkpoint-aware flush used by background auto-flush.
    /// Returns `Ok(false)` when a foreground write or another commit path is
    /// already holding the required locks, so ingestion is not blocked.
    pub fn try_flush_with_global_checkpoint(&self, global_checkpoint: u64) -> Result<bool> {
        let _maintenance = match self.maintenance_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                anyhow::bail!("maintenance lock poisoned during checkpoint-aware flush")
            }
        };
        let tl = match self.translog.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                anyhow::bail!("translog lock poisoned during checkpoint-aware flush")
            }
        };
        // Keep the existing writer-lock recovery policy here. A poisoned
        // translog lock makes the WAL retention boundary ambiguous; a poisoned
        // writer lock does not.
        let mut writer_state = match self.writer.try_write() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
        };
        if writer_state.writer.is_none() {
            return Ok(false);
        }
        let boundary = self.current_committed_boundary()?;
        let committed_boundary =
            self.commit_writer_at_boundary(&mut writer_state, "checkpoint-aware flush", boundary)?;
        drop(writer_state);
        self.reader.reload()?;
        self.persist_committed_boundary(&committed_boundary)?;
        self.validate_truncation_boundary(&*tl, &committed_boundary)?;
        if let Some(processed_checkpoint) = committed_boundary.processed_checkpoint {
            tl.truncate_below(global_checkpoint.min(processed_checkpoint))?;
        }
        Ok(true)
    }

    pub fn flush_with_global_checkpoint(&self, global_checkpoint: u64) -> Result<()> {
        let _maintenance = self.maintenance_guard("checkpoint-aware flush")?;
        self.with_translog("checkpoint-aware flush", |tl| {
            let mut writer_state = self.writer_state_with_replay(tl, "checkpoint-aware flush")?;
            let boundary = self.current_committed_boundary()?;
            let committed_boundary = self.commit_writer_at_boundary(
                &mut writer_state,
                "checkpoint-aware flush",
                boundary,
            )?;
            drop(writer_state);
            self.reader.reload()?;
            self.persist_committed_boundary(&committed_boundary)?;
            self.validate_truncation_boundary(tl, &committed_boundary)?;
            if let Some(processed_checkpoint) = committed_boundary.processed_checkpoint {
                tl.truncate_below(global_checkpoint.min(processed_checkpoint))?;
            }
            Ok(())
        })
    }

    pub fn flush_without_truncation(&self) -> Result<()> {
        let _maintenance = self.maintenance_guard("flush without truncation")?;
        self.with_translog("flush without truncation", |tl| {
            let mut writer_state = self.writer_state_with_replay(tl, "flush without truncation")?;
            let boundary = self.current_committed_boundary()?;
            let committed_boundary = self.commit_writer_at_boundary(
                &mut writer_state,
                "flush without truncation",
                boundary,
            )?;
            drop(writer_state);
            self.reader.reload()?;
            self.persist_committed_boundary(&committed_boundary)?;
            self.validate_truncation_boundary(tl, &committed_boundary)
        })
    }

    /// Extract the primary sort field and direction from a SearchRequest,
    /// if eligible for fast-field optimization (numeric FAST field, not _score).
    fn extract_fast_field_sort(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Option<(String, tantivy::Order)> {
        use crate::search::{SortClause, SortDirection, SortOrder};
        if req.sort.is_empty() {
            return None;
        }
        let clause = &req.sort[0];
        match clause {
            SortClause::Simple(name) if name != "_score" => {
                let field = self.resolve_field(name);
                let schema = self.index.schema();
                let entry = schema.get_field_entry(field);
                match entry.field_type() {
                    tantivy::schema::FieldType::I64(opts)
                    | tantivy::schema::FieldType::F64(opts)
                    | tantivy::schema::FieldType::U64(opts)
                        if opts.is_fast() =>
                    {
                        Some((name.clone(), tantivy::Order::Asc))
                    }
                    _ => None,
                }
            }
            SortClause::Field(map) => {
                if let Some((name, order)) = map.iter().next() {
                    if name == "_score" {
                        return None;
                    }
                    let field = self.resolve_field(name);
                    let schema = self.index.schema();
                    let entry = schema.get_field_entry(field);
                    let is_fast = match entry.field_type() {
                        tantivy::schema::FieldType::I64(opts)
                        | tantivy::schema::FieldType::F64(opts)
                        | tantivy::schema::FieldType::U64(opts) => opts.is_fast(),
                        _ => false,
                    };
                    if !is_fast {
                        return None;
                    }
                    let dir = match order {
                        SortOrder::Direction(d) => d.clone(),
                        SortOrder::Object { order } => order.clone(),
                    };
                    let tantivy_order = match dir {
                        SortDirection::Asc => tantivy::Order::Asc,
                        SortDirection::Desc => tantivy::Order::Desc,
                    };
                    Some((name.clone(), tantivy_order))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Collect hits from fast-field-sorted results (score is not meaningful).
    fn collect_hits_sorted<T>(
        &self,
        searcher: &tantivy::Searcher,
        top_docs: Vec<(T, tantivy::DocAddress)>,
    ) -> Result<Vec<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let mut results = Vec::new();
        for (_sort_value, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
            let doc_id = retrieved_doc
                .get_all(registry.id_field)
                .next()
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str()
                    && let Some(json_val) =
                        Self::decode_stored_source_with_registry(&registry, text)
                {
                    results.push(serde_json::json!({
                        "_id": doc_id,
                        "_score": 0.0,
                        "_source": json_val
                    }));
                }
            }
        }
        Ok(results)
    }

    /// Direct columnar scan for match_all + grouped_partials queries.
    /// Bypasses Tantivy's search/collector machinery entirely — iterates segment
    /// fast-field columns in batches of BATCH_SIZE without scoring or posting-list
    /// traversal. For single-column keyword GROUP BY, uses flat arrays indexed by
    /// ordinal instead of HashMap — eliminates hash computation, collision handling,
    /// and per-group heap allocation. Returns the same PartialAggResult as the collector path.
    fn grouped_partials_direct_scan(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<std::collections::HashMap<String, crate::search::PartialAggResult>> {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let segment_readers = searcher.segment_readers();

        // Extract shard_top_k from the first grouped metrics agg (if any).
        let shard_top_k: Option<&crate::search::ShardTopK> =
            req.aggs.values().find_map(|agg| match agg {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    params.shard_top_k.as_ref()
                }
                _ => None,
            });

        let specs: Vec<ResolvedGroupedAggSpec> = req
            .aggs
            .iter()
            .filter_map(|(name, agg)| match agg {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    Some(ResolvedGroupedAggSpec {
                        name: name.clone(),
                        group_by: params.group_by.clone(),
                        metrics: params
                            .metrics
                            .iter()
                            .map(|m| ResolvedGroupedMetricSpec {
                                output_name: m.output_name.clone(),
                                function: m.function.clone(),
                                field_name: m.field.clone(),
                                field_expr: m.field_expr.clone(),
                            })
                            .collect(),
                    })
                }
                _ => None,
            })
            .collect();

        // Parallel segment scan: each segment is scanned independently using scoped threads.
        // Uses std::thread::scope instead of rayon to avoid nested-pool deadlocks
        // (this method is already called from the search rayon pool).
        let all_segment_fruits: Vec<Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>> =
            std::thread::scope(|s| {
                let handles: Vec<_> = segment_readers
                    .iter()
                    .map(|segment_reader| {
                        let schema = &schema;
                        let specs = &specs;
                        s.spawn(move || {
                            let ff = segment_reader.fast_fields();
                            let max_doc = segment_reader.max_doc();
                            let cache_ctx = GroupedCacheContext {
                                column_cache: self.column_cache.as_ref(),
                                segment_id: segment_reader.segment_id(),
                                max_doc,
                                // Direct full-segment scans are the natural place to
                                // populate grouped-partials cache entries.
                                allow_populate: true,
                            };

                            let mut segment_results: Vec<(
                                String,
                                Vec<crate::search::GroupedMetricsBucket>,
                            )> = Vec::with_capacity(specs.len());

                            for spec in specs.iter() {
                                // Open key readers
                                let mut key_readers = Vec::with_capacity(spec.group_by.len());
                                let mut unsupported = false;
                                for field_name in &spec.group_by {
                                    match open_group_key_reader(
                                        schema,
                                        ff,
                                        field_name,
                                        Some(cache_ctx),
                                    ) {
                                        Some(reader) => key_readers.push(reader),
                                        None => {
                                            unsupported = true;
                                            break;
                                        }
                                    }
                                }
                                if unsupported {
                                    continue;
                                }

                                // Open metric readers + build layout
                                let mut metric_entries = Vec::with_capacity(spec.metrics.len());
                                for metric in &spec.metrics {
                                    let source = match metric.function {
                                        crate::search::GroupedMetricFunction::Count => {
                                            match &metric.field_name {
                                                None => GroupedMetricSource::CountAll,
                                                Some(field_name) => {
                                                    let reader = open_sql_field_reader(
                                                        schema, ff, field_name, None,
                                                    );
                                                    if matches!(
                                                        reader,
                                                        SqlFieldReader::SourceFallback
                                                    ) {
                                                        unsupported = true;
                                                        break;
                                                    }
                                                    GroupedMetricSource::CountField(reader)
                                                }
                                            }
                                        }
                                        crate::search::GroupedMetricFunction::Sum
                                        | crate::search::GroupedMetricFunction::Avg
                                        | crate::search::GroupedMetricFunction::Min
                                        | crate::search::GroupedMetricFunction::Max => {
                                            let Some(plan) = build_grouped_metric_plan(
                                                schema,
                                                ff,
                                                metric.field_name.as_deref(),
                                                metric.field_expr.as_ref(),
                                                Some(cache_ctx),
                                            ) else {
                                                unsupported = true;
                                                break;
                                            };
                                            GroupedMetricSource::Numeric(plan)
                                        }
                                    };
                                    if unsupported {
                                        break;
                                    }
                                    metric_entries.push(GroupedMetricEntry {
                                        output_name: metric.output_name.clone(),
                                        function: metric.function.clone(),
                                        source,
                                    });
                                }
                                if unsupported {
                                    continue;
                                }

                                // Determine if we can use flat array path:
                                // single-column keyword GROUP BY with known dictionary size < 2M
                                let use_flat = key_readers.len() == 1
                                    && matches!(key_readers[0], GroupKeyReader::Str(_))
                                    && key_readers[0].num_terms() < 2_000_000;

                                if use_flat {
                                    let num_groups = key_readers[0].num_terms();
                                    // Do NOT pass shard_top_k here — per-segment pruning
                                    // is incorrect because a group below the cutoff in every
                                    // segment can still be top-K after segment totals are
                                    // merged. Shard-level pruning happens after segment merge
                                    // at the end of grouped_partials_direct_scan.
                                    let buckets = flat_scan_segment(
                                        &key_readers[0],
                                        &metric_entries,
                                        max_doc,
                                        num_groups,
                                        None,
                                    );
                                    segment_results.push((spec.name.clone(), buckets));
                                } else {
                                    // Fallback: HashMap-based accumulation (multi-column or huge cardinality)
                                    let approx_groups =
                                        key_readers.first().map(|r| r.num_terms()).unwrap_or(256);
                                    let buckets = if key_readers.is_empty() {
                                        GroupedBuckets::Global(None)
                                    } else if key_readers.len() == 2 {
                                        let null_capacity =
                                            pair_null_bucket_capacity(approx_groups);
                                        GroupedBuckets::Pair(PairGroupedBuckets {
                                            values: PairHashMap::with_capacity_and_hasher(
                                                approx_groups,
                                                PairBuildHasher::default(),
                                            ),
                                            first_null: OrdHashMap::with_capacity_and_hasher(
                                                null_capacity,
                                                OrdBuildHasher::default(),
                                            ),
                                            second_null: OrdHashMap::with_capacity_and_hasher(
                                                null_capacity,
                                                OrdBuildHasher::default(),
                                            ),
                                            both_null: None,
                                        })
                                    } else if key_readers.len() > 2 {
                                        GroupedBuckets::Multi(
                                            std::collections::HashMap::with_capacity(approx_groups),
                                        )
                                    } else {
                                        GroupedBuckets::Single(SingleGroupedBuckets {
                                            values: OrdHashMap::with_capacity_and_hasher(
                                                approx_groups,
                                                OrdBuildHasher::default(),
                                            ),
                                            null_bucket: None,
                                        })
                                    };

                                    let mut numeric_buf_map: Vec<Option<usize>> =
                                        Vec::with_capacity(metric_entries.len());
                                    let mut num_numeric = 0usize;
                                    for me in &metric_entries {
                                        match &me.source {
                                            GroupedMetricSource::Numeric(plan) => {
                                                numeric_buf_map.push(Some(num_numeric));
                                                num_numeric += plan.leaf_count();
                                            }
                                            _ => {
                                                numeric_buf_map.push(None);
                                            }
                                        }
                                    }
                                    let mut accum_template =
                                        Vec::with_capacity(metric_entries.len());
                                    for me in &metric_entries {
                                        accum_template.push(match &me.source {
                                            GroupedMetricSource::CountAll
                                            | GroupedMetricSource::CountField(_) => {
                                                CompactMetricAccum::Count(0)
                                            }
                                            GroupedMetricSource::Numeric(_) => {
                                                CompactMetricAccum::Stats {
                                                    count: 0,
                                                    sum: 0.0,
                                                    min: f64::INFINITY,
                                                    max: f64::NEG_INFINITY,
                                                }
                                            }
                                        });
                                    }
                                    let numeric_buffers: Vec<Vec<Option<f64>>> =
                                        (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();

                                    let mut entry = GroupedAggSegmentEntry {
                                        key_readers,
                                        metric_entries,
                                        buckets,
                                        accum_template,
                                        doc_buffer: Vec::with_capacity(BATCH_SIZE),
                                        ord_buffer: vec![None; BATCH_SIZE],
                                        numeric_buffers,
                                        numeric_buf_map,
                                    };

                                    // Batched accumulation: buffer docs in chunks of BATCH_SIZE,
                                    // batch-read ordinals and numerics, then accumulate.
                                    // Avoids per-doc fast-field reads and Vec allocations.
                                    for doc in 0..max_doc {
                                        entry.doc_buffer.push(doc);
                                        if entry.doc_buffer.len() >= BATCH_SIZE {
                                            match &entry.buckets {
                                                GroupedBuckets::Pair(_) => {
                                                    flush_batch_pair(&mut entry)
                                                }
                                                GroupedBuckets::Multi(_)
                                                | GroupedBuckets::Global(_) => {
                                                    flush_batch_multi(&mut entry)
                                                }
                                                GroupedBuckets::Single(_) => {
                                                    flush_batch(&mut entry)
                                                }
                                            }
                                        }
                                    }
                                    if !entry.doc_buffer.is_empty() {
                                        match &entry.buckets {
                                            GroupedBuckets::Pair(_) => flush_batch_pair(&mut entry),
                                            GroupedBuckets::Multi(_)
                                            | GroupedBuckets::Global(_) => {
                                                flush_batch_multi(&mut entry)
                                            }
                                            GroupedBuckets::Single(_) => flush_batch(&mut entry),
                                        }
                                    }

                                    let raw_buckets = grouped_buckets_into_raw(entry.buckets);
                                    let resolved: Vec<crate::search::GroupedMetricsBucket> =
                                        raw_buckets
                                            .into_iter()
                                            .map(|b| {
                                                resolve_bucket(
                                                    b,
                                                    &entry.key_readers,
                                                    &entry.metric_entries,
                                                )
                                            })
                                            .collect();
                                    segment_results.push((spec.name.clone(), resolved));
                                }
                            }
                            segment_results
                        })
                    })
                    .collect();
                join_scoped_handles(handles, "segment scan")
            })?;

        // Merge across segments
        let mut merged: std::collections::HashMap<
            String,
            std::collections::HashMap<String, crate::search::GroupedMetricsBucket>,
        > = std::collections::HashMap::new();

        for fruit in all_segment_fruits {
            for (name, buckets) in fruit {
                let agg_buckets = merged.entry(name).or_default();
                for bucket in buckets {
                    let key = compact_group_key(&bucket.group_values);
                    let target = agg_buckets.entry(key).or_insert_with(|| {
                        crate::search::GroupedMetricsBucket {
                            group_values: bucket.group_values.clone(),
                            metrics: std::collections::HashMap::new(),
                        }
                    });
                    merge_grouped_bucket_metrics(target, &bucket);
                }
            }
        }

        let mut results = std::collections::HashMap::new();
        for spec in &specs {
            let mut buckets: Vec<_> = merged
                .remove(&spec.name)
                .unwrap_or_default()
                .into_values()
                .collect();

            // Shard-level top-K pruning after segment merge.
            if let Some(top_k) = shard_top_k {
                apply_shard_top_k(&mut buckets, top_k);
            }

            results.insert(
                spec.name.clone(),
                crate::search::PartialAggResult::GroupedMetrics { buckets },
            );
        }
        Ok(results)
    }
}

// -- Single-pass Aggregation Collector --
// Implements tantivy::collector::Collector to compute aggregations in the same
// pass as TopDocs hit collection, mirroring OpenSearch's aggregation architecture.

enum NumCol {
    F64(tantivy::columnar::Column<f64>),
    I64(tantivy::columnar::Column<i64>),
    CachedF64(std::sync::Arc<[Option<f64>]>),
    CachedI64(std::sync::Arc<[Option<i64>]>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NumericTermKey {
    Integer(i64),
    Float(u64),
}

impl NumericTermKey {
    fn float(value: f64) -> Self {
        let value = if value == 0.0 {
            0.0
        } else if value.is_nan() {
            f64::NAN
        } else {
            value
        };
        Self::Float(value.to_bits())
    }

    fn into_string(self) -> String {
        match self {
            Self::Integer(value) => value.to_string(),
            Self::Float(bits) => f64::from_bits(bits).to_string(),
        }
    }
}

impl NumCol {
    #[inline]
    fn first_term_key(&self, doc: u32) -> Option<NumericTermKey> {
        match self {
            Self::F64(column) => column.first(doc).map(NumericTermKey::float),
            Self::I64(column) => column.first(doc).map(NumericTermKey::Integer),
            Self::CachedF64(values) => values
                .get(doc as usize)
                .copied()
                .flatten()
                .map(NumericTermKey::float),
            Self::CachedI64(values) => values
                .get(doc as usize)
                .copied()
                .flatten()
                .map(NumericTermKey::Integer),
        }
    }

    #[inline]
    fn first_f64(&self, doc: u32) -> Option<f64> {
        match self {
            NumCol::F64(c) => c.first(doc),
            NumCol::I64(c) => c.first(doc).map(|v| v as f64),
            NumCol::CachedF64(values) => values.get(doc as usize).copied().flatten(),
            NumCol::CachedI64(values) => values
                .get(doc as usize)
                .copied()
                .flatten()
                .map(|v| v as f64),
        }
    }

    /// Batch-read f64 values for a slice of doc IDs.
    /// Much faster than per-doc `first_f64()` due to sequential memory access patterns.
    #[inline]
    fn first_vals_f64(&self, docs: &[tantivy::DocId], output: &mut [Option<f64>]) {
        match self {
            NumCol::F64(col) => {
                col.first_vals(docs, output);
            }
            NumCol::I64(col) => {
                let mut i64_buf: Vec<Option<i64>> = vec![None; docs.len()];
                col.first_vals(docs, &mut i64_buf);
                for (i, val) in i64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v as f64);
                }
            }
            NumCol::CachedF64(values) => {
                for (i, doc) in docs.iter().enumerate() {
                    output[i] = values.get(*doc as usize).copied().flatten();
                }
            }
            NumCol::CachedI64(values) => {
                for (i, doc) in docs.iter().enumerate() {
                    output[i] = values
                        .get(*doc as usize)
                        .copied()
                        .flatten()
                        .map(|value| value as f64);
                }
            }
        }
    }
}

const GROUPED_CACHE_BUILD_BATCH_SIZE: usize = 4096;

#[derive(Clone, Copy)]
struct GroupedCacheContext<'a> {
    column_cache: &'a super::column_cache::ColumnCache,
    segment_id: tantivy::index::SegmentId,
    max_doc: u32,
    allow_populate: bool,
}

#[derive(Clone, Copy)]
enum GroupedCacheKind {
    F64,
    I64,
    StrOrds,
}

fn estimate_full_grouped_column_bytes(kind: GroupedCacheKind, max_doc: u32) -> u64 {
    let doc_count = max_doc as u64;
    match kind {
        GroupedCacheKind::F64 => {
            doc_count.saturating_mul(std::mem::size_of::<Option<f64>>() as u64)
        }
        GroupedCacheKind::I64 => {
            doc_count.saturating_mul(std::mem::size_of::<Option<i64>>() as u64)
        }
        GroupedCacheKind::StrOrds => {
            doc_count.saturating_mul(std::mem::size_of::<Option<u64>>() as u64)
        }
    }
}

fn should_cache_full_grouped_column(kind: GroupedCacheKind, max_doc: u32, cache_max: u64) -> bool {
    cache_max > 0 && estimate_full_grouped_column_bytes(kind, max_doc) <= cache_max / 4
}

fn build_grouped_cached_f64_values(
    col: &tantivy::columnar::Column<f64>,
    max_doc: u32,
) -> std::sync::Arc<[Option<f64>]> {
    let mut values = vec![None; max_doc as usize];
    let mut docs = Vec::with_capacity(GROUPED_CACHE_BUILD_BATCH_SIZE);
    let mut start = 0u32;
    while start < max_doc {
        let end = (start + GROUPED_CACHE_BUILD_BATCH_SIZE as u32).min(max_doc);
        docs.clear();
        docs.extend(start..end);
        col.first_vals(&docs, &mut values[start as usize..end as usize]);
        start = end;
    }
    values.into()
}

fn build_grouped_cached_i64_values(
    col: &tantivy::columnar::Column<i64>,
    max_doc: u32,
) -> std::sync::Arc<[Option<i64>]> {
    let mut values = vec![None; max_doc as usize];
    let mut docs = Vec::with_capacity(GROUPED_CACHE_BUILD_BATCH_SIZE);
    let mut start = 0u32;
    while start < max_doc {
        let end = (start + GROUPED_CACHE_BUILD_BATCH_SIZE as u32).min(max_doc);
        docs.clear();
        docs.extend(start..end);
        col.first_vals(&docs, &mut values[start as usize..end as usize]);
        start = end;
    }
    values.into()
}

fn build_grouped_cached_string_ords(
    reader: &StringFastFieldReader,
    max_doc: u32,
) -> std::sync::Arc<[Option<u64>]> {
    let mut ords = vec![None; max_doc as usize];
    let mut docs = Vec::with_capacity(GROUPED_CACHE_BUILD_BATCH_SIZE);
    let mut start = 0u32;
    while start < max_doc {
        let end = (start + GROUPED_CACHE_BUILD_BATCH_SIZE as u32).min(max_doc);
        docs.clear();
        docs.extend(start..end);
        reader.first_ords_batch(&docs, &mut ords[start as usize..end as usize]);
        start = end;
    }
    ords.into()
}

fn get_or_build_cached_grouped_f64(
    cache_ctx: Option<GroupedCacheContext<'_>>,
    column_name: &str,
    col: &tantivy::columnar::Column<f64>,
) -> Option<std::sync::Arc<[Option<f64>]>> {
    let cache_ctx = cache_ctx?;
    if let Some(super::column_cache::GroupedColumnCache::F64(values)) = cache_ctx
        .column_cache
        .get_grouped(cache_ctx.segment_id, column_name)
    {
        return Some(values);
    }
    if !cache_ctx.allow_populate {
        return None;
    }
    let cache_max = cache_ctx.column_cache.max_capacity();
    if !should_cache_full_grouped_column(GroupedCacheKind::F64, cache_ctx.max_doc, cache_max)
        || !cache_ctx
            .column_cache
            .should_populate(cache_ctx.max_doc as usize, cache_ctx.max_doc)
    {
        return None;
    }

    let values = build_grouped_cached_f64_values(col, cache_ctx.max_doc);
    cache_ctx.column_cache.insert_grouped(
        cache_ctx.segment_id,
        column_name,
        super::column_cache::GroupedColumnCache::F64(values.clone()),
    );
    Some(values)
}

fn get_or_build_cached_grouped_i64(
    cache_ctx: Option<GroupedCacheContext<'_>>,
    column_name: &str,
    col: &tantivy::columnar::Column<i64>,
) -> Option<std::sync::Arc<[Option<i64>]>> {
    let cache_ctx = cache_ctx?;
    if let Some(super::column_cache::GroupedColumnCache::I64(values)) = cache_ctx
        .column_cache
        .get_grouped(cache_ctx.segment_id, column_name)
    {
        return Some(values);
    }
    if !cache_ctx.allow_populate {
        return None;
    }
    let cache_max = cache_ctx.column_cache.max_capacity();
    if !should_cache_full_grouped_column(GroupedCacheKind::I64, cache_ctx.max_doc, cache_max)
        || !cache_ctx
            .column_cache
            .should_populate(cache_ctx.max_doc as usize, cache_ctx.max_doc)
    {
        return None;
    }

    let values = build_grouped_cached_i64_values(col, cache_ctx.max_doc);
    cache_ctx.column_cache.insert_grouped(
        cache_ctx.segment_id,
        column_name,
        super::column_cache::GroupedColumnCache::I64(values.clone()),
    );
    Some(values)
}

fn get_or_build_cached_grouped_string_ords(
    cache_ctx: Option<GroupedCacheContext<'_>>,
    column_name: &str,
    reader: &StringFastFieldReader,
) -> Option<std::sync::Arc<[Option<u64>]>> {
    let cache_ctx = cache_ctx?;
    if let Some(super::column_cache::GroupedColumnCache::StrOrds(values)) = cache_ctx
        .column_cache
        .get_grouped(cache_ctx.segment_id, column_name)
    {
        return Some(values);
    }
    if !cache_ctx.allow_populate {
        return None;
    }
    let cache_max = cache_ctx.column_cache.max_capacity();
    if !should_cache_full_grouped_column(GroupedCacheKind::StrOrds, cache_ctx.max_doc, cache_max)
        || !cache_ctx
            .column_cache
            .should_populate(cache_ctx.max_doc as usize, cache_ctx.max_doc)
    {
        return None;
    }

    let values = build_grouped_cached_string_ords(reader, cache_ctx.max_doc);
    cache_ctx.column_cache.insert_grouped(
        cache_ctx.segment_id,
        column_name,
        super::column_cache::GroupedColumnCache::StrOrds(values.clone()),
    );
    Some(values)
}

fn open_num_col(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
    cache_ctx: Option<GroupedCacheContext<'_>>,
) -> Option<NumCol> {
    let field = schema.get_field(field_name).ok()?;
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => {
            let col = fast_fields.f64(entry.name()).ok()?;
            if let Some(values) = get_or_build_cached_grouped_f64(cache_ctx, entry.name(), &col) {
                Some(NumCol::CachedF64(values))
            } else {
                Some(NumCol::F64(col))
            }
        }
        tantivy::schema::FieldType::I64(_) => {
            let col = fast_fields.i64(entry.name()).ok()?;
            if let Some(values) = get_or_build_cached_grouped_i64(cache_ctx, entry.name(), &col) {
                Some(NumCol::CachedI64(values))
            } else {
                Some(NumCol::I64(col))
            }
        }
        tantivy::schema::FieldType::U64(_) => {
            let col = fast_fields.i64(entry.name()).ok()?;
            if let Some(values) = get_or_build_cached_grouped_i64(cache_ctx, entry.name(), &col) {
                Some(NumCol::CachedI64(values))
            } else {
                Some(NumCol::I64(col))
            }
        }
        _ => None,
    }
}

fn build_grouped_metric_plan(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: Option<&str>,
    field_expr: Option<&crate::search::MetricFieldExpr>,
    cache_ctx: Option<GroupedCacheContext<'_>>,
) -> Option<GroupedMetricExprPlan> {
    let expr = match (field_name, field_expr) {
        (_, Some(expr)) => expr,
        (Some(field_name), None) => {
            return Some(GroupedMetricExprPlan {
                leaves: vec![open_num_col(schema, fast_fields, field_name, cache_ctx)?],
                expr: MetricEvalExpr::Leaf(0),
            });
        }
        (None, None) => return None,
    };

    let mut leaves = Vec::new();
    let compiled = compile_grouped_metric_expr(schema, fast_fields, expr, &mut leaves, cache_ctx)?;
    Some(GroupedMetricExprPlan {
        leaves,
        expr: compiled,
    })
}

fn compile_grouped_metric_expr(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    expr: &crate::search::MetricFieldExpr,
    leaves: &mut Vec<NumCol>,
    cache_ctx: Option<GroupedCacheContext<'_>>,
) -> Option<MetricEvalExpr> {
    match expr {
        crate::search::MetricFieldExpr::Field { name } => {
            let index = leaves.len();
            leaves.push(open_num_col(schema, fast_fields, name, cache_ctx)?);
            Some(MetricEvalExpr::Leaf(index))
        }
        crate::search::MetricFieldExpr::Binary { left, op, right } => {
            let left = compile_grouped_metric_expr(schema, fast_fields, left, leaves, cache_ctx)?;
            let right = compile_grouped_metric_expr(schema, fast_fields, right, leaves, cache_ctx)?;
            Some(MetricEvalExpr::Binary {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
            })
        }
    }
}

/// Identity hasher for u64 ordinal keys — ordinals are already well-distributed
/// dictionary indices, so hashing them is wasted work. This eliminates hash
/// computation overhead in the per-doc GROUP BY hot path.
#[derive(Default)]
struct OrdHasher(u64);

impl std::hash::Hasher for OrdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _bytes: &[u8]) {}
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type OrdBuildHasher = std::hash::BuildHasherDefault<OrdHasher>;
type OrdHashMap<V> = std::collections::HashMap<u64, V, OrdBuildHasher>;

#[inline]
fn mix_pair_hash_value(value: u128) -> u64 {
    let hi = (value >> 64) as u64;
    let lo = value as u64;
    let mut mixed = 0x9E37_79B9_7F4A_7C15u64;
    mixed ^= hi.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    mixed = mixed.rotate_left(32);
    mixed ^= lo.wrapping_mul(0x1656_67B1_9E37_79F9);
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    mixed ^= mixed >> 33;
    mixed
}

/// Cheap hasher for packed `(u64, u64)` keys used by the width-2 GROUP BY path.
/// Route-like workloads have structured pairs, so we still mix the halves, but
/// avoid the default SipHash cost in the per-doc hot path.
#[derive(Default)]
struct PairHasher(u64);

impl std::hash::Hasher for PairHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() <= 16 {
            let mut chunk = [0u8; 16];
            chunk[..bytes.len()].copy_from_slice(bytes);
            self.0 = mix_pair_hash_value(u128::from_le_bytes(chunk) ^ (bytes.len() as u128));
            return;
        }

        // The hot path hashes packed u128 keys via `write_u128()`. Fall back to
        // a complete byte hash here so unexpected callers do not silently ignore
        // bytes beyond the first 16.
        let mut fallback = std::hash::DefaultHasher::new();
        fallback.write(bytes);
        self.0 = fallback.finish();
    }

    fn write_u128(&mut self, value: u128) {
        self.0 = mix_pair_hash_value(value);
    }
}

type PairBuildHasher = std::hash::BuildHasherDefault<PairHasher>;
type PairHashMap<V> = std::collections::HashMap<u128, V, PairBuildHasher>;

#[inline]
fn pair_null_bucket_capacity(approx_groups: usize) -> usize {
    approx_groups.min(16)
}

#[derive(Clone)]
struct StringFastFieldReader {
    str_col: tantivy::columnar::StrColumn,
    ord_col: tantivy::columnar::Column<u64>,
    cached_ords: Option<std::sync::Arc<[Option<u64>]>>,
}

impl StringFastFieldReader {
    fn open(fast_fields: &tantivy::fastfield::FastFieldReaders, field_name: &str) -> Option<Self> {
        let str_col = fast_fields.str(field_name).ok().flatten()?;
        let ord_col = str_col.ords().clone();
        Some(Self {
            str_col,
            ord_col,
            cached_ords: None,
        })
    }

    fn with_cached_ords(mut self, cached_ords: std::sync::Arc<[Option<u64>]>) -> Self {
        self.cached_ords = Some(cached_ords);
        self
    }

    #[inline]
    fn first_ord(&self, doc: tantivy::DocId) -> Option<u64> {
        if let Some(cached) = &self.cached_ords {
            cached.get(doc as usize).copied().flatten()
        } else {
            self.ord_col.first(doc)
        }
    }

    #[inline]
    fn first_ords_batch(&self, docs: &[tantivy::DocId], output: &mut [Option<u64>]) {
        if let Some(cached) = &self.cached_ords {
            for (index, doc) in docs.iter().enumerate() {
                output[index] = cached.get(*doc as usize).copied().flatten();
            }
        } else {
            self.ord_col.first_vals(docs, output);
        }
    }

    #[inline]
    fn first_text(&self, doc: tantivy::DocId, buf: &mut String) -> bool {
        let Some(ord) = self.first_ord(doc) else {
            return false;
        };
        buf.clear();
        self.ord_to_str(ord, buf)
    }

    #[inline]
    fn ord_to_str(&self, ord: u64, buf: &mut String) -> bool {
        self.str_col.ord_to_str(ord, buf).unwrap_or(false)
    }

    fn num_terms(&self) -> usize {
        self.str_col.num_terms()
    }
}

enum SqlFieldReader {
    F64(tantivy::columnar::Column<f64>),
    I64(tantivy::columnar::Column<i64>),
    DateMillis(tantivy::columnar::Column<i64>),
    Str(StringFastFieldReader),
    SourceFallback,
}

impl SqlFieldReader {
    fn type_name(&self) -> &'static str {
        match self {
            SqlFieldReader::F64(_) => "F64",
            SqlFieldReader::I64(_) => "I64",
            SqlFieldReader::DateMillis(_) => "DateMillis",
            SqlFieldReader::Str(_) => "Str",
            SqlFieldReader::SourceFallback => "SourceFallback",
        }
    }

    /// Read the first value for a doc as JSON. Used for count(field) null checks.
    #[allow(dead_code)]
    fn first_json(&self, doc: tantivy::DocId) -> serde_json::Value {
        match self {
            SqlFieldReader::F64(reader) => reader
                .first(doc)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            SqlFieldReader::I64(reader) => reader
                .first(doc)
                .map(serde_json::Value::from)
                .unwrap_or(serde_json::Value::Null),
            SqlFieldReader::DateMillis(reader) => reader
                .first(doc)
                .map(crate::common::date::epoch_millis_to_iso8601)
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
            SqlFieldReader::Str(reader) => {
                let mut text = String::new();
                if reader.first_text(doc, &mut text) {
                    serde_json::Value::String(text)
                } else {
                    serde_json::Value::Null
                }
            }
            SqlFieldReader::SourceFallback => serde_json::Value::Null,
        }
    }
}

fn open_sql_field_reader(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
    logical_field_type: Option<&crate::cluster::state::FieldType>,
) -> SqlFieldReader {
    let Ok(field) = schema.get_field(field_name) else {
        return SqlFieldReader::SourceFallback;
    };
    let entry = schema.get_field_entry(field);
    if matches!(
        logical_field_type,
        Some(crate::cluster::state::FieldType::Date)
    ) {
        return fast_fields
            .i64(entry.name())
            .map(SqlFieldReader::DateMillis)
            .unwrap_or(SqlFieldReader::SourceFallback);
    }
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => fast_fields
            .f64(entry.name())
            .map(SqlFieldReader::F64)
            .unwrap_or(SqlFieldReader::SourceFallback),
        tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => fast_fields
            .i64(entry.name())
            .map(SqlFieldReader::I64)
            .unwrap_or(SqlFieldReader::SourceFallback),
        _ => StringFastFieldReader::open(fast_fields, entry.name())
            .map(SqlFieldReader::Str)
            .unwrap_or(SqlFieldReader::SourceFallback),
    }
}

fn timestamp_array_from_values(
    values: Vec<Option<i64>>,
) -> datafusion::arrow::array::TimestampMillisecondArray {
    datafusion::arrow::array::TimestampMillisecondArray::from(values)
        .with_timezone("UTC".to_string())
}

/// Build a full-segment Arrow array from a fast-field reader.
/// Reads every doc in the segment (0..max_doc) to produce a complete column.
fn build_full_segment_array(
    reader: &SqlFieldReader,
    max_doc: u32,
) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
    use std::sync::Arc;

    match reader {
        SqlFieldReader::F64(col) => {
            let mut builder = Float64Builder::with_capacity(max_doc as usize);
            for doc in 0..max_doc {
                match col.first(doc) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::I64(col) => {
            let mut builder = Int64Builder::with_capacity(max_doc as usize);
            for doc in 0..max_doc {
                match col.first(doc) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::DateMillis(col) => {
            let mut values = Vec::with_capacity(max_doc as usize);
            for doc in 0..max_doc {
                values.push(col.first(doc));
            }
            Arc::new(timestamp_array_from_values(values))
        }
        SqlFieldReader::Str(col) => {
            let mut builder = StringBuilder::with_capacity(max_doc as usize, 0);
            let mut buf = String::new();
            for doc in 0..max_doc {
                if col.first_text(doc, &mut buf) {
                    builder.append_value(&buf);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::SourceFallback => {
            // Should never be called for SourceFallback — pre-checked by caller
            Arc::new(datafusion::arrow::array::StringBuilder::new().finish())
        }
    }
}

fn should_cache_full_segment_array(reader: &SqlFieldReader, max_doc: u32, cache_max: u64) -> bool {
    cache_max > 0 && estimate_full_segment_array_bytes(reader, max_doc) <= cache_max / 4
}

fn estimate_full_segment_array_bytes(reader: &SqlFieldReader, max_doc: u32) -> u64 {
    let doc_count = u64::from(max_doc);
    let null_bitmap_bytes = doc_count.saturating_add(7) / 8;

    match reader {
        SqlFieldReader::F64(_) | SqlFieldReader::I64(_) | SqlFieldReader::DateMillis(_) => {
            doc_count
                .saturating_mul(std::mem::size_of::<f64>() as u64)
                .saturating_add(null_bitmap_bytes)
        }
        SqlFieldReader::Str(col) => {
            let offset_bytes = doc_count
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<i32>() as u64);
            let avg_term_len = estimate_string_array_value_bytes(col);
            offset_bytes
                .saturating_add(null_bitmap_bytes)
                .saturating_add(doc_count.saturating_mul(avg_term_len))
        }
        SqlFieldReader::SourceFallback => 0,
    }
}

fn estimate_string_array_value_bytes(col: &StringFastFieldReader) -> u64 {
    const MAX_SAMPLES: usize = 32;

    let num_terms = col.num_terms();
    if num_terms == 0 {
        return 16;
    }

    let sample_count = num_terms.min(MAX_SAMPLES);
    let step = (num_terms.saturating_add(sample_count - 1) / sample_count).max(1);
    let mut total_len = 0u64;
    let mut sampled = 0u64;
    let mut buf = String::new();
    let mut ord = 0usize;

    while ord < num_terms && sampled < sample_count as u64 {
        buf.clear();
        if col.ord_to_str(ord as u64, &mut buf) {
            total_len = total_len.saturating_add(buf.len() as u64);
            sampled += 1;
        }
        ord = ord.saturating_add(step);
    }

    if sampled == 0 {
        return 16;
    }

    // 2× safety margin: dictionary term lengths don't account for multi-valued docs,
    // null bitmap overhead, or Arrow offset array padding. Over-estimating is safe
    // (it just falls back to build_selective_array), under-estimating causes a large
    // allocation that gets rejected by the cache insert guard.
    total_len.saturating_mul(2).saturating_div(sampled).max(16)
}

/// Build an Arrow array containing only the values at specific doc IDs.
/// Used when the segment is too large to cache the full column.
fn build_selective_array(
    reader: &SqlFieldReader,
    docs: &[(u32, f32)],
) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{Float64Builder, Int64Builder, StringBuilder};
    use std::sync::Arc;

    match reader {
        SqlFieldReader::F64(col) => {
            let mut builder = Float64Builder::with_capacity(docs.len());
            for (doc_id, _) in docs {
                match col.first(*doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::I64(col) => {
            let mut builder = Int64Builder::with_capacity(docs.len());
            for (doc_id, _) in docs {
                match col.first(*doc_id) {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::DateMillis(col) => {
            let mut values = Vec::with_capacity(docs.len());
            for (doc_id, _) in docs {
                values.push(col.first(*doc_id));
            }
            Arc::new(timestamp_array_from_values(values))
        }
        SqlFieldReader::Str(col) => {
            let mut builder = StringBuilder::with_capacity(docs.len(), docs.len() * 16);
            let doc_ids: Vec<tantivy::DocId> = docs.iter().map(|(doc_id, _)| *doc_id).collect();
            let mut ords = vec![None; docs.len()];
            col.first_ords_batch(&doc_ids, &mut ords);
            let mut buf = String::new();
            for ord in ords {
                if let Some(ord) = ord {
                    buf.clear();
                    if col.ord_to_str(ord, &mut buf) {
                        builder.append_value(&buf);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish())
        }
        SqlFieldReader::SourceFallback => Arc::new(StringBuilder::new().finish()),
    }
}

fn build_projected_fast_field_array(
    column_cache: &super::column_cache::ColumnCache,
    segment_id: tantivy::index::SegmentId,
    max_doc: u32,
    column_name: &str,
    reader: &SqlFieldReader,
    docs: &[(u32, f32)],
) -> Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::UInt32Array;

    let cache_max = column_cache.max_capacity();
    let use_segment_cache = should_cache_full_segment_array(reader, max_doc, cache_max);

    if use_segment_cache {
        let full_array = if let Some(cached) = column_cache.get(segment_id, column_name) {
            cached
        } else if column_cache.should_populate(docs.len(), max_doc) {
            let array = build_full_segment_array(reader, max_doc);
            column_cache.insert(segment_id, column_name, array.clone());
            array
        } else {
            return Ok(build_selective_array(reader, docs));
        };

        let indices = UInt32Array::from(docs.iter().map(|(d, _)| *d).collect::<Vec<u32>>());
        return Ok(datafusion::arrow::compute::take(
            &full_array,
            &indices,
            None,
        )?);
    }

    Ok(build_selective_array(reader, docs))
}

fn column_kind_for_column(
    schema: &Schema,
    logical_field_type: Option<&crate::cluster::state::FieldType>,
    column: &str,
) -> crate::hybrid::arrow_bridge::ColumnKind {
    if matches!(
        logical_field_type,
        Some(crate::cluster::state::FieldType::Date)
    ) {
        return crate::hybrid::arrow_bridge::ColumnKind::TimestampMillis;
    }

    if let Ok(field) = schema.get_field(column) {
        match schema.get_field_entry(field).field_type() {
            tantivy::schema::FieldType::F64(_) => crate::hybrid::arrow_bridge::ColumnKind::Float64,
            tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {
                crate::hybrid::arrow_bridge::ColumnKind::Int64
            }
            _ => crate::hybrid::arrow_bridge::ColumnKind::Utf8,
        }
    } else {
        crate::hybrid::arrow_bridge::ColumnKind::Utf8
    }
}

/// Create an empty Arrow array with the correct type.
fn empty_typed_array(
    kind: crate::hybrid::arrow_bridge::ColumnKind,
) -> datafusion::arrow::array::ArrayRef {
    use std::sync::Arc;
    match kind {
        crate::hybrid::arrow_bridge::ColumnKind::Float64 => Arc::new(
            datafusion::arrow::array::Float64Array::from(Vec::<f64>::new()),
        ),
        crate::hybrid::arrow_bridge::ColumnKind::Int64 => {
            Arc::new(datafusion::arrow::array::Int64Array::from(Vec::<i64>::new()))
        }
        crate::hybrid::arrow_bridge::ColumnKind::TimestampMillis => {
            Arc::new(timestamp_array_from_values(Vec::new()))
        }
        crate::hybrid::arrow_bridge::ColumnKind::Boolean => Arc::new(
            datafusion::arrow::array::BooleanArray::from(Vec::<bool>::new()),
        ),
        crate::hybrid::arrow_bridge::ColumnKind::Utf8 => Arc::new(
            datafusion::arrow::array::StringArray::from(Vec::<&str>::new()),
        ),
    }
}

fn open_group_key_reader(
    schema: &Schema,
    fast_fields: &tantivy::fastfield::FastFieldReaders,
    field_name: &str,
    cache_ctx: Option<GroupedCacheContext<'_>>,
) -> Option<GroupKeyReader> {
    if let Some(spec) = crate::search::decode_derived_group_key(field_name) {
        let base = StringFastFieldReader::open(fast_fields, &spec.source_field)?;
        let base = if let Some(cached_ords) =
            get_or_build_cached_grouped_string_ords(cache_ctx, &spec.source_field, &base)
        {
            base.with_cached_ords(cached_ords)
        } else {
            base
        };
        return Some(GroupKeyReader::DerivedStr(DerivedStringBucketReader::new(
            base, spec,
        )));
    }

    let field = schema.get_field(field_name).ok()?;
    let entry = schema.get_field_entry(field);
    match entry.field_type() {
        tantivy::schema::FieldType::F64(_) => {
            let col = fast_fields.f64(entry.name()).ok()?;
            if let Some(values) = get_or_build_cached_grouped_f64(cache_ctx, entry.name(), &col) {
                Some(GroupKeyReader::CachedF64(values))
            } else {
                Some(GroupKeyReader::F64(col))
            }
        }
        tantivy::schema::FieldType::I64(_) | tantivy::schema::FieldType::U64(_) => {
            let col = fast_fields.i64(entry.name()).ok()?;
            if let Some(values) = get_or_build_cached_grouped_i64(cache_ctx, entry.name(), &col) {
                Some(GroupKeyReader::CachedI64(values))
            } else {
                Some(GroupKeyReader::I64(col))
            }
        }
        tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::Bytes(_) => {
            let reader = StringFastFieldReader::open(fast_fields, entry.name())?;
            let reader = if let Some(cached_ords) =
                get_or_build_cached_grouped_string_ords(cache_ctx, entry.name(), &reader)
            {
                reader.with_cached_ords(cached_ords)
            } else {
                reader
            };
            Some(GroupKeyReader::Str(reader))
        }
        _ => None,
    }
}

struct ResolvedGroupedAggSpec {
    name: String,
    group_by: Vec<String>,
    metrics: Vec<ResolvedGroupedMetricSpec>,
}

struct ResolvedGroupedMetricSpec {
    output_name: String,
    function: crate::search::GroupedMetricFunction,
    field_name: Option<String>,
    field_expr: Option<crate::search::MetricFieldExpr>,
}

enum MetricEvalExpr {
    Leaf(usize),
    Binary {
        left: Box<MetricEvalExpr>,
        op: crate::search::MetricFieldOp,
        right: Box<MetricEvalExpr>,
    },
}

impl MetricEvalExpr {
    #[inline]
    fn eval_at(&self, buffers: &[Vec<Option<f64>>], base: usize, row: usize) -> Option<f64> {
        match self {
            Self::Leaf(index) => buffers[base + index][row],
            Self::Binary { left, op, right } => {
                let left = left.eval_at(buffers, base, row)?;
                let right = right.eval_at(buffers, base, row)?;
                op.eval(left, right)
            }
        }
    }
}

struct GroupedMetricExprPlan {
    leaves: Vec<NumCol>,
    expr: MetricEvalExpr,
}

impl GroupedMetricExprPlan {
    #[inline]
    fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    #[inline]
    fn eval_at(&self, buffers: &[Vec<Option<f64>>], base: usize, row: usize) -> Option<f64> {
        self.expr.eval_at(buffers, base, row)
    }
}

#[allow(dead_code)] // CountField reader will be used when null-aware count(field) is added
enum GroupedMetricSource {
    CountAll,
    CountField(SqlFieldReader),
    Numeric(GroupedMetricExprPlan),
}

struct GroupedMetricEntry {
    output_name: String,
    function: crate::search::GroupedMetricFunction,
    source: GroupedMetricSource,
}

struct DerivedStringBucketReader {
    base: StringFastFieldReader,
    spec: crate::search::DerivedGroupKey,
    ord_cache: std::cell::RefCell<std::collections::HashMap<u64, Option<u64>>>,
}

impl DerivedStringBucketReader {
    fn new(base: StringFastFieldReader, spec: crate::search::DerivedGroupKey) -> Self {
        Self {
            base,
            spec,
            ord_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    fn keys_batch(&self, docs: &[tantivy::DocId], output: &mut [Option<u64>]) {
        let mut ords = vec![None; docs.len()];
        self.base.first_ords_batch(docs, &mut ords);
        for (index, ord) in ords.into_iter().enumerate() {
            output[index] = ord.and_then(|value| self.bucket_key_for_ord(value));
        }
    }

    fn bucket_key_for_ord(&self, ord: u64) -> Option<u64> {
        if let Some(bucket_key) = self.ord_cache.borrow().get(&ord).cloned() {
            return bucket_key;
        }

        let mut text = String::new();
        let bucket_key = if self.base.ord_to_str(ord, &mut text) {
            self.matching_bucket_key(&text)
        } else {
            None
        };
        self.ord_cache.borrow_mut().insert(ord, bucket_key);
        bucket_key
    }

    fn matching_bucket_key(&self, value: &str) -> Option<u64> {
        derived_bucket_key_for_value(&self.spec, value)
    }

    fn num_terms(&self) -> usize {
        self.spec.buckets.len() + usize::from(self.spec.else_label.is_some())
    }

    fn resolve(&self, key: u64) -> serde_json::Value {
        if let Some(bucket) = self.spec.buckets.get(key as usize) {
            return serde_json::Value::String(bucket.label.clone());
        }
        if key == self.spec.buckets.len() as u64
            && let Some(label) = &self.spec.else_label
        {
            return serde_json::Value::String(label.clone());
        }
        serde_json::Value::Null
    }
}

fn derived_bucket_key_for_value(spec: &crate::search::DerivedGroupKey, value: &str) -> Option<u64> {
    for (index, bucket) in spec.buckets.iter().enumerate() {
        let lower_ok = if bucket.lower_inclusive {
            value >= bucket.lower.as_str()
        } else {
            value > bucket.lower.as_str()
        };
        let upper_ok = if bucket.upper_inclusive {
            value <= bucket.upper.as_str()
        } else {
            value < bucket.upper.as_str()
        };
        if lower_ok && upper_ok {
            return Some(index as u64);
        }
    }

    spec.else_label.as_ref().map(|_| spec.buckets.len() as u64)
}

/// Compact per-doc key reader. Extracts an optional u64 carrier per group-by
/// column without allocating Strings or serde_json::Values in the hot path.
enum GroupKeyReader {
    /// String column — reads ordinals from the underlying Column<u64> directly,
    /// bypassing the iterator-based `term_ords()` path.
    Str(StringFastFieldReader),
    DerivedStr(DerivedStringBucketReader),
    I64(tantivy::columnar::Column<i64>),
    CachedI64(std::sync::Arc<[Option<i64>]>),
    F64(tantivy::columnar::Column<f64>),
    CachedF64(std::sync::Arc<[Option<f64>]>),
}

impl GroupKeyReader {
    /// Batch-read ordinal keys for a slice of doc IDs into the output buffer.
    /// Much faster than per-doc reads due to sequential memory access.
    #[inline]
    fn keys_batch(&self, docs: &[tantivy::DocId], output: &mut [Option<u64>]) {
        match self {
            GroupKeyReader::Str(reader) => {
                reader.first_ords_batch(docs, output);
            }
            GroupKeyReader::DerivedStr(reader) => {
                reader.keys_batch(docs, output);
            }
            GroupKeyReader::I64(reader) => {
                // Reinterpret: read i64s into a temp buffer, convert to u64
                let mut i64_buf: Vec<Option<i64>> = vec![None; docs.len()];
                reader.first_vals(docs, &mut i64_buf);
                for (i, val) in i64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v as u64);
                }
            }
            GroupKeyReader::CachedI64(values) => {
                for (i, doc) in docs.iter().enumerate() {
                    output[i] = values
                        .get(*doc as usize)
                        .copied()
                        .flatten()
                        .map(|v| v as u64);
                }
            }
            GroupKeyReader::F64(reader) => {
                let mut f64_buf: Vec<Option<f64>> = vec![None; docs.len()];
                reader.first_vals(docs, &mut f64_buf);
                for (i, val) in f64_buf.iter().enumerate() {
                    output[i] = val.map(|v| v.to_bits());
                }
            }
            GroupKeyReader::CachedF64(values) => {
                for (i, doc) in docs.iter().enumerate() {
                    output[i] = values
                        .get(*doc as usize)
                        .copied()
                        .flatten()
                        .map(|v| v.to_bits());
                }
            }
        }
    }

    /// Number of unique terms (for string columns), used for HashMap pre-sizing.
    fn num_terms(&self) -> usize {
        match self {
            GroupKeyReader::Str(reader) => reader.num_terms(),
            GroupKeyReader::DerivedStr(reader) => reader.num_terms(),
            _ => 256,
        }
    }

    /// Resolve a compact key back to a serde_json::Value (called once per unique group).
    fn resolve(&self, key: Option<u64>) -> serde_json::Value {
        let Some(key) = key else {
            return serde_json::Value::Null;
        };
        match self {
            GroupKeyReader::Str(reader) => {
                let mut text = String::new();
                if reader.ord_to_str(key, &mut text) {
                    serde_json::Value::String(text)
                } else {
                    serde_json::Value::Null
                }
            }
            GroupKeyReader::DerivedStr(reader) => reader.resolve(key),
            GroupKeyReader::I64(_) => serde_json::Value::from(key as i64),
            GroupKeyReader::CachedI64(_) => serde_json::Value::from(key as i64),
            GroupKeyReader::F64(_) => serde_json::Value::from(f64::from_bits(key)),
            GroupKeyReader::CachedF64(_) => serde_json::Value::from(f64::from_bits(key)),
        }
    }
}

/// Compact per-bucket metric accumulator using Vec (indexed by metric position)
/// instead of HashMap<String, _>.
#[derive(Clone)]
enum CompactMetricAccum {
    Count(u64),
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

struct OrdGroupedBucket {
    /// Compact keys for each group-by column. `None` represents SQL null.
    ord_keys: Vec<Option<u64>>,
    /// One accumulator per metric, indexed by position.
    accums: Vec<CompactMetricAccum>,
}

struct PairGroupedBucket {
    /// One accumulator per metric, indexed by position.
    accums: Vec<CompactMetricAccum>,
}

struct SingleGroupedBuckets {
    values: OrdHashMap<OrdGroupedBucket>,
    null_bucket: Option<OrdGroupedBucket>,
}

struct PairGroupedBuckets {
    values: PairHashMap<PairGroupedBucket>,
    first_null: OrdHashMap<PairGroupedBucket>,
    second_null: OrdHashMap<PairGroupedBucket>,
    both_null: Option<PairGroupedBucket>,
}

/// Multi-column GROUP BY uses Vec<Option<u64>> as the key (collision-free).
/// Single-column uses u64 directly via OrdHashMap (identity hasher).
enum GroupedBuckets {
    /// Single group-by column: non-null ordinals in the fast map plus a dedicated null bucket.
    Single(SingleGroupedBuckets),
    /// Two-column group-by: packed non-null pairs plus dedicated null-mask buckets.
    Pair(PairGroupedBuckets),
    /// Multi-column group-by: explicit optional keys, standard hasher, zero collisions.
    Multi(std::collections::HashMap<Vec<Option<u64>>, OrdGroupedBucket>),
    /// No group-by columns (ungrouped aggregate): single global bucket.
    Global(Option<OrdGroupedBucket>),
}

struct GroupedAggSegmentEntry {
    key_readers: Vec<GroupKeyReader>,
    metric_entries: Vec<GroupedMetricEntry>,
    buckets: GroupedBuckets,
    /// Template of initial accumulators (one per metric).
    accum_template: Vec<CompactMetricAccum>,
    /// Buffered doc IDs for batch ordinal reads.
    doc_buffer: Vec<tantivy::DocId>,
    /// Reusable ordinal output buffer (avoids allocation per flush).
    ord_buffer: Vec<Option<u64>>,
    /// Reusable numeric value buffers — one per numeric metric (avoids per-doc reads).
    /// Indices correspond to the flattened leaf columns of each numeric metric.
    numeric_buffers: Vec<Vec<Option<f64>>>,
    /// Maps metric_entries index → first numeric buffer index (None if not numeric).
    numeric_buf_map: Vec<Option<usize>>,
}

const BATCH_SIZE: usize = 1024;

pub(crate) struct GroupedAggCollector {
    specs: Vec<ResolvedGroupedAggSpec>,
    schema: Schema,
    shard_top_k: Option<crate::search::ShardTopK>,
    column_cache: std::sync::Arc<super::column_cache::ColumnCache>,
}

pub(crate) struct GroupedAggSegmentCollector {
    entries: Vec<(String, Option<GroupedAggSegmentEntry>)>,
}

#[inline]
fn pack_pair_group_key(first: u64, second: u64) -> u128 {
    ((first as u128) << 64) | second as u128
}

#[inline]
fn unpack_pair_group_key(packed: u128) -> (u64, u64) {
    ((packed >> 64) as u64, packed as u64)
}

impl GroupedAggCollector {
    fn has_grouped_metrics(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
    ) -> bool {
        aggs.values()
            .any(|agg| matches!(agg, crate::search::AggregationRequest::GroupedMetrics(_)))
    }

    fn from_request(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
        schema: Schema,
        column_cache: std::sync::Arc<super::column_cache::ColumnCache>,
    ) -> Self {
        let specs = aggs
            .iter()
            .filter_map(|(name, req)| match req {
                crate::search::AggregationRequest::GroupedMetrics(params) => {
                    Some(ResolvedGroupedAggSpec {
                        name: name.clone(),
                        group_by: params.group_by.clone(),
                        metrics: params
                            .metrics
                            .iter()
                            .map(|metric| ResolvedGroupedMetricSpec {
                                output_name: metric.output_name.clone(),
                                function: metric.function.clone(),
                                field_name: metric.field.clone(),
                                field_expr: metric.field_expr.clone(),
                            })
                            .collect(),
                    })
                }
                _ => None,
            })
            .collect();
        let shard_top_k = aggs.values().find_map(|agg| match agg {
            crate::search::AggregationRequest::GroupedMetrics(params) => params.shard_top_k.clone(),
            _ => None,
        });
        Self {
            specs,
            schema,
            shard_top_k,
            column_cache,
        }
    }
}

impl tantivy::collector::Collector for GroupedAggCollector {
    type Fruit = std::collections::HashMap<String, crate::search::PartialAggResult>;
    type Child = GroupedAggSegmentCollector;

    fn for_segment(
        &self,
        _seg_id: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let ff = segment.fast_fields();
        let cache_ctx = GroupedCacheContext {
            column_cache: self.column_cache.as_ref(),
            segment_id: segment.segment_id(),
            max_doc: segment.max_doc(),
            // Filtered grouped queries reuse cache entries if already warm, but
            // do not populate fresh full-segment cache entries from partial scans.
            allow_populate: false,
        };
        let mut entries = Vec::with_capacity(self.specs.len());

        for spec in &self.specs {
            // Build compact key readers for GROUP BY columns
            let mut key_readers = Vec::with_capacity(spec.group_by.len());
            let mut unsupported_group = false;
            for field_name in &spec.group_by {
                match open_group_key_reader(&self.schema, ff, field_name, Some(cache_ctx)) {
                    Some(reader) => key_readers.push(reader),
                    None => {
                        unsupported_group = true;
                        break;
                    }
                }
            }
            if unsupported_group {
                entries.push((spec.name.clone(), None));
                continue;
            }

            let mut metric_entries = Vec::with_capacity(spec.metrics.len());
            let mut accum_template = Vec::with_capacity(spec.metrics.len());
            let mut unsupported = false;
            for metric in &spec.metrics {
                let source = match metric.function {
                    crate::search::GroupedMetricFunction::Count => match &metric.field_name {
                        None => GroupedMetricSource::CountAll,
                        Some(field_name) => {
                            let reader = open_sql_field_reader(&self.schema, ff, field_name, None);
                            if matches!(reader, SqlFieldReader::SourceFallback) {
                                unsupported = true;
                                break;
                            }
                            GroupedMetricSource::CountField(reader)
                        }
                    },
                    crate::search::GroupedMetricFunction::Sum
                    | crate::search::GroupedMetricFunction::Avg
                    | crate::search::GroupedMetricFunction::Min
                    | crate::search::GroupedMetricFunction::Max => {
                        let Some(plan) = build_grouped_metric_plan(
                            &self.schema,
                            ff,
                            metric.field_name.as_deref(),
                            metric.field_expr.as_ref(),
                            Some(cache_ctx),
                        ) else {
                            unsupported = true;
                            break;
                        };
                        GroupedMetricSource::Numeric(plan)
                    }
                };
                let template = match &source {
                    GroupedMetricSource::CountAll | GroupedMetricSource::CountField(_) => {
                        CompactMetricAccum::Count(0)
                    }
                    GroupedMetricSource::Numeric(_) => CompactMetricAccum::Stats {
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    },
                };
                accum_template.push(template);
                metric_entries.push(GroupedMetricEntry {
                    output_name: metric.output_name.clone(),
                    function: metric.function.clone(),
                    source,
                });
            }

            if unsupported {
                entries.push((spec.name.clone(), None));
                continue;
            }

            // Pre-size the HashMap from dictionary cardinality to avoid rehashing.
            let approx_groups = key_readers.first().map(|r| r.num_terms()).unwrap_or(256);

            let buckets = if key_readers.is_empty() {
                GroupedBuckets::Global(None)
            } else if key_readers.len() == 2 {
                let null_capacity = pair_null_bucket_capacity(approx_groups);
                GroupedBuckets::Pair(PairGroupedBuckets {
                    values: PairHashMap::with_capacity_and_hasher(
                        approx_groups,
                        PairBuildHasher::default(),
                    ),
                    first_null: OrdHashMap::with_capacity_and_hasher(
                        null_capacity,
                        OrdBuildHasher::default(),
                    ),
                    second_null: OrdHashMap::with_capacity_and_hasher(
                        null_capacity,
                        OrdBuildHasher::default(),
                    ),
                    both_null: None,
                })
            } else if key_readers.len() > 2 {
                GroupedBuckets::Multi(std::collections::HashMap::with_capacity(approx_groups))
            } else {
                GroupedBuckets::Single(SingleGroupedBuckets {
                    values: OrdHashMap::with_capacity_and_hasher(
                        approx_groups,
                        OrdBuildHasher::default(),
                    ),
                    null_bucket: None,
                })
            };

            // Build numeric buffer mapping: for each metric entry, assign the
            // first numeric_buffers index for that metric's leaf columns.
            let mut numeric_buf_map: Vec<Option<usize>> = Vec::with_capacity(metric_entries.len());
            let mut num_numeric = 0usize;
            for me in &metric_entries {
                match &me.source {
                    GroupedMetricSource::Numeric(plan) => {
                        numeric_buf_map.push(Some(num_numeric));
                        num_numeric += plan.leaf_count();
                    }
                    _ => {
                        numeric_buf_map.push(None);
                    }
                }
            }
            let numeric_buffers: Vec<Vec<Option<f64>>> =
                (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();

            entries.push((
                spec.name.clone(),
                Some(GroupedAggSegmentEntry {
                    key_readers,
                    metric_entries,
                    buckets,
                    accum_template,
                    doc_buffer: Vec::with_capacity(BATCH_SIZE),
                    ord_buffer: vec![None; BATCH_SIZE],
                    numeric_buffers,
                    numeric_buf_map,
                }),
            ));
        }

        Ok(GroupedAggSegmentCollector { entries })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged: std::collections::HashMap<
            String,
            std::collections::HashMap<String, crate::search::GroupedMetricsBucket>,
        > = std::collections::HashMap::new();

        for fruit in segment_fruits {
            for (name, buckets) in fruit {
                let agg_buckets = merged.entry(name).or_default();
                for bucket in buckets {
                    // Use a compact merge key — the group_values are already resolved
                    // serde_json::Value strings at this point (from harvest), but only
                    // runs #unique_groups × #segments times, not per-doc.
                    let key = compact_group_key(&bucket.group_values);
                    let target = agg_buckets.entry(key).or_insert_with(|| {
                        crate::search::GroupedMetricsBucket {
                            group_values: bucket.group_values.clone(),
                            metrics: std::collections::HashMap::new(),
                        }
                    });
                    merge_grouped_bucket_metrics(target, &bucket);
                }
            }
        }

        let mut results = std::collections::HashMap::new();
        for spec in &self.specs {
            let mut buckets: Vec<_> = merged
                .remove(&spec.name)
                .unwrap_or_default()
                .into_values()
                .collect();

            // Shard-level top-K pruning after segment merge (collector path).
            if let Some(top_k) = &self.shard_top_k {
                apply_shard_top_k(&mut buckets, top_k);
            }

            results.insert(
                spec.name.clone(),
                crate::search::PartialAggResult::GroupedMetrics { buckets },
            );
        }
        Ok(results)
    }
}

impl tantivy::collector::SegmentCollector for GroupedAggSegmentCollector {
    type Fruit = Vec<(String, Vec<crate::search::GroupedMetricsBucket>)>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        for (_, maybe_entry) in &mut self.entries {
            let Some(entry) = maybe_entry else {
                continue;
            };

            // For single-column GROUP BY, buffer docs for batch ordinal reads.
            // This works for both count-only and numeric metrics.
            if matches!(&entry.buckets, GroupedBuckets::Single(_)) && entry.key_readers.len() == 1 {
                entry.doc_buffer.push(doc);
                if entry.doc_buffer.len() >= BATCH_SIZE {
                    flush_batch(entry);
                }
                continue;
            }

            // Multi-column or Global: buffer docs and batch-flush.
            // Avoids per-doc Vec allocation for ord_keys and per-doc fast-field reads.
            entry.doc_buffer.push(doc);
            if entry.doc_buffer.len() >= BATCH_SIZE {
                match &entry.buckets {
                    GroupedBuckets::Pair(_) => flush_batch_pair(entry),
                    GroupedBuckets::Multi(_) | GroupedBuckets::Global(_) => {
                        flush_batch_multi(entry)
                    }
                    GroupedBuckets::Single(_) => flush_batch(entry),
                }
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.entries
            .into_iter()
            .filter_map(|(name, entry)| {
                let mut entry = entry?;
                // Flush any remaining buffered docs (single-column or multi-column)
                if !entry.doc_buffer.is_empty() {
                    if matches!(&entry.buckets, GroupedBuckets::Single(_))
                        && entry.key_readers.len() == 1
                    {
                        flush_batch(&mut entry);
                    } else if matches!(&entry.buckets, GroupedBuckets::Pair(_)) {
                        flush_batch_pair(&mut entry);
                    } else {
                        flush_batch_multi(&mut entry);
                    }
                }

                // Collect all buckets from whichever variant.
                let raw_buckets = grouped_buckets_into_raw(entry.buckets);

                // Resolve ordinals to values and convert to GroupedMetricsBucket.
                let buckets: Vec<crate::search::GroupedMetricsBucket> = raw_buckets
                    .into_iter()
                    .map(|bucket| resolve_bucket(bucket, &entry.key_readers, &entry.metric_entries))
                    .collect();
                Some((name, buckets))
            })
            .collect()
    }
}

/// Contiguous string arena — avoids N individual heap allocations when batch-
/// resolving ordinals to strings.  All strings live in one `Vec<u8>` and are
/// referenced by `(offset, len)`.  Only the final `serde_json::Value::String`
/// conversion allocates a new owned String per group.
struct StringArena {
    buf: Vec<u8>,
    /// (start_offset, byte_length) for each interned string.
    entries: Vec<(u32, u32)>,
}

impl StringArena {
    fn with_capacity(num_strings: usize, avg_len: usize) -> Self {
        Self {
            buf: Vec::with_capacity(num_strings * avg_len),
            entries: Vec::with_capacity(num_strings),
        }
    }

    /// Append a string slice to the arena; returns its index.
    fn push(&mut self, s: &str) -> u32 {
        let idx = self.entries.len() as u32;
        let offset = self.buf.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        self.entries.push((offset, s.len() as u32));
        idx
    }

    /// Retrieve a reference to a previously interned string.
    fn get(&self, idx: u32) -> &str {
        let (offset, len) = self.entries[idx as usize];
        // SAFETY: all data pushed through `push` is valid UTF-8 (`&str`).
        unsafe {
            std::str::from_utf8_unchecked(&self.buf[offset as usize..(offset + len) as usize])
        }
    }

    fn to_json_value(&self, idx: u32) -> serde_json::Value {
        serde_json::Value::String(self.get(idx).to_string())
    }
}

/// Extract a sort value from flat metric arrays for a given ordinal.
/// Used by shard-level top-K pruning to rank groups without resolving strings.
fn flat_sort_value(
    flat_metrics: &[FlatMetric],
    metric_entries: &[GroupedMetricEntry],
    sort_by: &str,
    ord: usize,
) -> f64 {
    for (idx, me) in metric_entries.iter().enumerate() {
        if me.output_name != sort_by {
            continue;
        }
        return match (&me.function, &flat_metrics[idx]) {
            (_, FlatMetric::Count(counts)) => counts[ord] as f64,
            (crate::search::GroupedMetricFunction::Sum, FlatMetric::Stats { sum, .. }) => sum[ord],
            (crate::search::GroupedMetricFunction::Avg, FlatMetric::Stats { count, sum, .. }) => {
                if count[ord] > 0 {
                    sum[ord] / count[ord] as f64
                } else {
                    f64::NEG_INFINITY
                }
            }
            (crate::search::GroupedMetricFunction::Min, FlatMetric::Stats { min, .. }) => min[ord],
            (crate::search::GroupedMetricFunction::Max, FlatMetric::Stats { max, .. }) => max[ord],
            _ => 0.0,
        };
    }
    0.0
}

/// Extract a sort value from a fully-resolved `GroupedMetricsBucket`.
/// Uses the metric function from `ShardTopK` to compute the correct value
/// (avg = sum/count, min = min, max = max) instead of blindly using sum.
fn bucket_sort_value(
    bucket: &crate::search::GroupedMetricsBucket,
    sort_by: &str,
    sort_function: &crate::search::GroupedMetricFunction,
) -> f64 {
    let Some(partial) = bucket.metrics.get(sort_by) else {
        return f64::NEG_INFINITY;
    };
    match partial {
        crate::search::GroupedMetricPartial::Count { count } => *count as f64,
        crate::search::GroupedMetricPartial::Stats {
            count,
            sum,
            min,
            max,
        } => match sort_function {
            crate::search::GroupedMetricFunction::Count => *count as f64,
            crate::search::GroupedMetricFunction::Sum => {
                if *count > 0 {
                    *sum
                } else {
                    f64::NEG_INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Avg => {
                if *count > 0 {
                    *sum / *count as f64
                } else {
                    f64::NEG_INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Min => {
                if *count > 0 {
                    *min
                } else {
                    f64::INFINITY
                }
            }
            crate::search::GroupedMetricFunction::Max => {
                if *count > 0 {
                    *max
                } else {
                    f64::NEG_INFINITY
                }
            }
        },
    }
}

/// Apply shard-level top-K pruning to a vec of grouped buckets.
/// Sorts by the named metric and truncates to `top_k.limit`.
fn apply_shard_top_k(
    buckets: &mut Vec<crate::search::GroupedMetricsBucket>,
    top_k: &crate::search::ShardTopK,
) {
    if buckets.len() <= top_k.limit {
        return;
    }
    let k = top_k.limit.min(buckets.len());
    // Partial sort: place the top-K elements in [0..k) in O(N) average.
    buckets.select_nth_unstable_by(k - 1, |a, b| {
        let va = bucket_sort_value(a, &top_k.sort_by, &top_k.sort_function);
        let vb = bucket_sort_value(b, &top_k.sort_by, &top_k.sort_function);
        if top_k.descending {
            vb.total_cmp(&va)
        } else {
            va.total_cmp(&vb)
        }
    });
    buckets.truncate(k);
}

/// Flat-array accumulation for single-column keyword GROUP BY on match_all scans.
/// Replaces HashMap with pre-allocated Vec<u64>/Vec<f64> arrays indexed directly
/// by ordinal — zero hash computation, zero collision handling, cache-friendly
/// sequential access. ~22MB for 300K groups × 5 metrics, fits in L3 cache.
fn flat_scan_segment(
    key_reader: &GroupKeyReader,
    metric_entries: &[GroupedMetricEntry],
    max_doc: u32,
    num_groups: usize,
    shard_top_k: Option<&crate::search::ShardTopK>,
) -> Vec<crate::search::GroupedMetricsBucket> {
    // Build flat metric arrays — one layout per metric
    let mut flat_metrics: Vec<FlatMetric> = metric_entries
        .iter()
        .map(|me| match &me.source {
            GroupedMetricSource::CountAll | GroupedMetricSource::CountField(_) => {
                FlatMetric::Count(vec![0u64; num_groups])
            }
            GroupedMetricSource::Numeric(_) => FlatMetric::Stats {
                count: vec![0u64; num_groups],
                sum: vec![0.0f64; num_groups],
                min: vec![f64::INFINITY; num_groups],
                max: vec![f64::NEG_INFINITY; num_groups],
            },
        })
        .collect();

    // Batch read ordinals + numerics, accumulate into flat arrays.
    // Use contiguous ranges instead of pushing individual doc IDs.
    let mut doc_buffer: Vec<u32> = Vec::with_capacity(BATCH_SIZE);
    let mut ord_buffer: Vec<Option<u64>> = vec![None; BATCH_SIZE];

    let mut num_numeric = 0usize;
    for me in metric_entries.iter() {
        if let GroupedMetricSource::Numeric(plan) = &me.source {
            num_numeric += plan.leaf_count();
        }
    }
    let mut numeric_bufs: Vec<Vec<Option<f64>>> =
        (0..num_numeric).map(|_| vec![None; BATCH_SIZE]).collect();
    // Map: metric index → numeric_bufs index
    let numeric_buf_map: Vec<Option<usize>> = {
        let mut idx = 0usize;
        metric_entries
            .iter()
            .map(|me| match &me.source {
                GroupedMetricSource::Numeric(plan) => {
                    let i = idx;
                    idx += plan.leaf_count();
                    Some(i)
                }
                _ => None,
            })
            .collect()
    };

    // Process contiguous batches of BATCH_SIZE docs at a time.
    let mut start = 0u32;
    while start < max_doc {
        let end = (start + BATCH_SIZE as u32).min(max_doc);
        doc_buffer.clear();
        doc_buffer.extend(start..end);
        flat_flush_batch(
            key_reader,
            metric_entries,
            &doc_buffer,
            &mut ord_buffer,
            &numeric_buf_map,
            &mut numeric_bufs,
            &mut flat_metrics,
            num_groups,
        );
        start = end;
    }

    // Convert flat arrays → GroupedMetricsBucket.
    //
    // Optimization 1 — shard-level top-K: when ORDER BY + LIMIT is present,
    // select only the top-K ordinals by the sort metric value BEFORE resolving
    // strings. This avoids 364K ord_to_str calls for LIMIT 10 queries.
    //
    // Optimization 2 — StringArena: batch-resolve all needed ordinals into one
    // contiguous buffer instead of N individual String heap allocations.

    // Step 1: collect populated ordinals.
    let mut populated_ords: Vec<usize> = (0..num_groups)
        .filter(|&ord| {
            flat_metrics.iter().any(|fm| match fm {
                FlatMetric::Count(counts) => counts[ord] > 0,
                FlatMetric::Stats { count, .. } => count[ord] > 0,
            })
        })
        .collect();

    // Step 2: top-K pruning on ordinals (before string resolution).
    if let Some(top_k) = shard_top_k
        && populated_ords.len() > top_k.limit
    {
        let k = top_k.limit.min(populated_ords.len());
        populated_ords.select_nth_unstable_by(k - 1, |&a, &b| {
            let va = flat_sort_value(&flat_metrics, metric_entries, &top_k.sort_by, a);
            let vb = flat_sort_value(&flat_metrics, metric_entries, &top_k.sort_by, b);
            if top_k.descending {
                vb.total_cmp(&va)
            } else {
                va.total_cmp(&vb)
            }
        });
        populated_ords.truncate(k);
    }

    // Step 3: batch-resolve ordinals into a StringArena.
    let mut arena = StringArena::with_capacity(populated_ords.len(), 16);
    let mut resolve_buf = String::with_capacity(64);
    // Map: position in populated_ords → arena index.
    // For non-string keys, arena is unused and we resolve inline.
    let is_str_key = matches!(key_reader, GroupKeyReader::Str(_));
    if is_str_key {
        for &ord in &populated_ords {
            resolve_buf.clear();
            if let GroupKeyReader::Str(reader) = key_reader {
                reader.ord_to_str(ord as u64, &mut resolve_buf);
            }
            arena.push(&resolve_buf);
        }
    }

    // Step 4: build buckets from the pruned + arena-resolved ordinals.
    let mut buckets = Vec::with_capacity(populated_ords.len());
    for (pos, &ord) in populated_ords.iter().enumerate() {
        let group_value = if is_str_key {
            arena.to_json_value(pos as u32)
        } else {
            key_reader.resolve(Some(ord as u64))
        };

        let mut metrics = std::collections::HashMap::new();
        for (metric_idx, me) in metric_entries.iter().enumerate() {
            let partial = match &flat_metrics[metric_idx] {
                FlatMetric::Count(counts) => {
                    crate::search::GroupedMetricPartial::Count { count: counts[ord] }
                }
                FlatMetric::Stats {
                    count,
                    sum,
                    min,
                    max,
                } => crate::search::GroupedMetricPartial::Stats {
                    count: count[ord],
                    sum: sum[ord],
                    min: min[ord],
                    max: max[ord],
                },
            };
            metrics.insert(me.output_name.clone(), partial);
        }

        buckets.push(crate::search::GroupedMetricsBucket {
            group_values: vec![group_value],
            metrics,
        });
    }
    buckets
}

/// Flat metric storage: parallel arrays indexed by ordinal.
enum FlatMetric {
    Count(Vec<u64>),
    Stats {
        count: Vec<u64>,
        sum: Vec<f64>,
        min: Vec<f64>,
        max: Vec<f64>,
    },
}

/// Flush a batch of docs into flat arrays: batch-read ordinals + numerics,
/// then update flat metric arrays with direct indexed access (no HashMap).
#[allow(clippy::too_many_arguments)]
#[inline]
fn flat_flush_batch(
    key_reader: &GroupKeyReader,
    metric_entries: &[GroupedMetricEntry],
    doc_buffer: &[u32],
    ord_buffer: &mut Vec<Option<u64>>,
    numeric_buf_map: &[Option<usize>],
    numeric_bufs: &mut [Vec<Option<f64>>],
    flat_metrics: &mut [FlatMetric],
    num_groups: usize,
) {
    let batch_len = doc_buffer.len();

    // Batch-read ordinals
    if ord_buffer.len() < batch_len {
        ord_buffer.resize(batch_len, None);
    }
    for slot in &mut ord_buffer[..batch_len] {
        *slot = None;
    }
    key_reader.keys_batch(doc_buffer, &mut ord_buffer[..batch_len]);

    batch_read_metric_leaf_buffers(
        metric_entries,
        numeric_buf_map,
        numeric_bufs,
        doc_buffer,
        batch_len,
    );

    // Accumulate into flat arrays — direct indexed, no HashMap
    for (i, maybe_ord) in ord_buffer.iter().enumerate().take(batch_len) {
        let Some(ord) = *maybe_ord else {
            continue; // NULL ordinal — skip
        };
        let ord = ord as usize;
        if ord >= num_groups {
            continue; // safety guard
        }

        for (metric_idx, fm) in flat_metrics.iter_mut().enumerate() {
            match fm {
                FlatMetric::Count(counts) => {
                    counts[ord] += 1;
                }
                FlatMetric::Stats {
                    count,
                    sum,
                    min,
                    max,
                } => {
                    if let Some(val) = metric_numeric_value(
                        &metric_entries[metric_idx],
                        numeric_buf_map[metric_idx],
                        numeric_bufs,
                        i,
                    ) {
                        count[ord] += 1;
                        sum[ord] += val;
                        if val < min[ord] {
                            min[ord] = val;
                        }
                        if val > max[ord] {
                            max[ord] = val;
                        }
                    }
                }
            }
        }
    }
}

fn batch_read_metric_leaf_buffers(
    metric_entries: &[GroupedMetricEntry],
    numeric_buf_map: &[Option<usize>],
    numeric_buffers: &mut [Vec<Option<f64>>],
    docs: &[u32],
    batch_len: usize,
) {
    for (metric_idx, metric) in metric_entries.iter().enumerate() {
        let Some(buf_idx) = numeric_buf_map[metric_idx] else {
            continue;
        };
        let GroupedMetricSource::Numeric(plan) = &metric.source else {
            continue;
        };
        for (leaf_idx, col) in plan.leaves.iter().enumerate() {
            let buf = &mut numeric_buffers[buf_idx + leaf_idx];
            if buf.len() < batch_len {
                buf.resize(batch_len, None);
            }
            for slot in &mut buf[..batch_len] {
                *slot = None;
            }
            col.first_vals_f64(docs, &mut buf[..batch_len]);
        }
    }
}

#[inline]
fn metric_numeric_value(
    metric: &GroupedMetricEntry,
    buf_idx: Option<usize>,
    numeric_buffers: &[Vec<Option<f64>>],
    row: usize,
) -> Option<f64> {
    let buf_idx = buf_idx?;
    match &metric.source {
        GroupedMetricSource::Numeric(plan) => plan.eval_at(numeric_buffers, buf_idx, row),
        _ => None,
    }
}

/// Batch-read all numeric leaf columns into the entry's numeric buffers.
fn batch_read_numeric_buffers(entry: &mut GroupedAggSegmentEntry, batch_len: usize) {
    batch_read_metric_leaf_buffers(
        &entry.metric_entries,
        &entry.numeric_buf_map,
        &mut entry.numeric_buffers,
        &entry.doc_buffer,
        batch_len,
    );
}

fn accumulate_grouped_bucket_metrics(
    accums: &mut [CompactMetricAccum],
    metric_entries: &[GroupedMetricEntry],
    numeric_buf_map: &[Option<usize>],
    numeric_buffers: &[Vec<Option<f64>>],
    row: usize,
) {
    let has_numeric = !numeric_buffers.is_empty();
    if has_numeric {
        for (metric_idx, metric) in metric_entries.iter().enumerate() {
            match (&metric.function, &numeric_buf_map[metric_idx]) {
                (crate::search::GroupedMetricFunction::Count, _) => {
                    if let CompactMetricAccum::Count(count) = &mut accums[metric_idx] {
                        *count += 1;
                    }
                }
                (
                    crate::search::GroupedMetricFunction::Sum
                    | crate::search::GroupedMetricFunction::Avg
                    | crate::search::GroupedMetricFunction::Min
                    | crate::search::GroupedMetricFunction::Max,
                    Some(buf_idx),
                ) => {
                    let Some(value) =
                        metric_numeric_value(metric, Some(*buf_idx), numeric_buffers, row)
                    else {
                        continue;
                    };
                    if let CompactMetricAccum::Stats {
                        count,
                        sum,
                        min,
                        max,
                    } = &mut accums[metric_idx]
                    {
                        *count += 1;
                        *sum += value;
                        if value < *min {
                            *min = value;
                        }
                        if value > *max {
                            *max = value;
                        }
                    }
                }
                _ => {}
            }
        }
    } else {
        for accum in accums {
            if let CompactMetricAccum::Count(count) = accum {
                *count += 1;
            }
        }
    }
}

/// Flush a batch of buffered doc IDs: batch-read ordinals AND numeric values,
/// then update accumulators. Avoids per-doc fast-field reads for numeric metrics.
fn flush_batch(entry: &mut GroupedAggSegmentEntry) {
    let batch_len = entry.doc_buffer.len();
    if batch_len == 0 {
        return;
    }

    if entry.ord_buffer.len() < batch_len {
        entry.ord_buffer.resize(batch_len, None);
    }

    for slot in &mut entry.ord_buffer[..batch_len] {
        *slot = None;
    }

    if let Some(reader) = entry.key_readers.first() {
        reader.keys_batch(&entry.doc_buffer, &mut entry.ord_buffer[..batch_len]);
    }

    // Batch-read all numeric columns upfront (sequential memory access).
    batch_read_numeric_buffers(entry, batch_len);

    let GroupedBuckets::Single(single) = &mut entry.buckets else {
        entry.doc_buffer.clear();
        return;
    };

    for i in 0..batch_len {
        let bucket = match entry.ord_buffer[i] {
            Some(ord) => single
                .values
                .entry(ord)
                .or_insert_with(|| OrdGroupedBucket {
                    ord_keys: vec![Some(ord)],
                    accums: entry.accum_template.clone(),
                }),
            None => single.null_bucket.get_or_insert_with(|| OrdGroupedBucket {
                ord_keys: vec![None],
                accums: entry.accum_template.clone(),
            }),
        };

        accumulate_grouped_bucket_metrics(
            &mut bucket.accums,
            &entry.metric_entries,
            &entry.numeric_buf_map,
            &entry.numeric_buffers,
            i,
        );
    }

    entry.doc_buffer.clear();
}

/// Batched flush for the specialized width-2 GROUP BY path.
/// Uses a packed fixed-width key instead of HashMap<Vec<u64>, ...>.
fn flush_batch_pair(entry: &mut GroupedAggSegmentEntry) {
    let batch_len = entry.doc_buffer.len();
    if batch_len == 0 {
        return;
    }

    debug_assert_eq!(entry.key_readers.len(), 2);

    let mut first_keys = vec![None; batch_len];
    let mut second_keys = vec![None; batch_len];
    entry.key_readers[0].keys_batch(&entry.doc_buffer, &mut first_keys);
    entry.key_readers[1].keys_batch(&entry.doc_buffer, &mut second_keys);

    batch_read_numeric_buffers(entry, batch_len);

    let GroupedBuckets::Pair(pair) = &mut entry.buckets else {
        entry.doc_buffer.clear();
        return;
    };

    for i in 0..batch_len {
        let bucket = match (first_keys[i], second_keys[i]) {
            (Some(first), Some(second)) => pair
                .values
                .entry(pack_pair_group_key(first, second))
                .or_insert_with(|| PairGroupedBucket {
                    accums: entry.accum_template.clone(),
                }),
            (None, Some(second)) => {
                pair.first_null
                    .entry(second)
                    .or_insert_with(|| PairGroupedBucket {
                        accums: entry.accum_template.clone(),
                    })
            }
            (Some(first), None) => {
                pair.second_null
                    .entry(first)
                    .or_insert_with(|| PairGroupedBucket {
                        accums: entry.accum_template.clone(),
                    })
            }
            (None, None) => pair.both_null.get_or_insert_with(|| PairGroupedBucket {
                accums: entry.accum_template.clone(),
            }),
        };

        accumulate_grouped_bucket_metrics(
            &mut bucket.accums,
            &entry.metric_entries,
            &entry.numeric_buf_map,
            &entry.numeric_buffers,
            i,
        );
    }

    entry.doc_buffer.clear();
}

/// Batched flush for multi-column GROUP BY or Global (ungrouped aggregates).
/// Batch-reads ordinals for ALL key readers and all numeric columns, then
/// accumulates — avoids per-doc fast-field reads and per-doc Vec allocations.
fn flush_batch_multi(entry: &mut GroupedAggSegmentEntry) {
    let batch_len = entry.doc_buffer.len();
    if batch_len == 0 {
        return;
    }

    let num_keys = entry.key_readers.len();

    // Batch-read ordinals for each key reader into a flat buffer.
    // Layout: ord_bufs[key_idx][doc_in_batch] = ordinal
    let mut ord_bufs: Vec<Vec<Option<u64>>> = Vec::with_capacity(num_keys);
    for reader in &entry.key_readers {
        let mut buf = vec![None; batch_len];
        reader.keys_batch(&entry.doc_buffer, &mut buf);
        ord_bufs.push(buf);
    }

    // Batch-read all numeric columns upfront (sequential memory access).
    batch_read_numeric_buffers(entry, batch_len);

    // Reusable ord_keys buffer — avoids per-doc Vec allocation.
    let mut ord_keys = vec![None; num_keys];

    for i in 0..batch_len {
        // Build composite key from batch-read ordinals (no per-doc fast-field read).
        for (k, buf) in ord_bufs.iter().enumerate() {
            ord_keys[k] = buf[i];
        }

        let template = &entry.accum_template;
        let bucket = match &mut entry.buckets {
            GroupedBuckets::Multi(map) => {
                map.entry(ord_keys.clone())
                    .or_insert_with(|| OrdGroupedBucket {
                        ord_keys: ord_keys.clone(),
                        accums: template.clone(),
                    })
            }
            GroupedBuckets::Global(slot) => slot.get_or_insert_with(|| OrdGroupedBucket {
                ord_keys: vec![],
                accums: template.clone(),
            }),
            GroupedBuckets::Single(_) => unreachable!("single buckets should use flush_batch"),
            GroupedBuckets::Pair(_) => unreachable!("pair buckets should use flush_batch_pair"),
        };

        accumulate_grouped_bucket_metrics(
            &mut bucket.accums,
            &entry.metric_entries,
            &entry.numeric_buf_map,
            &entry.numeric_buffers,
            i,
        );
    }

    entry.doc_buffer.clear();
}

fn grouped_buckets_into_raw(buckets: GroupedBuckets) -> Vec<OrdGroupedBucket> {
    match buckets {
        GroupedBuckets::Single(single) => {
            let mut raw: Vec<OrdGroupedBucket> = single.values.into_values().collect();
            if let Some(bucket) = single.null_bucket {
                raw.push(bucket);
            }
            raw
        }
        GroupedBuckets::Pair(pair) => {
            let mut raw = Vec::with_capacity(
                pair.values.len()
                    + pair.first_null.len()
                    + pair.second_null.len()
                    + usize::from(pair.both_null.is_some()),
            );
            raw.extend(pair.values.into_iter().map(|(packed, bucket)| {
                let (first, second) = unpack_pair_group_key(packed);
                OrdGroupedBucket {
                    ord_keys: vec![Some(first), Some(second)],
                    accums: bucket.accums,
                }
            }));
            raw.extend(
                pair.first_null
                    .into_iter()
                    .map(|(second, bucket)| OrdGroupedBucket {
                        ord_keys: vec![None, Some(second)],
                        accums: bucket.accums,
                    }),
            );
            raw.extend(
                pair.second_null
                    .into_iter()
                    .map(|(first, bucket)| OrdGroupedBucket {
                        ord_keys: vec![Some(first), None],
                        accums: bucket.accums,
                    }),
            );
            if let Some(bucket) = pair.both_null {
                raw.push(OrdGroupedBucket {
                    ord_keys: vec![None, None],
                    accums: bucket.accums,
                });
            }
            raw
        }
        GroupedBuckets::Multi(map) => map.into_values().collect(),
        GroupedBuckets::Global(opt) => opt.into_iter().collect(),
    }
}

/// Resolve an OrdGroupedBucket to a GroupedMetricsBucket (ordinals → values).
fn resolve_bucket(
    bucket: OrdGroupedBucket,
    key_readers: &[GroupKeyReader],
    metric_entries: &[GroupedMetricEntry],
) -> crate::search::GroupedMetricsBucket {
    let group_values: Vec<serde_json::Value> = bucket
        .ord_keys
        .iter()
        .zip(key_readers.iter())
        .map(|(ord, reader)| reader.resolve(*ord))
        .collect();

    let mut metrics = std::collections::HashMap::new();
    for (idx, metric) in metric_entries.iter().enumerate() {
        let partial = match &bucket.accums[idx] {
            CompactMetricAccum::Count(c) => {
                crate::search::GroupedMetricPartial::Count { count: *c }
            }
            CompactMetricAccum::Stats {
                count,
                sum,
                min,
                max,
            } => crate::search::GroupedMetricPartial::Stats {
                count: *count,
                sum: *sum,
                min: *min,
                max: *max,
            },
        };
        metrics.insert(metric.output_name.clone(), partial);
    }

    crate::search::GroupedMetricsBucket {
        group_values,
        metrics,
    }
}

/// Build a compact string key from group_values for segment merge.
/// Much cheaper than serde_json::to_string — concatenates string representations
/// with a separator. Only called #unique_groups × #segments times.
fn compact_group_key(values: &[serde_json::Value]) -> String {
    use std::fmt::Write;
    let mut key = String::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            key.push('\x1F'); // unit separator
        }
        match v {
            serde_json::Value::String(s) => key.push_str(s),
            serde_json::Value::Number(n) => write!(key, "{n}").unwrap(),
            serde_json::Value::Bool(b) => write!(key, "{b}").unwrap(),
            serde_json::Value::Null => key.push('\0'),
            _ => write!(key, "{v}").unwrap(),
        }
    }
    key
}

fn merge_grouped_bucket_metrics(
    target: &mut crate::search::GroupedMetricsBucket,
    source: &crate::search::GroupedMetricsBucket,
) {
    for (metric_name, incoming) in &source.metrics {
        match incoming {
            crate::search::GroupedMetricPartial::Count { count } => {
                let entry = target
                    .metrics
                    .entry(metric_name.clone())
                    .or_insert(crate::search::GroupedMetricPartial::Count { count: 0 });
                if let crate::search::GroupedMetricPartial::Count {
                    count: merged_count,
                } = entry
                {
                    *merged_count += count;
                }
            }
            crate::search::GroupedMetricPartial::Stats {
                count,
                sum,
                min,
                max,
            } => {
                let entry = target.metrics.entry(metric_name.clone()).or_insert(
                    crate::search::GroupedMetricPartial::Stats {
                        count: 0,
                        sum: 0.0,
                        min: f64::INFINITY,
                        max: f64::NEG_INFINITY,
                    },
                );
                if let crate::search::GroupedMetricPartial::Stats {
                    count: merged_count,
                    sum: merged_sum,
                    min: merged_min,
                    max: merged_max,
                } = entry
                {
                    *merged_count += count;
                    *merged_sum += sum;
                    if *min < *merged_min {
                        *merged_min = *min;
                    }
                    if *max > *merged_max {
                        *merged_max = *max;
                    }
                }
            }
        }
    }
}

pub(crate) enum SegmentAggData {
    Stats {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    Histogram {
        interval: f64,
        buckets: std::collections::HashMap<i64, u64>,
    },
    Terms {
        counts: std::collections::HashMap<String, u64>,
    },
}

fn merge_segment_data(target: &mut SegmentAggData, source: &SegmentAggData) {
    match (target, source) {
        (
            SegmentAggData::Stats {
                count: ca,
                sum: sa,
                min: mna,
                max: mxa,
            },
            SegmentAggData::Stats {
                count: cb,
                sum: sb,
                min: mnb,
                max: mxb,
            },
        ) => {
            *ca += cb;
            *sa += sb;
            if *mnb < *mna {
                *mna = *mnb;
            }
            if *mxb > *mxa {
                *mxa = *mxb;
            }
        }
        (
            SegmentAggData::Histogram { buckets: ba, .. },
            SegmentAggData::Histogram { buckets: bb, .. },
        ) => {
            for (k, v) in bb {
                *ba.entry(*k).or_insert(0) += v;
            }
        }
        (SegmentAggData::Terms { counts: ca }, SegmentAggData::Terms { counts: cb }) => {
            for (k, v) in cb {
                *ca.entry(k.clone()).or_insert(0) += v;
            }
        }
        _ => {}
    }
}

fn convert_to_partial(kind: &AggKind, data: SegmentAggData) -> crate::search::PartialAggResult {
    use crate::search::{HistogramBucket, PartialAggResult, TermsBucket};
    match (kind, data) {
        (
            AggKind::Stats,
            SegmentAggData::Stats {
                count,
                sum,
                min,
                max,
            },
        ) => PartialAggResult::Stats {
            count,
            sum,
            min,
            max,
        },
        (AggKind::Min, SegmentAggData::Stats { count, min, .. }) => PartialAggResult::Metric {
            value: if count > 0 { Some(min) } else { None },
        },
        (AggKind::Max, SegmentAggData::Stats { count, max, .. }) => PartialAggResult::Metric {
            value: if count > 0 { Some(max) } else { None },
        },
        (
            AggKind::Avg,
            SegmentAggData::Stats {
                count,
                sum,
                min,
                max,
            },
        ) => PartialAggResult::Stats {
            count,
            sum,
            min,
            max,
        },
        (AggKind::Sum, SegmentAggData::Stats { sum, .. }) => {
            PartialAggResult::Metric { value: Some(sum) }
        }
        (AggKind::ValueCount, SegmentAggData::Stats { count, .. }) => PartialAggResult::Metric {
            value: Some(count as f64),
        },
        (_, SegmentAggData::Histogram { interval, buckets }) => {
            let mut hb: Vec<HistogramBucket> = buckets
                .into_iter()
                .map(|(k, c)| HistogramBucket {
                    key: k as f64 * interval,
                    doc_count: c,
                })
                .collect();
            hb.sort_by(|a, b| {
                a.key
                    .partial_cmp(&b.key)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            PartialAggResult::Histogram { buckets: hb }
        }
        (AggKind::Terms, SegmentAggData::Terms { counts }) => {
            let mut tb: Vec<TermsBucket> = counts
                .into_iter()
                .map(|(k, c)| TermsBucket {
                    key: k,
                    doc_count: c,
                })
                .collect();
            tb.sort_by_key(|a| std::cmp::Reverse(a.doc_count));
            // Preserve every bucket in the shard partial. Coordinator merge is
            // where the requested size is applied so distributed top terms stay correct.
            PartialAggResult::Terms { buckets: tb }
        }
        _ => PartialAggResult::Metric { value: None },
    }
}

#[derive(Clone)]
enum AggKind {
    Stats,
    Min,
    Max,
    Avg,
    Sum,
    ValueCount,
    Histogram { interval: f64 },
    Terms,
}

struct ResolvedAggSpec {
    name: String,
    field_name: String,
    kind: AggKind,
}

const DENSE_TERMS_ORDINAL_LIMIT: usize = 1024;

fn resolve_term_ordinals(
    column: &tantivy::columnar::StrColumn,
    counts: impl IntoIterator<Item = (u64, u64)>,
    capacity: usize,
) -> tantivy::Result<HashMap<String, u64>> {
    let mut resolved = HashMap::with_capacity(capacity);
    let mut text = String::new();
    for (ordinal, count) in counts {
        text.clear();
        if !column.ord_to_str(ordinal, &mut text)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("term ordinal {ordinal} is absent from the string dictionary"),
            )
            .into());
        }
        *resolved.entry(text.clone()).or_insert(0) += count;
    }
    Ok(resolved)
}

enum SegmentAggEntry {
    NumericStats {
        column: NumCol,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
    Histogram {
        column: NumCol,
        interval: f64,
        buckets: std::collections::HashMap<i64, u64>,
    },
    TermsStrDense {
        column: tantivy::columnar::StrColumn,
        counts: Vec<u64>,
        invalid_ordinal: Option<u64>,
    },
    TermsStrSparse {
        column: tantivy::columnar::StrColumn,
        counts: HashMap<u64, u64>,
    },
    TermsNum {
        column: NumCol,
        counts: std::collections::HashMap<NumericTermKey, u64>,
    },
    Skip,
}

/// Single-pass aggregation collector. Combine with TopDocs via tuple collector.
pub(crate) struct AggCollector {
    specs: Vec<ResolvedAggSpec>,
    schema: Schema,
}

impl AggCollector {
    pub(crate) fn from_request(
        aggs: &std::collections::HashMap<String, crate::search::AggregationRequest>,
        schema: Schema,
    ) -> Self {
        use crate::search::AggregationRequest;
        let specs = aggs
            .iter()
            .filter_map(|(name, req)| {
                let (field_name, kind) = match req {
                    AggregationRequest::Stats(p) => (p.field.clone(), AggKind::Stats),
                    AggregationRequest::Min(p) => (p.field.clone(), AggKind::Min),
                    AggregationRequest::Max(p) => (p.field.clone(), AggKind::Max),
                    AggregationRequest::Avg(p) => (p.field.clone(), AggKind::Avg),
                    AggregationRequest::Sum(p) => (p.field.clone(), AggKind::Sum),
                    AggregationRequest::ValueCount(p) => (p.field.clone(), AggKind::ValueCount),
                    AggregationRequest::Histogram(p) => (
                        p.field.clone(),
                        AggKind::Histogram {
                            interval: p.interval,
                        },
                    ),
                    AggregationRequest::Terms(p) => (p.field.clone(), AggKind::Terms),
                    AggregationRequest::GroupedMetrics(_) => return None,
                };
                Some(ResolvedAggSpec {
                    name: name.clone(),
                    field_name,
                    kind,
                })
            })
            .collect();
        Self { specs, schema }
    }
}

pub(crate) struct AggSegmentCollector {
    entries: Vec<(String, SegmentAggEntry)>,
}

impl tantivy::collector::Collector for AggCollector {
    type Fruit = std::collections::HashMap<String, crate::search::PartialAggResult>;
    type Child = AggSegmentCollector;

    fn for_segment(
        &self,
        _seg_id: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let ff = segment.fast_fields();
        let mut entries = Vec::with_capacity(self.specs.len());
        for spec in &self.specs {
            let entry = match &spec.kind {
                AggKind::Stats
                | AggKind::Min
                | AggKind::Max
                | AggKind::Avg
                | AggKind::Sum
                | AggKind::ValueCount => {
                    match open_num_col(&self.schema, ff, &spec.field_name, None) {
                        Some(col) => SegmentAggEntry::NumericStats {
                            column: col,
                            count: 0,
                            sum: 0.0,
                            min: f64::INFINITY,
                            max: f64::NEG_INFINITY,
                        },
                        None => SegmentAggEntry::Skip,
                    }
                }
                AggKind::Histogram { interval } => {
                    match open_num_col(&self.schema, ff, &spec.field_name, None) {
                        Some(col) => SegmentAggEntry::Histogram {
                            column: col,
                            interval: *interval,
                            buckets: std::collections::HashMap::new(),
                        },
                        None => SegmentAggEntry::Skip,
                    }
                }
                AggKind::Terms => {
                    if let Ok(Some(str_col)) = ff.str(&spec.field_name) {
                        let dictionary_size = str_col.num_terms();
                        if dictionary_size <= DENSE_TERMS_ORDINAL_LIMIT {
                            SegmentAggEntry::TermsStrDense {
                                column: str_col,
                                counts: vec![0; dictionary_size],
                                invalid_ordinal: None,
                            }
                        } else {
                            SegmentAggEntry::TermsStrSparse {
                                column: str_col,
                                counts: HashMap::new(),
                            }
                        }
                    } else if let Some(num_col) =
                        open_num_col(&self.schema, ff, &spec.field_name, None)
                    {
                        SegmentAggEntry::TermsNum {
                            column: num_col,
                            counts: std::collections::HashMap::new(),
                        }
                    } else {
                        SegmentAggEntry::Skip
                    }
                }
            };
            entries.push((spec.name.clone(), entry));
        }
        Ok(AggSegmentCollector { entries })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<tantivy::Result<Vec<(String, SegmentAggData)>>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged: std::collections::HashMap<String, SegmentAggData> =
            std::collections::HashMap::new();
        for fruit in segment_fruits {
            for (name, data) in fruit? {
                merged
                    .entry(name)
                    .and_modify(|e| merge_segment_data(e, &data))
                    .or_insert(data);
            }
        }
        let mut results = std::collections::HashMap::new();
        for spec in &self.specs {
            if let Some(data) = merged.remove(&spec.name) {
                results.insert(spec.name.clone(), convert_to_partial(&spec.kind, data));
            }
        }
        Ok(results)
    }
}

impl tantivy::collector::SegmentCollector for AggSegmentCollector {
    type Fruit = tantivy::Result<Vec<(String, SegmentAggData)>>;

    #[inline]
    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        for (_, entry) in &mut self.entries {
            match entry {
                SegmentAggEntry::NumericStats {
                    column,
                    count,
                    sum,
                    min,
                    max,
                } => {
                    if let Some(val) = column.first_f64(doc) {
                        *count += 1;
                        *sum += val;
                        if val < *min {
                            *min = val;
                        }
                        if val > *max {
                            *max = val;
                        }
                    }
                }
                SegmentAggEntry::Histogram {
                    column,
                    interval,
                    buckets,
                } => {
                    if let Some(val) = column.first_f64(doc) {
                        *buckets.entry((val / *interval).floor() as i64).or_insert(0) += 1;
                    }
                }
                SegmentAggEntry::TermsStrDense {
                    column,
                    counts,
                    invalid_ordinal,
                } => {
                    for ord in column.term_ords(doc) {
                        if let Some(count) = usize::try_from(ord)
                            .ok()
                            .and_then(|ordinal| counts.get_mut(ordinal))
                        {
                            *count += 1;
                        } else {
                            *invalid_ordinal = Some(ord);
                        }
                    }
                }
                SegmentAggEntry::TermsStrSparse { column, counts } => {
                    for ord in column.term_ords(doc) {
                        *counts.entry(ord).or_default() += 1;
                    }
                }
                SegmentAggEntry::TermsNum { column, counts } => {
                    if let Some(key) = column.first_term_key(doc) {
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
                SegmentAggEntry::Skip => {}
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        let mut fruits = Vec::with_capacity(self.entries.len());
        for (name, entry) in self.entries {
            let data = match entry {
                SegmentAggEntry::NumericStats {
                    count,
                    sum,
                    min,
                    max,
                    ..
                } => SegmentAggData::Stats {
                    count,
                    sum,
                    min,
                    max,
                },
                SegmentAggEntry::Histogram {
                    interval, buckets, ..
                } => SegmentAggData::Histogram { interval, buckets },
                SegmentAggEntry::TermsStrDense {
                    column,
                    counts,
                    invalid_ordinal,
                } => {
                    if let Some(ordinal) = invalid_ordinal {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "term ordinal {ordinal} exceeds dictionary size {}",
                                counts.len()
                            ),
                        )
                        .into());
                    }
                    let observed = counts.iter().filter(|count| **count > 0).count();
                    SegmentAggData::Terms {
                        counts: resolve_term_ordinals(
                            &column,
                            counts
                                .into_iter()
                                .enumerate()
                                .filter_map(|(ordinal, count)| {
                                    (count > 0).then_some((ordinal as u64, count))
                                }),
                            observed,
                        )?,
                    }
                }
                SegmentAggEntry::TermsStrSparse { column, counts } => {
                    let observed = counts.len();
                    SegmentAggData::Terms {
                        counts: resolve_term_ordinals(&column, counts, observed)?,
                    }
                }
                SegmentAggEntry::TermsNum { counts, .. } => SegmentAggData::Terms {
                    counts: counts
                        .into_iter()
                        .map(|(key, count)| (key.into_string(), count))
                        .collect(),
                },
                SegmentAggEntry::Skip => continue,
            };
            fruits.push((name, data));
        }
        Ok(fruits)
    }
}

impl super::SearchEngine for HotEngine {
    #[cfg(test)]
    fn writer_is_failed_for_test(&self) -> bool {
        HotEngine::writer_is_failed_for_test(self)
    }

    fn add_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
    ) -> Result<super::IndexWriteReceipt> {
        self.add_primary_index_with_side_effect(doc_id, payload, primary_term, |_| Ok(()))
    }

    fn add_document_with_condition_at_term(
        &self,
        doc_id: &str,
        payload: serde_json::Value,
        primary_term: u64,
        condition: super::WriteCondition,
    ) -> Result<super::IndexWriteReceipt> {
        self.add_primary_index_with_condition_and_side_effect(
            doc_id,
            payload,
            primary_term,
            condition,
            |_| Ok(()),
        )
    }

    fn bulk_add_documents_with_receipt_at_term(
        &self,
        docs: Vec<(String, serde_json::Value)>,
        primary_term: u64,
    ) -> Result<super::BulkWriteReceipt> {
        self.add_primary_bulk_with_side_effect(docs, primary_term, |_| Ok(()))
    }

    fn delete_document_with_receipt_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
    ) -> Result<super::DeleteWriteReceipt> {
        self.delete_primary_with_side_effect(doc_id, primary_term, |_| Ok(()))
    }

    fn delete_document_with_condition_at_term(
        &self,
        doc_id: &str,
        primary_term: u64,
        condition: super::WriteCondition,
    ) -> Result<super::DeleteWriteReceipt> {
        self.delete_primary_with_condition_and_side_effect(doc_id, primary_term, condition, |_| {
            Ok(())
        })
    }

    fn apply_replica_operation(
        &self,
        operation: super::SequencedOperation,
    ) -> Result<super::ReplicaApplyReceipt> {
        if let super::DocumentMutation::Index { source, .. } = &operation.mutation {
            self.validate_keyword_documents(std::iter::once(source))?;
        }
        self.apply_sequenced_operation_with_side_effect(&operation, |_| Ok(()))
    }

    fn apply_replica_batch(
        &self,
        operations: Vec<super::SequencedOperation>,
    ) -> Result<super::ReplicaBulkApplyReceipt> {
        self.validate_keyword_documents(operations.iter().filter_map(|operation| {
            if let super::DocumentMutation::Index { source, .. } = &operation.mutation {
                Some(source)
            } else {
                None
            }
        }))?;
        self.apply_sequenced_batch_with_side_effect(&operations, true, |_| Ok(()))
    }

    fn get_document(&self, doc_id: &str) -> Result<Option<serde_json::Value>> {
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let searcher = self.reader.searcher();
        let term = Term::from_field_text(registry.id_field, doc_id);
        let query = tantivy::query::TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        if let Some((_score, doc_address)) = top_docs.first() {
            let retrieved_doc = searcher.doc::<TantivyDocument>(*doc_address)?;
            for value in retrieved_doc.get_all(registry.source_field) {
                if let Some(text) = value.as_str()
                    && let Some(json_val) =
                        Self::decode_stored_source_with_registry(&registry, text)
                {
                    return Ok(Some(json_val));
                }
            }
        }
        Ok(None)
    }

    fn get_document_with_metadata(
        &self,
        doc_id: &str,
        realtime: bool,
    ) -> Result<Option<super::DocumentRead>> {
        if !realtime {
            return self.read_refreshed_document(doc_id);
        }
        self.with_translog("realtime GET", |translog| {
            let version = self.apply_state.lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
                .versions.lookup(doc_id)?;
            let Some(VersionValue::Index(version)) = version else {
                return if version.is_some() { Ok(None) } else { self.read_refreshed_document(doc_id) };
            };
            if let Some(position) = version.wal_position
                && let Some(mut entry) = translog.read_entry_at(position)?
            {
                if entry.seq_no != version.seq_no || entry.primary_term != version.primary_term {
                    anyhow::bail!("realtime WAL position has the wrong identity for document [{doc_id}]");
                }
                match document_operation(&entry)? {
                    WalDocumentOperation::Index { doc_id: entry_id, .. } if entry_id == doc_id => {}
                    _ => anyhow::bail!("realtime WAL position has the wrong operation for document [{doc_id}]"),
                }
                let mut source = entry.payload.get_mut("_source")
                    .expect("validated index WAL entry has a source").take();
                let registry = self.field_registry.read().unwrap_or_else(|error| error.into_inner());
                Self::normalize_result_source_with_registry(&registry, &mut source);
                return Ok(Some(super::DocumentRead {
                    source, seq_no: version.seq_no, primary_term: version.primary_term,
                }));
            }
            // Flush reloads the reader before pruning WAL, under this same mutex.
            // Refresh drops old map entries only after reader publication.
            let document = self.read_refreshed_document(doc_id)?;
            if document.as_ref().is_some_and(|document| document.seq_no > version.seq_no
                    || (document.seq_no == version.seq_no
                        && document.primary_term == version.primary_term)) {
                Ok(document)
            } else {
                anyhow::bail!(
                    "realtime source for document [{doc_id}] is missing from both WAL and the visible reader"
                )
            }
        })
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_documents(&self) -> Result<Vec<(String, serde_json::Value, u64, u64)>> {
        self.protocol_trace_documents_snapshot()
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_processed_sequences(&self) -> Result<Vec<u64>> {
        HotEngine::protocol_trace_processed_sequences(self)
    }

    #[cfg(feature = "protocol-trace")]
    fn protocol_trace_copy_evidence(&self) -> Result<super::ProtocolTraceCopyEvidence> {
        HotEngine::protocol_trace_copy_evidence(self)
    }

    fn refresh(&self) -> Result<()> {
        self.refresh_with_pruned_tombstones().map(|_| ())
    }

    fn flush(&self) -> Result<()> {
        let _maintenance = self.maintenance_guard("flush")?;
        self.with_translog("flush", |tl| {
            let mut writer_state = self.writer_state_with_replay(tl, "flush")?;
            let boundary = self.current_committed_boundary()?;
            let committed_boundary =
                self.commit_writer_at_boundary(&mut writer_state, "flush", boundary)?;
            drop(writer_state); // release lock before reader reload
            self.reader.reload()?;
            self.persist_committed_boundary(&committed_boundary)?;
            self.validate_truncation_boundary(tl, &committed_boundary)?;
            if let Some(processed_checkpoint) = committed_boundary.processed_checkpoint {
                tl.truncate_below(processed_checkpoint)?;
            }
            Ok(())
        })
    }

    fn force_merge(&self, max_num_segments: usize) -> Result<()> {
        if max_num_segments == 0 {
            anyhow::bail!("max_num_segments must be at least 1");
        }

        #[cfg(test)]
        if let Some(barrier) = {
            self.force_merge_entry_barrier
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        } {
            barrier.wait();
        }

        let _maintenance = self.maintenance_guard("force merge")?;
        let committed_boundary = self.with_translog("force merge", |translog| {
            self.pause_and_drain_automatic_merges(translog)
        })?;
        let restore_policy = AutomaticMergePolicyRestore {
            engine: self,
            armed: true,
        };

        let merge_result = (|| {
            self.persist_committed_boundary(&committed_boundary)?;
            self.reader.reload()?;

            loop {
                let segment_ids = self.index.searchable_segment_ids()?;
                if segment_ids.len() <= max_num_segments {
                    break;
                }

                let future = {
                    let mut writer_state = self
                        .writer
                        .write()
                        .unwrap_or_else(|error| error.into_inner());
                    writer_state.writer_mut("force merge")?.merge(&segment_ids)
                };
                let _: Option<tantivy::SegmentMeta> = future
                    .wait()
                    .context("Tantivy force-merge operation failed")?;
                self.reader.reload()?;
            }

            let final_segment_count = self.index.searchable_segment_ids()?.len();
            if final_segment_count > max_num_segments {
                anyhow::bail!(
                    "force merge completed with {final_segment_count} segments, above requested maximum {max_num_segments}"
                );
            }
            Ok(())
        })();
        let restore_result = restore_policy.restore();

        match (merge_result, restore_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => {
                Err(error).context("force merge completed but merge-policy restoration failed")
            }
            (Err(merge_error), Err(restore_error)) => Err(anyhow::anyhow!(
                "force merge failed: {merge_error:#}; merge-policy restoration also failed: {restore_error:#}"
            )),
        }
    }

    fn segment_infos(&self) -> Vec<super::SegmentInfo> {
        self.index
            .searchable_segments()
            .unwrap_or_default()
            .iter()
            .map(|seg| {
                let meta = seg.meta();
                super::SegmentInfo {
                    segment_id: meta.id().uuid_string(),
                    num_docs: meta.num_docs(),
                    deleted_docs: meta.num_deleted_docs(),
                }
            })
            .collect()
    }

    fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        let body_field = self.resolve_field("body");
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![body_field]);
        let query = query_parser.parse_query(query_str)?;
        self.execute_search(searcher, &*query, 100)
    }

    fn search_query(
        &self,
        req: &crate::search::SearchRequest,
    ) -> Result<(
        Vec<serde_json::Value>,
        usize,
        std::collections::HashMap<String, crate::search::PartialAggResult>,
    )> {
        let searcher = self.reader.searcher();
        // Use the exact requested limit when from+size is explicit.
        // The coordinator handles cross-shard merging at the API layer.
        let limit = req.from + req.size;
        let user_query = self.build_query(&req.query)?;
        // search_after: build a separate hits_query that ANDs the cursor filter
        // onto the user query. The cursor filter must NOT bias total counts or
        // aggregations — Count and aggregation collectors always run against
        // user_query so /_search returns the same total/aggs across all pages.
        let hits_query: Option<Box<dyn tantivy::query::Query>> =
            if let Some(cursor) = &req.search_after {
                use tantivy::query::{BooleanQuery, Occur};
                let filter = self.build_search_after_filter(&req.sort, cursor)?;
                let q_for_hits = self.build_query(&req.query)?;
                Some(Box::new(BooleanQuery::new(vec![
                    (Occur::Must, q_for_hits),
                    (Occur::Must, filter),
                ])))
            } else {
                None
            };
        let effective_limit = if limit == 0 { 1 } else { limit };

        if GroupedAggCollector::has_grouped_metrics(&req.aggs) {
            // Fast path: match_all + grouped partials → direct columnar scan
            // Bypasses Tantivy's scorer/collector entirely for unfiltered aggregations.
            if req.query.is_match_all() && req.size == 0 {
                let total = searcher
                    .segment_readers()
                    .iter()
                    .map(|r| r.max_doc() as usize)
                    .sum();
                let partial_aggs = self.grouped_partials_direct_scan(req)?;
                return Ok((Vec::new(), total, partial_aggs));
            }

            let grouped_collector = GroupedAggCollector::from_request(
                &req.aggs,
                self.index.schema(),
                self.column_cache.clone(),
            );

            if req.size == 0 {
                // size=0: no hits returned; cursor is moot. Run against user_query.
                let (partial_aggs, total) =
                    searcher.search(&*user_query, &(grouped_collector, Count))?;
                return Ok((Vec::new(), total, partial_aggs));
            }

            if let Some(hq) = hits_query.as_ref() {
                // search_after with grouped aggs: two searches.
                // 1) hits_query → TopDocs (post-cursor hits only)
                // 2) user_query → grouped aggs + Count (unfiltered totals)
                let top_docs =
                    searcher.search(hq.as_ref(), &TopDocs::with_limit(effective_limit))?;
                let (partial_aggs, total) =
                    searcher.search(&*user_query, &(grouped_collector, Count))?;
                let mut hits = self.collect_hits(&searcher, top_docs)?;
                crate::search::sort_hits(&mut hits, &req.sort);
                return Ok((hits, total, partial_aggs));
            }

            let (top_docs, partial_aggs, total) = searcher.search(
                &*user_query,
                &(
                    TopDocs::with_limit(effective_limit),
                    grouped_collector,
                    Count,
                ),
            )?;
            let mut hits = self.collect_hits(&searcher, top_docs)?;
            crate::search::sort_hits(&mut hits, &req.sort);
            return Ok((hits, total, partial_aggs));
        }

        // Build optional aggregation collector (None = zero overhead when no aggs)
        let agg_collector = if !req.aggs.is_empty() {
            Some(AggCollector::from_request(&req.aggs, self.index.schema()))
        } else {
            None
        };

        // size=0 requests never need hits, so skip TopDocs entirely.
        if req.size == 0 {
            let (partial_aggs, total) = searcher.search(&*user_query, &(agg_collector, Count))?;
            return Ok((Vec::new(), total, partial_aggs.unwrap_or_default()));
        }

        // Fast-field sort: push sorting into Tantivy collector for numeric fields
        if let Some((sort_field, order)) = self.extract_fast_field_sort(req) {
            let schema = self.index.schema();
            let field = self.resolve_field(&sort_field);
            let field_type = schema.get_field_entry(field).field_type();

            // Helper closure: runs the fast-field-sorted TopDocs collector and,
            // when search_after is present, runs Count + aggs separately against
            // the unfiltered user_query.
            macro_rules! run_fast_field_sort {
                ($ty:ty) => {{
                    let td = TopDocs::with_limit(effective_limit)
                        .order_by_fast_field::<$ty>(&sort_field, order);
                    if let Some(hq) = hits_query.as_ref() {
                        let top_docs = searcher.search(hq.as_ref(), &td)?;
                        let (partial_aggs, total) =
                            searcher.search(&*user_query, &(agg_collector, Count))?;
                        let mut hits = self.collect_hits_sorted(&searcher, top_docs)?;
                        crate::search::sort_hits(&mut hits, &req.sort);
                        return Ok((hits, total, partial_aggs.unwrap_or_default()));
                    }
                    let (top_docs, partial_aggs, total) =
                        searcher.search(&*user_query, &(td, agg_collector, Count))?;
                    let mut hits = self.collect_hits_sorted(&searcher, top_docs)?;
                    crate::search::sort_hits(&mut hits, &req.sort);
                    return Ok((hits, total, partial_aggs.unwrap_or_default()));
                }};
            }

            match field_type {
                tantivy::schema::FieldType::F64(_) => run_fast_field_sort!(f64),
                tantivy::schema::FieldType::I64(_) => run_fast_field_sort!(i64),
                tantivy::schema::FieldType::U64(_) => run_fast_field_sort!(u64),
                _ => {} // fall through to default score-based collection
            }
        }

        if let Some(hq) = hits_query.as_ref() {
            // search_after default path: two searches.
            let top_docs = searcher.search(hq.as_ref(), &TopDocs::with_limit(effective_limit))?;
            let (partial_aggs, total) = searcher.search(&*user_query, &(agg_collector, Count))?;
            let mut hits = self.collect_hits(&searcher, top_docs)?;
            crate::search::sort_hits(&mut hits, &req.sort);
            return Ok((hits, total, partial_aggs.unwrap_or_default()));
        }

        let (top_docs, partial_aggs, total) = searcher.search(
            &*user_query,
            &(TopDocs::with_limit(effective_limit), agg_collector, Count),
        )?;
        let mut hits = self.collect_hits(&searcher, top_docs)?;
        crate::search::sort_hits(&mut hits, &req.sort);
        Ok((hits, total, partial_aggs.unwrap_or_default()))
    }

    fn sql_record_batch(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
    ) -> Result<Option<super::SqlBatchResult>> {
        Ok(Some(HotEngine::sql_record_batch(
            self,
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
        if !self.can_stream_sql_batches(columns, needs_score) {
            return Ok(None);
        }

        Ok(Some(HotEngine::sql_streaming_batch_handle(
            self,
            req,
            columns,
            needs_id,
            needs_score,
            batch_size,
        )?))
    }

    fn create_peer_recovery_snapshot(
        &self,
        snapshot_dir: &Path,
    ) -> Result<super::PeerRecoverySnapshot> {
        let prepared = self.prepare_peer_recovery_snapshot(snapshot_dir)?;

        #[cfg(test)]
        if let Some(sender) = self
            .peer_recovery_snapshot_ready_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let snapshot_next_seq_no = prepared
                .committed_boundary
                .max_seq_no
                .and_then(|seq_no| seq_no.checked_add(1))
                .unwrap_or(0);
            let _ = sender.send(snapshot_next_seq_no);
        }
        #[cfg(test)]
        if let Some(receiver) = self
            .peer_recovery_snapshot_release_receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = receiver.recv();
        }

        let prepared = match prepared.hash_files(snapshot_dir) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = std::fs::remove_dir_all(snapshot_dir);
                return Err(error);
            }
        };
        Ok(super::PeerRecoverySnapshot {
            snapshot_cursor: prepared.snapshot_cursor,
            #[cfg(test)]
            snapshot_next_seq_no: prepared.snapshot_next_seq_no,
            #[cfg(feature = "protocol-trace")]
            trace_processed_seqs: prepared.trace_processed_seqs,
            #[cfg(feature = "protocol-trace")]
            trace_documents: prepared.trace_documents,
            committed_boundary: prepared.committed_boundary,
            retention_pin_id: prepared.retention_pin.into_pin_id(),
            files: prepared.files,
        })
    }

    fn prepare_peer_recovery_snapshot(
        &self,
        snapshot_dir: &Path,
    ) -> Result<super::PeerRecoverySnapshotPreparation> {
        if snapshot_dir.exists() {
            anyhow::bail!("peer recovery snapshot directory already exists: {snapshot_dir:?}");
        }
        std::fs::create_dir_all(snapshot_dir)?;

        let _maintenance = self.maintenance_guard("peer recovery snapshot")?;
        let preparation = self.with_translog("peer recovery snapshot", |translog| {
            let mut writer_state =
                self.writer_state_with_replay(translog, "peer recovery snapshot")?;
            let boundary = self.current_committed_boundary()?;
            if boundary.processed_checkpoint != boundary.max_seq_no {
                anyhow::bail!(
                    "peer recovery snapshot requires a gap-free source: processed checkpoint {:?} does not equal maximum sequence {:?}",
                    boundary.processed_checkpoint,
                    boundary.max_seq_no
                );
            }
            let committed_boundary = self.commit_writer_at_boundary(
                &mut writer_state,
                "peer recovery snapshot",
                boundary,
            )?;
            drop(writer_state);
            self.persist_committed_boundary_durable(&committed_boundary)?;
            let snapshot_cursor = translog.recovery_read_snapshot()?.end_cursor();
            let retention_floor = committed_boundary
                .processed_checkpoint
                .and_then(|checkpoint| checkpoint.checked_add(1))
                .unwrap_or(0);
            let retention_pin_id = translog.register_retention_pin(retention_floor)?;

            let result = (|| {
                let file_names = self.peer_recovery_file_names()?;
                let index_path = self
                    .committed_boundary_path
                    .parent()
                    .expect("committed checkpoint path has a parent")
                    .join("index");
                for name in &file_names {
                    let source = index_path.join(name);
                    let destination = snapshot_dir.join(name);
                    std::fs::hard_link(&source, &destination).with_context(|| {
                        format!(
                            "hard-link peer recovery file {source:?} to {destination:?}; unlocked copies are not permitted"
                        )
                    })?;
                }
                std::fs::File::open(snapshot_dir)?.sync_all()?;
                #[cfg(feature = "protocol-trace")]
                {
                    self.reader.reload()?;
                    let processed_seqs = self.protocol_trace_processed_sequences()?;
                    let documents = self.protocol_trace_documents_snapshot()?;
                    Ok((file_names, processed_seqs, documents))
                }
                #[cfg(not(feature = "protocol-trace"))]
                Ok(file_names)
            })();

            #[cfg(feature = "protocol-trace")]
            match result {
                Ok((file_names, processed_seqs, documents)) => Ok((
                    snapshot_cursor,
                    committed_boundary,
                    retention_pin_id,
                    file_names,
                    processed_seqs,
                    documents,
                )),
                Err(error) => {
                    let _ = translog.release_retention_pin(retention_pin_id);
                    let _ = std::fs::remove_dir_all(snapshot_dir);
                    Err(error)
                }
            }
            #[cfg(not(feature = "protocol-trace"))]
            match result {
                Ok(file_names) => Ok((
                    snapshot_cursor,
                    committed_boundary,
                    retention_pin_id,
                    file_names,
                )),
                Err(error) => {
                    let _ = translog.release_retention_pin(retention_pin_id);
                    let _ = std::fs::remove_dir_all(snapshot_dir);
                    Err(error)
                }
            }
        });
        #[cfg(feature = "protocol-trace")]
        let (
            snapshot_cursor,
            committed_boundary,
            retention_pin_id,
            file_names,
            trace_processed_seqs,
            trace_documents,
        ) = match preparation {
            Ok(preparation) => preparation,
            Err(error) => {
                let _ = std::fs::remove_dir_all(snapshot_dir);
                return Err(error);
            }
        };
        #[cfg(not(feature = "protocol-trace"))]
        let (snapshot_cursor, committed_boundary, retention_pin_id, file_names) = match preparation
        {
            Ok(preparation) => preparation,
            Err(error) => {
                let _ = std::fs::remove_dir_all(snapshot_dir);
                return Err(error);
            }
        };
        drop(_maintenance);
        Ok(super::PeerRecoverySnapshotPreparation {
            snapshot_cursor,
            #[cfg(test)]
            snapshot_next_seq_no: committed_boundary
                .max_seq_no
                .and_then(|seq_no| seq_no.checked_add(1))
                .unwrap_or(0),
            #[cfg(feature = "protocol-trace")]
            trace_processed_seqs,
            #[cfg(feature = "protocol-trace")]
            trace_documents,
            committed_boundary,
            retention_pin: super::PeerRecoveryRetentionPin::new(
                self.translog.clone(),
                retention_pin_id,
            ),
            file_names,
        })
    }

    fn release_peer_recovery_pin(&self, pin_id: u64) -> Result<()> {
        self.with_translog("peer recovery pin release", |translog| {
            translog.release_retention_pin(pin_id)
        })
    }

    fn peer_recovery_ops(
        &self,
        cursor: crate::wal::WalCursor,
        end_cursor: Option<crate::wal::WalCursor>,
        max_ops: usize,
        max_bytes: usize,
    ) -> Result<super::PeerRecoveryOpsBatch> {
        let snapshot = self.with_translog("peer recovery operation snapshot", |translog| {
            translog.recovery_read_snapshot()
        })?;
        let end_cursor = end_cursor.unwrap_or_else(|| snapshot.end_cursor());
        if end_cursor.position() > snapshot.end_cursor().position() {
            anyhow::bail!("peer recovery end cursor exceeds the captured WAL end");
        }
        let checkpoints = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
            .checkpoints
            .clone();
        let batch = snapshot.read_bounded_cursor_while(
            cursor,
            end_cursor,
            max_ops,
            max_bytes,
            |seq_no| checkpoints.has_processed(seq_no),
        )?;
        Ok(super::PeerRecoveryOpsBatch {
            operations: batch.entries,
            next_cursor: batch.next_cursor,
            source_max_seq_no: checkpoints.stats().max_seq_no,
            complete: batch.complete,
        })
    }

    fn retained_recovery_ops(
        &self,
        min_seq_no: u64,
        max_ops: usize,
        max_bytes: usize,
    ) -> Result<super::PeerRecoveryOpsBatch> {
        let snapshot = self.with_translog("retained recovery operation snapshot", |translog| {
            translog.recovery_read_snapshot()
        })?;
        let (operations, complete) = snapshot.read_bounded_range(min_seq_no, max_ops, max_bytes)?;
        let checkpoints = self
            .apply_state
            .lock()
            .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
            .checkpoints
            .clone();
        Ok(super::PeerRecoveryOpsBatch {
            operations: operations
                .into_iter()
                .filter(|entry| checkpoints.has_processed(entry.seq_no))
                .collect(),
            next_cursor: snapshot.end_cursor(),
            source_max_seq_no: checkpoints.stats().max_seq_no,
            complete,
        })
    }

    fn peer_recovery_barrier(&self) -> Result<super::PeerRecoveryBarrier> {
        self.with_translog("peer recovery barrier", |translog| {
            drop(self.writer_state_with_replay(translog, "peer recovery barrier")?);
            let wal_end = translog.recovery_read_snapshot()?.end_cursor();
            let sequence = self
                .apply_state
                .lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?
                .checkpoints
                .stats();
            Ok(super::PeerRecoveryBarrier { wal_end, sequence })
        })
    }

    fn prepare_primary_activation(
        &self,
        primary_term: u64,
    ) -> Result<Vec<super::SequencedOperation>> {
        let _maintenance = self.maintenance_guard("primary activation")?;
        self.with_translog("primary activation", |translog| {
            {
                let mut writer_state = self
                    .writer
                    .write()
                    .unwrap_or_else(|error| error.into_inner());
                writer_state.fail("primary activation requires full local WAL replay");
            }
            drop(self.writer_state_with_replay(translog, "primary activation")?);

            let max_seq_no = self
                .sequence_stats()
                .max_seq_no
                .into_iter()
                .chain(translog.max_seq_no())
                .max();
            let Some(max_seq_no) = max_seq_no else {
                return Ok(Vec::new());
            };
            let missing = self.missing_sequence_intervals_through(max_seq_no);
            let operations = missing
                .into_iter()
                .flat_map(|range| range.map(|seq_no| (seq_no, primary_term)))
                .map(|(seq_no, primary_term)| super::SequencedOperation {
                    seq_no,
                    primary_term,
                    mutation: super::DocumentMutation::NoOp {
                        reason: "promotion gap".to_string(),
                    },
                })
                .collect::<Vec<_>>();
            if operations.is_empty() {
                return Ok(operations);
            }
            #[cfg(feature = "protocol-trace")]
            crate::protocol_trace::with_apply_scope(
                crate::protocol_trace::ApplyOrigin::Promotion,
                operations.clone(),
                || {
                    self.apply_sequenced_batch_locked(
                        translog,
                        &operations,
                        WalDisposition::Append,
                        None,
                        false,
                        |_| Ok(()),
                    )
                },
            )?;
            #[cfg(not(feature = "protocol-trace"))]
            self.apply_sequenced_batch_locked(
                translog,
                &operations,
                WalDisposition::Append,
                None,
                false,
                |_| Ok(()),
            )?;
            translog.sync()?;
            let mut state = self
                .apply_state
                .lock()
                .map_err(|_| anyhow::anyhow!("apply state lock poisoned"))?;
            for operation in &operations {
                state.checkpoints.mark_persisted(operation.seq_no);
            }
            #[cfg(feature = "protocol-trace")]
            if let Some(copy) = crate::protocol_trace::current_open_copy() {
                crate::protocol_trace::record_promotion_noop_fill(
                    &copy,
                    primary_term,
                    &operations,
                    state.checkpoints.stats(),
                )?;
            }
            Ok(operations)
        })
    }

    fn peer_recovery_commit_files(&self) -> Result<Vec<String>> {
        self.peer_recovery_file_names()
    }

    fn doc_count(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    fn sequence_stats(&self) -> SequenceStats {
        HotEngine::sequence_stats(self)
    }

    fn wal_max_seq_no(&self) -> Option<u64> {
        HotEngine::wal_max_seq_no(self)
    }

    fn reconcile_term_sequence_state(
        &self,
        identity_fence: u64,
        identity_fence_max_seq_no: Option<u64>,
    ) -> Result<()> {
        HotEngine::reconcile_term_sequence_state(self, identity_fence, identity_fence_max_seq_no)
    }

    fn current_primary_term(&self) -> u64 {
        HotEngine::current_primary_term(self)
    }
}

// ─── BitSet Collector ───────────────────────────────────────────────────────
// Collects ALL matching doc IDs as a bitset per segment.
// Memory: 1 bit per doc in the segment. 4M docs = 500KB. Trivial.
// Used for GROUP BY fallback queries that need to see all matches.

/// Per-segment bitset of matched doc IDs.
struct SegmentBitSet {
    segment_ord: u32,
    words: Vec<u64>,
    max_doc: u32,
    count: u32,
}

impl SegmentBitSet {
    fn new(segment_ord: u32, max_doc: u32) -> Self {
        let num_words = (max_doc as usize).div_ceil(64);
        Self {
            segment_ord,
            words: vec![0u64; num_words],
            max_doc,
            count: 0,
        }
    }

    #[inline]
    fn set(&mut self, doc_id: u32) {
        let word_idx = (doc_id / 64) as usize;
        let bit_idx = doc_id % 64;
        let mask = 1u64 << bit_idx;
        if self.words[word_idx] & mask == 0 {
            self.words[word_idx] |= mask;
            self.count += 1;
        }
    }
}

/// Collects matched doc IDs into per-segment bitsets.
struct BitSetCollector;

struct BitSetSegmentCollector {
    bitset: SegmentBitSet,
}

impl tantivy::collector::Collector for BitSetCollector {
    type Fruit = Vec<SegmentBitSet>;
    type Child = BitSetSegmentCollector;

    fn for_segment(
        &self,
        seg_ord: u32,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(BitSetSegmentCollector {
            bitset: SegmentBitSet::new(seg_ord, segment.max_doc()),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<SegmentBitSet>,
    ) -> tantivy::Result<Vec<SegmentBitSet>> {
        Ok(segment_fruits)
    }
}

impl tantivy::collector::SegmentCollector for BitSetSegmentCollector {
    type Fruit = SegmentBitSet;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        self.bitset.set(doc);
    }

    fn harvest(self) -> SegmentBitSet {
        self.bitset
    }
}

// ─── Streaming batch reader ────────────────────────────────────────────────
// Reads fast-field columns for matched docs (from bitset) and produces
// Arrow RecordBatches of `batch_size` rows each.

/// Streaming batch size for bitset-based GROUP BY fallback queries.
const STREAMING_BATCH_SIZE: usize = 8192;

struct SegmentBitSetCursor {
    segment_ord: u32,
    max_doc: u32,
    words: Vec<u64>,
    next_word_idx: usize,
    current_word: u64,
    current_base: u32,
}

impl SegmentBitSetCursor {
    fn new(bitset: SegmentBitSet) -> Self {
        Self {
            segment_ord: bitset.segment_ord,
            max_doc: bitset.max_doc,
            words: bitset.words,
            next_word_idx: 0,
            current_word: 0,
            current_base: 0,
        }
    }

    fn next_doc(&mut self) -> Option<u32> {
        loop {
            if self.current_word != 0 {
                let tz = self.current_word.trailing_zeros();
                self.current_word &= self.current_word - 1;
                return Some(self.current_base + tz);
            }

            let next_word = *self.words.get(self.next_word_idx)?;
            self.current_base = (self.next_word_idx as u32) * 64;
            self.current_word = next_word;
            self.next_word_idx += 1;
        }
    }

    fn finished(&self) -> bool {
        self.current_word == 0 && self.next_word_idx >= self.words.len()
    }
}

struct StreamingSegmentState {
    cursor: SegmentBitSetCursor,
    id_reader: Option<StringFastFieldReader>,
    field_readers: Vec<SqlFieldReader>,
    id_builder: datafusion::arrow::array::StringBuilder,
    score_builder: datafusion::arrow::array::Float32Builder,
    col_builders: Vec<ColumnBuilder>,
    id_text: String,
    doc_buffer: Vec<tantivy::DocId>,
    id_ords: Vec<Option<u64>>,
    rows_in_batch: usize,
}

impl StreamingSegmentState {
    fn new(
        cursor: SegmentBitSetCursor,
        id_reader: Option<StringFastFieldReader>,
        field_readers: Vec<SqlFieldReader>,
        batch_size: usize,
    ) -> Self {
        let col_builders = field_readers
            .iter()
            .map(|reader| ColumnBuilder::new(reader, batch_size))
            .collect();

        Self {
            cursor,
            id_reader,
            field_readers,
            id_builder: datafusion::arrow::array::StringBuilder::with_capacity(
                batch_size,
                batch_size * 16,
            ),
            score_builder: datafusion::arrow::array::Float32Builder::with_capacity(batch_size),
            col_builders,
            id_text: String::new(),
            doc_buffer: Vec::with_capacity(batch_size),
            id_ords: vec![None; batch_size],
            rows_in_batch: 0,
        }
    }
}

struct StreamingBatchState {
    searcher: tantivy::Searcher,
    schema: tantivy::schema::Schema,
    columns: Vec<String>,
    field_types: HashMap<String, crate::cluster::state::FieldType>,
    arrow_schema: std::sync::Arc<datafusion::arrow::datatypes::Schema>,
    batch_size: usize,
    needs_id: bool,
    remaining_segments: std::vec::IntoIter<SegmentBitSet>,
    current_segment: Option<StreamingSegmentState>,
    empty_batch_pending: bool,
}

impl StreamingBatchState {
    fn open_next_segment(&mut self) -> Result<bool> {
        while let Some(segment_bitset) = self.remaining_segments.next() {
            if segment_bitset.count == 0 {
                continue;
            }

            let cursor = SegmentBitSetCursor::new(segment_bitset);
            let seg_ord = cursor.segment_ord as usize;
            let Some(seg_reader) = self.searcher.segment_readers().get(seg_ord) else {
                continue;
            };
            let fast_fields = seg_reader.fast_fields();
            let field_readers = self
                .columns
                .iter()
                .map(|col_name| {
                    open_sql_field_reader(
                        &self.schema,
                        fast_fields,
                        col_name,
                        self.field_types.get(col_name),
                    )
                })
                .collect();
            let id_reader = if self.needs_id {
                StringFastFieldReader::open(fast_fields, "_id")
            } else {
                None
            };

            self.current_segment = Some(StreamingSegmentState::new(
                cursor,
                id_reader,
                field_readers,
                self.batch_size,
            ));
            return Ok(true);
        }

        self.current_segment = None;
        Ok(false)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.empty_batch_pending {
            self.empty_batch_pending = false;
            return Ok(Some(RecordBatch::new_empty(self.arrow_schema.clone())));
        }

        loop {
            if self.current_segment.is_none() && !self.open_next_segment()? {
                return Ok(None);
            }

            let segment = self
                .current_segment
                .as_mut()
                .expect("segment state must exist after open_next_segment");

            segment.doc_buffer.clear();
            while segment.doc_buffer.len() < self.batch_size {
                let Some(doc_id) = segment.cursor.next_doc() else {
                    break;
                };

                if doc_id >= segment.cursor.max_doc {
                    break;
                }

                segment.doc_buffer.push(doc_id);
            }

            if !segment.doc_buffer.is_empty() {
                HotEngine::append_streaming_batch_values(segment, self.needs_id);
                segment.rows_in_batch = segment.doc_buffer.len();
            }

            if segment.rows_in_batch > 0 {
                let batch = HotEngine::finalize_streaming_batch(
                    &self.arrow_schema,
                    &mut segment.id_builder,
                    &mut segment.score_builder,
                    &mut segment.col_builders,
                )?;
                segment.rows_in_batch = 0;
                if segment.cursor.finished() {
                    self.current_segment = None;
                }
                return Ok(Some(batch));
            }

            self.current_segment = None;
        }
    }
}

impl HotEngine {
    /// Streaming is only valid when every requested column is fast-field backed
    /// on every segment and the query does not require synthetic BM25 `_score`.
    fn can_stream_sql_batches(&self, columns: &[String], needs_score: bool) -> bool {
        if needs_score {
            return false;
        }

        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        searcher.segment_readers().iter().all(|segment_reader| {
            let fast_fields = segment_reader.fast_fields();
            columns.iter().all(|column| {
                !matches!(
                    open_sql_field_reader(
                        &schema,
                        fast_fields,
                        column,
                        registry.field_types.get(column),
                    ),
                    SqlFieldReader::SourceFallback
                )
            })
        })
    }

    /// Build a lazy batch handle for bitset-based SQL streaming. The caller gets
    /// `total_hits` / `collected_rows` immediately and can pull batches incrementally.
    pub fn sql_streaming_batch_handle(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        _needs_score: bool,
        batch_size: usize,
    ) -> Result<super::SqlStreamingBatchHandle> {
        let searcher = self.reader.searcher();
        let query = self.build_query(&req.query)?;

        // Collect all matching docs as bitsets + total count
        let (segment_bitsets, total_hits) = searcher.search(&*query, &(BitSetCollector, Count))?;

        let schema = self.index.schema();
        let registry = self
            .field_registry
            .read()
            .unwrap_or_else(|e| e.into_inner());

        // Build the Arrow schema: _id, _score, then user columns
        let mut arrow_fields = Vec::with_capacity(columns.len() + 2);
        arrow_fields.push(datafusion::arrow::datatypes::Field::new(
            "_id",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ));
        arrow_fields.push(datafusion::arrow::datatypes::Field::new(
            "_score",
            datafusion::arrow::datatypes::DataType::Float32,
            false,
        ));
        for col_name in columns {
            let dt = column_kind_for_column(&schema, registry.field_types.get(col_name), col_name)
                .to_arrow_type();
            arrow_fields.push(datafusion::arrow::datatypes::Field::new(col_name, dt, true));
        }
        let arrow_schema =
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(arrow_fields));

        let batch_size = if batch_size == 0 {
            STREAMING_BATCH_SIZE
        } else {
            batch_size
        };

        let non_empty: Vec<SegmentBitSet> = segment_bitsets
            .into_iter()
            .filter(|s| s.count > 0)
            .collect();
        let mut state = StreamingBatchState {
            searcher,
            schema,
            columns: columns.to_vec(),
            field_types: registry.field_types.clone(),
            arrow_schema,
            batch_size,
            needs_id,
            remaining_segments: non_empty.into_iter(),
            current_segment: None,
            empty_batch_pending: total_hits == 0,
        };

        Ok(super::SqlStreamingBatchHandle::new(
            total_hits,
            total_hits,
            move || state.next_batch(),
        ))
    }

    /// Collect ALL matching docs via bitset, read fast-field columns in streaming
    /// batches, and drain them eagerly into memory. The lazy handle above is the
    /// primary implementation; this wrapper exists for tests and buffered callers.
    pub fn sql_streaming_batches(
        &self,
        req: &crate::search::SearchRequest,
        columns: &[String],
        needs_id: bool,
        needs_score: bool,
        batch_size: usize,
    ) -> Result<super::SqlStreamingResult> {
        let mut handle =
            self.sql_streaming_batch_handle(req, columns, needs_id, needs_score, batch_size)?;
        let total_hits = handle.total_hits;
        let collected_rows = handle.collected_rows;
        let mut batches = Vec::new();
        while let Some(batch) = handle.next_batch()? {
            batches.push(batch);
        }

        Ok(super::SqlStreamingResult {
            batches,
            total_hits,
            collected_rows,
        })
    }

    fn append_streaming_batch_values(segment: &mut StreamingSegmentState, needs_id: bool) {
        let doc_ids = segment.doc_buffer.as_slice();

        if needs_id {
            if segment.id_ords.len() < doc_ids.len() {
                segment.id_ords.resize(doc_ids.len(), None);
            }

            if let Some(reader) = &segment.id_reader {
                for ord in &mut segment.id_ords[..doc_ids.len()] {
                    *ord = None;
                }
                reader.first_ords_batch(doc_ids, &mut segment.id_ords[..doc_ids.len()]);

                for ord in &segment.id_ords[..doc_ids.len()] {
                    if let Some(ord) = ord {
                        segment.id_text.clear();
                        if reader.ord_to_str(*ord, &mut segment.id_text) {
                            segment.id_builder.append_value(&segment.id_text);
                        } else {
                            segment.id_builder.append_value("");
                        }
                    } else {
                        segment.id_builder.append_value("");
                    }
                }
            } else {
                for _ in doc_ids {
                    segment.id_builder.append_value("");
                }
            }
        } else {
            for _ in doc_ids {
                segment.id_builder.append_value("");
            }
        }

        for _ in doc_ids {
            segment.score_builder.append_value(0.0);
        }

        for (builder, reader) in segment
            .col_builders
            .iter_mut()
            .zip(segment.field_readers.iter())
        {
            builder.append_batch(reader, doc_ids);
        }
    }

    fn finalize_streaming_batch(
        schema: &std::sync::Arc<datafusion::arrow::datatypes::Schema>,
        id_builder: &mut datafusion::arrow::array::StringBuilder,
        score_builder: &mut datafusion::arrow::array::Float32Builder,
        col_builders: &mut [ColumnBuilder],
    ) -> Result<RecordBatch> {
        let mut arrays: Vec<datafusion::arrow::array::ArrayRef> =
            Vec::with_capacity(col_builders.len() + 2);
        arrays.push(std::sync::Arc::new(id_builder.finish()));
        arrays.push(std::sync::Arc::new(score_builder.finish()));
        for builder in col_builders.iter_mut() {
            arrays.push(builder.finish());
        }
        Ok(RecordBatch::try_new(schema.clone(), arrays)?)
    }
}

// ─── Column builder helpers for streaming batches ──────────────────────────

enum ColumnBuilder {
    F64 {
        builder: datafusion::arrow::array::Float64Builder,
        values: Vec<Option<f64>>,
    },
    I64 {
        builder: datafusion::arrow::array::Int64Builder,
        values: Vec<Option<i64>>,
    },
    TimestampMillis {
        builder: datafusion::arrow::array::TimestampMillisecondBuilder,
        values: Vec<Option<i64>>,
    },
    Str {
        builder: datafusion::arrow::array::StringBuilder,
        scratch: String,
        ords: Vec<Option<u64>>,
    },
    Null(datafusion::arrow::array::StringBuilder),
}

impl ColumnBuilder {
    fn new(reader: &SqlFieldReader, capacity: usize) -> Self {
        match reader {
            SqlFieldReader::F64(_) => ColumnBuilder::F64 {
                builder: datafusion::arrow::array::Float64Builder::with_capacity(capacity),
                values: vec![None; capacity],
            },
            SqlFieldReader::I64(_) => ColumnBuilder::I64 {
                builder: datafusion::arrow::array::Int64Builder::with_capacity(capacity),
                values: vec![None; capacity],
            },
            SqlFieldReader::DateMillis(_) => ColumnBuilder::TimestampMillis {
                builder: datafusion::arrow::array::TimestampMillisecondBuilder::with_capacity(
                    capacity,
                )
                .with_timezone("UTC".to_string()),
                values: vec![None; capacity],
            },
            SqlFieldReader::Str(_) => ColumnBuilder::Str {
                builder: datafusion::arrow::array::StringBuilder::with_capacity(
                    capacity,
                    capacity * 16,
                ),
                scratch: String::new(),
                ords: vec![None; capacity],
            },
            SqlFieldReader::SourceFallback => {
                ColumnBuilder::Null(datafusion::arrow::array::StringBuilder::new())
            }
        }
    }

    fn append_batch(&mut self, reader: &SqlFieldReader, docs: &[tantivy::DocId]) {
        match (self, reader) {
            (ColumnBuilder::F64 { builder, values }, SqlFieldReader::F64(col)) => {
                if values.len() < docs.len() {
                    values.resize(docs.len(), None);
                }
                for value in &mut values[..docs.len()] {
                    *value = None;
                }
                col.first_vals(docs, &mut values[..docs.len()]);
                for value in values[..docs.len()].iter().copied() {
                    match value {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
            }
            (ColumnBuilder::I64 { builder, values }, SqlFieldReader::I64(col)) => {
                if values.len() < docs.len() {
                    values.resize(docs.len(), None);
                }
                for value in &mut values[..docs.len()] {
                    *value = None;
                }
                col.first_vals(docs, &mut values[..docs.len()]);
                for value in values[..docs.len()].iter().copied() {
                    match value {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
            }
            (
                ColumnBuilder::TimestampMillis { builder, values },
                SqlFieldReader::DateMillis(col),
            ) => {
                if values.len() < docs.len() {
                    values.resize(docs.len(), None);
                }
                for value in &mut values[..docs.len()] {
                    *value = None;
                }
                col.first_vals(docs, &mut values[..docs.len()]);
                for value in values[..docs.len()].iter().copied() {
                    match value {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
            }
            (
                ColumnBuilder::Str {
                    builder,
                    scratch,
                    ords,
                },
                SqlFieldReader::Str(col),
            ) => {
                if ords.len() < docs.len() {
                    ords.resize(docs.len(), None);
                }
                for ord in &mut ords[..docs.len()] {
                    *ord = None;
                }
                col.first_ords_batch(docs, &mut ords[..docs.len()]);
                for ord in ords[..docs.len()].iter().copied() {
                    if let Some(ord) = ord {
                        scratch.clear();
                        if col.ord_to_str(ord, scratch) {
                            builder.append_value(scratch.as_str());
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
            }
            (ColumnBuilder::Null(builder), _) => {
                for _ in docs {
                    builder.append_null();
                }
            }
            (_, SqlFieldReader::SourceFallback) => {
                unreachable!(
                    "SourceFallback column should have been rejected by can_stream_sql_batches"
                );
            }
            (builder, reader) => {
                unreachable!(
                    "ColumnBuilder/SqlFieldReader type mismatch: builder={}, reader={}",
                    builder.type_name(),
                    reader.type_name(),
                );
            }
        }
    }

    fn finish(&mut self) -> datafusion::arrow::array::ArrayRef {
        use std::sync::Arc;
        match self {
            ColumnBuilder::F64 { builder, .. } => Arc::new(builder.finish()),
            ColumnBuilder::I64 { builder, .. } => Arc::new(builder.finish()),
            ColumnBuilder::TimestampMillis { builder, .. } => Arc::new(builder.finish()),
            ColumnBuilder::Str { builder, .. } => Arc::new(builder.finish()),
            ColumnBuilder::Null(b) => Arc::new(b.finish()),
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            ColumnBuilder::F64 { .. } => "F64",
            ColumnBuilder::I64 { .. } => "I64",
            ColumnBuilder::TimestampMillis { .. } => "TimestampMillis",
            ColumnBuilder::Str { .. } => "Str",
            ColumnBuilder::Null(_) => "Null",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{QueryClause, SearchRequest};
    use serde_json::json;
    use std::collections::HashMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
    use std::time::Duration;
    use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
    use tantivy::directory::{
        Directory, FileHandle, RamDirectory, WatchCallback, WatchHandle, WritePtr,
    };

    const TEST_SYNC_TIMEOUT: Duration = Duration::from_secs(10);
    const MERGE_GATE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Helper: create a HotEngine backed by a temp directory.
    fn create_engine() -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        (dir, engine)
    }

    #[test]
    fn field_registry_excludes_reserved_internal_metadata_fields() {
        let (_dir, engine) = create_engine();
        let registry = engine.field_registry.read().unwrap();

        for field in [
            "_id",
            "_doc_id",
            "_source",
            "_seq_no",
            "_primary_term",
            "_version",
            "_index",
            "_routing",
        ] {
            assert!(
                !registry.fields.contains_key(field),
                "reserved field {field} must not be source-addressable"
            );
        }
        assert!(registry.fields.contains_key("body"));
    }

    fn apply_index(
        engine: &dyn SearchEngine,
        doc_id: &str,
        source: serde_json::Value,
        seq_no: u64,
        primary_term: u64,
    ) -> super::super::ReplicaApplyReceipt {
        engine
            .apply_replica_operation(super::super::SequencedOperation {
                seq_no,
                primary_term,
                mutation: super::super::DocumentMutation::Index {
                    doc_id: doc_id.to_string(),
                    source,
                },
            })
            .unwrap()
    }

    fn persist_empty_committed_boundary(path: &Path) {
        CommittedBoundaryRecord::empty(0)
            .persist(&path.join("translog.committed"))
            .unwrap();
    }

    fn create_pre_d1_schema_fixture(path: &Path) {
        let index_path = path.join("index");
        std::fs::create_dir_all(&index_path).unwrap();
        let mut schema = Schema::builder();
        let id = schema.add_text_field("_id", (STRING | STORED).set_fast(None));
        let source = schema.add_text_field("_source", STORED);
        let body = schema.add_text_field("body", TEXT | STORED);
        let index = Index::open_or_create(
            tantivy::directory::MmapDirectory::open(&index_path).unwrap(),
            schema.build(),
        )
        .unwrap();
        let mut writer = index.writer(TANTIVY_WRITER_HEAP_BYTES).unwrap();
        let mut document = TantivyDocument::new();
        document.add_text(id, "legacy");
        document.add_text(source, r#"{"value":"legacy"}"#);
        document.add_text(body, "legacy");
        writer.add_document(document).unwrap();
        writer.commit().unwrap();
        drop(writer);
        drop(index);
    }

    struct MergeWriteGate {
        armed: AtomicBool,
        entered_sender: Sender<()>,
        release_receiver: Mutex<Receiver<()>>,
    }

    impl MergeWriteGate {
        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        fn wait_if_armed(&self) -> std::io::Result<()> {
            let is_merge_thread = std::thread::current()
                .name()
                .is_some_and(|name| name.starts_with("merge_thread_"));
            if !is_merge_thread || !self.armed.swap(false, Ordering::SeqCst) {
                return Ok(());
            }

            self.entered_sender.send(()).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!("merge-start receiver dropped: {error}"),
                )
            })?;
            self.release_receiver
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv_timeout(MERGE_GATE_TIMEOUT)
                .map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("timed out waiting to release blocked merge: {error}"),
                    )
                })?;
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BlockingMergeDirectory {
        inner: RamDirectory,
        gate: Arc<MergeWriteGate>,
    }

    impl fmt::Debug for BlockingMergeDirectory {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("BlockingMergeDirectory")
        }
    }

    impl Directory for BlockingMergeDirectory {
        fn get_file_handle(
            &self,
            path: &Path,
        ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
            self.inner.get_file_handle(path)
        }

        fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
            self.inner.delete(path)
        }

        fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
            self.inner.exists(path)
        }

        fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
            self.gate
                .wait_if_armed()
                .map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
            self.inner.open_write(path)
        }

        fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
            self.inner.atomic_read(path)
        }

        fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
            self.inner.atomic_write(path, data)
        }

        fn sync_directory(&self) -> std::io::Result<()> {
            self.inner.sync_directory()
        }

        fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
            self.inner.watch(watch_callback)
        }
    }

    fn create_engine_with_blocked_merge() -> (
        tempfile::TempDir,
        HotEngine,
        Arc<MergeWriteGate>,
        Receiver<()>,
        Sender<()>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let gate = Arc::new(MergeWriteGate {
            armed: AtomicBool::new(false),
            entered_sender,
            release_receiver: Mutex::new(release_receiver),
        });
        let directory = BlockingMergeDirectory {
            inner: RamDirectory::create(),
            gate: gate.clone(),
        };

        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("_id", (STRING | STORED).set_fast(None));
        schema_builder.add_text_field("_source", STORED);
        schema_builder.add_u64_field(SEQ_NO_FIELD_NAME, FAST | STORED);
        schema_builder.add_u64_field(PRIMARY_TERM_FIELD_NAME, FAST | STORED);
        schema_builder.add_text_field("body", TEXT | STORED);
        let index = Index::open_or_create(directory, schema_builder.build()).unwrap();
        let schema = index.schema();
        let id_field = schema.get_field("_id").unwrap();
        let source_field = schema.get_field("_source").unwrap();
        let seq_no_field = schema.get_field(SEQ_NO_FIELD_NAME).unwrap();
        let primary_term_field = schema.get_field(PRIMARY_TERM_FIELD_NAME).unwrap();
        let body_field = schema.get_field("body").unwrap();
        let writer = index.writer(TANTIVY_WRITER_HEAP_BYTES).unwrap();
        let automatic_merge_policy = writer.get_merge_policy();
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .unwrap();
        let translog =
            HotTranslog::open_with_durability(dir.path(), TranslogDurability::Request).unwrap();
        let committed_boundary_path = dir.path().join("translog.committed");
        let committed_boundary = CommittedBoundaryRecord::empty(0);
        committed_boundary
            .persist(&committed_boundary_path)
            .unwrap();

        let engine = HotEngine {
            index,
            reader,
            writer: Arc::new(RwLock::new(WriterState::ready(writer))),
            maintenance_lock: Mutex::new(()),
            automatic_merge_policy: RwLock::new(automatic_merge_policy),
            force_merge_entry_barrier: Mutex::new(None),
            force_merge_before_wait_sender: Mutex::new(None),
            writer_replacement_failure: Mutex::new(None),
            engine_apply_failure: Mutex::new(None),
            refresh_commit_failures: Mutex::new(0),
            post_apply_refresh_failures: Mutex::new(0),
            refresh_before_writer_sender: Mutex::new(None),
            refresh_after_commit_sender: Mutex::new(None),
            refresh_after_commit_release_receiver: Mutex::new(None),
            peer_recovery_snapshot_ready_sender: Mutex::new(None),
            peer_recovery_snapshot_release_receiver: Mutex::new(None),
            field_registry: RwLock::new(FieldRegistry {
                id_field,
                source_field,
                seq_no_field: Some(seq_no_field),
                primary_term_field: Some(primary_term_field),
                fields: HashMap::from([("body".to_string(), body_field)]),
                field_types: HashMap::new(),
                date_fields: Vec::new(),
            }),
            refresh_interval: Duration::from_secs(60),
            translog: Arc::new(Mutex::new(translog)),
            apply_state: Mutex::new(ApplyState::new(committed_boundary).unwrap()),
            identity_term_state: Mutex::new(None),
            committed_boundary_path,
            durability: TranslogDurability::Request,
            delete_tombstone_retention: Duration::from_secs(60),
            column_cache: Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        };

        (dir, engine, gate, entered_receiver, release_sender)
    }

    // ── schema evolution ────────────────────────────────────────────────

    #[test]
    fn evolve_meta_json_appends_new_fields() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        // Create an initial engine with one mapped field.
        let initial_mappings = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &initial_mappings,
            crate::wal::TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        drop(engine);

        // Evolve the schema by adding two new fields.
        let evolved_mappings = {
            let mut m = initial_mappings.clone();
            m.insert(
                "count".to_string(),
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            );
            m.insert(
                "active".to_string(),
                FieldMapping {
                    field_type: FieldType::Boolean,
                    dimension: None,
                },
            );
            m
        };

        let meta_path = dir.path().join("index/meta.json");
        evolve_meta_json_schema(&meta_path, &evolved_mappings).unwrap();

        // Re-open the index — Tantivy must accept the evolved schema.
        let engine2 = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &evolved_mappings,
            crate::wal::TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        // Verify all three mapped fields exist in the field registry.
        let registry = engine2.field_registry.read().unwrap();
        assert!(registry.fields.contains_key("title"));
        assert!(registry.fields.contains_key("count"));
        assert!(registry.fields.contains_key("active"));
    }

    #[test]
    fn evolve_meta_json_noop_when_no_new_fields() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let mappings = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            crate::wal::TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        drop(engine);

        let meta_path = dir.path().join("index/meta.json");
        let before = std::fs::read_to_string(&meta_path).unwrap();
        evolve_meta_json_schema(&meta_path, &mappings).unwrap();
        let after = std::fs::read_to_string(&meta_path).unwrap();

        // File should be unchanged — no rewrite needed.
        assert_eq!(before, after);
    }

    #[test]
    fn explicit_text_body_mapping_reuses_builtin_schema_field() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let mappings = HashMap::from([(
            "body".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        assert_eq!(
            engine
                .index
                .schema()
                .fields()
                .filter(|(_, entry)| entry.name() == "body")
                .count(),
            1
        );
        apply_index(&engine, "doc", json!({"body": 42}), 0, 1);
        engine.refresh().unwrap();
        assert_eq!(engine.get_document("doc").unwrap().unwrap()["body"], 42);
    }

    #[test]
    fn invalid_authoritative_builtin_mappings_require_recreate_on_open() {
        use crate::cluster::state::{FieldMapping, FieldType};

        for (field_name, mapping) in [
            (
                "body",
                FieldMapping {
                    field_type: FieldType::Integer,
                    dimension: None,
                },
            ),
            (
                "_routing",
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            drop(HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
            let mappings = HashMap::from([(field_name.to_string(), mapping)]);

            let error = match HotEngine::open_existing_with_mappings(
                dir.path(),
                Duration::from_secs(60),
                &mappings,
                TranslogDurability::Request,
                Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
            ) {
                Ok(_) => panic!("invalid authoritative mapping unexpectedly opened"),
                Err(error) => error,
            };
            assert!(
                error.is::<crate::common::UnsupportedIndexFormatError>(),
                "{field_name}: {error:#}"
            );
            assert!(
                error.to_string().contains("recreate the index"),
                "{field_name}: {error:#}"
            );
        }
    }

    #[test]
    fn no_compat_missing_sequence_schema_requires_recreate() {
        let dir = tempfile::tempdir().unwrap();
        create_pre_d1_schema_fixture(dir.path());

        let error = match HotEngine::open_existing_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &HashMap::new(),
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        ) {
            Ok(_) => panic!("pre-D1 schema unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
    }

    #[test]
    fn indexed_document_stores_exact_sequence_identity_fast_fields() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        apply_index(&engine, "doc", json!({"value": 1}), 7, 3);
        engine.refresh().unwrap();
        let registry = engine.field_registry.read().unwrap();
        let searcher = engine.reader.searcher();
        let query = tantivy::query::TermQuery::new(
            Term::from_field_text(registry.id_field, "doc"),
            tantivy::schema::IndexRecordOption::Basic,
        );
        let (_, address) = searcher
            .search(&query, &TopDocs::with_limit(1))
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let segment = &searcher.segment_readers()[address.segment_ord as usize];
        assert_eq!(
            segment
                .fast_fields()
                .u64(SEQ_NO_FIELD_NAME)
                .unwrap()
                .first(address.doc_id),
            Some(7)
        );
        assert_eq!(
            segment
                .fast_fields()
                .u64(PRIMARY_TERM_FIELD_NAME)
                .unwrap()
                .first(address.doc_id),
            Some(3)
        );
    }

    #[test]
    fn malformed_internal_sequence_field_options_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("index");
        std::fs::create_dir_all(&index_path).unwrap();
        let mut schema = Schema::builder();
        schema.add_text_field("_id", (STRING | STORED).set_fast(None));
        schema.add_text_field("_source", STORED);
        schema.add_u64_field(SEQ_NO_FIELD_NAME, STORED);
        schema.add_u64_field(PRIMARY_TERM_FIELD_NAME, FAST | STORED);
        schema.add_text_field("body", TEXT | STORED);
        Index::open_or_create(
            tantivy::directory::MmapDirectory::open(&index_path).unwrap(),
            schema.build(),
        )
        .unwrap();
        HotTranslog::open(dir.path()).unwrap();
        persist_empty_committed_boundary(dir.path());

        let error = match HotEngine::new(dir.path(), Duration::from_secs(60)) {
            Ok(_) => panic!("malformed internal field unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.is::<crate::common::UnsupportedIndexFormatError>());
        assert!(error.to_string().contains("recreate the index"));
    }

    #[test]
    fn remote_split_purpose_does_not_require_sequence_fields() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new_remote_split_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &HashMap::new(),
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        engine.add_document("doc", json!({"value": 1})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.get_document("doc").unwrap().unwrap()["value"], 1);
        let schema = engine.index.schema();
        assert!(schema.get_field(SEQ_NO_FIELD_NAME).is_err());
        assert!(schema.get_field(PRIMARY_TERM_FIELD_NAME).is_err());
    }

    #[test]
    fn reopen_with_evolved_schema_can_index_new_field() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        let initial = HashMap::from([(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        )]);
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &initial,
            crate::wal::TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        engine.add_document("1", json!({"title": "hello"})).unwrap();
        engine.refresh().unwrap();
        drop(engine);

        // Add a new integer field.
        let mut evolved = initial;
        evolved.insert(
            "count".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let engine2 = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &evolved,
            crate::wal::TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        // Index a doc with the new field.
        engine2
            .add_document("2", json!({"title": "world", "count": 42}))
            .unwrap();
        engine2.refresh().unwrap();

        // Both docs should be retrievable.
        let d1 = engine2.get_document("1").unwrap().unwrap();
        assert_eq!(d1["title"], "hello");
        let d2 = engine2.get_document("2").unwrap().unwrap();
        assert_eq!(d2["title"], "world");
        assert_eq!(d2["count"], 42);
    }

    // ── basic CRUD ──────────────────────────────────────────────────────

    #[test]
    fn add_and_get_document() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let doc = engine.get_document("doc1").unwrap();
        assert!(doc.is_some());
        assert_eq!(doc.unwrap()["title"], "hello world");
    }

    #[test]
    fn get_nonexistent_document_returns_none() {
        let (_dir, engine) = create_engine();
        let doc = engine.get_document("no-such-doc").unwrap();
        assert!(doc.is_none());
    }

    #[test]
    fn add_document_upsert_semantics() {
        let (_dir, engine) = create_engine();
        engine.add_document("doc1", json!({"version": 1})).unwrap();
        engine.refresh().unwrap();

        // Overwrite with new payload
        engine.add_document("doc1", json!({"version": 2})).unwrap();
        engine.refresh().unwrap();

        let doc = engine.get_document("doc1").unwrap().unwrap();
        assert_eq!(doc["version"], 2);
        // Should still be 1 doc, not 2
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn delete_document() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc1", json!({"title": "delete me"}))
            .unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        engine.delete_document("doc1").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 0);
        assert!(engine.get_document("doc1").unwrap().is_none());
    }

    // ── bulk operations ─────────────────────────────────────────────────

    #[test]
    fn bulk_add_documents() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..10)
            .map(|i| (format!("doc-{i}"), json!({"num": i})))
            .collect();
        let ids = engine.bulk_add_documents(docs).unwrap();
        engine.refresh().unwrap();

        assert_eq!(ids.len(), 10);
        assert_eq!(engine.doc_count(), 10);

        let doc = engine.get_document("doc-5").unwrap().unwrap();
        assert_eq!(doc["num"], 5);
    }

    #[test]
    fn terms_partial_keeps_all_buckets_for_coordinator_merge() {
        let partial = convert_to_partial(
            &AggKind::Terms,
            SegmentAggData::Terms {
                counts: std::collections::HashMap::from([
                    ("a".to_string(), 100),
                    ("global".to_string(), 99),
                    ("other".to_string(), 42),
                ]),
            },
        );

        let crate::search::PartialAggResult::Terms { buckets } = partial else {
            panic!("expected terms partial");
        };

        assert_eq!(buckets.len(), 3);
        assert!(buckets.iter().any(|b| b.key == "a" && b.doc_count == 100));
        assert!(
            buckets
                .iter()
                .any(|b| b.key == "global" && b.doc_count == 99)
        );
        assert!(
            buckets
                .iter()
                .any(|b| b.key == "other" && b.doc_count == 42)
        );
    }

    fn terms_counts(engine: &HotEngine, field: &str, query: QueryClause) -> HashMap<String, u64> {
        let request: SearchRequest = serde_json::from_value(json!({
            "query": query,
            "size": 0,
            "aggs": {"values": {"terms": {"field": field, "size": 100}}}
        }))
        .unwrap();
        let (_, _, partials) = engine.search_query(&request).unwrap();
        let crate::search::PartialAggResult::Terms { buckets } = &partials["values"] else {
            panic!("expected terms buckets");
        };
        buckets
            .iter()
            .map(|bucket| (bucket.key.clone(), bucket.doc_count))
            .collect()
    }

    #[test]
    fn terms_integer_keys_preserve_full_i64_precision() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "value".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        )]));
        let values = [
            i64::MIN,
            -(1_i64 << 53) - 1,
            -(1_i64 << 53),
            -1,
            0,
            1,
            1_i64 << 53,
            (1_i64 << 53) + 1,
            i64::MAX,
        ];
        for (id, value) in values.iter().enumerate() {
            engine
                .add_document(&id.to_string(), json!({"value": value}))
                .unwrap();
        }
        engine.refresh().unwrap();
        let counts = terms_counts(&engine, "value", QueryClause::MatchAll(json!({})));
        assert_eq!(
            counts,
            values
                .into_iter()
                .map(|value| (value.to_string(), 1))
                .collect()
        );
    }

    #[test]
    fn terms_float_keys_do_not_saturate_to_i64() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "value".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        )]));
        let values = [1e20_f64, 1e21, -1e20, -1e21, 0.0, -0.0, 1.5];
        for (id, value) in values.iter().enumerate() {
            engine
                .add_document(&id.to_string(), json!({"value": value}))
                .unwrap();
        }
        engine.refresh().unwrap();
        let counts = terms_counts(&engine, "value", QueryClause::MatchAll(json!({})));
        assert_eq!(
            counts,
            HashMap::from([
                (1e20_f64.to_string(), 1),
                (1e21_f64.to_string(), 1),
                ((-1e20_f64).to_string(), 1),
                ((-1e21_f64).to_string(), 1),
                ("0".to_string(), 2),
                ("1.5".to_string(), 1),
            ])
        );
    }

    #[test]
    fn keyword_arrays_are_searchable_and_count_documents_once() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mappings = HashMap::from([(
            "tags".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        let (dir, engine) = create_engine_with_mappings(mappings.clone());
        let documents = [
            json!({"tags": ["b", "a", "a", null]}),
            json!({"tags": ["b"]}),
            json!({"tags": []}),
            json!({"tags": null}),
            json!({"tags": "a"}),
        ];
        for (id, document) in documents.iter().enumerate() {
            engine
                .add_document(&id.to_string(), document.clone())
                .unwrap();
        }
        engine.refresh().unwrap();
        assert_eq!(
            engine.get_document("0").unwrap(),
            Some(documents[0].clone())
        );
        assert_eq!(
            terms_counts(&engine, "tags", QueryClause::MatchAll(json!({}))),
            HashMap::from([("a".to_string(), 2), ("b".to_string(), 2)])
        );
        let query = QueryClause::Term(HashMap::from([("tags".to_string(), json!("b"))]));
        assert_eq!(
            terms_counts(&engine, "tags", query),
            HashMap::from([("a".to_string(), 1), ("b".to_string(), 2)])
        );

        engine.flush().unwrap();
        drop(engine);
        let reopened = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        assert_eq!(
            terms_counts(&reopened, "tags", QueryClause::MatchAll(json!({}))),
            HashMap::from([("a".to_string(), 2), ("b".to_string(), 2)])
        );
        assert_eq!(
            reopened.get_document("0").unwrap(),
            Some(documents[0].clone())
        );
    }

    #[test]
    fn keyword_arrays_flatten_and_coerce_scalar_values() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "tags".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]));
        let source = json!({"tags": [["a", 1, true], ["a", "1", null], false]});
        engine.add_document("one", source.clone()).unwrap();
        engine.add_document("two", json!({"tags": 42})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.get_document("one").unwrap(), Some(source));
        assert_eq!(
            terms_counts(&engine, "tags", QueryClause::MatchAll(json!({}))),
            ["a", "1", "true", "false", "42"]
                .into_iter()
                .map(|value| (value.to_string(), 1))
                .collect()
        );
    }

    #[test]
    fn terms_dense_and_sparse_counters_preserve_all_buckets() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "tags".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]));
        let distinct = DENSE_TERMS_ORDINAL_LIMIT + 17;
        let docs: Vec<_> = (0..distinct)
            .map(|id| {
                (
                    id.to_string(),
                    json!({"tags": [format!("tag-{id}"), "shared"]}),
                )
            })
            .collect();
        engine.bulk_add_documents(docs).unwrap();
        engine.refresh().unwrap();
        engine.force_merge(1).unwrap();
        let searcher = engine.reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let column = searcher.segment_readers()[0]
            .fast_fields()
            .str("tags")
            .unwrap()
            .unwrap();
        assert!(column.num_terms() > DENSE_TERMS_ORDINAL_LIMIT);
        let counts = terms_counts(&engine, "tags", QueryClause::MatchAll(json!({})));
        assert_eq!(counts.len(), distinct + 1);
        assert_eq!(counts["shared"], distinct as u64);
        for id in 0..distinct {
            assert_eq!(counts[&format!("tag-{id}")], 1);
        }

        assert_eq!(
            resolve_term_ordinals(&column, [(0, 2)], 1).unwrap(),
            HashMap::from([("shared".to_string(), 2)])
        );
    }

    #[test]
    fn terms_invalid_ordinal_returns_an_error_instead_of_partial_counts() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "tag".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]));
        engine.add_document("one", json!({"tag": "a"})).unwrap();
        engine.refresh().unwrap();
        let searcher = engine.reader.searcher();
        let column = searcher.segment_readers()[0]
            .fast_fields()
            .str("tag")
            .unwrap()
            .unwrap();
        assert!(resolve_term_ordinals(&column, [(u64::MAX, 1)], 1).is_err());
    }

    #[test]
    fn keyword_objects_are_rejected_before_wal_or_writer_mutation() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let (_dir, engine) = create_engine_with_mappings(HashMap::from([(
            "tags".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]));
        let invalid = json!({"tags": ["valid", {"nested": "invalid"}]});
        let error = engine.add_document("one", invalid.clone()).unwrap_err();
        assert!(error.is::<super::super::DocumentValidationError>());
        assert!(error.to_string().contains("tags"));
        assert!(
            engine
                .bulk_add_documents(vec![
                    ("valid".to_string(), json!({"tags": ["a"]})),
                    ("invalid".to_string(), invalid.clone()),
                ])
                .is_err()
        );
        assert!(
            engine
                .apply_replica_operation(super::super::SequencedOperation {
                    seq_no: 42,
                    primary_term: 1,
                    mutation: super::super::DocumentMutation::Index {
                        doc_id: "replica".into(),
                        source: invalid,
                    },
                })
                .is_err()
        );
        assert_eq!(
            engine
                .with_translog("keyword validation test", |wal| Ok(wal.next_seq_no()))
                .unwrap(),
            0
        );
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 0);
    }

    // ── search ──────────────────────────────────────────────────────────

    #[test]
    fn simple_query_string_search() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust programming language"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python programming language"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "cooking recipes"}))
            .unwrap();
        engine.refresh().unwrap();

        let results = engine.search("rust").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn search_match_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("a", json!({"x": 1})).unwrap();
        engine.add_document("b", json!({"x": 2})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(total, 2);
    }

    #[test]
    fn search_query_total_count_exceeds_limit() {
        let (_dir, engine) = create_engine();
        // Insert 200 docs — more than the default limit of 100
        for i in 0..200 {
            engine
                .add_document(&format!("doc-{i}"), json!({"val": i}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // With size=0, hits are skipped entirely but total should still count all matches.
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 200, "total should count all matching docs");
        assert!(results.is_empty(), "size=0 should skip hit materialization");
    }

    #[test]
    fn size_zero_with_aggs_returns_no_hits_and_partial_aggs() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{AggregationRequest, MetricAggParams, TermsAggParams};

        let mut mappings = HashMap::new();
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"category": "books", "price": 10.0}))
            .unwrap();
        engine
            .add_document("d2", json!({"category": "books", "price": 20.0}))
            .unwrap();
        engine
            .add_document("d3", json!({"category": "toys", "price": 30.0}))
            .unwrap();
        engine.refresh().unwrap();

        let mut aggs = HashMap::new();
        aggs.insert(
            "top_categories".into(),
            AggregationRequest::Terms(TermsAggParams {
                field: "category".into(),
                size: 10,
            }),
        );
        aggs.insert(
            "price_stats".into(),
            AggregationRequest::Stats(MetricAggParams {
                field: "price".into(),
            }),
        );

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs,
        };
        let (results, total, partial_aggs) = engine.search_query(&req).unwrap();

        assert!(results.is_empty());
        assert_eq!(total, 3);
        assert!(partial_aggs.contains_key("top_categories"));
        assert!(partial_aggs.contains_key("price_stats"));
    }

    #[test]
    fn size_zero_with_grouped_metrics_returns_grouped_partials() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{
            AggregationRequest, GroupedMetricAgg, GroupedMetricFunction, GroupedMetricsAggParams,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"brand": "Apple", "price": 999.0, "body": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"brand": "Apple", "price": 899.0, "body": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"brand": "Samsung", "price": 799.0, "body": "iphone"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match(HashMap::from([("body".to_string(), json!("iphone"))])),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into()],
                    metrics: vec![
                        GroupedMetricAgg {
                            output_name: "total".into(),
                            function: GroupedMetricFunction::Count,
                            field: None,
                            field_expr: None,
                        },
                        GroupedMetricAgg {
                            output_name: "avg_price".into(),
                            function: GroupedMetricFunction::Avg,
                            field: Some("price".into()),
                            field_expr: None,
                        },
                    ],
                    shard_top_k: None,
                }),
            )]),
        };

        let (hits, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert!(hits.is_empty());
        assert_eq!(total, 3);

        let crate::search::PartialAggResult::GroupedMetrics { buckets } =
            &partial_aggs["sql_grouped"]
        else {
            panic!("expected grouped metrics partial");
        };
        assert_eq!(buckets.len(), 2);
    }

    #[test]
    fn search_query_total_with_filter() {
        let (_dir, engine) = create_engine();
        for i in 0..50 {
            engine
                .add_document(&format!("d{i}"), json!({"body": "matching term"}))
                .unwrap();
        }
        for i in 50..100 {
            engine
                .add_document(&format!("d{i}"), json!({"body": "other content"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let mut fields = HashMap::new();
        fields.insert("body".to_string(), json!("matching"));
        let req = SearchRequest {
            query: QueryClause::Match(fields),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 50, "total should count all matching docs");
        assert!(results.len() <= 100);
    }

    #[test]
    fn search_match_query() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "database internals"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "web development"}))
            .unwrap();
        engine.refresh().unwrap();

        let mut fields = HashMap::new();
        fields.insert("body".to_string(), json!("database"));
        let req = SearchRequest {
            query: QueryClause::Match(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── refresh / flush ─────────────────────────────────────────────────

    #[test]
    fn documents_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "invisible"}))
            .unwrap();
        // doc_count uses the reader which hasn't been reloaded yet
        assert_eq!(engine.doc_count(), 0);

        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn flush_truncates_translog() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.flush().unwrap();

        // After flush the translog should be empty
        let tl = engine.translog.lock().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    // ── translog replay / crash recovery ────────────────────────────────

    #[test]
    fn translog_replay_recovers_documents_after_crash() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate: write docs but never flush (simulating crash before commit)
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("crash-doc", json!({"recovered": true}))
                .unwrap();
            // intentionally do NOT flush — translog has the entry, Tantivy segments may not
        }

        // Reopen — replay should recover the document
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        // After replay, the engine commits and reloads
        let doc = engine2.get_document("crash-doc").unwrap();
        assert!(
            doc.is_some(),
            "document should be recovered from translog replay"
        );
        assert_eq!(doc.unwrap()["recovered"], true);
    }

    #[test]
    fn translog_replay_rejects_invalid_keyword_objects() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let dir = tempfile::tempdir().unwrap();
        let wal = HotTranslog::open(dir.path()).unwrap();
        wal.append(
            1,
            crate::wal::WalOperation::Index,
            json!({
                "_doc_id": "invalid",
                "_source": {"tags": ["valid", {"nested": "invalid"}]}
            }),
        )
        .unwrap();
        drop(wal);
        persist_empty_committed_boundary(dir.path());

        let mappings = HashMap::from([(
            "tags".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        )]);
        let error = match HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            Arc::new(super::super::column_cache::ColumnCache::new(0, 0)),
        ) {
            Ok(_) => panic!("invalid replayed keyword value should fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("tags"));
    }

    #[test]
    fn flush_then_reopen_has_empty_translog() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("safe", json!({"flushed": true}))
                .unwrap();
            engine.flush().unwrap();
        }
        // Reopen
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let tl = engine2.translog.lock().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(
            entries.is_empty(),
            "translog should be empty after flush + reopen"
        );
    }

    #[test]
    fn refresh_then_reopen_does_not_duplicate_committed_docs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("safe", json!({"refreshed": true}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (_hits, total, _) = engine2.search_query(&req).unwrap();
        assert_eq!(total, 1, "refresh-committed docs must not replay twice");
    }

    #[test]
    fn stale_apply_during_commit_to_reload_window_uses_old_version_map() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
        apply_index(engine.as_ref(), "doc", json!({"value": 2}), 1, 1);
        let (committed_tx, committed_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        engine.pause_after_refresh_commit_for_test(committed_tx, release_rx);

        let refresh_engine = engine.clone();
        let refresh = std::thread::spawn(move || refresh_engine.refresh());
        committed_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        apply_index(engine.as_ref(), "doc", json!({"value": 1}), 0, 1);
        release_tx.send(()).unwrap();
        refresh.join().unwrap().unwrap();

        assert_eq!(engine.get_document("doc").unwrap().unwrap()["value"], 2);
    }

    #[test]
    fn map_cap_refresh_failure_rejects_before_sequence_or_wal_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.set_version_map_max_bytes_for_test(LiveVersionMap::estimate_reservation(["a"]));
        let first = engine
            .add_document_with_receipt("a", json!({"value": 1}))
            .unwrap();
        assert_eq!(first.seq_no, 0);
        let before_next = engine
            .with_translog("map cap test", |translog| Ok(translog.next_seq_no()))
            .unwrap();
        let before_size = engine.translog_size_bytes();
        let before_stats = engine.sequence_stats();

        engine.inject_refresh_commit_failures_for_test(1);
        let error = engine
            .add_document_with_receipt("b", json!({"value": 2}))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected refresh commit failure")
        );
        assert_eq!(
            engine
                .with_translog("map cap test", |translog| Ok(translog.next_seq_no()))
                .unwrap(),
            before_next
        );
        assert_eq!(engine.translog_size_bytes(), before_size);
        assert_eq!(engine.sequence_stats(), before_stats);
        assert!(engine.get_document("b").unwrap().is_none());
    }

    #[test]
    fn oversized_bulk_success_is_independent_of_post_apply_refresh_failure() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.set_version_map_max_bytes_for_test(1);
        engine.inject_post_apply_refresh_failures_for_test(1);

        let receipt = engine
            .bulk_add_documents_with_receipt(vec![("oversized".to_string(), json!({"value": 1}))])
            .unwrap();
        assert_eq!(receipt.start_seq_no, Some(0));
        assert_eq!(
            engine
                .retained_recovery_ops(0, usize::MAX, usize::MAX)
                .unwrap()
                .operations
                .len(),
            1
        );
        engine.refresh().unwrap();
        assert_eq!(
            engine.get_document("oversized").unwrap().unwrap()["value"],
            1
        );
    }

    #[test]
    fn refresh_then_reopen_replays_only_uncommitted_entries_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
            engine
                .add_document("committed", json!({"kind": "committed"}))
                .unwrap();
            engine.refresh().unwrap();
            engine
                .add_document("pending", json!({"kind": "pending"}))
                .unwrap();
        }

        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (_hits, total, _) = engine2.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "reopen should keep committed docs and replay only pending ones"
        );
        assert!(engine2.get_document("committed").unwrap().is_some());
        assert!(engine2.get_document("pending").unwrap().is_some());
    }

    #[test]
    fn reopen_with_same_mappings_in_different_hashmap_order_preserves_data() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let dir = tempfile::tempdir().unwrap();
        {
            let mut mappings = HashMap::new();
            mappings.insert(
                "title".to_string(),
                FieldMapping {
                    field_type: FieldType::Text,
                    dimension: None,
                },
            );
            mappings.insert(
                "category".to_string(),
                FieldMapping {
                    field_type: FieldType::Keyword,
                    dimension: None,
                },
            );

            let engine = HotEngine::new_with_mappings(
                dir.path(),
                Duration::from_secs(60),
                &mappings,
                TranslogDurability::Request,
                std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
            )
            .unwrap();
            engine
                .add_document("stable", json!({"title": "schema order", "category": "ok"}))
                .unwrap();
            engine.refresh().unwrap();
        }

        let mut reopened_mappings = HashMap::new();
        reopened_mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        reopened_mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let reopened = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &reopened_mappings,
            TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        let doc = reopened.get_document("stable").unwrap();
        assert!(
            doc.is_some(),
            "document should survive reopen with reordered mappings"
        );
        assert_eq!(doc.unwrap()["title"], "schema order");
    }

    #[test]
    fn streaming_replay_with_batched_commits_recovers_all_docs() {
        // Write more docs than the replay batch size to exercise the intermediate
        // commit path in streaming replay.  Use bulk_add_documents so the WAL is
        // written with a single fsync instead of 10K+ individual fsyncs.
        let dir = tempfile::tempdir().unwrap();
        let doc_count = TRANSLOG_REPLAY_BATCH_SIZE + 500;
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
            let docs: Vec<(String, serde_json::Value)> = (0..doc_count)
                .map(|i| (format!("d-{i}"), json!({"n": i})))
                .collect();
            engine.bulk_add_documents(docs).unwrap();
            // Intentionally do NOT flush — simulating crash.
        }

        // Reopen — streaming replay should recover all documents via batched commits.
        let engine2 = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
        engine2.refresh().unwrap();
        assert_eq!(
            engine2.doc_count(),
            doc_count,
            "all {doc_count} docs must be recovered by streaming replay"
        );
        // Spot-check first and last
        assert!(engine2.get_document("d-0").unwrap().is_some());
        assert!(
            engine2
                .get_document(&format!("d-{}", doc_count - 1))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn translog_size_bytes_returns_nonzero_after_writes() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.translog_size_bytes(), 0);

        engine.add_document("x", json!({"a": 1})).unwrap();
        assert!(engine.translog_size_bytes() > 0);
    }

    #[test]
    fn translog_size_bytes_resets_after_flush() {
        let (_dir, engine) = create_engine();
        engine.add_document("x", json!({"a": 1})).unwrap();
        assert!(engine.translog_size_bytes() > 0);

        engine.flush().unwrap();
        assert_eq!(engine.translog_size_bytes(), 0);
    }

    #[test]
    fn reopen_resumes_from_intermediate_replay_commit_below_fence_maximum() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
            let docs = (0..TRANSLOG_REPLAY_BATCH_SIZE)
                .map(|seq_no| (format!("d-{seq_no}"), json!({"n": seq_no})))
                .collect();
            engine
                .bulk_add_documents_with_receipt_at_term(docs, 1)
                .unwrap();
            engine.refresh().unwrap();
            engine
                .with_translog("append final pre-promotion operation", |translog| {
                    translog.append_with_seq(
                        TRANSLOG_REPLAY_BATCH_SIZE,
                        1,
                        crate::wal::WalOperation::Index,
                        json!({
                            "_doc_id": format!("d-{}", TRANSLOG_REPLAY_BATCH_SIZE),
                            "_source": {"n": TRANSLOG_REPLAY_BATCH_SIZE}
                        }),
                    )?;
                    Ok(())
                })
                .unwrap();
            engine
                .reconcile_term_sequence_state(2, Some(TRANSLOG_REPLAY_BATCH_SIZE))
                .unwrap();

            engine.refresh().unwrap();
            let committed = CommittedBoundaryRecord::load(&dir.path().join("translog.committed"))
                .unwrap()
                .unwrap();
            assert_eq!(
                committed.processed_checkpoint,
                Some(TRANSLOG_REPLAY_BATCH_SIZE - 1)
            );
            assert_eq!(committed.max_seq_no, Some(TRANSLOG_REPLAY_BATCH_SIZE - 1));
            assert_eq!(
                committed.term_sequence_state.max_seq_no_at_term_start,
                Some(TRANSLOG_REPLAY_BATCH_SIZE)
            );
        }

        let reopened = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
        reopened
            .reconcile_term_sequence_state(2, Some(TRANSLOG_REPLAY_BATCH_SIZE))
            .unwrap();
        assert_eq!(
            reopened.sequence_stats().processed_checkpoint,
            Some(TRANSLOG_REPLAY_BATCH_SIZE)
        );
        assert_eq!(
            reopened.sequence_stats().max_seq_no,
            Some(TRANSLOG_REPLAY_BATCH_SIZE)
        );
        assert_eq!(
            reopened
                .get_document(&format!("d-{TRANSLOG_REPLAY_BATCH_SIZE}"))
                .unwrap()
                .unwrap()["n"],
            json!(TRANSLOG_REPLAY_BATCH_SIZE)
        );
    }

    #[test]
    fn replay_is_idempotent_when_a_batched_suffix_is_replayed_again() {
        // Simulate a crash after replay committed a batch to Tantivy segments but
        // before the final checkpoint was fully advanced. The next startup should
        // safely replay that suffix again without producing duplicates.
        let dir = tempfile::tempdir().unwrap();
        let doc_count = TRANSLOG_REPLAY_BATCH_SIZE + 500;
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
            let docs: Vec<(String, serde_json::Value)> = (0..doc_count)
                .map(|i| (format!("d-{i}"), json!({"n": i})))
                .collect();
            engine.bulk_add_documents(docs).unwrap();
            engine.delete_document("d-0").unwrap();
        }

        // First reopen replays the full translog into Tantivy segments.
        {
            let engine2 = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
            engine2.refresh().unwrap();
            assert_eq!(engine2.doc_count(), doc_count - 1);
            assert!(engine2.get_document("d-0").unwrap().is_none());
        }

        // Simulate a stale checkpoint left behind by an interrupted replay after
        // the first batch checkpoint had already been persisted.
        let checkpoint_path = dir.path().join("translog.committed");
        let mut committed = CommittedBoundaryRecord::load(&checkpoint_path)
            .unwrap()
            .unwrap();
        committed.processed_checkpoint = Some(TRANSLOG_REPLAY_BATCH_SIZE - 1);
        committed.persisted_checkpoint = Some(TRANSLOG_REPLAY_BATCH_SIZE - 1);
        committed.persist(&checkpoint_path).unwrap();

        // Second reopen replays the already-committed suffix again.
        let engine3 = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
        engine3.refresh().unwrap();
        assert_eq!(
            engine3.doc_count(),
            doc_count - 1,
            "replaying a committed suffix must not create duplicates or resurrect deletes"
        );
        assert!(engine3.get_document("d-0").unwrap().is_none());
        assert!(
            engine3
                .get_document(&format!("d-{}", doc_count - 1))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn startup_replay_preserves_uncommitted_delete() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
            engine.add_document("victim", json!({"value": 1})).unwrap();
            engine.refresh().unwrap();
            engine.delete_document("victim").unwrap();
        }

        let reopened = HotEngine::new(dir.path(), Duration::from_secs(3600)).unwrap();
        assert!(
            reopened.get_document("victim").unwrap().is_none(),
            "startup WAL replay must preserve an acknowledged delete"
        );
    }

    #[test]
    fn startup_replay_rejects_malformed_document_operations() {
        for (operation, payload, expected) in [
            (
                crate::wal::WalOperation::Index,
                json!({"_source": {"value": 1}}),
                "has no _doc_id",
            ),
            (
                crate::wal::WalOperation::Index,
                json!({"_id": "legacy-only", "_source": {"value": 1}}),
                "has no _doc_id",
            ),
            (
                crate::wal::WalOperation::Index,
                json!({"_doc_id": "missing-source"}),
                "has no _source",
            ),
            (
                crate::wal::WalOperation::Delete,
                json!({}),
                "has no _doc_id",
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let translog =
                HotTranslog::open_with_durability(dir.path(), TranslogDurability::Request).unwrap();
            translog.append(1, operation, payload).unwrap();
            drop(translog);
            persist_empty_committed_boundary(dir.path());

            let error = match HotEngine::new(dir.path(), Duration::from_secs(3600)) {
                Ok(_) => panic!("startup replay accepted malformed WAL operation"),
                Err(error) => error,
            };
            assert!(
                error
                    .chain()
                    .any(|cause| cause.is::<crate::wal::WalCorruptionError>()),
                "{error:#}"
            );
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    // ── doc_count ───────────────────────────────────────────────────────

    #[test]
    fn doc_count_reflects_operations() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.doc_count(), 0);

        engine.add_document("a", json!({"x": 1})).unwrap();
        engine.add_document("b", json!({"x": 2})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);

        engine.delete_document("a").unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    // ── from/size pagination ────────────────────────────────────────────

    #[test]
    fn search_query_respects_size() {
        let (_dir, engine) = create_engine();
        for i in 0..20 {
            engine
                .add_document(&format!("doc-{i}"), json!({"title": "hello world"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Engine returns exactly from+size hits; coordinator does further merging.
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 5, "engine returns exactly size hits");
        assert_eq!(total, 20, "total reflects all matching docs");
    }

    #[test]
    fn search_query_from_skips_results() {
        let (_dir, engine) = create_engine();
        for i in 0..10 {
            engine
                .add_document(&format!("doc-{i}"), json!({"title": "hello world"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Engine always fetches from+size hits (10 here) — returns all 10
        let req_all = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (all_results, _, _) = engine.search_query(&req_all).unwrap();
        assert_eq!(all_results.len(), 10);

        // from=7, size=10 → engine fetches top 17, returns all 10 (< 17)
        let req_paged = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 7,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (paged_results, _, _) = engine.search_query(&req_paged).unwrap();
        assert_eq!(
            paged_results.len(),
            10,
            "engine returns all available hits; coordinator slices"
        );
    }

    #[test]
    fn search_query_from_beyond_total_returns_all_available() {
        let (_dir, engine) = create_engine();
        for i in 0..5 {
            engine
                .add_document(&format!("doc-{i}"), json!({"title": "test"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 100,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            5,
            "engine returns all 5 hits; coordinator will slice to empty"
        );
    }

    #[test]
    fn pagination_total_is_accurate_after_coordinator_slice() {
        // Simulates what the API coordinator does: collect engine hits,
        // report total from Count collector, then return the hits.
        let (_dir, engine) = create_engine();
        for i in 0..15 {
            engine
                .add_document(&format!("doc-{i}"), json!({"title": "hello"}))
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 15, "total should reflect all matching docs");
        assert_eq!(hits.len(), 5, "engine returns exactly size hits");
    }

    // ── Bool query tests ────────────────────────────────────────────────

    #[test]
    fn bool_must_filters_documents() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "must:rust should match d1 and d3");
    }

    #[test]
    fn bool_must_not_excludes_documents() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::MatchAll(json!({}))],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("python"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "must_not:python should exclude d2");
    }

    #[test]
    fn bool_should_with_no_must_matches_any() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python search"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "java build"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                should: vec![
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("body".into(), json!("rust"));
                        m
                    }),
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("body".into(), json!("python"));
                        m
                    }),
                ],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "should match d1 (rust) and d2 (python), not d3"
        );
    }

    #[test]
    fn bool_filter_acts_like_must() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                filter: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1, "filter:rust should match only d1");
    }

    #[test]
    fn bool_empty_matches_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "one"})).unwrap();
        engine.add_document("d2", json!({"title": "two"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery::default()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "empty bool should match all docs");
    }

    #[test]
    fn bool_combined_must_and_must_not() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust web server"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "python search tool"}))
            .unwrap();
        engine.refresh().unwrap();

        // must: rust, must_not: web → should only match d1
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("web"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "must:rust + must_not:web should only match d1"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn bool_nested_bool_inside_must() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python web framework"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rust web server"}))
            .unwrap();
        engine
            .add_document("d4", json!({"title": "java build tool"}))
            .unwrap();
        engine.refresh().unwrap();

        // Nested: must[ bool{ should[rust, python] } ], must_not[web]
        // Should match: d1 (rust, no web) — d2 (python, has web→excluded), d3 (rust, has web→excluded)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Bool(crate::search::BoolQuery {
                    should: vec![
                        QueryClause::Match({
                            let mut m = HashMap::new();
                            m.insert("body".into(), json!("rust"));
                            m
                        }),
                        QueryClause::Match({
                            let mut m = HashMap::new();
                            m.insert("body".into(), json!("python"));
                            m
                        }),
                    ],
                    ..Default::default()
                })],
                must_not: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("web"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "nested bool + must_not should match only d1"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn build_query_match_empty_fields_returns_all() {
        // Match with empty HashMap should return AllQuery (match all)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "empty Match should fall back to match all"
        );
    }

    #[test]
    fn build_query_term_empty_fields_returns_all() {
        // Term with empty HashMap should return AllQuery (match all)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "hello"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "world"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2, "empty Term should fall back to match all");
    }

    #[test]
    fn build_query_match_with_numeric_value() {
        // Match with a non-string value should stringify it
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "document 42"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "other text"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match({
                let mut m = HashMap::new();
                m.insert("body".into(), json!(42));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            1,
            "match with numeric value should find doc with '42'"
        );
        assert_eq!(results[0]["_id"], "d1");
    }

    #[test]
    fn build_query_term_via_search_query() {
        // Verify term query works through search_query (build_query path)
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"status": "published"}))
            .unwrap();
        engine
            .add_document("d2", json!({"status": "draft"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("published"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── Range query tests ───────────────────────────────────────────────

    #[test]
    fn range_query_gte_lt_on_text() {
        // Range on text field uses lexicographic ordering
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"name": "alice"})).unwrap();
        engine.add_document("d2", json!({"name": "bob"})).unwrap();
        engine
            .add_document("d3", json!({"name": "charlie"}))
            .unwrap();
        engine.add_document("d4", json!({"name": "dave"})).unwrap();
        engine.refresh().unwrap();

        // gte "b", lt "d" → should match "bob" and "charlie"
        let mut fields = HashMap::new();
        fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gte: Some(json!("b")),
                lt: Some(json!("d")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Range(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"), "bob should match");
        assert!(ids.contains(&"d3"), "charlie should match");
    }

    #[test]
    fn range_query_gt_lte_on_text() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"name": "alice"})).unwrap();
        engine.add_document("d2", json!({"name": "bob"})).unwrap();
        engine
            .add_document("d3", json!({"name": "charlie"}))
            .unwrap();
        engine.refresh().unwrap();

        // gt "alice", lte "charlie" → bob and charlie
        let mut fields = HashMap::new();
        fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gt: Some(json!("alice")),
                lte: Some(json!("charlie")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Range(fields),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"), "bob should match");
        assert!(ids.contains(&"d3"), "charlie should match");
        assert!(
            !ids.contains(&"d1"),
            "alice should be excluded (gt, not gte)"
        );
    }

    #[test]
    fn sql_record_batch_reads_filtered_fast_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Float,
                dimension: None,
            },
        );

        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            crate::wal::TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();

        engine
            .add_document(
                "d1",
                json!({"title": "iPhone Budget", "price": 499.0, "description": "iphone"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "iPhone Pro", "price": 999.0, "description": "iphone"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Bool(crate::search::BoolQuery {
                must: vec![crate::search::QueryClause::Match(HashMap::from([(
                    "description".to_string(),
                    json!("iphone"),
                )]))],
                should: Vec::new(),
                must_not: Vec::new(),
                filter: vec![crate::search::QueryClause::Range(HashMap::from([(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gt: Some(json!(500)),
                        ..Default::default()
                    },
                )]))],
            }),
            size: 100,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };

        let batch = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(batch.total_hits, 1);
        assert_eq!(batch.batch.num_rows(), 1);

        let title = batch
            .batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let price = batch
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();

        assert_eq!(title.value(0), "iPhone Pro");
        assert_eq!(price.value(0), 999.0);
    }

    #[test]
    fn range_query_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": "a"})).unwrap();
        engine.add_document("d2", json!({"x": "b"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            results.len(),
            2,
            "empty Range should fall back to match all"
        );
    }

    #[test]
    fn range_inside_bool_filter_engine() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rust alpha"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust beta"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "python gamma"}))
            .unwrap();
        engine.refresh().unwrap();

        // must: match "rust", filter: range body >= "alpha" and < "beta"
        // "alpha" matches d1, "beta" is excluded → only d1
        let mut range_fields = HashMap::new();
        range_fields.insert(
            "body".into(),
            crate::search::RangeCondition {
                gte: Some(json!("alpha")),
                lt: Some(json!("beta")),
                ..Default::default()
            },
        );
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("body".into(), json!("rust"));
                    m
                })],
                filter: vec![QueryClause::Range(range_fields)],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["_id"], "d1");
    }

    // ── Wildcard / Prefix query tests ───────────────────────────────────

    #[test]
    fn wildcard_star_matches_suffix() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "rustacean"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "rusty"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("rust*"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn wildcard_question_mark_matches_single_char() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine.add_document("d2", json!({"title": "rest"})).unwrap();
        engine
            .add_document("d3", json!({"title": "roast"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("r?st"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"), "rust should match r?st");
        assert!(ids.contains(&"d2"), "rest should match r?st");
        assert!(!ids.contains(&"d3"), "roast should NOT match r?st");
    }

    #[test]
    fn prefix_query_matches_beginning() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "search engine"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "sea turtle"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "mountain"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Prefix({
                let mut m = HashMap::new();
                m.insert("body".into(), json!("sea"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        let ids: Vec<&str> = results.iter().map(|r| r["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"), "search should match prefix 'sea'");
        assert!(ids.contains(&"d2"), "sea should match prefix 'sea'");
        assert!(
            !ids.contains(&"d3"),
            "mountain should NOT match prefix 'sea'"
        );
    }

    #[test]
    fn wildcard_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Wildcard(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn prefix_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Prefix(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Fuzzy query tests ───────────────────────────────────────────────

    #[test]
    fn fuzzy_matches_typo() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine
            .add_document("d2", json!({"title": "python"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy({
                let mut m = HashMap::new();
                m.insert(
                    "body".into(),
                    crate::search::FuzzyParams {
                        value: "rsut".into(),
                        fuzziness: 2,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]["_id"], "d1",
            "fuzzy should match 'rust' for 'rsut'"
        );
    }

    #[test]
    fn fuzzy_fuzziness_0_is_exact() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "rust"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy({
                let mut m = HashMap::new();
                m.insert(
                    "body".into(),
                    crate::search::FuzzyParams {
                        value: "rsut".into(),
                        fuzziness: 0,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert!(results.is_empty(), "fuzziness 0 should be exact match only");
    }

    #[test]
    fn fuzzy_empty_fields_returns_all() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"title": "a"})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Fuzzy(HashMap::new()),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (results, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(results.len(), 1);
    }

    // ── Field mappings tests ────────────────────────────────────────────

    fn create_engine_with_mappings_and_cache(
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
        column_cache: std::sync::Arc<crate::engine::column_cache::ColumnCache>,
    ) -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            TranslogDurability::Request,
            column_cache,
        )
        .unwrap();
        (dir, engine)
    }

    fn create_engine_with_mappings(
        mappings: HashMap<String, crate::cluster::state::FieldMapping>,
    ) -> (tempfile::TempDir, HotEngine) {
        create_engine_with_mappings_and_cache(
            mappings,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
    }

    #[test]
    fn mapped_text_field_is_searchable_by_name() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "rust programming"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "python scripting"}))
            .unwrap();
        engine.refresh().unwrap();

        // Match query on "title" field should hit the named text field, not just body
        let req = SearchRequest {
            query: QueryClause::Match({
                let mut m = HashMap::new();
                m.insert("title".to_string(), json!("rust"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn mapped_keyword_field_supports_term_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "status".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"status": "published", "title": "a"}))
            .unwrap();
        engine
            .add_document("d2", json!({"status": "draft", "title": "b"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("status".to_string(), json!("published"));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn mapped_integer_field_supports_range_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "old", "year": 1999}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "new", "year": 2024}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "mid", "year": 2010}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "year".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(2010)),
                        lt: None,
                        lte: None,
                        gt: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 2, "year >= 2010 should match d2 and d3");
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn mapped_date_field_indexes_and_queries_with_iso8601() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "created_at".to_string(),
            FieldMapping {
                field_type: FieldType::Date,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "morning", "created_at": "2025-01-05T08:00:00"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "noon", "created_at": "2025-01-05T12:00:00"}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "evening", "created_at": "2025-01-05T18:00:00"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Range query: created_at >= '2025-01-05T10:00:00' should match d2 and d3
        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "created_at".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!("2025-01-05T10:00:00")),
                        lt: None,
                        lte: None,
                        gt: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 2, "created_at >= 10:00 should match d2 and d3");
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d2"));
        assert!(ids.contains(&"d3"));

        // Range query with both bounds: 10:00 <= created_at < 15:00 → only d2
        let req2 = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "created_at".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!("2025-01-05T10:00:00")),
                        lt: Some(json!("2025-01-05T15:00:00")),
                        lte: None,
                        gt: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits2, _, _) = engine.search_query(&req2).unwrap();
        assert_eq!(
            hits2.len(),
            1,
            "10:00 <= created_at < 15:00 should match only d2"
        );
        assert_eq!(hits2[0]["_id"], "d2");

        // get_document normalizes stored date values to a UTC ISO 8601 string
        let doc = engine.get_document("d1").unwrap().unwrap();
        assert_eq!(doc["created_at"], "2025-01-05T08:00:00Z");
    }

    #[test]
    fn mapped_date_field_accepts_epoch_millis_number() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "ts".to_string(),
            FieldMapping {
                field_type: FieldType::Date,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // Index with epoch millis directly (2025-01-05T08:00:00Z = 1736064000000)
        engine
            .add_document("d1", json!({"title": "epoch", "ts": 1736064000000_i64}))
            .unwrap();
        engine.refresh().unwrap();

        // Range query with ISO 8601 string should match the epoch-indexed doc
        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "ts".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!("2025-01-05T07:00:00")),
                        lt: Some(json!("2025-01-05T09:00:00")),
                        lte: None,
                        gt: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d1");
        assert_eq!(hits[0]["_source"]["ts"], "2025-01-05T08:00:00Z");

        let doc = engine.get_document("d1").unwrap().unwrap();
        assert_eq!(doc["ts"], "2025-01-05T08:00:00Z");
    }

    #[test]
    fn mapped_integer_field_does_not_parse_iso_string_query_as_date() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mut mappings = HashMap::new();
        mappings.insert(
            "counter".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"counter": 1_736_064_900_000_i64}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(HashMap::from([(
                "counter".to_string(),
                json!("2025-01-05T08:15:00Z"),
            )])),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::new(),
        };

        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert!(
            hits.is_empty(),
            "integer fields must not reinterpret ISO date strings as epoch millis"
        );
    }

    #[test]
    fn mapped_date_field_sql_batch_uses_timestamp_schema_and_iso_rows() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use datafusion::arrow::array::TimestampMillisecondArray;
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        let mut mappings = HashMap::new();
        mappings.insert(
            "created_at".to_string(),
            FieldMapping {
                field_type: FieldType::Date,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "morning", "created_at": "2025-01-05T08:15:00+05:30"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["created_at".to_string()], false, false)
            .unwrap();

        let schema = result.batch.schema();
        let field = schema.field_with_name("created_at").unwrap().clone();
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        let array = result
            .batch
            .column_by_name("created_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(
            crate::common::date::epoch_millis_to_iso8601(array.value(0)),
            "2025-01-05T02:45:00Z"
        );

        let (_, rows) = crate::hybrid::merge::record_batches_to_json_rows(&[result.batch]).unwrap();
        assert_eq!(rows[0]["created_at"], json!("2025-01-05T02:45:00Z"));
    }

    #[test]
    fn mapped_date_field_streaming_batch_uses_timestamp_type() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use datafusion::arrow::array::TimestampMillisecondArray;
        use datafusion::arrow::datatypes::{DataType, TimeUnit};

        let mut mappings = HashMap::new();
        mappings.insert(
            "created_at".to_string(),
            FieldMapping {
                field_type: FieldType::Date,
                dimension: None,
            },
        );
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "morning", "created_at": "2025-01-05T08:15:00+05:30"}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "noon", "created_at": "2025-01-05T12:00:00Z"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            engine.can_stream_sql_batches(&["created_at".into(), "title".into()], false),
            "Date + Keyword columns should be streamable"
        );

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(
                &req,
                &["created_at".into(), "title".into()],
                true,
                false,
                8192,
            )
            .unwrap();

        assert!(!streaming.batches.is_empty());

        // Verify the Arrow schema uses Timestamp, not Int64
        let schema = streaming.batches[0].schema();
        let field = schema.field_with_name("created_at").unwrap();
        assert_eq!(
            *field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        // Collect values across all batches (docs may span multiple segments)
        let mut millis = Vec::new();
        for batch in &streaming.batches {
            let ts_col = batch
                .column_by_name("created_at")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            for i in 0..ts_col.len() {
                millis.push(ts_col.value(i));
            }
        }
        millis.sort();
        assert_eq!(
            millis
                .iter()
                .map(|m| crate::common::date::epoch_millis_to_iso8601(*m))
                .collect::<Vec<_>>(),
            vec!["2025-01-05T02:45:00Z", "2025-01-05T12:00:00Z"]
        );
    }

    #[test]
    fn mapped_float_field_supports_range_query() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "cheap", "price": 9.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "expensive", "price": 99.99}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gt: Some(json!(50.0)),
                        gte: None,
                        lt: None,
                        lte: None,
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, _, _) = engine.search_query(&req).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn range_query_integer_bounds_on_float_field() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine.add_document("d1", json!({"price": 5.0})).unwrap();
        engine.add_document("d2", json!({"price": 50.0})).unwrap();
        engine.add_document("d3", json!({"price": 500.0})).unwrap();
        engine.refresh().unwrap();

        // Use integer JSON bounds (10, 100) on a float field — must still match
        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(10)),
                        lte: Some(json!(100)),
                        ..Default::default()
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 1, "integer bounds on float field should match d2");
        assert_eq!(hits[0]["_id"], "d2");
    }

    #[test]
    fn term_query_on_integer_field() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "year".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine.add_document("d1", json!({"year": 2024})).unwrap();
        engine.add_document("d2", json!({"year": 2025})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term({
                let mut m = HashMap::new();
                m.insert("year".to_string(), json!(2024));
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 1, "term query on integer field should match");
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_all_clause_types_with_numeric_fields() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust book", "category": "books", "price": 29.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "rust course", "category": "education", "price": 49.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "python book", "category": "books", "price": 19.99}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // must: match "rust", must_not: category=education, filter: price 10-100 (integer bounds)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("education"));
                    m
                })],
                filter: vec![QueryClause::Range({
                    let mut m = HashMap::new();
                    m.insert(
                        "price".into(),
                        crate::search::RangeCondition {
                            gte: Some(json!(10)),
                            lte: Some(json!(100)),
                            ..Default::default()
                        },
                    );
                    m
                })],
                should: vec![],
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "complex bool with all clause types should match d1 only"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn float_field_indexed_with_integer_value_is_searchable() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "price".to_string(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // Index with integer JSON value on a float field
        engine.add_document("d1", json!({"price": 100})).unwrap();
        engine.add_document("d2", json!({"price": 200})).unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Range({
                let mut m = HashMap::new();
                m.insert(
                    "price".to_string(),
                    crate::search::RangeCondition {
                        gte: Some(json!(50.0)),
                        lte: Some(json!(150.0)),
                        ..Default::default()
                    },
                );
                m
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "integer value indexed on float field should be searchable"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_should_only_matches_any_clause() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust book", "category": "books", "price": 29.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "python course", "category": "education", "price": 49.99}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "go tutorial", "category": "education", "price": 9.99}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // should with no must: any matching should clause is sufficient
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                should: vec![
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("title".into(), json!("rust"));
                        m
                    }),
                    QueryClause::Match({
                        let mut m = HashMap::new();
                        m.insert("title".into(), json!("go"));
                        m
                    }),
                ],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "should-only bool should match d1 (rust) and d3 (go)"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn bool_must_not_with_filter_only() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"category": "books", "price": 10.0}))
            .unwrap();
        engine
            .add_document("d2", json!({"category": "education", "price": 50.0}))
            .unwrap();
        engine
            .add_document("d3", json!({"category": "books", "price": 100.0}))
            .unwrap();
        engine.refresh().unwrap();

        // must_not + filter, no must: exclude education, filter price >= 5
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("education"));
                    m
                })],
                filter: vec![QueryClause::Range({
                    let mut m = HashMap::new();
                    m.insert(
                        "price".into(),
                        crate::search::RangeCondition {
                            gte: Some(json!(5)),
                            ..Default::default()
                        },
                    );
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "must_not + filter should return d1 and d3 (books only)"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d3"));
    }

    #[test]
    fn bool_multiple_must_not_excludes_all() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "item one", "category": "books"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "item two", "category": "education"}))
            .unwrap();
        engine
            .add_document("d3", json!({"title": "item three", "category": "sports"}))
            .unwrap();
        engine
            .add_document("d4", json!({"title": "item four", "category": "toys"}))
            .unwrap();
        engine.refresh().unwrap();

        // Exclude books AND education simultaneously
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("item"));
                    m
                })],
                must_not: vec![
                    QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("books"));
                        m
                    }),
                    QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("education"));
                        m
                    }),
                ],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 2,
            "multiple must_not should exclude both books and education"
        );
        let ids: Vec<&str> = hits.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"d3"));
        assert!(ids.contains(&"d4"));
    }

    #[test]
    fn nested_bool_inside_must() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document(
                "d1",
                json!({"title": "rust guide", "category": "books", "price": 25.0}),
            )
            .unwrap();
        engine
            .add_document(
                "d2",
                json!({"title": "rust video", "category": "education", "price": 75.0}),
            )
            .unwrap();
        engine
            .add_document(
                "d3",
                json!({"title": "python guide", "category": "books", "price": 15.0}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // Outer: must match "rust". Inner (nested bool in filter): price 20-50 AND category != education
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                filter: vec![QueryClause::Bool(crate::search::BoolQuery {
                    filter: vec![QueryClause::Range({
                        let mut m = HashMap::new();
                        m.insert(
                            "price".into(),
                            crate::search::RangeCondition {
                                gte: Some(json!(20)),
                                lte: Some(json!(50)),
                                ..Default::default()
                            },
                        );
                        m
                    })],
                    must_not: vec![QueryClause::Term({
                        let mut m = HashMap::new();
                        m.insert("category".into(), json!("education"));
                        m
                    })],
                    ..Default::default()
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 1,
            "nested bool should match only d1 (rust, books, price 25)"
        );
        assert_eq!(hits[0]["_id"], "d1");
    }

    #[test]
    fn bool_must_not_excludes_everything_returns_zero() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".into(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );
        mappings.insert(
            "category".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        engine
            .add_document("d1", json!({"title": "rust book", "category": "books"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "rust course", "category": "books"}))
            .unwrap();
        engine.refresh().unwrap();

        // must matches both, but must_not also excludes all (same category)
        let req = SearchRequest {
            query: QueryClause::Bool(crate::search::BoolQuery {
                must: vec![QueryClause::Match({
                    let mut m = HashMap::new();
                    m.insert("title".into(), json!("rust"));
                    m
                })],
                must_not: vec![QueryClause::Term({
                    let mut m = HashMap::new();
                    m.insert("category".into(), json!("books"));
                    m
                })],
                ..Default::default()
            }),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "must_not excluding all docs should return 0 hits");
        assert!(hits.is_empty());
    }

    #[test]
    fn unmapped_fields_still_searchable_via_body() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // "description" is not mapped — should still be searchable via body catch-all
        engine
            .add_document(
                "d1",
                json!({"title": "test", "description": "rust programming"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("rust").unwrap();
        assert_eq!(
            hits.len(),
            1,
            "unmapped field should be searchable via body"
        );
    }

    #[test]
    fn empty_mappings_behave_like_default_engine() {
        let (_dir, engine) = create_engine_with_mappings(HashMap::new());
        engine
            .add_document("d1", json!({"title": "hello world"}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("hello").unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn knn_vector_mapping_is_skipped_in_tantivy_schema() {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "embedding".to_string(),
            FieldMapping {
                field_type: FieldType::KnnVector,
                dimension: Some(3),
            },
        );
        mappings.insert(
            "title".to_string(),
            FieldMapping {
                field_type: FieldType::Text,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        // knn_vector should be ignored by Tantivy — no field created for it
        engine
            .add_document("d1", json!({"title": "test", "embedding": [1.0, 0.0, 0.0]}))
            .unwrap();
        engine.refresh().unwrap();

        let hits = engine.search("test").unwrap();
        assert_eq!(
            hits.len(),
            1,
            "doc should be searchable despite knn_vector field"
        );
    }

    // ── last_seq_no and flush_with_global_checkpoint ────────────────────

    #[test]
    fn last_seq_no_returns_zero_on_empty_engine() {
        let (_dir, engine) = create_engine();
        assert_eq!(engine.last_seq_no(), 0);
    }

    #[test]
    fn last_seq_no_tracks_writes() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        assert_eq!(engine.last_seq_no(), 0);

        engine.add_document("d2", json!({"x": 2})).unwrap();
        assert_eq!(engine.last_seq_no(), 1);
    }

    #[test]
    fn last_seq_no_after_bulk() {
        let (_dir, engine) = create_engine();
        engine
            .bulk_add_documents(vec![
                ("a".into(), json!({"x": 1})),
                ("b".into(), json!({"x": 2})),
                ("c".into(), json!({"x": 3})),
            ])
            .unwrap();
        assert_eq!(engine.last_seq_no(), 2, "3 entries → seq_nos 0,1,2");
    }

    #[test]
    fn flush_with_global_checkpoint_retains_above() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap(); // seq_no=0
        engine.add_document("d2", json!({"x": 2})).unwrap(); // seq_no=1
        engine.add_document("d3", json!({"x": 3})).unwrap(); // seq_no=2

        // Flush retaining entries above global_checkpoint=1
        engine.flush_with_global_checkpoint(1).unwrap();

        // Seq_no should be preserved
        assert_eq!(engine.last_seq_no(), 2);

        // All docs should still be searchable
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 3);
    }

    #[test]
    fn flush_with_global_checkpoint_zero_truncates_all() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();

        // global_checkpoint=0 → truncate() (discard all)
        engine.flush_with_global_checkpoint(0).unwrap();

        // Seq_no preserved
        assert_eq!(engine.last_seq_no(), 1);

        // Docs still searchable (committed)
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);
    }

    #[test]
    fn peer_recovery_snapshot_has_exact_boundary_and_retained_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
        engine.add_document("d0", json!({"value": 0})).unwrap();
        engine.add_document("d1", json!({"value": 1})).unwrap();

        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *engine.peer_recovery_snapshot_ready_sender.lock().unwrap() = Some(ready_tx);
        *engine
            .peer_recovery_snapshot_release_receiver
            .lock()
            .unwrap() = Some(release_rx);
        let snapshot_dir = dir.path().join("peer-recovery").join("session");
        let snapshot_engine = engine.clone();
        let snapshot_dir_for_thread = snapshot_dir.clone();
        let snapshot_handle = std::thread::spawn(move || {
            snapshot_engine.create_peer_recovery_snapshot(&snapshot_dir_for_thread)
        });

        assert_eq!(
            ready_rx.recv_timeout(TEST_SYNC_TIMEOUT).unwrap(),
            2,
            "snapshot boundary must be captured before the concurrent write"
        );
        assert!(
            engine.maintenance_lock.try_lock().is_ok(),
            "snapshot hashing must not hold the maintenance lock"
        );
        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let writer_engine = engine.clone();
        let writer_handle = std::thread::spawn(move || {
            attempted_tx.send(()).unwrap();
            let result = writer_engine.add_document_with_receipt("d2", json!({"value": 2}));
            completed_tx.send(()).unwrap();
            result
        });
        attempted_rx.recv_timeout(TEST_SYNC_TIMEOUT).unwrap();
        completed_rx.recv_timeout(TEST_SYNC_TIMEOUT).unwrap();
        release_tx.send(()).unwrap();
        let write_receipt = writer_handle.join().unwrap().unwrap();
        let snapshot = snapshot_handle.join().unwrap().unwrap();
        assert_eq!(snapshot.snapshot_next_seq_no, 2);
        assert_eq!(write_receipt.seq_no, 2);

        let snapshot_index =
            Index::open(tantivy::directory::MmapDirectory::open(&snapshot_dir).unwrap()).unwrap();
        let snapshot_reader = snapshot_index.reader().unwrap();
        assert_eq!(snapshot_reader.searcher().num_docs(), 2);

        let suffix = engine.retained_recovery_ops(2, 16, 1024 * 1024).unwrap();
        assert!(suffix.complete);
        assert_eq!(suffix.source_max_seq_no, Some(2));
        assert_eq!(
            suffix
                .operations
                .iter()
                .map(|entry| entry.seq_no)
                .collect::<Vec<_>>(),
            [2]
        );
        engine
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
    }

    #[test]
    fn peer_recovery_snapshot_rejects_a_processed_gap() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        apply_index(&engine, "zero", json!({"value": 0}), 0, 1);
        apply_index(&engine, "two", json!({"value": 2}), 2, 1);

        let snapshot_dir = dir.path().join("peer-recovery").join("gap");
        let error = match engine.prepare_peer_recovery_snapshot(&snapshot_dir) {
            Ok(prepared) => {
                drop(prepared);
                panic!("a snapshot cannot discard processed intervals above a gap");
            }
            Err(error) => error,
        };

        assert!(
            error.to_string().contains("processed checkpoint")
                && error.to_string().contains("maximum sequence"),
            "{error:#}"
        );
        assert!(!snapshot_dir.exists());
    }

    #[test]
    fn peer_recovery_cursor_stops_before_an_unprocessed_wal_entry() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        apply_index(&engine, "zero", json!({"value": 0}), 0, 1);
        let after_processed = engine
            .with_translog("capture processed cursor", |translog| {
                Ok(translog.recovery_read_snapshot()?.end_cursor())
            })
            .unwrap();
        engine
            .with_translog("append unapplied recovery entry", |translog| {
                translog.append_with_seq(
                    1,
                    1,
                    crate::wal::WalOperation::Index,
                    json!({
                        "_doc_id": "one",
                        "_source": {"value": 1}
                    }),
                )?;
                Ok(())
            })
            .unwrap();
        let wal_end = engine
            .with_translog("capture WAL end", |translog| {
                Ok(translog.recovery_read_snapshot()?.end_cursor())
            })
            .unwrap();

        let batch = engine
            .peer_recovery_ops(
                crate::wal::WalCursor {
                    generation_id: after_processed.generation_id,
                    byte_offset: 0,
                },
                Some(wal_end),
                16,
                usize::MAX,
            )
            .unwrap();

        assert_eq!(
            batch
                .operations
                .iter()
                .map(|entry| entry.seq_no)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(batch.next_cursor, after_processed);
        assert!(!batch.complete);
    }

    #[test]
    fn peer_recovery_pin_is_respected_by_every_flush_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("base", json!({"value": 0})).unwrap();
        let snapshot_dir = dir.path().join("peer-recovery").join("session");
        let snapshot = engine.create_peer_recovery_snapshot(&snapshot_dir).unwrap();
        assert_eq!(snapshot.snapshot_next_seq_no, 1);

        let assert_retained = |expected: &[u64]| {
            let batch = engine
                .retained_recovery_ops(snapshot.snapshot_next_seq_no, 32, 1024 * 1024)
                .unwrap();
            assert_eq!(
                batch
                    .operations
                    .iter()
                    .map(|entry| entry.seq_no)
                    .collect::<Vec<_>>(),
                expected
            );
        };

        engine.add_document("flush", json!({"value": 1})).unwrap();
        engine.flush().unwrap();
        assert_retained(&[1]);

        engine
            .add_document("checkpoint", json!({"value": 2}))
            .unwrap();
        engine.flush_with_global_checkpoint(u64::MAX).unwrap();
        assert_retained(&[1, 2]);

        engine.add_document("zero", json!({"value": 3})).unwrap();
        engine.flush_with_global_checkpoint(0).unwrap();
        assert_retained(&[1, 2, 3]);

        engine.add_document("try", json!({"value": 4})).unwrap();
        assert!(engine.try_flush_with_global_checkpoint(u64::MAX).unwrap());
        assert_retained(&[1, 2, 3, 4]);

        engine
            .release_peer_recovery_pin(snapshot.retention_pin_id)
            .unwrap();
        engine.flush().unwrap();
        assert!(
            engine
                .retained_recovery_ops(snapshot.snapshot_next_seq_no, 32, 1024 * 1024)
                .unwrap()
                .operations
                .is_empty()
        );
    }

    #[test]
    fn peer_recovery_scan_does_not_block_concurrent_write() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap());
        let padding = "x".repeat(32 * 1024);
        for index in 0..32 {
            engine
                .add_document(
                    &format!("prefix-{index}"),
                    json!({"value": index, "padding": padding}),
                )
                .unwrap();
        }
        let suffix = engine
            .add_document_with_receipt("suffix", json!({"value": 32}))
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        engine.set_peer_recovery_scan_barrier_for_test(barrier.clone());

        let scan_engine = engine.clone();
        let scan = std::thread::spawn(move || {
            scan_engine.retained_recovery_ops(suffix.seq_no, 16, 4 * 1024 * 1024)
        });
        barrier.wait();

        let (completed_tx, completed_rx) = mpsc::channel();
        let writer_engine = engine.clone();
        let writer = std::thread::spawn(move || {
            let result =
                writer_engine.add_document_with_receipt("concurrent", json!({"value": 33}));
            completed_tx.send(()).unwrap();
            result
        });
        completed_rx
            .recv_timeout(TEST_SYNC_TIMEOUT)
            .expect("WAL scan outside the lock must not block writes");
        barrier.wait();

        let scanned = scan.join().unwrap().unwrap();
        assert_eq!(scanned.operations[0].seq_no, suffix.seq_no);
        assert_eq!(writer.join().unwrap().unwrap().seq_no, suffix.seq_no + 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn peer_recovery_pin_drop_does_not_block_tokio_worker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();
        engine.add_document("base", json!({"value": 0})).unwrap();
        let snapshot_dir = dir.path().join("peer-recovery/session");
        let preparation = engine
            .prepare_peer_recovery_snapshot(&snapshot_dir)
            .unwrap();

        let translog = engine.translog.clone();
        let (held_tx, held_rx) = mpsc::channel();
        let (runtime_progress_tx, runtime_progress_rx) = mpsc::channel();
        let (holder_result_tx, holder_result_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _guard = translog.lock().unwrap();
            held_tx.send(()).unwrap();
            let observed = runtime_progress_rx.recv_timeout(TEST_SYNC_TIMEOUT).is_ok();
            holder_result_tx.send(observed).unwrap();
        });
        held_rx.recv_timeout(TEST_SYNC_TIMEOUT).unwrap();

        drop(preparation);
        runtime_progress_tx
            .send(())
            .expect("pin drop must return control to the Tokio worker");
        assert!(
            holder_result_rx.recv_timeout(TEST_SYNC_TIMEOUT).unwrap(),
            "blocking pin cleanup ran inline on the Tokio worker"
        );
        holder.join().unwrap();
        let _ = std::fs::remove_dir_all(snapshot_dir);
    }

    #[test]
    fn try_flush_with_global_checkpoint_returns_false_when_translog_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        let _tl = engine.translog.lock().unwrap();

        let flushed = engine.try_flush_with_global_checkpoint(1).unwrap();
        assert!(!flushed);
    }

    #[test]
    fn try_flush_with_global_checkpoint_returns_false_when_writer_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        let _writer = engine.writer_lock_for_test();

        let flushed = engine.try_flush_with_global_checkpoint(1).unwrap();
        assert!(!flushed);
    }

    #[test]
    fn try_flush_with_global_checkpoint_returns_error_when_translog_is_poisoned() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = engine.translog.lock().unwrap();
            panic!("poison translog lock");
        }));

        let err = engine.try_flush_with_global_checkpoint(1).unwrap_err();
        assert!(
            err.to_string()
                .contains("translog lock poisoned during checkpoint-aware flush")
        );
    }

    #[test]
    fn add_document_returns_error_when_translog_lock_is_poisoned() {
        let (_dir, engine) = create_engine();

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = engine.translog.lock().unwrap();
            panic!("poison translog lock");
        }));

        let err = engine
            .add_document("doc-1", json!({"title": "poisoned"}))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("translog lock poisoned during document indexing")
        );
        assert_eq!(engine.doc_count(), 0);
    }

    #[test]
    fn refresh_returns_error_when_translog_lock_is_poisoned() {
        let (_dir, engine) = create_engine();

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = engine.translog.lock().unwrap();
            panic!("poison translog lock");
        }));

        let err = engine.refresh().unwrap_err();
        assert!(
            err.to_string()
                .contains("translog lock poisoned during refresh")
        );
    }

    // ── refresh visibility ──────────────────────────────────────────────

    #[test]
    fn docs_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "invisible"}))
            .unwrap();

        // No refresh — doc should NOT be searchable
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "docs must not be visible before refresh");
        assert!(hits.is_empty());

        // get_document should also return None (not committed)
        let doc = engine.get_document("d1").unwrap();
        assert!(doc.is_none(), "get_document must not find uncommitted doc");
    }

    #[test]
    fn docs_visible_after_refresh() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("d1", json!({"title": "now visible"}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "also visible"}))
            .unwrap();

        // Refresh commits + reloads reader
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(engine.doc_count(), 2);
    }

    #[test]
    fn bulk_docs_not_visible_before_refresh() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..50)
            .map(|i| (format!("b{i}"), json!({"val": i})))
            .collect();
        engine.bulk_add_documents(docs).unwrap();

        // No refresh — none should be searchable
        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(total, 0, "bulk docs must not be visible before refresh");
        assert!(hits.is_empty());
    }

    #[test]
    fn bulk_docs_visible_after_refresh() {
        let (_dir, engine) = create_engine();
        let docs: Vec<(String, serde_json::Value)> = (0..50)
            .map(|i| (format!("b{i}"), json!({"val": i})))
            .collect();
        engine.bulk_add_documents(docs).unwrap();

        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let (hits, total, _) = engine.search_query(&req).unwrap();
        assert_eq!(
            total, 50,
            "all 50 bulk docs should be visible after refresh"
        );
        assert_eq!(hits.len(), 10, "engine returns exactly size=10 hits");
        assert_eq!(engine.doc_count(), 50);
    }

    #[test]
    fn incremental_refresh_visibility() {
        let (_dir, engine) = create_engine();

        // Batch 1: add + refresh
        engine.add_document("a1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        // Batch 2: add without refresh — old docs still visible, new ones not
        engine.add_document("a2", json!({"x": 2})).unwrap();
        assert_eq!(engine.doc_count(), 1, "a2 not visible until refresh");

        // Refresh again — both visible
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 2);
    }

    #[test]
    fn refresh_idempotent() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);

        // Multiple refreshes with no new writes should be fine
        engine.refresh().unwrap();
        engine.refresh().unwrap();
        assert_eq!(engine.doc_count(), 1);
    }

    #[test]
    fn force_merge_compacts_segments() {
        let (_dir, engine) = create_engine();

        // Create multiple segments by committing between writes.
        for i in 0..10 {
            engine
                .add_document(&format!("d{i}"), json!({"x": i}))
                .unwrap();
            engine.refresh().unwrap(); // each refresh commits → new segment
        }

        let before = engine.index.searchable_segment_ids().unwrap().len();
        assert!(
            before > 1,
            "expected multiple segments before merge, got {before}"
        );

        engine.force_merge(1).unwrap();

        let after = engine.index.searchable_segment_ids().unwrap().len();
        assert_eq!(after, 1, "expected 1 segment after force_merge(1)");
        assert_eq!(engine.doc_count(), 10, "doc count must be preserved");
    }

    #[test]
    fn force_merge_noop_when_already_compact() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();

        // Already 1 segment — force_merge(1) should be a no-op.
        engine.force_merge(1).unwrap();
        assert_eq!(engine.doc_count(), 1);
        assert_eq!(engine.index.searchable_segment_ids().unwrap().len(), 1);
    }

    #[test]
    fn force_merge_respects_max_num_segments() {
        let (_dir, engine) = create_engine();
        for i in 0..20 {
            engine
                .add_document(&format!("d{i}"), json!({"x": i}))
                .unwrap();
            engine.refresh().unwrap();
        }

        engine.force_merge(3).unwrap();

        let after = engine.index.searchable_segment_ids().unwrap().len();
        assert!(
            after <= 3,
            "expected at most 3 segments after force_merge(3), got {after}"
        );
        assert_eq!(engine.doc_count(), 20);
    }

    #[test]
    fn concurrent_force_merges_do_not_race_segment_replacement() {
        let (_dir, engine) = create_engine();
        engine
            .set_merge_policy_for_test(Box::new(NoMergePolicy))
            .unwrap();

        for i in 0..8 {
            engine
                .add_document(
                    &format!("d{i}"),
                    json!({"ordinal": i, "value": format!("value-{i}")}),
                )
                .unwrap();
            engine.refresh().unwrap();
        }
        engine
            .add_document("d3", json!({"ordinal": 303, "value": "updated"}))
            .unwrap();
        engine.delete_document("d5").unwrap();
        engine.refresh().unwrap();

        let engine = Arc::new(engine);
        *engine
            .force_merge_entry_barrier
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(std::sync::Barrier::new(2)));

        let first_engine = engine.clone();
        let first = std::thread::spawn(move || first_engine.force_merge(1));
        let second_engine = engine.clone();
        let second = std::thread::spawn(move || second_engine.force_merge(1));

        let first_result = first.join().unwrap();
        let second_result = second.join().unwrap();
        assert!(
            first_result.is_ok() && second_result.is_ok(),
            "both overlapping force merges must succeed: first={first_result:?}, second={second_result:?}"
        );
        assert_eq!(engine.index.searchable_segment_ids().unwrap().len(), 1);
        assert_eq!(engine.doc_count(), 7);
        assert!(engine.get_document("d5").unwrap().is_none());
        for i in 0..8 {
            if i == 5 {
                continue;
            }
            let document = engine
                .get_document(&format!("d{i}"))
                .unwrap()
                .expect("surviving document must remain readable");
            if i == 3 {
                assert_eq!(document["ordinal"], 303);
                assert_eq!(document["value"], "updated");
            } else {
                assert_eq!(document["ordinal"], i);
                assert_eq!(document["value"], format!("value-{i}"));
            }
        }
    }

    #[test]
    fn force_merge_drains_in_flight_automatic_merge_before_manual_compaction() {
        #[derive(Debug)]
        struct OneShotMergeAllPolicy {
            calls: Arc<AtomicUsize>,
            candidates: Arc<AtomicUsize>,
        }

        impl MergePolicy for OneShotMergeAllPolicy {
            fn compute_merge_candidates(&self, segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if segments.len() > 1
                    && self
                        .candidates
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    return vec![MergeCandidate(
                        segments.iter().map(SegmentMeta::id).collect(),
                    )];
                }
                Vec::new()
            }
        }

        let (_dir, engine, merge_gate, merge_started, release_merge) =
            create_engine_with_blocked_merge();
        engine
            .set_merge_policy_for_test(Box::new(NoMergePolicy))
            .unwrap();
        for i in 0..8 {
            engine
                .add_document(
                    &format!("d{i}"),
                    json!({"ordinal": i, "value": format!("value-{i}")}),
                )
                .unwrap();
            engine.refresh().unwrap();
        }

        let policy_calls = Arc::new(AtomicUsize::new(0));
        let automatic_candidates = Arc::new(AtomicUsize::new(0));
        engine
            .set_merge_policy_for_test(Box::new(OneShotMergeAllPolicy {
                calls: policy_calls.clone(),
                candidates: automatic_candidates.clone(),
            }))
            .unwrap();
        merge_gate.arm();
        engine
            .add_document("d8", json!({"ordinal": 8, "value": "value-8"}))
            .unwrap();
        engine.refresh().unwrap();

        merge_started
            .recv_timeout(TEST_SYNC_TIMEOUT)
            .expect("automatic merge must reach the merge-thread write boundary");
        assert_eq!(
            automatic_candidates.load(Ordering::SeqCst),
            1,
            "the blocked merge must come from the automatic merge policy"
        );

        engine
            .add_document("d3", json!({"ordinal": 303, "value": "updated"}))
            .unwrap();
        engine.delete_document("d5").unwrap();
        let expected_next_seq = engine
            .with_translog("automatic force-merge test", |wal| Ok(wal.next_seq_no()))
            .unwrap();
        let checkpoint_before_force_merge = engine.load_committed_next_seq_no().unwrap();
        assert!(
            checkpoint_before_force_merge < expected_next_seq,
            "the force merge must commit the update and delete queued while the automatic merge is blocked"
        );

        let engine = Arc::new(engine);
        let (before_wait_sender, before_wait_receiver) = mpsc::channel();
        *engine
            .force_merge_before_wait_sender
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(before_wait_sender);
        let (result_sender, result_receiver) = mpsc::channel();
        let force_merge_engine = engine.clone();
        let force_merge_thread = std::thread::spawn(move || {
            let _ = result_sender.send(force_merge_engine.force_merge(1));
        });

        if let Err(error) = before_wait_receiver.recv_timeout(TEST_SYNC_TIMEOUT) {
            let _ = release_merge.send(());
            panic!("force merge did not reach the automatic-merge drain boundary: {error}");
        }
        let early_result = result_receiver.try_recv();
        release_merge
            .send(())
            .expect("blocked automatic merge must still be waiting for release");
        assert!(
            matches!(early_result, Err(TryRecvError::Empty)),
            "force merge must not complete before the in-flight automatic merge is released: {early_result:?}"
        );

        let force_merge_result = result_receiver
            .recv_timeout(MERGE_GATE_TIMEOUT)
            .expect("force merge must complete after the automatic merge is released");
        force_merge_result.unwrap();
        force_merge_thread.join().unwrap();

        assert_eq!(engine.index.searchable_segment_ids().unwrap().len(), 1);
        assert_eq!(engine.doc_count(), 8);
        assert_eq!(
            engine.load_committed_next_seq_no().unwrap(),
            expected_next_seq,
            "force merge must persist the exact committed WAL watermark"
        );
        assert!(engine.get_document("d5").unwrap().is_none());
        for i in 0..=8 {
            if i == 5 {
                continue;
            }
            let document = engine
                .get_document(&format!("d{i}"))
                .unwrap()
                .expect("surviving document must remain readable");
            if i == 3 {
                assert_eq!(document["ordinal"], 303);
                assert_eq!(document["value"], "updated");
            } else {
                assert_eq!(document["ordinal"], i);
                assert_eq!(document["value"], format!("value-{i}"));
            }
        }

        let calls_after_force_merge = policy_calls.load(Ordering::SeqCst);
        engine
            .add_document("after", json!({"value": "after"}))
            .unwrap();
        engine.refresh().unwrap();
        assert!(
            policy_calls.load(Ordering::SeqCst) > calls_after_force_merge,
            "the original automatic merge policy must be active after force merge"
        );
    }

    #[test]
    fn force_merge_restores_automatic_merge_policy() {
        #[derive(Debug)]
        struct CountingMergePolicy(Arc<std::sync::atomic::AtomicUsize>);

        impl MergePolicy for CountingMergePolicy {
            fn compute_merge_candidates(&self, _segments: &[SegmentMeta]) -> Vec<MergeCandidate> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Vec::new()
            }
        }

        let (_dir, engine) = create_engine();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        engine
            .set_merge_policy_for_test(Box::new(CountingMergePolicy(calls.clone())))
            .unwrap();
        for i in 0..3 {
            engine
                .add_document(&format!("before-{i}"), json!({"value": i}))
                .unwrap();
            engine.refresh().unwrap();
        }

        engine.force_merge(1).unwrap();
        let calls_after_force_merge = calls.load(std::sync::atomic::Ordering::SeqCst);
        engine
            .add_document("after", json!({"value": "after"}))
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > calls_after_force_merge,
            "the original automatic merge policy must be active after force merge"
        );
    }

    #[test]
    fn force_merge_rejects_zero_segments_without_mutation() {
        let (_dir, engine) = create_engine();
        engine.add_document("d1", json!({"value": 1})).unwrap();
        engine.refresh().unwrap();
        let before_segments = engine.index.searchable_segment_ids().unwrap();

        let error = engine.force_merge(0).unwrap_err();

        assert!(error.to_string().contains("at least 1"));
        assert_eq!(
            engine.index.searchable_segment_ids().unwrap(),
            before_segments
        );
        assert_eq!(engine.get_document("d1").unwrap().unwrap()["value"], 1);
    }

    #[test]
    fn force_merge_persists_committed_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let engine = HotEngine::new(dir.path(), Duration::from_secs(60)).unwrap();

        engine.add_document("d1", json!({"x": 1})).unwrap();
        engine.refresh().unwrap();
        engine.add_document("d2", json!({"x": 2})).unwrap();
        engine.add_document("d3", json!({"x": 3})).unwrap();

        let expected_next_seq = {
            let tl = engine.translog.lock().unwrap();
            tl.next_seq_no()
        };
        let checkpoint_path = dir.path().join("translog.committed");
        let before_force_merge = CommittedBoundaryRecord::load(&checkpoint_path)
            .unwrap()
            .unwrap()
            .processed_checkpoint
            .and_then(|checkpoint| checkpoint.checked_add(1))
            .unwrap_or(0);
        assert!(
            before_force_merge < expected_next_seq,
            "expected pending writes before force-merge checkpoint advance"
        );

        engine.force_merge(1).unwrap();

        let after_force_merge = CommittedBoundaryRecord::load(&checkpoint_path)
            .unwrap()
            .unwrap()
            .processed_checkpoint
            .and_then(|checkpoint| checkpoint.checked_add(1))
            .unwrap_or(0);
        assert_eq!(
            after_force_merge, expected_next_seq,
            "force_merge must advance translog.committed to the committed next seq_no"
        );
    }

    // ── Stored-fields optimization tests (fast-field _id path) ──────────

    fn create_typed_engine() -> (tempfile::TempDir, HotEngine) {
        let dir = tempfile::tempdir().unwrap();
        let mut mappings = HashMap::new();
        mappings.insert(
            "title".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Float,
                dimension: None,
            },
        );
        mappings.insert(
            "category".to_string(),
            crate::cluster::state::FieldMapping {
                field_type: crate::cluster::state::FieldType::Keyword,
                dimension: None,
            },
        );
        let engine = HotEngine::new_with_mappings(
            dir.path(),
            Duration::from_secs(60),
            &mappings,
            crate::wal::TranslogDurability::Request,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
        .unwrap();
        (dir, engine)
    }

    #[test]
    fn sql_batch_fast_path_reads_id_from_fast_field() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // All requested columns (title, price) have fast fields → fast path
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-alpha", "doc-beta"]);
    }

    #[test]
    fn sql_batch_fast_path_reads_keyword_values_correctly() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-gamma",
                json!({"title": "Widget", "price": 9.99, "category": "gadgets"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "category".to_string()],
                true,
                false,
            )
            .unwrap();

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let title_col = result
            .batch
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let category_col = result
            .batch
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();

        let mut rows: Vec<(String, String, String)> = (0..result.batch.num_rows())
            .map(|i| {
                (
                    id_col.value(i).to_string(),
                    title_col.value(i).to_string(),
                    category_col.value(i).to_string(),
                )
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("doc-alpha".into(), "Widget".into(), "gadgets".into()),
                ("doc-beta".into(), "Sprocket".into(), "parts".into()),
                ("doc-gamma".into(), "Widget".into(), "gadgets".into()),
            ]
        );
    }

    #[test]
    fn sql_batch_fast_path_reorders_ids_with_multi_segment_sort() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("doc-low", json!({"title": "Low", "price": 10.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document("doc-high", json!({"title": "High", "price": 30.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine
            .add_document("doc-mid", json!({"title": "Mid", "price": 20.0}))
            .unwrap();
        engine.refresh().unwrap();

        let mut price_sort = HashMap::new();
        price_sort.insert(
            "price".to_string(),
            crate::search::SortOrder::Direction(crate::search::SortDirection::Desc),
        );

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![crate::search::SortClause::Field(price_sort)],
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, false)
            .unwrap();

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let price_col = result
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();

        let rows: Vec<(String, f64)> = (0..result.batch.num_rows())
            .map(|i| (id_col.value(i).to_string(), price_col.value(i)))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("doc-high".into(), 30.0),
                ("doc-mid".into(), 20.0),
                ("doc-low".into(), 10.0),
            ]
        );
    }

    #[test]
    fn sql_batch_fast_path_id_only_query_returns_ids() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("doc-alpha", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("doc-beta", json!({"title": "Sprocket", "price": 5.50}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine.sql_record_batch(&req, &[], true, false).unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();

        let mut ids: Vec<String> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["doc-alpha", "doc-beta"]);
        assert_eq!(score_col.value(0), 0.0);
        assert_eq!(score_col.value(1), 0.0);
    }

    #[test]
    fn sql_batch_source_fallback_still_works() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "fb-1",
                json!({"title": "Laptop", "price": 1200.0, "description": "A fine laptop"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // "description" is not a mapped fast field → SourceFallback path
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "description".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "fb-1");

        let desc_col = result
            .batch
            .column_by_name("description")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(desc_col.value(0), "A fine laptop");
    }

    #[test]
    fn sql_batch_fast_path_preserves_correct_ids_after_delete() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("keep-me", json!({"title": "Keep", "price": 10.0}))
            .unwrap();
        engine
            .add_document("delete-me", json!({"title": "Delete", "price": 20.0}))
            .unwrap();
        engine.refresh().unwrap();
        engine.delete_document("delete-me").unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "keep-me");
    }

    #[test]
    fn sql_batch_fast_path_empty_result() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 5.0}))
            .unwrap();
        engine.refresh().unwrap();

        // Query that matches nothing
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::Term(HashMap::from([(
                "title".to_string(),
                json!("nonexistent"),
            )])),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
        assert_eq!(result.total_hits, 0);
    }

    #[test]
    fn sql_batch_fast_path_many_docs_ids_correct() {
        let (_dir, engine) = create_typed_engine();
        for i in 0..50 {
            engine
                .add_document(
                    &format!("doc-{i:03}"),
                    json!({"title": format!("item-{}", i), "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 100,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, true)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 50);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<String> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        ids.sort();
        let mut expected: Vec<String> = (0..50).map(|i| format!("doc-{i:03}")).collect();
        expected.sort();
        assert_eq!(ids, expected);
    }

    #[test]
    fn sql_batch_mixed_fast_and_fallback_columns() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "m1",
                json!({"title": "Gizmo", "price": 42.0, "notes": "special order"}),
            )
            .unwrap();
        engine
            .add_document(
                "m2",
                json!({"title": "Doodad", "price": 7.5, "notes": "in stock"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        // "price" is fast, "notes" is SourceFallback
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["price".to_string(), "notes".to_string()],
                true,
                true,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut ids: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| id_col.value(i))
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["m1", "m2"]);

        let notes_col = result
            .batch
            .column_by_name("notes")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let mut notes: Vec<&str> = (0..result.batch.num_rows())
            .map(|i| notes_col.value(i))
            .collect();
        notes.sort();
        assert_eq!(notes, vec!["in stock", "special order"]);
    }

    // ── _id/_score skip optimization tests ──────────────────────────────

    #[test]
    fn sql_batch_skip_id_and_score_when_not_needed() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "Gadget", "price": 29.99}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=false, needs_score=false
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(
                &req,
                &["title".to_string(), "price".to_string()],
                false,
                false,
            )
            .unwrap();
        assert_eq!(result.batch.num_rows(), 2);

        // _id column should exist but contain empty strings
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "");
        assert_eq!(id_col.value(1), "");

        // _score column should exist but contain zeros
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 0.0);
        assert_eq!(score_col.value(1), 0.0);

        // Data columns should still have correct values
        let price_col = result
            .batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap();
        let mut prices: Vec<f64> = (0..result.batch.num_rows())
            .map(|i| price_col.value(i))
            .collect();
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(prices, vec![19.99, 29.99]);
    }

    #[test]
    fn sql_batch_skip_id_only() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("x1", json!({"title": "A", "price": 1.0}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=false, needs_score=true
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], false, true)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        // _id should be empty string
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "");

        // _score should have real value (> 0)
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert!(score_col.value(0) > 0.0);
    }

    #[test]
    fn sql_batch_skip_score_only() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("y1", json!({"title": "B", "price": 2.0}))
            .unwrap();
        engine.refresh().unwrap();

        // needs_id=true, needs_score=false
        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], true, false)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 1);

        // _id should have real value
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "y1");

        // _score should be zero
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert_eq!(score_col.value(0), 0.0);
    }

    #[test]
    fn sql_batch_skip_both_many_docs() {
        let (_dir, engine) = create_typed_engine();
        for i in 0..100 {
            engine
                .add_document(
                    &format!("d-{i}"),
                    json!({"title": format!("item-{}", i), "price": i as f64 * 1.5}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine
            .sql_record_batch(&req, &["price".to_string()], false, false)
            .unwrap();
        assert_eq!(result.batch.num_rows(), 100);

        // All _id values should be empty
        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..100 {
            assert_eq!(id_col.value(i), "");
        }
    }

    #[test]
    fn sql_batch_zero_projection_preserves_row_count_without_needs_score() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document("d1", json!({"title": "Widget", "price": 19.99}))
            .unwrap();
        engine
            .add_document("d2", json!({"title": "Gadget", "price": 29.99}))
            .unwrap();
        engine.refresh().unwrap();

        let req = crate::search::SearchRequest {
            query: crate::search::QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: Vec::new(),
            search_after: None,
            aggs: HashMap::new(),
        };
        let result = engine.sql_record_batch(&req, &[], false, false).unwrap();
        assert_eq!(result.batch.num_rows(), 2);
        assert_eq!(result.batch.num_columns(), 2);

        let id_col = result
            .batch
            .column_by_name("_id")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let score_col = result
            .batch
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();

        for row in 0..result.batch.num_rows() {
            assert_eq!(id_col.value(row), "");
            assert_eq!(score_col.value(row), 0.0);
        }
    }

    // ── Batch numeric reads in GroupedAggCollector ───────────────────────

    /// Helper: extract count from a GroupedMetricPartial
    fn metric_count(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> u64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Count { count } => *count,
            crate::search::GroupedMetricPartial::Stats { count, .. } => *count,
        }
    }

    /// Helper: extract sum from a GroupedMetricPartial::Stats
    fn metric_sum(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { sum, .. } => *sum,
            _ => panic!("expected Stats for {name}"),
        }
    }

    /// Helper: extract avg from a GroupedMetricPartial::Stats
    fn metric_avg(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { count, sum, .. } => *sum / *count as f64,
            _ => panic!("expected Stats for {name}"),
        }
    }

    /// Helper: extract min from a GroupedMetricPartial::Stats
    fn metric_min(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { min, .. } => *min,
            _ => panic!("expected Stats for {name}"),
        }
    }

    /// Helper: extract max from a GroupedMetricPartial::Stats
    fn metric_max(bucket: &crate::search::GroupedMetricsBucket, name: &str) -> f64 {
        match &bucket.metrics[name] {
            crate::search::GroupedMetricPartial::Stats { max, .. } => *max,
            _ => panic!("expected Stats for {name}"),
        }
    }

    /// Helper: create an engine with brand (keyword), price (float), quantity (integer)
    /// mappings and insert `n` documents spread across two brands.
    fn create_grouped_numeric_engine(n: usize) -> (tempfile::TempDir, HotEngine) {
        create_grouped_numeric_engine_with_cache(
            n,
            std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(0, 0)),
        )
    }

    fn create_grouped_numeric_engine_with_cache(
        n: usize,
        column_cache: std::sync::Arc<crate::engine::column_cache::ColumnCache>,
    ) -> (tempfile::TempDir, HotEngine) {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );
        mappings.insert(
            "quantity".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let (_dir, engine) = create_engine_with_mappings_and_cache(mappings, column_cache);
        for i in 0..n {
            let brand = if i % 2 == 0 { "Apple" } else { "Samsung" };
            let price = 100.0 + i as f64;
            let quantity = (i + 1) as i64;
            engine
                .add_document(
                    &format!("d{i}"),
                    json!({"brand": brand, "price": price, "quantity": quantity, "body": "phone"}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();
        (_dir, engine)
    }

    fn grouped_numeric_request(
        _n_docs: usize,
        metrics: Vec<crate::search::GroupedMetricAgg>,
    ) -> SearchRequest {
        use crate::search::*;
        SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into()],
                    metrics,
                    shard_top_k: None,
                }),
            )]),
        }
    }

    /// Helper: create an engine with a two-key GROUP BY shape.
    fn create_grouped_pair_engine(n: usize) -> (tempfile::TempDir, HotEngine) {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "zone".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for i in 0..n {
            let brand = if i % 2 == 0 { "Apple" } else { "Samsung" };
            let zone = (i % 3) as i64;
            let price = 100.0 + i as f64;
            engine
                .add_document(
                    &format!("pair-doc-{i}"),
                    json!({"brand": brand, "zone": zone, "price": price, "body": "phone"}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();
        (_dir, engine)
    }

    fn grouped_pair_request(
        query: QueryClause,
        metrics: Vec<crate::search::GroupedMetricAgg>,
    ) -> SearchRequest {
        use crate::search::*;
        SearchRequest {
            query,
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into(), "zone".into()],
                    metrics,
                    shard_top_k: None,
                }),
            )]),
        }
    }

    fn expected_pair_group_metrics(n: usize) -> HashMap<(String, i64), (u64, f64)> {
        let mut expected = HashMap::new();
        for i in 0..n {
            let brand = if i % 2 == 0 { "Apple" } else { "Samsung" };
            let zone = (i % 3) as i64;
            let price = 100.0 + i as f64;
            let entry = expected
                .entry((brand.to_string(), zone))
                .or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += price;
        }
        expected
    }

    fn assert_pair_grouped_metrics(
        buckets: &[crate::search::GroupedMetricsBucket],
        expected: &HashMap<(String, i64), (u64, f64)>,
    ) {
        assert_eq!(buckets.len(), expected.len());
        for bucket in buckets {
            let brand = bucket.group_values[0].as_str().unwrap().to_string();
            let zone = bucket.group_values[1].as_i64().unwrap();
            let (expected_count, expected_sum) = expected.get(&(brand, zone)).unwrap();
            assert_eq!(metric_count(bucket, "cnt"), *expected_count);
            assert!(
                (metric_sum(bucket, "sum_price") - *expected_sum).abs() < 0.01,
                "bucket sum mismatch: actual={} expected={}",
                metric_sum(bucket, "sum_price"),
                expected_sum
            );
        }
    }

    #[test]
    fn grouped_match_all_direct_scan_populates_grouped_cache() {
        use crate::search::{GroupedMetricAgg, GroupedMetricFunction, PartialAggResult};

        let cache = std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(
            1024 * 1024,
            0,
        ));
        let (_dir, engine) = create_grouped_numeric_engine_with_cache(4097, cache);

        let req = grouped_numeric_request(
            4097,
            vec![GroupedMetricAgg {
                output_name: "avg_price".into(),
                function: GroupedMetricFunction::Avg,
                field: Some("price".into()),
                field_expr: None,
            }],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, 4097);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics partial");
        };
        assert_eq!(buckets.len(), 2);

        let searcher = engine.reader.searcher();
        let segments = searcher.segment_readers();
        assert!(!segments.is_empty());
        for segment in segments {
            let segment_id = segment.segment_id();
            assert!(matches!(
                engine.column_cache.get_grouped(segment_id, "brand"),
                Some(crate::engine::column_cache::GroupedColumnCache::StrOrds(_))
            ));
            assert!(matches!(
                engine.column_cache.get_grouped(segment_id, "price"),
                Some(crate::engine::column_cache::GroupedColumnCache::F64(_))
            ));
        }
        assert_eq!(
            engine.column_cache.entry_count(),
            searcher.segment_readers().len() as u64 * 2
        );
    }

    #[test]
    fn grouped_readers_only_reuse_warm_cache_entries() {
        use crate::search::GroupedMetricAgg;
        use crate::search::GroupedMetricFunction;

        let cache = std::sync::Arc::new(crate::engine::column_cache::ColumnCache::new(
            1024 * 1024,
            0,
        ));
        let (_dir, engine) = create_grouped_numeric_engine_with_cache(64, cache);
        let schema = engine.index.schema();

        {
            let searcher = engine.reader.searcher();
            let segments = searcher.segment_readers();
            assert!(!segments.is_empty());
            let segment = &segments[0];
            let ff = segment.fast_fields();
            let cache_ctx = GroupedCacheContext {
                column_cache: engine.column_cache.as_ref(),
                segment_id: segment.segment_id(),
                max_doc: segment.max_doc(),
                allow_populate: false,
            };

            match open_group_key_reader(&schema, ff, "brand", Some(cache_ctx)).unwrap() {
                GroupKeyReader::Str(reader) => assert!(reader.cached_ords.is_none()),
                _ => panic!("expected uncached string group key reader"),
            }

            let cold_plan =
                build_grouped_metric_plan(&schema, ff, Some("price"), None, Some(cache_ctx))
                    .unwrap();
            assert!(matches!(cold_plan.leaves.as_slice(), [NumCol::F64(_)]));
            assert_eq!(engine.column_cache.entry_count(), 0);
        }

        let warm_req = grouped_numeric_request(
            64,
            vec![GroupedMetricAgg {
                output_name: "avg_price".into(),
                function: GroupedMetricFunction::Avg,
                field: Some("price".into()),
                field_expr: None,
            }],
        );
        engine.search_query(&warm_req).unwrap();

        let searcher = engine.reader.searcher();
        let segments = searcher.segment_readers();
        assert!(!segments.is_empty());
        let segment = &segments[0];
        let ff = segment.fast_fields();
        let cache_ctx = GroupedCacheContext {
            column_cache: engine.column_cache.as_ref(),
            segment_id: segment.segment_id(),
            max_doc: segment.max_doc(),
            allow_populate: false,
        };

        match open_group_key_reader(&schema, ff, "brand", Some(cache_ctx)).unwrap() {
            GroupKeyReader::Str(reader) => assert!(reader.cached_ords.is_some()),
            _ => panic!("expected warm cached string group key reader"),
        }

        let warm_plan =
            build_grouped_metric_plan(&schema, ff, Some("price"), None, Some(cache_ctx)).unwrap();
        assert!(matches!(
            warm_plan.leaves.as_slice(),
            [NumCol::CachedF64(_)]
        ));
        assert_eq!(engine.column_cache.entry_count(), segments.len() as u64 * 2);
    }

    #[test]
    fn grouped_pair_match_all_direct_scan_matches_expected_metrics() {
        use crate::search::*;

        let n = 2050;
        let (_dir, engine) = create_grouped_pair_engine(n);
        let req = grouped_pair_request(
            QueryClause::MatchAll(json!({})),
            vec![
                GroupedMetricAgg {
                    output_name: "cnt".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                    field_expr: None,
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let expected = expected_pair_group_metrics(n);
        assert_pair_grouped_metrics(buckets, &expected);
    }

    #[test]
    fn grouped_pair_filtered_collector_matches_expected_metrics() {
        use crate::search::*;

        let n = 2050;
        let (_dir, engine) = create_grouped_pair_engine(n);
        let req = grouped_pair_request(
            QueryClause::Match(HashMap::from([("body".to_string(), json!("phone"))])),
            vec![
                GroupedMetricAgg {
                    output_name: "cnt".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                    field_expr: None,
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let expected = expected_pair_group_metrics(n);
        assert_pair_grouped_metrics(buckets, &expected);
    }

    #[test]
    fn pair_hash_map_keeps_swapped_keys_distinct() {
        let mut buckets = PairHashMap::with_capacity_and_hasher(4, PairBuildHasher::default());
        buckets.insert(
            pack_pair_group_key(7, 11),
            PairGroupedBucket {
                accums: vec![CompactMetricAccum::Count(1)],
            },
        );
        buckets.insert(
            pack_pair_group_key(11, 7),
            PairGroupedBucket {
                accums: vec![CompactMetricAccum::Count(2)],
            },
        );

        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key(&pack_pair_group_key(7, 11)));
        assert!(buckets.contains_key(&pack_pair_group_key(11, 7)));
    }

    #[test]
    fn pair_hasher_write_fallback_respects_full_input() {
        use std::hash::Hasher;

        let mut first = PairHasher::default();
        let mut second = PairHasher::default();
        let left = [0u8; 24];
        let mut right = [0u8; 24];
        right[23] = 1;

        first.write(&left);
        second.write(&right);

        assert_ne!(first.finish(), second.finish());
    }

    #[test]
    fn grouped_integer_group_by_distinguishes_negative_one_from_null() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{
            AggregationRequest, GroupedMetricAgg, GroupedMetricFunction, GroupedMetricsAggParams,
            PartialAggResult, QueryClause, SearchRequest,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "zone".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for (doc_id, payload) in [
            ("neg-1-a", json!({"zone": -1, "body": "phone"})),
            ("neg-1-b", json!({"zone": -1, "body": "phone"})),
            ("null-a", json!({"zone": null, "body": "phone"})),
            ("null-b", json!({"body": "phone"})),
            ("zero", json!({"zone": 0, "body": "phone"})),
        ] {
            engine.add_document(doc_id, payload).unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["zone".into()],
                    metrics: vec![GroupedMetricAgg {
                        output_name: "cnt".into(),
                        function: GroupedMetricFunction::Count,
                        field: None,
                        field_expr: None,
                    }],
                    shard_top_k: None,
                }),
            )]),
        };

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, 5);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let mut actual = HashMap::new();
        for bucket in buckets {
            actual.insert(bucket.group_values[0].as_i64(), metric_count(bucket, "cnt"));
        }

        let expected = HashMap::from([(Some(-1), 2), (None, 2), (Some(0), 1)]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_pair_group_by_distinguishes_negative_one_from_null() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{
            AggregationRequest, GroupedMetricAgg, GroupedMetricFunction, GroupedMetricsAggParams,
            PartialAggResult, QueryClause, SearchRequest,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "zone".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for (doc_id, payload) in [
            (
                "apple-neg-a",
                json!({"brand": "Apple", "zone": -1, "body": "phone"}),
            ),
            (
                "apple-neg-b",
                json!({"brand": "Apple", "zone": -1, "body": "phone"}),
            ),
            (
                "apple-null",
                json!({"brand": "Apple", "zone": null, "body": "phone"}),
            ),
            ("samsung-null", json!({"brand": "Samsung", "body": "phone"})),
            (
                "samsung-zero",
                json!({"brand": "Samsung", "zone": 0, "body": "phone"}),
            ),
        ] {
            engine.add_document(doc_id, payload).unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match(HashMap::from([("body".to_string(), json!("phone"))])),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into(), "zone".into()],
                    metrics: vec![GroupedMetricAgg {
                        output_name: "cnt".into(),
                        function: GroupedMetricFunction::Count,
                        field: None,
                        field_expr: None,
                    }],
                    shard_top_k: None,
                }),
            )]),
        };

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, 5);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let mut actual = HashMap::new();
        for bucket in buckets {
            let brand = bucket.group_values[0].as_str().unwrap().to_string();
            actual.insert(
                (brand, bucket.group_values[1].as_i64()),
                metric_count(bucket, "cnt"),
            );
        }

        let expected = HashMap::from([
            (("Apple".to_string(), Some(-1)), 2),
            (("Apple".to_string(), None), 1),
            (("Samsung".to_string(), None), 1),
            (("Samsung".to_string(), Some(0)), 1),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_multi_group_by_distinguishes_negative_one_from_null() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::{
            AggregationRequest, GroupedMetricAgg, GroupedMetricFunction, GroupedMetricsAggParams,
            PartialAggResult, QueryClause, SearchRequest,
        };

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "zone".into(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "shelf".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for (doc_id, payload) in [
            (
                "apple-neg-top-a",
                json!({"brand": "Apple", "zone": -1, "shelf": "top", "body": "phone"}),
            ),
            (
                "apple-neg-top-b",
                json!({"brand": "Apple", "zone": -1, "shelf": "top", "body": "phone"}),
            ),
            (
                "apple-null-top",
                json!({"brand": "Apple", "zone": null, "shelf": "top", "body": "phone"}),
            ),
            (
                "apple-null-bottom",
                json!({"brand": "Apple", "shelf": "bottom", "body": "phone"}),
            ),
            (
                "samsung-zero-bottom",
                json!({"brand": "Samsung", "zone": 0, "shelf": "bottom", "body": "phone"}),
            ),
        ] {
            engine.add_document(doc_id, payload).unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 0,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: HashMap::from([(
                "sql_grouped".into(),
                AggregationRequest::GroupedMetrics(GroupedMetricsAggParams {
                    group_by: vec!["brand".into(), "zone".into(), "shelf".into()],
                    metrics: vec![GroupedMetricAgg {
                        output_name: "cnt".into(),
                        function: GroupedMetricFunction::Count,
                        field: None,
                        field_expr: None,
                    }],
                    shard_top_k: None,
                }),
            )]),
        };

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, 5);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let mut actual = HashMap::new();
        for bucket in buckets {
            let brand = bucket.group_values[0].as_str().unwrap().to_string();
            let shelf = bucket.group_values[2].as_str().unwrap().to_string();
            actual.insert(
                (brand, bucket.group_values[1].as_i64(), shelf),
                metric_count(bucket, "cnt"),
            );
        }

        let expected = HashMap::from([
            (("Apple".to_string(), Some(-1), "top".to_string()), 2),
            (("Apple".to_string(), None, "top".to_string()), 1),
            (("Apple".to_string(), None, "bottom".to_string()), 1),
            (("Samsung".to_string(), Some(0), "bottom".to_string()), 1),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn grouped_batch_numeric_sum_correctness() {
        use crate::search::*;
        // Use > BATCH_SIZE (1024) docs to ensure multiple flushes exercise the batch path.
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "total".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                    field_expr: None,
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };
        assert_eq!(buckets.len(), 2);

        // Apple = even indices: 0,2,4,...,2048 → 1025 docs, prices: 100,102,104,...,2148
        // Samsung = odd indices: 1,3,5,...,2049 → 1025 docs, prices: 101,103,105,...,2149
        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        assert_eq!(metric_count(apple, "total"), 1025);
        assert_eq!(metric_count(samsung, "total"), 1025);

        // sum(price) for Apple = sum of (100 + 2k) for k in 0..1025 = 1025*100 + 2*(0+1+...+1024) = 102500 + 2*524800 = 1152100
        let apple_sum = metric_sum(apple, "sum_price");
        let expected_apple_sum: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| 100.0 + i as f64)
            .sum();
        assert!(
            (apple_sum - expected_apple_sum).abs() < 0.01,
            "apple sum: {apple_sum} != {expected_apple_sum}"
        );

        let samsung_sum = metric_sum(samsung, "sum_price");
        let expected_samsung_sum: f64 = (0..n)
            .filter(|i| i % 2 == 1)
            .map(|i| 100.0 + i as f64)
            .sum();
        assert!(
            (samsung_sum - expected_samsung_sum).abs() < 0.01,
            "samsung sum: {samsung_sum} != {expected_samsung_sum}"
        );
    }

    #[test]
    fn grouped_batch_numeric_min_max_correctness() {
        use crate::search::*;
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "min_price".into(),
                    function: GroupedMetricFunction::Min,
                    field: Some("price".into()),
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "max_price".into(),
                    function: GroupedMetricFunction::Max,
                    field: Some("price".into()),
                    field_expr: None,
                },
            ],
        );

        let (_, _, partial_aggs) = engine.search_query(&req).unwrap();
        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        // Apple: even indices 0..2048, prices 100.0..2148.0
        assert!((metric_min(apple, "min_price") - 100.0).abs() < 0.01);
        assert!((metric_max(apple, "max_price") - (100.0 + (n - 2) as f64)).abs() < 0.01);
        // Samsung: odd indices 1..2049, prices 101.0..2149.0
        assert!((metric_min(samsung, "min_price") - 101.0).abs() < 0.01);
        assert!((metric_max(samsung, "max_price") - (100.0 + (n - 1) as f64)).abs() < 0.01);
    }

    #[test]
    fn grouped_batch_numeric_avg_correctness() {
        use crate::search::*;
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "avg_price".into(),
                function: GroupedMetricFunction::Avg,
                field: Some("price".into()),
                field_expr: None,
            }],
        );

        let (_, _, partial_aggs) = engine.search_query(&req).unwrap();
        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let expected_avg: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| 100.0 + i as f64)
            .sum::<f64>()
            / 1025.0;
        let actual_avg = metric_sum(apple, "avg_price") / metric_count(apple, "avg_price") as f64;
        assert!(
            (actual_avg - expected_avg).abs() < 0.01,
            "avg: {actual_avg} != {expected_avg}"
        );
    }

    #[test]
    fn grouped_batch_numeric_multiple_columns() {
        use crate::search::*;
        // Verify batch reads work with multiple numeric columns (price + quantity).
        let n = 1500;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![
                GroupedMetricAgg {
                    output_name: "total".into(),
                    function: GroupedMetricFunction::Count,
                    field: None,
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_price".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("price".into()),
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "sum_qty".into(),
                    function: GroupedMetricFunction::Sum,
                    field: Some("quantity".into()),
                    field_expr: None,
                },
                GroupedMetricAgg {
                    output_name: "max_qty".into(),
                    function: GroupedMetricFunction::Max,
                    field: Some("quantity".into()),
                    field_expr: None,
                },
            ],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        assert_eq!(metric_count(apple, "total"), 750);

        // quantity for Apple (even i): i+1 for i in [0,2,4,...,1498] → [1,3,5,...,1499]
        let expected_qty_sum: f64 = (0..n).filter(|i| i % 2 == 0).map(|i| (i + 1) as f64).sum();
        let actual_qty_sum = metric_sum(apple, "sum_qty");
        assert!((actual_qty_sum - expected_qty_sum).abs() < 0.01);

        let expected_max_qty = (n - 1) as f64; // last even index (1498) → quantity = 1499
        assert!((metric_max(apple, "max_qty") - expected_max_qty).abs() < 0.01);
    }

    #[test]
    fn grouped_batch_count_only_still_works() {
        use crate::search::*;
        // Verify count-only (no numeric metrics) still uses the optimized batch path correctly.
        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "cnt".into(),
                function: GroupedMetricFunction::Count,
                field: None,
                field_expr: None,
            }],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };
        assert_eq!(buckets.len(), 2);

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();
        assert_eq!(metric_count(apple, "cnt"), 1025);
        assert_eq!(metric_count(samsung, "cnt"), 1025);
    }

    #[test]
    fn grouped_batch_numeric_expression_sum_correctness() {
        use crate::search::*;

        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "gross".into(),
                function: GroupedMetricFunction::Sum,
                field: None,
                field_expr: Some(MetricFieldExpr::binary(
                    MetricFieldExpr::field("price"),
                    MetricFieldOp::Add,
                    MetricFieldExpr::field("quantity"),
                )),
            }],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        let expected_apple: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| (100.0 + i as f64) + (i + 1) as f64)
            .sum();
        let expected_samsung: f64 = (0..n)
            .filter(|i| i % 2 == 1)
            .map(|i| (100.0 + i as f64) + (i + 1) as f64)
            .sum();

        assert!((metric_sum(apple, "gross") - expected_apple).abs() < 0.01);
        assert!((metric_sum(samsung, "gross") - expected_samsung).abs() < 0.01);
    }

    #[test]
    fn grouped_batch_nested_numeric_expression_avg_correctness() {
        use crate::search::*;

        let n = 2050;
        let (_dir, engine) = create_grouped_numeric_engine(n);
        let req = grouped_numeric_request(
            n,
            vec![GroupedMetricAgg {
                output_name: "platform_margin".into(),
                function: GroupedMetricFunction::Avg,
                field: None,
                field_expr: Some(MetricFieldExpr::binary(
                    MetricFieldExpr::binary(
                        MetricFieldExpr::field("price"),
                        MetricFieldOp::Sub,
                        MetricFieldExpr::field("quantity"),
                    ),
                    MetricFieldOp::Div,
                    MetricFieldExpr::field("price"),
                )),
            }],
        );

        let (_, total, partial_aggs) = engine.search_query(&req).unwrap();
        assert_eq!(total, n);

        let PartialAggResult::GroupedMetrics { buckets } = &partial_aggs["sql_grouped"] else {
            panic!("expected grouped metrics");
        };

        let apple = buckets
            .iter()
            .find(|b| b.group_values[0] == "Apple")
            .unwrap();
        let samsung = buckets
            .iter()
            .find(|b| b.group_values[0] == "Samsung")
            .unwrap();

        let expected_apple: f64 = (0..n)
            .filter(|i| i % 2 == 0)
            .map(|i| {
                let price = 100.0 + i as f64;
                let quantity = (i + 1) as f64;
                (price - quantity) / price
            })
            .sum::<f64>()
            / (n / 2) as f64;
        let expected_samsung: f64 = (0..n)
            .filter(|i| i % 2 == 1)
            .map(|i| {
                let price = 100.0 + i as f64;
                let quantity = (i + 1) as f64;
                (price - quantity) / price
            })
            .sum::<f64>()
            / (n / 2) as f64;

        assert!((metric_avg(apple, "platform_margin") - expected_apple).abs() < 0.000001);
        assert!((metric_avg(samsung, "platform_margin") - expected_samsung).abs() < 0.000001);
    }

    // ── BitSet collector + streaming batches ────────────────────────────

    #[test]
    fn bitset_collector_matches_topdocs_results() {
        let (_dir, engine) = create_engine();
        for i in 0..100 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("rust post {i}"), "brand": "tech", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        // TopDocs path
        let single = engine
            .sql_record_batch(&req, &["brand".into(), "price".into()], false, false)
            .unwrap();
        assert_eq!(single.batch.num_rows(), 100);

        // Streaming bitset path
        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into(), "price".into()], false, false, 0)
            .unwrap();
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
        assert_eq!(streaming.total_hits, 100);
    }

    #[test]
    fn bitset_streaming_produces_multiple_batches() {
        let (_dir, engine) = create_engine();
        // Insert more than one batch worth of docs (STREAMING_BATCH_SIZE = 8192)
        for i in 0..100 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("item {i}"), "brand": "test", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 200,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        // Use tiny batch size to force multiple batches
        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], false, false, 10)
            .unwrap();
        assert!(
            streaming.batches.len() >= 10,
            "100 docs with batch_size=10 should produce >=10 batches, got {}",
            streaming.batches.len()
        );
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn lazy_streaming_handle_matches_eager_batches() {
        let (_dir, engine) = create_engine();
        for i in 0..48 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("item {i}"), "brand": "test", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let eager = engine
            .sql_streaming_batches(&req, &["brand".into(), "price".into()], true, false, 7)
            .unwrap();
        let mut handle = engine
            .sql_streaming_batch_handle(&req, &["brand".into(), "price".into()], true, false, 7)
            .unwrap();

        let total_hits = handle.total_hits;
        let collected_rows = handle.collected_rows;
        let mut lazy_batches = Vec::new();
        while let Some(batch) = handle.next_batch().unwrap() {
            lazy_batches.push(batch);
        }

        assert_eq!(total_hits, 48);
        assert_eq!(collected_rows, 48);
        assert_eq!(eager.total_hits, total_hits);
        assert_eq!(eager.collected_rows, collected_rows);
        assert_eq!(lazy_batches.len(), eager.batches.len());

        let eager_rows: usize = eager.batches.iter().map(|batch| batch.num_rows()).sum();
        let lazy_rows: usize = lazy_batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(lazy_rows, eager_rows);
        assert_eq!(lazy_rows, 48);
    }

    #[test]
    fn bitset_streaming_empty_result() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc-1", json!({"title": "hello", "brand": "test"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(
                [("title".into(), json!("nonexistent"))]
                    .into_iter()
                    .collect(),
            ),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], false, false, 0)
            .unwrap();
        assert_eq!(streaming.total_hits, 0);
        assert_eq!(streaming.batches.len(), 1); // one empty batch with correct schema
        assert_eq!(streaming.batches[0].num_rows(), 0);
    }

    #[test]
    fn lazy_streaming_handle_empty_result_emits_single_empty_batch() {
        let (_dir, engine) = create_engine();
        engine
            .add_document("doc-1", json!({"title": "hello", "brand": "test"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Term(
                [("title".into(), json!("nonexistent"))]
                    .into_iter()
                    .collect(),
            ),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let mut handle = engine
            .sql_streaming_batch_handle(&req, &["brand".into()], false, false, 0)
            .unwrap();
        assert_eq!(handle.total_hits, 0);
        assert_eq!(handle.collected_rows, 0);

        let first = handle
            .next_batch()
            .unwrap()
            .expect("empty result should still emit one empty batch");
        assert_eq!(first.num_rows(), 0);
        assert!(handle.next_batch().unwrap().is_none());
    }

    #[test]
    fn join_scoped_handles_returns_error_when_worker_panics() {
        let message = std::thread::scope(|scope| {
            let handles = vec![
                scope.spawn(|| 1usize),
                scope.spawn(|| -> usize { panic!("segment boom") }),
            ];
            join_scoped_handles(handles, "segment scan")
                .unwrap_err()
                .to_string()
        });

        assert!(message.contains("segment scan thread panicked"));
        assert!(message.contains("segment boom"));
    }

    #[test]
    fn bitset_streaming_schema_matches_topdocs_schema() {
        use crate::cluster::state::{FieldMapping, FieldType};

        let mut mappings = HashMap::new();
        mappings.insert(
            "brand".into(),
            FieldMapping {
                field_type: FieldType::Keyword,
                dimension: None,
            },
        );
        mappings.insert(
            "price".into(),
            FieldMapping {
                field_type: FieldType::Float,
                dimension: None,
            },
        );

        let (_dir, engine) = create_engine_with_mappings(mappings);
        for i in 0..10 {
            engine
                .add_document(
                    &format!("doc-{i}"),
                    json!({"title": format!("item {i}"), "brand": "test", "price": i as f64}),
                )
                .unwrap();
        }
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };
        let cols = vec!["brand".to_string(), "price".to_string()];

        let single = engine.sql_record_batch(&req, &cols, true, false).unwrap();
        let streaming = engine
            .sql_streaming_batches(&req, &cols, true, false, 0)
            .unwrap();

        // Verify both schemas match: column names, types, and nullability
        let single_schema = single.batch.schema();
        let streaming_schema = streaming.batches[0].schema();

        let single_fields: Vec<(&str, &datafusion::arrow::datatypes::DataType, bool)> =
            single_schema
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
                .collect();
        let streaming_fields: Vec<(&str, &datafusion::arrow::datatypes::DataType, bool)> =
            streaming_schema
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.data_type(), f.is_nullable()))
                .collect();
        assert_eq!(
            single_fields, streaming_fields,
            "streaming batch schema (names, types, nullability) must match single-batch schema"
        );
    }

    #[test]
    fn bitset_streaming_with_filtered_query() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "rust programming", "brand": "tech"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-2",
                json!({"title": "python scripting", "brand": "tech"}),
            )
            .unwrap();
        engine
            .add_document("doc-3", json!({"title": "rust systems", "brand": "infra"}))
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::Match([("title".into(), json!("rust"))].into_iter().collect()),
            size: 100,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["brand".into()], true, false, 0)
            .unwrap();
        let total_rows: usize = streaming.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "only 2 docs match 'rust'");
        assert_eq!(streaming.total_hits, 2);
    }

    #[test]
    fn bitset_streaming_preserves_ids_and_keyword_values() {
        let (_dir, engine) = create_typed_engine();
        engine
            .add_document(
                "doc-alpha",
                json!({"title": "Widget", "price": 19.99, "category": "gadgets"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-beta",
                json!({"title": "Sprocket", "price": 5.50, "category": "parts"}),
            )
            .unwrap();
        engine
            .add_document(
                "doc-gamma",
                json!({"title": "Widget", "price": 9.99, "category": "gadgets"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(&req, &["title".into(), "category".into()], true, false, 2)
            .unwrap();

        let mut rows = Vec::new();
        for batch in &streaming.batches {
            let id_col = batch
                .column_by_name("_id")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let title_col = batch
                .column_by_name("title")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let category_col = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                rows.push((
                    id_col.value(i).to_string(),
                    title_col.value(i).to_string(),
                    category_col.value(i).to_string(),
                ));
            }
        }

        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("doc-alpha".into(), "Widget".into(), "gadgets".into()),
                ("doc-beta".into(), "Sprocket".into(), "parts".into()),
                ("doc-gamma".into(), "Widget".into(), "gadgets".into()),
            ]
        );
    }

    #[test]
    fn bitset_streaming_multisegment_preserves_values_batch_for_batch() {
        let (_dir, engine) = create_typed_engine();

        for (doc_id, title, category, price) in [
            ("doc-1", "Widget", "gadgets", 19.99),
            ("doc-2", "Sprocket", "parts", 5.50),
            ("doc-3", "Bolt", "parts", 1.25),
            ("doc-4", "Cog", "gadgets", 3.75),
        ] {
            engine
                .add_document(
                    doc_id,
                    json!({"title": title, "category": category, "price": price}),
                )
                .unwrap();
            engine.refresh().unwrap();
        }

        let segment_count = engine.segment_infos().len();
        assert!(
            segment_count >= 2,
            "expected multiple segments before streaming, got {segment_count}"
        );

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 10,
            from: 0,
            knn: None,
            sort: vec![],
            search_after: None,
            aggs: std::collections::HashMap::new(),
        };

        let streaming = engine
            .sql_streaming_batches(
                &req,
                &["title".into(), "category".into(), "price".into()],
                true,
                false,
                2,
            )
            .unwrap();

        assert!(
            streaming.batches.len() >= 2,
            "expected multiple streamed batches"
        );

        let mut rows = Vec::new();
        for batch in &streaming.batches {
            assert!(batch.num_rows() <= 2, "batch exceeded requested batch size");
            let id_col = batch
                .column_by_name("_id")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let title_col = batch
                .column_by_name("title")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let category_col = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let price_col = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap();

            for i in 0..batch.num_rows() {
                rows.push((
                    id_col.value(i).to_string(),
                    title_col.value(i).to_string(),
                    category_col.value(i).to_string(),
                    price_col.value(i),
                ));
            }
        }

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            rows,
            vec![
                ("doc-1".into(), "Widget".into(), "gadgets".into(), 19.99),
                ("doc-2".into(), "Sprocket".into(), "parts".into(), 5.50),
                ("doc-3".into(), "Bolt".into(), "parts".into(), 1.25),
                ("doc-4".into(), "Cog".into(), "gadgets".into(), 3.75),
            ]
        );
    }

    #[test]
    fn bitset_streaming_rejected_for_source_fallback_columns() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "iphone", "description": "text-only field"}),
            )
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            !engine.can_stream_sql_batches(&["description".into()], false),
            "text/source-fallback columns must not use bitset streaming"
        );
    }

    #[test]
    fn bitset_streaming_rejected_when_score_needed() {
        let (_dir, engine) = create_engine();
        engine
            .add_document(
                "doc-1",
                json!({"title": "iphone", "brand": "Apple", "price": 999.0}),
            )
            .unwrap();
        engine.refresh().unwrap();

        assert!(
            !engine.can_stream_sql_batches(&["brand".into()], true),
            "score-dependent queries must stay on the scored path"
        );
    }

    // ── StringArena tests ──────────────────────────────────────────

    #[test]
    fn string_arena_push_and_get() {
        let mut arena = StringArena::with_capacity(4, 8);
        let i0 = arena.push("hello");
        let i1 = arena.push("world");
        let i2 = arena.push("");
        let i3 = arena.push("ferris");
        assert_eq!(arena.get(i0), "hello");
        assert_eq!(arena.get(i1), "world");
        assert_eq!(arena.get(i2), "");
        assert_eq!(arena.get(i3), "ferris");
    }

    #[test]
    fn string_arena_to_json_value() {
        let mut arena = StringArena::with_capacity(2, 8);
        let idx = arena.push("test_value");
        let json = arena.to_json_value(idx);
        assert_eq!(json, serde_json::Value::String("test_value".into()));
    }

    #[test]
    fn derived_bucket_key_for_value_maps_hour_ranges() {
        let spec = crate::search::DerivedGroupKey {
            source_field: "pickup_datetime".to_string(),
            buckets: vec![
                crate::search::DerivedGroupBucket {
                    lower: "2025-01-05T08:00:00".to_string(),
                    lower_inclusive: true,
                    upper: "2025-01-05T09:00:00".to_string(),
                    upper_inclusive: false,
                    label: "08".to_string(),
                },
                crate::search::DerivedGroupBucket {
                    lower: "2025-01-05T12:00:00".to_string(),
                    lower_inclusive: true,
                    upper: "2025-01-05T13:00:00".to_string(),
                    upper_inclusive: false,
                    label: "12".to_string(),
                },
                crate::search::DerivedGroupBucket {
                    lower: "2025-01-05T17:00:00".to_string(),
                    lower_inclusive: true,
                    upper: "2025-01-05T18:00:00".to_string(),
                    upper_inclusive: false,
                    label: "17".to_string(),
                },
            ],
            else_label: None,
        };

        assert_eq!(
            derived_bucket_key_for_value(&spec, "2025-01-05T08:15:00"),
            Some(0)
        );
        assert_eq!(
            derived_bucket_key_for_value(&spec, "2025-01-05T12:30:00"),
            Some(1)
        );
        assert_eq!(
            derived_bucket_key_for_value(&spec, "2025-01-05T17:45:00"),
            Some(2)
        );
        assert_eq!(
            derived_bucket_key_for_value(&spec, "2025-01-05T10:00:00"),
            None
        );
    }

    // ── Shard top-K pruning tests ──────────────────────────────────

    #[test]
    fn apply_shard_top_k_prunes_to_limit() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..100)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::String(format!("author_{i}"))],
                metrics: std::collections::HashMap::from([(
                    "posts".to_string(),
                    crate::search::GroupedMetricPartial::Count { count: i as u64 },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 10,
                sort_by: "posts".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 10);
        // All remaining buckets should have count >= 90 (top 10 of 0..100)
        for bucket in &buckets {
            let count = match bucket.metrics.get("posts").unwrap() {
                crate::search::GroupedMetricPartial::Count { count } => *count,
                _ => panic!("expected Count"),
            };
            assert!(count >= 90, "top-K pruning kept count={count} below cutoff");
        }
    }

    #[test]
    fn apply_shard_top_k_noop_when_under_limit() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..5)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "cnt".to_string(),
                    crate::search::GroupedMetricPartial::Count { count: i as u64 },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 20,
                sort_by: "cnt".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 5, "should not prune when under limit");
    }

    #[test]
    fn apply_shard_top_k_ascending_order() {
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..50)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "val".to_string(),
                    crate::search::GroupedMetricPartial::Stats {
                        count: 1,
                        sum: i as f64,
                        min: i as f64,
                        max: i as f64,
                    },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 5,
                sort_by: "val".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Sum,
                descending: false,
            },
        );

        assert_eq!(buckets.len(), 5);
        // All remaining should have sum <= 4 (bottom 5 of 0..50)
        for bucket in &buckets {
            let sum = match bucket.metrics.get("val").unwrap() {
                crate::search::GroupedMetricPartial::Stats { sum, .. } => *sum,
                _ => panic!("expected Stats"),
            };
            assert!(sum <= 4.0, "ascending top-K kept sum={sum} above cutoff");
        }
    }

    #[test]
    fn flat_sort_value_extracts_correct_metric() {
        let flat_metrics = vec![
            FlatMetric::Count(vec![100, 200, 50]),
            FlatMetric::Stats {
                count: vec![10, 20, 5],
                sum: vec![1000.0, 2000.0, 500.0],
                min: vec![1.0, 2.0, 3.0],
                max: vec![99.0, 199.0, 49.0],
            },
        ];
        let metric_entries = vec![
            GroupedMetricEntry {
                output_name: "posts".to_string(),
                function: crate::search::GroupedMetricFunction::Count,
                source: GroupedMetricSource::CountAll,
            },
            GroupedMetricEntry {
                output_name: "total".to_string(),
                function: crate::search::GroupedMetricFunction::Sum,
                source: GroupedMetricSource::CountAll, // dummy, unused by sort
            },
        ];

        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "posts", 0),
            100.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "posts", 1),
            200.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "total", 0),
            1000.0
        );
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "total", 2),
            500.0
        );
        // Non-existent metric falls back to 0.0
        assert_eq!(
            flat_sort_value(&flat_metrics, &metric_entries, "unknown", 0),
            0.0
        );
    }

    #[test]
    fn bucket_sort_value_count_metric() {
        let bucket = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("alice".into())],
            metrics: std::collections::HashMap::from([(
                "posts".to_string(),
                crate::search::GroupedMetricPartial::Count { count: 42 },
            )]),
        };
        assert_eq!(
            bucket_sort_value(
                &bucket,
                "posts",
                &crate::search::GroupedMetricFunction::Count
            ),
            42.0
        );
        assert_eq!(
            bucket_sort_value(
                &bucket,
                "missing",
                &crate::search::GroupedMetricFunction::Count
            ),
            f64::NEG_INFINITY
        );
    }

    #[test]
    fn shard_top_k_serialization_roundtrip() {
        let params = crate::search::GroupedMetricsAggParams {
            group_by: vec!["author".into()],
            metrics: vec![crate::search::GroupedMetricAgg {
                output_name: "posts".into(),
                function: crate::search::GroupedMetricFunction::Count,
                field: None,
                field_expr: None,
            }],
            shard_top_k: Some(crate::search::ShardTopK {
                limit: 40,
                sort_by: "posts".into(),
                sort_function: crate::search::GroupedMetricFunction::Count,
                descending: true,
            }),
        };
        let json = serde_json::to_string(&params).unwrap();
        let restored: crate::search::GroupedMetricsAggParams = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.shard_top_k.as_ref().unwrap().limit, 40);
        assert_eq!(restored.shard_top_k.as_ref().unwrap().sort_by, "posts");
        assert!(restored.shard_top_k.as_ref().unwrap().descending);
    }

    #[test]
    fn shard_top_k_none_omitted_in_serialization() {
        let params = crate::search::GroupedMetricsAggParams {
            group_by: vec!["x".into()],
            metrics: vec![],
            shard_top_k: None,
        };
        let json = serde_json::to_string(&params).unwrap();
        assert!(!json.contains("shard_top_k"), "None should be omitted");
    }

    #[test]
    fn bucket_sort_value_avg_sorts_by_average_not_sum() {
        // GPT-identified regression: sorting by avg must use sum/count, not sum.
        // A group with sum=100, count=100 (avg=1.0) should rank LOWER than
        // sum=50, count=1 (avg=50.0) when sorting by avg DESC.
        let high_sum_low_avg = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("prolific".into())],
            metrics: std::collections::HashMap::from([(
                "avg_upvotes".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 100,
                    sum: 100.0,
                    min: 1.0,
                    max: 1.0,
                },
            )]),
        };
        let low_sum_high_avg = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("rare".into())],
            metrics: std::collections::HashMap::from([(
                "avg_upvotes".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 1,
                    sum: 50.0,
                    min: 50.0,
                    max: 50.0,
                },
            )]),
        };

        let avg_fn = crate::search::GroupedMetricFunction::Avg;
        let va = bucket_sort_value(&high_sum_low_avg, "avg_upvotes", &avg_fn);
        let vb = bucket_sort_value(&low_sum_high_avg, "avg_upvotes", &avg_fn);

        assert!(
            va < vb,
            "avg=1.0 should rank below avg=50.0, got va={va} vb={vb}"
        );
    }

    #[test]
    fn bucket_sort_value_min_and_max() {
        let bucket = crate::search::GroupedMetricsBucket {
            group_values: vec![serde_json::Value::String("test".into())],
            metrics: std::collections::HashMap::from([(
                "price".to_string(),
                crate::search::GroupedMetricPartial::Stats {
                    count: 10,
                    sum: 500.0,
                    min: 5.0,
                    max: 99.0,
                },
            )]),
        };

        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Min),
            5.0
        );
        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Max),
            99.0
        );
        assert_eq!(
            bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Sum),
            500.0
        );
        // avg = 500.0 / 10 = 50.0
        assert!(
            (bucket_sort_value(&bucket, "price", &crate::search::GroupedMetricFunction::Avg)
                - 50.0)
                .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn apply_shard_top_k_avg_keeps_highest_average() {
        // 20 groups: group i has count=10, sum=i*10 → avg=i
        // Top 5 by avg DESC should keep groups 15..20 (avg 15..19)
        let mut buckets: Vec<crate::search::GroupedMetricsBucket> = (0..20)
            .map(|i| crate::search::GroupedMetricsBucket {
                group_values: vec![serde_json::Value::from(i)],
                metrics: std::collections::HashMap::from([(
                    "avg_val".to_string(),
                    crate::search::GroupedMetricPartial::Stats {
                        count: 10,
                        sum: i as f64 * 10.0,
                        min: i as f64,
                        max: i as f64,
                    },
                )]),
            })
            .collect();

        apply_shard_top_k(
            &mut buckets,
            &crate::search::ShardTopK {
                limit: 5,
                sort_by: "avg_val".to_string(),
                sort_function: crate::search::GroupedMetricFunction::Avg,
                descending: true,
            },
        );

        assert_eq!(buckets.len(), 5);
        for bucket in &buckets {
            let avg = match bucket.metrics.get("avg_val").unwrap() {
                crate::search::GroupedMetricPartial::Stats { count, sum, .. } => {
                    sum / *count as f64
                }
                _ => panic!("expected Stats"),
            };
            assert!(
                avg >= 15.0,
                "avg-sorted top-K should keep avg >= 15, got {avg}"
            );
        }
    }

    // ── search_after cursor pagination ──────────────────────────────────

    fn search_after_engine_with_ints() -> (tempfile::TempDir, HotEngine) {
        use crate::cluster::state::{FieldMapping, FieldType};
        let mut mappings = HashMap::new();
        mappings.insert(
            "n".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let (dir, engine) = create_engine_with_mappings(mappings);
        for i in 0..20i64 {
            engine
                .add_document(&format!("d{i:02}"), json!({ "n": i }))
                .unwrap();
        }
        engine.refresh().unwrap();
        (dir, engine)
    }

    fn sort_by(name: &str, dir: crate::search::SortDirection) -> crate::search::SortClause {
        use crate::search::{SortClause, SortOrder};
        SortClause::Field(HashMap::from([(
            name.to_string(),
            SortOrder::Direction(dir),
        )]))
    }

    #[test]
    fn search_after_integer_asc_paginates_contiguously() {
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Asc)],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (page1, total, _) = engine.search_query(&req1).unwrap();
        assert_eq!(total, 20);
        assert_eq!(page1.len(), 5);

        let cursor = page1.last().unwrap()["sort"].clone();
        assert!(cursor.is_array());

        let req2 = SearchRequest {
            search_after: Some(cursor.as_array().unwrap().clone()),
            ..req1.clone()
        };
        let (page2, _, _) = engine.search_query(&req2).unwrap();
        assert_eq!(page2.len(), 5);

        let ids1: Vec<String> = page1
            .iter()
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect();
        let ids2: Vec<String> = page2
            .iter()
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids1, vec!["d00", "d01", "d02", "d03", "d04"]);
        assert_eq!(ids2, vec!["d05", "d06", "d07", "d08", "d09"]);

        // Hits carry sort: [...] arrays
        for hit in page1.iter().chain(page2.iter()) {
            assert!(hit.get("sort").is_some(), "hit missing sort: {hit:?}");
        }
    }

    #[test]
    fn search_after_integer_desc_paginates_contiguously() {
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 4,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Desc)],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (page1, _, _) = engine.search_query(&req1).unwrap();
        let cursor = page1.last().unwrap()["sort"].as_array().unwrap().clone();

        let req2 = SearchRequest {
            search_after: Some(cursor),
            ..req1.clone()
        };
        let (page2, _, _) = engine.search_query(&req2).unwrap();

        let ids: Vec<String> = page1
            .iter()
            .chain(page2.iter())
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            ids,
            vec!["d19", "d18", "d17", "d16", "d15", "d14", "d13", "d12"]
        );
    }

    #[test]
    fn search_after_multi_field_uses_tuple_lex_order() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::SortDirection;

        let mut mappings = HashMap::new();
        mappings.insert(
            "k".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "tie".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let (_dir, engine) = create_engine_with_mappings(mappings);

        // Two docs share k=1; tie distinguishes them.
        for (id, k, tie) in [
            ("a", 1, 100),
            ("b", 1, 200),
            ("c", 2, 50),
            ("d", 2, 60),
            ("e", 3, 10),
        ] {
            engine
                .add_document(id, json!({ "k": k, "tie": tie }))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Sort by k asc, tie asc.
        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 2,
            from: 0,
            knn: None,
            sort: vec![
                sort_by("k", SortDirection::Asc),
                sort_by("tie", SortDirection::Asc),
            ],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (page1, _, _) = engine.search_query(&req1).unwrap();
        let ids1: Vec<&str> = page1.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert_eq!(ids1, vec!["a", "b"]);

        // Cursor should be the last hit's sort tuple [1, 200]; next page must skip past it.
        let cursor = page1.last().unwrap()["sort"].as_array().unwrap().clone();
        let req2 = SearchRequest {
            search_after: Some(cursor),
            ..req1.clone()
        };
        let (page2, _, _) = engine.search_query(&req2).unwrap();
        let ids2: Vec<&str> = page2.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert_eq!(ids2, vec!["c", "d"]);

        let cursor2 = page2.last().unwrap()["sort"].as_array().unwrap().clone();
        let req3 = SearchRequest {
            search_after: Some(cursor2),
            ..req1
        };
        let (page3, _, _) = engine.search_query(&req3).unwrap();
        let ids3: Vec<&str> = page3.iter().map(|h| h["_id"].as_str().unwrap()).collect();
        assert_eq!(ids3, vec!["e"]);
    }

    #[test]
    fn search_after_by_id_paginates_keyword_field() {
        // _id sorts rely on Tantivy's string fast-field collector, which is
        // not wired up in the current planner. Skipping until the multi-key
        // fast-field collector lands. The engine still produces a deterministic
        // intra-K order via crate::search::sort_hits, but the per-shard top-K
        // is not globally correct without primary-key Tantivy ordering, so
        // this test only sanity-checks the validation path.
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 3,
            from: 0,
            knn: None,
            sort: vec![sort_by("_id", SortDirection::Asc)],
            search_after: Some(vec![json!("zzzz")]),
            aggs: HashMap::new(),
        };
        // Cursor past every _id returns no hits — confirms the filter is wired
        // for the _id (string) path even though full top-K sort is approximate.
        let (page, _, _) = engine.search_query(&req).unwrap();
        assert!(page.is_empty(), "cursor past all _ids should yield 0 hits");
    }

    #[test]
    fn search_after_rejects_score_sort() {
        use crate::search::SortClause;
        let (_dir, engine) = search_after_engine_with_ints();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 3,
            from: 0,
            knn: None,
            sort: vec![SortClause::Simple("_score".to_string())],
            search_after: Some(vec![json!(1.0)]),
            aggs: HashMap::new(),
        };
        let err = engine.search_query(&req).unwrap_err().to_string();
        assert!(
            err.contains("_score"),
            "expected _score rejection, got {err}"
        );
    }

    #[test]
    fn search_after_rejects_length_mismatch() {
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 3,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Asc)],
            search_after: Some(vec![json!(1), json!(2)]),
            aggs: HashMap::new(),
        };
        let err = engine.search_query(&req).unwrap_err().to_string();
        assert!(
            err.contains("does not match sort length"),
            "expected length mismatch error, got {err}"
        );
    }

    #[test]
    fn search_after_rejects_unknown_field() {
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 3,
            from: 0,
            knn: None,
            sort: vec![sort_by("does_not_exist", SortDirection::Asc)],
            search_after: Some(vec![json!(1)]),
            aggs: HashMap::new(),
        };
        let err = engine.search_query(&req).unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "expected unknown-field error, got {err}"
        );
    }

    // ── High #2 fix: total + aggregations must NOT shrink across pages ──
    // The cursor filter is applied only to TopDocs collection. Count and
    // aggregation collectors run against the unfiltered user_query so totals
    // and aggregation buckets are identical on every page of a paginated
    // request.
    #[test]
    fn search_after_total_hits_unchanged_across_pages() {
        use crate::search::SortDirection;
        let (_dir, engine) = search_after_engine_with_ints();

        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Asc)],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (page1, total1, _) = engine.search_query(&req1).unwrap();
        assert_eq!(total1, 20);

        let cursor = page1.last().unwrap()["sort"].clone();
        let req2 = SearchRequest {
            search_after: Some(cursor.as_array().unwrap().clone()),
            ..req1.clone()
        };
        let (_, total2, _) = engine.search_query(&req2).unwrap();
        // Without the High #2 fix, total2 would shrink to 15 (post-cursor count).
        assert_eq!(total2, 20, "total must be invariant across cursor pages");

        // And page 3 too.
        let cursor3 = {
            let req2_full = SearchRequest {
                size: 5,
                ..req2.clone()
            };
            let (p2, _, _) = engine.search_query(&req2_full).unwrap();
            p2.last().unwrap()["sort"].clone()
        };
        let req3 = SearchRequest {
            search_after: Some(cursor3.as_array().unwrap().clone()),
            ..req1.clone()
        };
        let (_, total3, _) = engine.search_query(&req3).unwrap();
        assert_eq!(total3, 20, "total must be invariant on page 3 too");
    }

    #[test]
    fn search_after_aggregations_unchanged_across_pages() {
        use crate::search::{AggregationRequest, MetricAggParams, PartialAggResult, SortDirection};
        let (_dir, engine) = search_after_engine_with_ints();

        fn value_count(aggs: &HashMap<String, PartialAggResult>) -> f64 {
            match aggs.get("n_count").expect("agg present") {
                PartialAggResult::Metric { value } => value.expect("value present"),
                other => panic!("unexpected agg shape: {other:?}"),
            }
        }

        let mut aggs = HashMap::new();
        aggs.insert(
            "n_count".to_string(),
            AggregationRequest::ValueCount(MetricAggParams {
                field: "n".to_string(),
            }),
        );

        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 5,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Asc)],
            search_after: None,
            aggs: aggs.clone(),
        };
        let (page1, total1, aggs1) = engine.search_query(&req1).unwrap();
        assert_eq!(total1, 20);
        assert_eq!(value_count(&aggs1), 20.0);

        let cursor = page1.last().unwrap()["sort"].clone();
        let req2 = SearchRequest {
            search_after: Some(cursor.as_array().unwrap().clone()),
            ..req1.clone()
        };
        let (_, total2, aggs2) = engine.search_query(&req2).unwrap();
        assert_eq!(total2, 20);
        // Without the High #2 fix, aggs2.n_count would be 15 (post-cursor count).
        assert_eq!(
            value_count(&aggs2),
            20.0,
            "aggregations must be invariant across cursor pages"
        );
    }

    // ── High #1 documented limitation: single-key Tantivy collector + ties.
    // Tantivy 0.25's `order_by_fast_field` is single-key. When many docs share
    // the same primary sort value, Tantivy breaks ties by doc-address. Our
    // `search_after` advances past the cursor tuple, but ties not picked into
    // page N can be skipped on page N+1. This test pins the current behavior
    // so that any future custom multi-key collector work has a regression
    // baseline.
    #[test]
    fn search_after_with_tied_primary_sort_documents_single_key_limit() {
        use crate::cluster::state::{FieldMapping, FieldType};
        use crate::search::SortDirection;
        let mut mappings = HashMap::new();
        mappings.insert(
            "n".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        mappings.insert(
            "tie".to_string(),
            FieldMapping {
                field_type: FieldType::Integer,
                dimension: None,
            },
        );
        let (_dir, engine) = create_engine_with_mappings(mappings);
        // 5 docs all share n=2020; secondary sort key `tie` varies.
        for i in 0..5i64 {
            engine
                .add_document(&format!("t{i:02}"), json!({ "n": 2020, "tie": i }))
                .unwrap();
        }
        engine.refresh().unwrap();

        // Single-key sort: only `n`. With all ties on n, the cursor `[2020]`
        // matches no doc strictly greater than 2020 → page 2 is empty.
        let req1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 2,
            from: 0,
            knn: None,
            sort: vec![sort_by("n", SortDirection::Asc)],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (page1, _, _) = engine.search_query(&req1).unwrap();
        assert_eq!(page1.len(), 2);
        let cursor = page1.last().unwrap()["sort"].clone();
        let req2 = SearchRequest {
            search_after: Some(cursor.as_array().unwrap().clone()),
            ..req1.clone()
        };
        let (page2, _, _) = engine.search_query(&req2).unwrap();
        assert_eq!(
            page2.len(),
            0,
            "documented limitation: single-key sort with ties on the primary \
             key drops the remaining tied docs at the cursor boundary"
        );

        // Multi-key sort: [n, tie]. Tantivy's collector still only orders by
        // `n` (the primary). Within the tied n=2020 group, Tantivy picks docs
        // in *its* tie-break order (typically doc-address), which has nothing
        // to do with the secondary sort key. The cursor advances past the
        // last-returned tuple, so the secondary values of docs that did NOT
        // make it into page 1 can be either smaller or larger than the cursor.
        //
        // This is the documented High #1 limitation. The only guarantees we
        // can assert here are:
        //   1. The query does not crash.
        //   2. No id appears in both pages (the strict cursor filter prevents
        //      duplicates regardless of which tied doc landed last on page 1).
        let req_multi1 = SearchRequest {
            query: QueryClause::MatchAll(json!({})),
            size: 2,
            from: 0,
            knn: None,
            sort: vec![
                sort_by("n", SortDirection::Asc),
                sort_by("tie", SortDirection::Asc),
            ],
            search_after: None,
            aggs: HashMap::new(),
        };
        let (mp1, _, _) = engine.search_query(&req_multi1).unwrap();
        assert_eq!(mp1.len(), 2);
        let mc = mp1.last().unwrap()["sort"].clone();
        let req_multi2 = SearchRequest {
            search_after: Some(mc.as_array().unwrap().clone()),
            ..req_multi1.clone()
        };
        let (mp2, _, _) = engine.search_query(&req_multi2).unwrap();

        // No duplicates across pages (strict cursor filter guarantees this
        // even with the single-key collector limitation).
        let ids1: std::collections::HashSet<String> = mp1
            .iter()
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect();
        let ids2: std::collections::HashSet<String> = mp2
            .iter()
            .map(|h| h["_id"].as_str().unwrap().to_string())
            .collect();
        assert!(
            ids1.is_disjoint(&ids2),
            "strict cursor filter must prevent duplicates across pages, \
             even when primary-key ties leave secondary ordering undefined"
        );
        // Documented limitation: page 2 length is NOT asserted. Depending on
        // which tied doc Tantivy placed last on page 1, page 2 may legitimately
        // be empty (cursor past all secondary values) or non-empty.
    }
}
