//! Fenced manifest publication models, operation records, lineage verification,
//! and single-sequencer commit execution (ADR 0002, ADR 0005, FS-017..FS-019).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    CompareAndSwapOutcome, CompareToken, ConditionalStorageError, REMOTE_MANIFEST_FORMAT_VERSION,
    REMOTE_MANIFEST_MIN_READ_FORMAT_VERSION, REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
    REMOTE_MANIFEST_POINTER_MIN_READ_FORMAT_VERSION, RemoteManifestPointer, RemoteSplitManifest,
    RemoteStoreManifest, StorageManager, hex_lower, manifest_generation_key, manifest_pointer_key,
};

pub const PUBLICATION_RECORD_FORMAT_VERSION: u32 = 1;
pub const SPLIT_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const MAX_LINEAGE_WALK_GENERATIONS: usize = 4_096;

/// Component physical format capability window (ADR 0005 section 3.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatCapability {
    pub component: &'static str,
    pub min_read_version: u32,
    pub max_read_version: u32,
    pub write_version: u32,
}

impl FormatCapability {
    pub const fn supports_read(&self, version: u32) -> bool {
        version >= self.min_read_version && version <= self.max_read_version
    }
}

pub const REMOTE_MANIFEST_FORMAT_CAPABILITY: FormatCapability = FormatCapability {
    component: "remote_store_manifest",
    min_read_version: REMOTE_MANIFEST_MIN_READ_FORMAT_VERSION,
    max_read_version: REMOTE_MANIFEST_FORMAT_VERSION,
    write_version: REMOTE_MANIFEST_FORMAT_VERSION,
};

pub const REMOTE_MANIFEST_POINTER_FORMAT_CAPABILITY: FormatCapability = FormatCapability {
    component: "remote_manifest_pointer",
    min_read_version: REMOTE_MANIFEST_POINTER_MIN_READ_FORMAT_VERSION,
    max_read_version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
    write_version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
};

pub const PUBLICATION_RECORD_FORMAT_CAPABILITY: FormatCapability = FormatCapability {
    component: "remote_publication_record",
    min_read_version: PUBLICATION_RECORD_FORMAT_VERSION,
    max_read_version: PUBLICATION_RECORD_FORMAT_VERSION,
    write_version: PUBLICATION_RECORD_FORMAT_VERSION,
};

pub const SPLIT_BUNDLE_FORMAT_CAPABILITY: FormatCapability = FormatCapability {
    component: "remote_split_bundle",
    min_read_version: SPLIT_BUNDLE_FORMAT_VERSION,
    max_read_version: SPLIT_BUNDLE_FORMAT_VERSION,
    write_version: SPLIT_BUNDLE_FORMAT_VERSION,
};

/// Logical phases of one remote-store publication (ADR 0002 section 3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPhase {
    Received,
    IntentCreated,
    ArtifactUploaded,
    Prepared,
    ManifestCreated,
    PointerCommitted,
    CommitRecorded,
    Reported,
}

/// Terminal or intermediate outcome of one sequencer attempt record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAttemptOutcome {
    Committed,
    Replayed,
    Superseded,
    Conflict,
    Indeterminate,
    Aborted,
}

impl PublicationAttemptOutcome {
    pub const fn as_metric_label(&self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Replayed => "replayed",
            Self::Superseded => "superseded",
            Self::Conflict => "conflict",
            Self::Indeterminate => "indeterminate",
            Self::Aborted => "aborted",
        }
    }
}

/// Fault-injection boundary for deterministic crash/failover tests (FS-019).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationCrashStage {
    AfterIntentBeforeBundleUpload,
    AfterBundleUploadBeforePreparedRecord,
    AfterPreparedBeforeManifestCreated,
    AfterManifestCreatedBeforePointerCas,
    ForcePointerCasIndeterminateBeforeCommit,
    ForcePointerCasIndeterminateAfterCommit,
    AfterPointerCommittedBeforeCommitRecord,
    AfterCommitRecordBeforeResponse,
}

/// Typed errors produced by the fenced manifest publication pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationError {
    OperationConflict {
        operation_id: String,
        reason: String,
    },
    StaleWriterEpoch {
        index_uuid: String,
        attempted_epoch: u64,
        current_epoch: u64,
    },
    UnfencedPointer {
        index_uuid: String,
        expected_epoch: u64,
        observed_epoch: u64,
    },
    PointerConflict {
        index_uuid: String,
        reason: String,
    },
    Indeterminate {
        operation_id: String,
        reason: String,
    },
    NotCommitted {
        operation_id: String,
        reason: String,
    },
    InjectedCrash {
        operation_id: String,
        stage: PublicationCrashStage,
    },
}

impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OperationConflict {
                operation_id,
                reason,
            } => write!(
                f,
                "publication operation_id '{operation_id}' conflicts with existing operation state: {reason}"
            ),
            Self::StaleWriterEpoch {
                index_uuid,
                attempted_epoch,
                current_epoch,
            } => write!(
                f,
                "manifest writer epoch {attempted_epoch} for index '{index_uuid}' is fenced by newer epoch {current_epoch}"
            ),
            Self::UnfencedPointer {
                index_uuid,
                expected_epoch,
                observed_epoch,
            } => write!(
                f,
                "manifest pointer for index '{index_uuid}' has epoch {observed_epoch}, expected fenced epoch {expected_epoch}"
            ),
            Self::PointerConflict { index_uuid, reason } => write!(
                f,
                "manifest pointer CAS conflicted for index '{index_uuid}': {reason}"
            ),
            Self::Indeterminate {
                operation_id,
                reason,
            } => write!(
                f,
                "publication outcome for operation_id '{operation_id}' is indeterminate; retry with the same operation_id: {reason}"
            ),
            Self::NotCommitted {
                operation_id,
                reason,
            } => write!(
                f,
                "publication operation_id '{operation_id}' was not committed: {reason}"
            ),
            Self::InjectedCrash {
                operation_id,
                stage,
            } => write!(
                f,
                "injected publication crash for operation_id '{operation_id}' at stage {stage:?}"
            ),
        }
    }
}

impl std::error::Error for PublicationError {}

/// Immutable intent record written at `<index_uuid>/publications/<operation_id>/intent.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationIntentRecord {
    pub format_version: u32,
    pub index_uuid: String,
    pub index_name: String,
    pub operation_id: String,
    pub request_fingerprint: String,
    pub schema_generation: u64,
    pub schema_hash: String,
    pub split_id: String,
    pub planned_bundle_path: String,
    pub doc_count: u64,
    pub producer_node_id: String,
    pub created_at: String,
    #[serde(default)]
    pub created_at_epoch_ms: u64,
}

impl PublicationIntentRecord {
    pub fn validate(&self, expected_index_uuid: &str) -> Result<()> {
        if !PUBLICATION_RECORD_FORMAT_CAPABILITY.supports_read(self.format_version) {
            return Err(crate::common::unsupported_index_format(
                "remote publication intent record",
                format!(
                    "format_version {} is outside supported range {}..={}",
                    self.format_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.min_read_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if self.index_uuid != expected_index_uuid {
            anyhow::bail!(
                "publication intent index_uuid mismatch: expected {}, got {}",
                expected_index_uuid,
                self.index_uuid
            );
        }
        validate_publication_operation_id(&self.operation_id).map_err(anyhow::Error::msg)?;
        if !self.request_fingerprint.starts_with("sha256:") || self.request_fingerprint.len() != 71
        {
            anyhow::bail!(
                "publication intent for '{}' has invalid request_fingerprint '{}'",
                self.operation_id,
                self.request_fingerprint
            );
        }
        if self.schema_generation == 0 || !self.schema_hash.starts_with("sha256:") {
            anyhow::bail!(
                "publication intent for '{}' has invalid schema identity",
                self.operation_id
            );
        }
        if self.split_id.is_empty() || self.planned_bundle_path.is_empty() || self.doc_count == 0 {
            anyhow::bail!(
                "publication intent for '{}' is missing split_id, planned_bundle_path, or doc_count",
                self.operation_id
            );
        }
        Ok(())
    }
}

/// Immutable prepared record written at `<index_uuid>/publications/<operation_id>/prepared.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationPreparedRecord {
    pub format_version: u32,
    pub index_uuid: String,
    pub index_name: String,
    pub operation_id: String,
    pub request_fingerprint: String,
    pub schema_generation: u64,
    pub schema_hash: String,
    pub producer_node_id: String,
    pub prepared_at: String,
    #[serde(default)]
    pub prepared_at_epoch_ms: u64,
    pub split: RemoteSplitManifest,
}

impl PublicationPreparedRecord {
    pub fn validate(&self, expected_index_uuid: &str) -> Result<()> {
        if !PUBLICATION_RECORD_FORMAT_CAPABILITY.supports_read(self.format_version) {
            return Err(crate::common::unsupported_index_format(
                "remote publication prepared record",
                format!(
                    "format_version {} is outside supported range {}..={}",
                    self.format_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.min_read_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if self.index_uuid != expected_index_uuid {
            anyhow::bail!(
                "publication prepared record index_uuid mismatch: expected {}, got {}",
                expected_index_uuid,
                self.index_uuid
            );
        }
        validate_publication_operation_id(&self.operation_id).map_err(anyhow::Error::msg)?;
        if !self.request_fingerprint.starts_with("sha256:") || self.request_fingerprint.len() != 71
        {
            anyhow::bail!(
                "publication prepared record for '{}' has invalid request_fingerprint '{}'",
                self.operation_id,
                self.request_fingerprint
            );
        }
        if self.schema_generation == 0 || !self.schema_hash.starts_with("sha256:") {
            anyhow::bail!(
                "publication prepared record for '{}' has invalid schema identity",
                self.operation_id
            );
        }
        if !is_canonical_split_bundle_path(
            expected_index_uuid,
            &self.split.split_id,
            &self.operation_id,
            &self.split.bundle_path,
        ) {
            anyhow::bail!(
                "publication prepared record bundle_path '{}' is not canonical for index '{}'",
                self.split.bundle_path,
                expected_index_uuid
            );
        }
        if !self.split.checksum.starts_with("sha256:")
            || self.split.checksum.starts_with("sha256:pending:")
        {
            anyhow::bail!(
                "publication prepared record split '{}' has invalid checksum '{}'",
                self.split.split_id,
                self.split.checksum
            );
        }
        if self.split.size_bytes == 0
            || (self.split.doc_count == 0
                && !self
                    .split
                    .seal_range()?
                    .is_some_and(|r| !r.tombstones.is_empty()))
        {
            anyhow::bail!(
                "publication prepared record split '{}' has zero size_bytes or doc_count",
                self.split.split_id
            );
        }
        Ok(())
    }
}

/// Live document version entry recorded inside a sealed split's metadata (`FS-034`, `FS-035`, `FS-036`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SplitDocVersionRecord {
    pub doc_id: String,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
}

/// Tombstone entry recorded inside a sealed split's metadata (`FS-034`, `FS-035`, `FS-036`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SplitTombstoneRecord {
    pub doc_id: String,
    pub seq_no: u64,
    pub primary_term: u64,
    pub version: u64,
}

/// Logical hot-delta sequence range covered by a sealed split (`FS-034`, `FS-035`, `FS-036`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SplitSealRangeMetadata {
    pub shard_id: u32,
    pub start_seq_no: u64,
    pub end_seq_no: u64,
    pub primary_term: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub doc_versions: Vec<SplitDocVersionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tombstones: Vec<SplitTombstoneRecord>,
}

/// Contiguous per-shard publication visibility watermark committed in a manifest generation (`FS-036`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShardPublishedWatermark {
    pub shard_id: u32,
    pub sealed_through_seq_no: u64,
    pub primary_term: u64,
    pub last_split_id: String,
    pub last_operation_id: String,
    pub updated_at_generation: u64,
}

pub(crate) fn advance_shard_watermark(
    watermarks: &mut std::collections::BTreeMap<u32, ShardPublishedWatermark>,
    range: &SplitSealRangeMetadata,
    split_id: &str,
    operation_id: &str,
    generation: u64,
) -> Result<()> {
    if range.end_seq_no < range.start_seq_no || range.primary_term == 0 {
        anyhow::bail!(
            "invalid seal range for shard {}: start_seq_no={} end_seq_no={} primary_term={}",
            range.shard_id,
            range.start_seq_no,
            range.end_seq_no,
            range.primary_term
        );
    }
    match watermarks.get(&range.shard_id) {
        None => {
            if range.start_seq_no != 0 {
                anyhow::bail!(
                    "non-contiguous initial seal watermark for shard {}: expected start_seq_no 0, got {}",
                    range.shard_id,
                    range.start_seq_no
                );
            }
        }
        Some(prev) => {
            let expected_start = prev
                .sealed_through_seq_no
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("shard watermark sequence number overflow"))?;
            if range.start_seq_no != expected_start {
                anyhow::bail!(
                    "non-contiguous seal watermark for shard {}: parent sealed_through_seq_no={}, candidate start_seq_no={} (expected {})",
                    range.shard_id,
                    prev.sealed_through_seq_no,
                    range.start_seq_no,
                    expected_start
                );
            }
            if range.primary_term < prev.primary_term {
                anyhow::bail!(
                    "regressing primary_term in seal watermark for shard {}: parent term {}, candidate term {}",
                    range.shard_id,
                    prev.primary_term,
                    range.primary_term
                );
            }
        }
    }
    watermarks.insert(
        range.shard_id,
        ShardPublishedWatermark {
            shard_id: range.shard_id,
            sealed_through_seq_no: range.end_seq_no,
            primary_term: range.primary_term,
            last_split_id: split_id.to_string(),
            last_operation_id: operation_id.to_string(),
            updated_at_generation: generation,
        },
    );
    Ok(())
}

/// Immutable committed record written at `<index_uuid>/publications/<operation_id>/committed.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationCommittedRecord {
    pub format_version: u32,
    pub index_uuid: String,
    pub index_name: String,
    pub operation_id: String,
    pub request_fingerprint: String,
    pub split_id: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub generation: u64,
    pub parent_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_path: Option<String>,
    pub manifest_path: String,
    pub manifest_checksum: String,
    pub writer_epoch: u64,
    pub lease_id: u64,
    pub sequencer_node_id: String,
    pub pointer_revision: u64,
    pub observed_compare_token: String,
    pub committed_at: String,
    #[serde(default)]
    pub committed_at_epoch_ms: u64,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub published_watermarks: std::collections::BTreeMap<u32, ShardPublishedWatermark>,
}

impl PublicationCommittedRecord {
    pub fn validate(&self, expected_index_uuid: &str) -> Result<()> {
        if !PUBLICATION_RECORD_FORMAT_CAPABILITY.supports_read(self.format_version) {
            return Err(crate::common::unsupported_index_format(
                "remote publication committed record",
                format!(
                    "format_version {} is outside supported range {}..={}",
                    self.format_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.min_read_version,
                    PUBLICATION_RECORD_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if self.index_uuid != expected_index_uuid {
            anyhow::bail!(
                "publication committed record index_uuid mismatch: expected {}, got {}",
                expected_index_uuid,
                self.index_uuid
            );
        }
        validate_publication_operation_id(&self.operation_id).map_err(anyhow::Error::msg)?;
        if self.generation == 0 || self.writer_epoch == 0 || self.pointer_revision == 0 {
            anyhow::bail!(
                "publication committed record for '{}' has zero generation, writer_epoch, or pointer_revision",
                self.operation_id
            );
        }
        if !self.manifest_checksum.starts_with("sha256:") {
            anyhow::bail!(
                "publication committed record for '{}' has invalid manifest_checksum '{}'",
                self.operation_id,
                self.manifest_checksum
            );
        }
        Ok(())
    }
}

/// Immutable attempt record written at `<index_uuid>/publication-attempts/<operation_id>/<attempt_id>.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationAttemptRecord {
    pub format_version: u32,
    pub index_uuid: String,
    pub operation_id: String,
    pub attempt_id: String,
    pub sequencer_node_id: String,
    pub writer_epoch: u64,
    pub lease_id: u64,
    pub parent_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_path: Option<String>,
    pub candidate_generation: u64,
    pub candidate_manifest_path: String,
    pub candidate_manifest_checksum: String,
    pub outcome: PublicationAttemptOutcome,
    pub recorded_at: String,
    #[serde(default)]
    pub recorded_at_epoch_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Request sent to the active sequencer to commit a prepared publication.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerCommitRequest {
    pub index_uuid: String,
    pub index_name: String,
    pub operation_id: String,
    pub request_fingerprint: String,
    pub schema_generation: u64,
    pub schema_hash: String,
    pub writer_epoch: u64,
    pub lease_id: u64,
    pub sequencer_node_id: String,
    pub fenced_pointer_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<crate::cluster::state::RemoteStoreSettings>,
}

/// Result of a committed or replayed publication operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationCommitOutcome {
    pub index_uuid: String,
    pub index_name: String,
    pub operation_id: String,
    pub request_fingerprint: String,
    pub split_id: String,
    pub doc_count: u64,
    pub size_bytes: u64,
    pub generation: u64,
    pub parent_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_path: Option<String>,
    pub manifest_path: String,
    pub manifest_checksum: String,
    pub writer_epoch: u64,
    pub pointer_revision: u64,
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub published_watermarks: std::collections::BTreeMap<u32, ShardPublishedWatermark>,
}

/// Result of resolving an operation ID against the committed manifest chain and
/// durable operation records.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationResolution {
    Committed(PublicationCommitOutcome),
    Conflict {
        operation_id: String,
        reason: String,
    },
    NotCommitted {
        intent: Option<PublicationIntentRecord>,
        prepared: Option<PublicationPreparedRecord>,
    },
}

/// Validate a publication operation ID so it is non-empty, bounded, and safe to
/// embed in object-store path segments.
pub fn validate_publication_operation_id(operation_id: &str) -> Result<(), String> {
    crate::engine::ClientOperationIntent::validate_id(operation_id).map_err(|e| e.to_string())?;
    if operation_id == "." || operation_id == ".." {
        return Err("operation_id cannot be '.' or '..'".into());
    }
    if !operation_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
    {
        return Err(
            "operation_id may contain only ASCII alphanumeric characters, '-', '_', '.', or ':'"
                .into(),
        );
    }
    Ok(())
}

/// Deterministically derive the split ID for a publication operation.
pub fn deterministic_publication_split_id(index_uuid: &str, operation_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ferris-remote-split-v1\0");
    hasher.update(index_uuid.as_bytes());
    hasher.update(b"\0");
    hasher.update(operation_id.as_bytes());
    let digest = hasher.finalize();
    format!("split-{}", &hex_lower(&digest)[..24])
}

/// Deterministically derive a document `_id` when the client omitted `_id` at
/// `doc_ordinal` within `operation_id` (ADR 0002 section 3.1).
pub fn deterministic_publication_doc_id(
    index_uuid: &str,
    operation_id: &str,
    doc_ordinal: usize,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ferris-remote-doc-v1\0");
    hasher.update(index_uuid.as_bytes());
    hasher.update(b"\0");
    hasher.update(operation_id.as_bytes());
    hasher.update(b"\0");
    hasher.update((doc_ordinal as u64).to_le_bytes());
    let digest = hasher.finalize();
    format!("doc-{}-{doc_ordinal}", &hex_lower(&digest)[..20])
}

/// Compute a canonical request fingerprint over the index UUID, schema
/// generation, schema hash, and normalized documents (`(doc_id, source)`).
pub fn compute_publication_request_fingerprint(
    index_uuid: &str,
    schema_generation: u64,
    schema_hash: &str,
    prepared_docs: &[(String, serde_json::Value)],
) -> Result<String, String> {
    let canonical = serde_json::json!({
        "kind": "remote_store_publish_v1",
        "index_uuid": index_uuid,
        "schema_generation": schema_generation,
        "schema_hash": schema_hash,
        "docs": prepared_docs,
    });
    let raw = crate::engine::ClientOperationIntent::fingerprint_json(&canonical)
        .map_err(|e| e.to_string())?;
    if raw.starts_with("sha256:") {
        Ok(raw)
    } else {
        Ok(format!("sha256:{raw}"))
    }
}

pub fn manifest_operation_key(index_uuid: &str, generation: u64, operation_id: &str) -> String {
    format!("{index_uuid}/manifests/{generation:012}/{operation_id}.json")
}

pub fn manifest_operation_epoch_key(
    index_uuid: &str,
    generation: u64,
    operation_id: &str,
    writer_epoch: u64,
) -> String {
    format!("{index_uuid}/manifests/{generation:012}/{operation_id}--epoch-{writer_epoch:012}.json")
}

pub fn publication_intent_key(index_uuid: &str, operation_id: &str) -> String {
    format!("{index_uuid}/publications/{operation_id}/intent.json")
}

pub fn publication_prepared_key(index_uuid: &str, operation_id: &str) -> String {
    format!("{index_uuid}/publications/{operation_id}/prepared.json")
}

pub fn publication_committed_key(index_uuid: &str, operation_id: &str) -> String {
    format!("{index_uuid}/publications/{operation_id}/committed.json")
}

pub fn publication_attempt_key(index_uuid: &str, operation_id: &str, attempt_id: &str) -> String {
    format!("{index_uuid}/publication-attempts/{operation_id}/{attempt_id}.json")
}

pub fn is_canonical_manifest_path(
    index_uuid: &str,
    generation: u64,
    operation_id: Option<&str>,
    manifest_path: &str,
) -> bool {
    if manifest_path == manifest_generation_key(index_uuid, generation) {
        return true;
    }
    let prefix = format!("{index_uuid}/manifests/{generation:012}/");
    let Some(rest) = manifest_path.strip_prefix(&prefix) else {
        return false;
    };
    let Some(stem) = rest.strip_suffix(".json") else {
        return false;
    };
    if stem.is_empty() || stem.contains('/') {
        return false;
    }
    let op_part = match stem.rsplit_once("--epoch-") {
        Some((op, epoch_str))
            if !op.is_empty()
                && epoch_str.len() == 12
                && epoch_str.chars().all(|c| c.is_ascii_digit()) =>
        {
            op
        }
        Some(_) => return false,
        None => stem,
    };
    match operation_id {
        Some(expected_op) if !expected_op.is_empty() => op_part == expected_op,
        _ => true,
    }
}

pub fn is_canonical_split_bundle_path(
    index_uuid: &str,
    split_id: &str,
    operation_id: &str,
    bundle_path: &str,
) -> bool {
    let expected_split_key = StorageManager::split_bundle_key(index_uuid, split_id);
    let expected_staged_key = format!("{index_uuid}/staged/{operation_id}/bundle");
    let expected_staged_split_key = format!("{index_uuid}/staged/{split_id}/bundle");
    bundle_path == expected_split_key
        || (!operation_id.is_empty() && bundle_path == expected_staged_key)
        || bundle_path == expected_staged_split_key
}

pub(crate) fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex_lower(&hasher.finalize()))
}

pub(crate) fn is_newer_writer_authority(
    candidate: &crate::cluster::state::ManifestWriterAuthority,
    existing: &crate::cluster::state::ManifestWriterAuthority,
) -> bool {
    if candidate.index_uuid != existing.index_uuid {
        return true;
    }
    if candidate.last_writer_epoch != existing.last_writer_epoch {
        return candidate.last_writer_epoch > existing.last_writer_epoch;
    }
    if candidate.last_lease_id != existing.last_lease_id {
        return candidate.last_lease_id > existing.last_lease_id;
    }
    let phase_rank = |p: &crate::cluster::state::ManifestWriterPhase| match p {
        crate::cluster::state::ManifestWriterPhase::Unassigned => 2u8,
        crate::cluster::state::ManifestWriterPhase::Active { .. } => 1,
        crate::cluster::state::ManifestWriterPhase::Fencing { .. } => 0,
    };
    if phase_rank(&candidate.phase) != phase_rank(&existing.phase) {
        return phase_rank(&candidate.phase) > phase_rank(&existing.phase);
    }
    candidate.renewal_seq >= existing.renewal_seq
}

impl StorageManager {
    pub(crate) async fn sequencer_lock(
        &self,
        index_uuid: &str,
    ) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut map = self.sequencer_locks.lock().await;
        map.entry(index_uuid.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(crate) fn cached_writer_authority(
        &self,
        index_uuid: &str,
    ) -> Option<crate::cluster::state::ManifestWriterAuthority> {
        self.cached_writer_authorities
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(index_uuid)
            .cloned()
    }

    pub(crate) fn observe_writer_authority(
        &self,
        authority: &crate::cluster::state::ManifestWriterAuthority,
    ) {
        let mut map = self
            .cached_writer_authorities
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let should_update = match map.get(&authority.index_uuid) {
            None => true,
            Some(existing) => is_newer_writer_authority(authority, existing),
        };
        if should_update {
            map.insert(authority.index_uuid.clone(), authority.clone());
        }
    }

    pub fn set_publication_crash_stage_for_test(&self, stage: Option<PublicationCrashStage>) {
        *self
            .publication_crash_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = stage;
    }

    pub fn take_publication_crash_stage_if_matches(&self, expected: PublicationCrashStage) -> bool {
        let mut guard = self
            .publication_crash_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *guard == Some(expected) {
            *guard = None;
            true
        } else {
            false
        }
    }

    /// Stage an immutable `intent.json` record for `operation_id`.
    ///
    /// If `intent.json` already exists with the same `request_fingerprint` and
    /// `index_uuid`, returns the existing record so an interrupted producer can
    /// resume. If it exists with a different fingerprint, returns
    /// [`PublicationError::OperationConflict`].
    pub async fn stage_publication_intent(
        &self,
        intent: &PublicationIntentRecord,
    ) -> Result<PublicationIntentRecord> {
        self.ensure_publication_supported().await?;
        intent.validate(&intent.index_uuid)?;
        let key = publication_intent_key(&intent.index_uuid, &intent.operation_id);
        let path = self.object_path(&key)?;
        let bytes = serde_json::to_vec_pretty(intent)
            .context("failed to serialize publication intent record")?;
        let outcome = self.create_immutable(&path, bytes).await?;
        if matches!(outcome, super::CreateImmutableOutcome::Created { .. }) {
            return Ok(intent.clone());
        }
        let existing_bytes = self.get_bytes(&path).await?;
        let existing: PublicationIntentRecord =
            serde_json::from_slice(&existing_bytes).map_err(|error| {
                crate::common::unsupported_index_format(
                    "remote publication intent record",
                    format!("cannot decode existing intent at {path}: {error}"),
                )
            })?;
        existing.validate(&intent.index_uuid)?;
        if existing.request_fingerprint != intent.request_fingerprint
            || existing.schema_hash != intent.schema_hash
            || existing.schema_generation != intent.schema_generation
        {
            return Err(PublicationError::OperationConflict {
                operation_id: intent.operation_id.clone(),
                reason: format!(
                    "intent at {path} already exists with fingerprint {}, requested {}",
                    existing.request_fingerprint, intent.request_fingerprint
                ),
            }
            .into());
        }
        Ok(existing)
    }

    pub async fn load_publication_intent(
        &self,
        index_uuid: &str,
        operation_id: &str,
    ) -> Result<Option<PublicationIntentRecord>> {
        validate_publication_operation_id(operation_id).map_err(anyhow::Error::msg)?;
        let key = publication_intent_key(index_uuid, operation_id);
        let path = self.object_path(&key)?;
        match self.get_object(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|error| {
                    crate::metrics::record_object_store_request("get", "error");
                    anyhow::Error::from(error)
                })?;
                crate::metrics::record_object_store_request("get", "success");
                crate::metrics::record_object_store_transfer("download", bytes.len());
                let record: PublicationIntentRecord =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        crate::common::unsupported_index_format(
                            "remote publication intent record",
                            format!("cannot decode intent at {path}: {error}"),
                        )
                    })?;
                record.validate(index_uuid)?;
                Ok(Some(record))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow::Error::from(error)),
        }
    }

    /// Verify that the split bundle referenced by `prepared` exists in the
    /// object store and matches `prepared.split.checksum` and `size_bytes`.
    pub async fn verify_prepared_split_bundle(
        &self,
        index_uuid: &str,
        prepared: &PublicationPreparedRecord,
    ) -> Result<()> {
        prepared.validate(index_uuid)?;
        let path = self.object_path(&prepared.split.bundle_path)?;
        let (bytes, actual_checksum) = self.download_and_hash(&path).await.with_context(|| {
            format!(
                "prepared split bundle '{}' for operation '{}' is missing or unreadable",
                prepared.split.bundle_path, prepared.operation_id
            )
        })?;
        if actual_checksum != prepared.split.checksum {
            anyhow::bail!(
                "prepared split bundle checksum mismatch for '{}': expected {}, got {}",
                prepared.split.split_id,
                prepared.split.checksum,
                actual_checksum
            );
        }
        if bytes.len() as u64 != prepared.split.size_bytes {
            anyhow::bail!(
                "prepared split bundle size mismatch for '{}': expected {}, got {}",
                prepared.split.split_id,
                prepared.split.size_bytes,
                bytes.len()
            );
        }
        Ok(())
    }

    /// Write an immutable `prepared.json` record after verifying the uploaded
    /// bundle. Resumes cleanly if an identical prepared record already exists.
    pub async fn write_publication_prepared(
        &self,
        prepared: &PublicationPreparedRecord,
    ) -> Result<PublicationPreparedRecord> {
        self.ensure_publication_supported().await?;
        self.verify_prepared_split_bundle(&prepared.index_uuid, prepared)
            .await?;
        let key = publication_prepared_key(&prepared.index_uuid, &prepared.operation_id);
        let path = self.object_path(&key)?;
        let bytes = serde_json::to_vec_pretty(prepared)
            .context("failed to serialize publication prepared record")?;
        let outcome = self.create_immutable(&path, bytes).await?;
        if matches!(outcome, super::CreateImmutableOutcome::Created { .. }) {
            return Ok(prepared.clone());
        }
        let existing_bytes = self.get_bytes(&path).await?;
        let existing: PublicationPreparedRecord =
            serde_json::from_slice(&existing_bytes).map_err(|error| {
                crate::common::unsupported_index_format(
                    "remote publication prepared record",
                    format!("cannot decode existing prepared record at {path}: {error}"),
                )
            })?;
        existing.validate(&prepared.index_uuid)?;
        if existing.request_fingerprint != prepared.request_fingerprint
            || existing.split.checksum != prepared.split.checksum
            || existing.split.split_id != prepared.split.split_id
        {
            return Err(PublicationError::OperationConflict {
                operation_id: prepared.operation_id.clone(),
                reason: format!(
                    "prepared record at {path} already exists with different fingerprint or split identity"
                ),
            }
            .into());
        }
        Ok(existing)
    }

    pub async fn load_publication_prepared(
        &self,
        index_uuid: &str,
        operation_id: &str,
    ) -> Result<Option<PublicationPreparedRecord>> {
        validate_publication_operation_id(operation_id).map_err(anyhow::Error::msg)?;
        let key = publication_prepared_key(index_uuid, operation_id);
        let path = self.object_path(&key)?;
        match self.get_object(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|error| {
                    crate::metrics::record_object_store_request("get", "error");
                    anyhow::Error::from(error)
                })?;
                crate::metrics::record_object_store_request("get", "success");
                crate::metrics::record_object_store_transfer("download", bytes.len());
                let record: PublicationPreparedRecord =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        crate::common::unsupported_index_format(
                            "remote publication prepared record",
                            format!("cannot decode prepared record at {path}: {error}"),
                        )
                    })?;
                record.validate(index_uuid)?;
                Ok(Some(record))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow::Error::from(error)),
        }
    }

    pub async fn load_publication_committed(
        &self,
        index_uuid: &str,
        operation_id: &str,
    ) -> Result<Option<PublicationCommittedRecord>> {
        validate_publication_operation_id(operation_id).map_err(anyhow::Error::msg)?;
        let key = publication_committed_key(index_uuid, operation_id);
        let path = self.object_path(&key)?;
        match self.get_object(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|error| {
                    crate::metrics::record_object_store_request("get", "error");
                    anyhow::Error::from(error)
                })?;
                crate::metrics::record_object_store_request("get", "success");
                crate::metrics::record_object_store_transfer("download", bytes.len());
                let record: PublicationCommittedRecord =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        crate::common::unsupported_index_format(
                            "remote publication committed record",
                            format!("cannot decode committed record at {path}: {error}"),
                        )
                    })?;
                record.validate(index_uuid)?;
                Ok(Some(record))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(anyhow::Error::from(error)),
        }
    }

    pub async fn write_publication_committed(
        &self,
        committed: &PublicationCommittedRecord,
    ) -> Result<()> {
        committed.validate(&committed.index_uuid)?;
        let key = publication_committed_key(&committed.index_uuid, &committed.operation_id);
        let path = self.object_path(&key)?;
        let bytes = serde_json::to_vec_pretty(committed)
            .context("failed to serialize publication committed record")?;
        let _ = self.create_immutable(&path, bytes).await?;
        Ok(())
    }

    pub async fn record_publication_attempt(
        &self,
        attempt: &PublicationAttemptRecord,
    ) -> Result<()> {
        crate::metrics::record_remote_store_publication_attempt(attempt.outcome.as_metric_label());
        let key = publication_attempt_key(
            &attempt.index_uuid,
            &attempt.operation_id,
            &attempt.attempt_id,
        );
        let path = self.object_path(&key)?;
        let bytes = serde_json::to_vec_pretty(attempt)
            .context("failed to serialize publication attempt record")?;
        let _ = self.create_immutable(&path, bytes).await?;
        Ok(())
    }

    /// Fence `manifest.current.json` for `writer_epoch` before activating a
    /// Raft `ManifestWriterAuthority` lease (ADR 0002 section 3.4 step 2).
    ///
    /// - If no pointer exists yet, writes an explicit generation-0 empty pointer
    ///   at `pointer_revision = 1` and `writer_epoch`.
    /// - If a pointer exists with `existing.writer_epoch <= writer_epoch`,
    ///   conditionally rewrites it with `pointer_revision + 1` and
    ///   `writer_epoch` while preserving the committed manifest reference.
    /// - If an old writer advances the pointer before our fence, reloads and
    ///   retries.
    pub async fn fence_manifest_pointer(
        &self,
        index_uuid: &str,
        writer_epoch: u64,
    ) -> Result<(RemoteManifestPointer, CompareToken)> {
        if index_uuid.is_empty() || writer_epoch == 0 {
            anyhow::bail!(
                "fence_manifest_pointer requires non-empty index_uuid and positive writer_epoch"
            );
        }
        self.ensure_publication_supported().await?;
        let pointer_path = self.object_path(&manifest_pointer_key(index_uuid))?;

        for _ in 0..16 {
            let current = self.load_manifest_pointer_with_token(index_uuid).await?;
            let (fenced_pointer, expected_token) = match current {
                None => (
                    RemoteManifestPointer {
                        version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
                        index_uuid: index_uuid.to_string(),
                        pointer_revision: 1,
                        writer_epoch,
                        current_generation: 0,
                        manifest_path: String::new(),
                        manifest_checksum: None,
                        last_operation_id: None,
                        manifest_etag: None,
                    },
                    None,
                ),
                Some((existing, token)) => {
                    if existing.writer_epoch > writer_epoch {
                        crate::metrics::record_remote_store_pointer_fence("failed");
                        return Err(PublicationError::StaleWriterEpoch {
                            index_uuid: index_uuid.to_string(),
                            attempted_epoch: writer_epoch,
                            current_epoch: existing.writer_epoch,
                        }
                        .into());
                    }
                    if existing.current_generation > 0 {
                        let _ = self
                            .load_manifest_for_pointer(index_uuid, &existing, None)
                            .await?;
                    }
                    if existing.writer_epoch == writer_epoch && existing.pointer_revision > 0 {
                        crate::metrics::record_remote_store_pointer_fence("fenced");
                        return Ok((existing, token));
                    }
                    (
                        RemoteManifestPointer {
                            version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
                            index_uuid: index_uuid.to_string(),
                            pointer_revision: existing.pointer_revision.saturating_add(1).max(1),
                            writer_epoch,
                            current_generation: existing.current_generation,
                            manifest_path: existing.manifest_path,
                            manifest_checksum: existing.manifest_checksum,
                            last_operation_id: existing.last_operation_id,
                            manifest_etag: existing.manifest_etag,
                        },
                        Some(token),
                    )
                }
            };
            fenced_pointer.validate_for_index(index_uuid)?;
            let pointer_bytes = serde_json::to_vec_pretty(&fenced_pointer)
                .context("failed to serialize fenced manifest pointer")?;
            match self
                .compare_and_swap_optional(&pointer_path, expected_token.as_ref(), pointer_bytes)
                .await?
            {
                CompareAndSwapOutcome::Swapped { token } => {
                    crate::metrics::record_remote_store_pointer_fence("fenced");
                    return Ok((fenced_pointer, token));
                }
                CompareAndSwapOutcome::Conflict => {
                    crate::metrics::record_remote_store_pointer_fence("conflict");
                    continue;
                }
            }
        }

        crate::metrics::record_remote_store_pointer_fence("failed");
        anyhow::bail!(
            "failed to fence manifest pointer for index '{index_uuid}' at epoch {writer_epoch} after repeated CAS conflicts"
        )
    }

    /// Walk the committed manifest chain starting from `manifest.current.json`
    /// to resolve whether `operation_id` has already been committed, conflicts
    /// with a different fingerprint, or remains uncommitted.
    pub async fn resolve_publication_operation(
        &self,
        index_uuid: &str,
        operation_id: &str,
        expected_fingerprint: &str,
    ) -> Result<PublicationResolution> {
        validate_publication_operation_id(operation_id).map_err(anyhow::Error::msg)?;

        if let Some((pointer, token)) = self.load_manifest_pointer_with_token(index_uuid).await?
            && pointer.current_generation > 0
        {
            let mut next_path = Some(pointer.manifest_path.clone());
            let mut expected_gen = pointer.current_generation;
            let mut steps = 0usize;

            while let Some(manifest_path) = next_path.take() {
                steps += 1;
                if steps > MAX_LINEAGE_WALK_GENERATIONS {
                    anyhow::bail!(
                        "manifest lineage walk exceeded {MAX_LINEAGE_WALK_GENERATIONS} generations for index {index_uuid}"
                    );
                }
                let (manifest, manifest_checksum) = self
                    .load_manifest_at_path(index_uuid, &manifest_path, expected_gen, None)
                    .await?;
                if steps == 1 {
                    pointer.validate_manifest_agreement(&manifest, &manifest_checksum)?;
                }
                if manifest.operation_id == operation_id {
                    if !manifest.request_fingerprint.is_empty()
                        && manifest.request_fingerprint != expected_fingerprint
                    {
                        return Ok(PublicationResolution::Conflict {
                            operation_id: operation_id.to_string(),
                            reason: format!(
                                "operation_id '{}' was already committed at generation {} with fingerprint {}, requested {}",
                                operation_id,
                                manifest.generation,
                                manifest.request_fingerprint,
                                expected_fingerprint
                            ),
                        });
                    }
                    let split = manifest
                        .splits
                        .iter()
                        .find(|s| {
                            s.tags.get("operation_id").map(String::as_str) == Some(operation_id)
                                || s.split_id
                                    == deterministic_publication_split_id(index_uuid, operation_id)
                        })
                        .or_else(|| manifest.splits.last())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "committed manifest generation {} for operation '{}' has no splits",
                                manifest.generation,
                                operation_id
                            )
                        })?;
                    let existing_committed = self
                        .load_publication_committed(index_uuid, operation_id)
                        .await?;
                    let pointer_revision = existing_committed
                        .as_ref()
                        .map(|c| c.pointer_revision)
                        .unwrap_or(pointer.pointer_revision);
                    if existing_committed.is_none() {
                        let now_ms = super::now_epoch_ms();
                        let repair = PublicationCommittedRecord {
                            format_version: PUBLICATION_RECORD_FORMAT_VERSION,
                            index_uuid: index_uuid.to_string(),
                            index_name: manifest.index_name.clone(),
                            operation_id: operation_id.to_string(),
                            request_fingerprint: expected_fingerprint.to_string(),
                            split_id: split.split_id.clone(),
                            doc_count: split.doc_count,
                            size_bytes: split.size_bytes,
                            generation: manifest.generation,
                            parent_generation: manifest.parent_generation,
                            parent_manifest_path: manifest.parent_manifest_path.clone(),
                            manifest_path: manifest_path.clone(),
                            manifest_checksum: manifest_checksum.clone(),
                            writer_epoch: manifest.writer_epoch,
                            lease_id: 1,
                            sequencer_node_id: "repaired".to_string(),
                            pointer_revision,
                            observed_compare_token: token.as_str().to_string(),
                            committed_at: chrono::Utc::now().to_rfc3339(),
                            committed_at_epoch_ms: now_ms,
                            published_watermarks: manifest.published_watermarks.clone(),
                        };
                        let _ = self.write_publication_committed(&repair).await;
                    }
                    return Ok(PublicationResolution::Committed(PublicationCommitOutcome {
                        index_uuid: index_uuid.to_string(),
                        index_name: manifest.index_name.clone(),
                        operation_id: operation_id.to_string(),
                        request_fingerprint: expected_fingerprint.to_string(),
                        split_id: split.split_id.clone(),
                        doc_count: split.doc_count,
                        size_bytes: split.size_bytes,
                        generation: manifest.generation,
                        parent_generation: manifest.parent_generation,
                        parent_manifest_path: manifest.parent_manifest_path.clone(),
                        manifest_path,
                        manifest_checksum,
                        writer_epoch: manifest.writer_epoch,
                        pointer_revision,
                        replayed: true,
                        published_watermarks: manifest.published_watermarks,
                    }));
                }

                if manifest.parent_generation == 0 {
                    break;
                }
                expected_gen = manifest.parent_generation;
                next_path = manifest.parent_manifest_path.clone();
            }
        }

        let intent = self
            .load_publication_intent(index_uuid, operation_id)
            .await?;
        if let Some(ref intent_rec) = intent
            && intent_rec.request_fingerprint != expected_fingerprint
        {
            return Ok(PublicationResolution::Conflict {
                operation_id: operation_id.to_string(),
                reason: format!(
                    "operation_id '{}' already has an intent with fingerprint {}, requested {}",
                    operation_id, intent_rec.request_fingerprint, expected_fingerprint
                ),
            });
        }
        let prepared = self
            .load_publication_prepared(index_uuid, operation_id)
            .await?;
        if let Some(ref prep_rec) = prepared
            && prep_rec.request_fingerprint != expected_fingerprint
        {
            return Ok(PublicationResolution::Conflict {
                operation_id: operation_id.to_string(),
                reason: format!(
                    "operation_id '{}' already has a prepared record with fingerprint {}, requested {}",
                    operation_id, prep_rec.request_fingerprint, expected_fingerprint
                ),
            });
        }

        Ok(PublicationResolution::NotCommitted { intent, prepared })
    }

    /// Execute the ADR 0002 section 3.5 10-step fenced sequencer commit
    /// algorithm for a prepared publication.
    pub async fn commit_prepared_publication(
        &self,
        req: &SequencerCommitRequest,
    ) -> Result<PublicationCommitOutcome> {
        // Step 1: Serialize commits locally for this index.
        let lock_arc = self.sequencer_lock(&req.index_uuid).await;
        let _sequencer_guard = lock_arc.lock().await;
        self.commit_prepared_publication_locked(req).await
    }

    pub(crate) async fn commit_prepared_publication_locked(
        &self,
        req: &SequencerCommitRequest,
    ) -> Result<PublicationCommitOutcome> {
        self.ensure_publication_supported().await?;
        validate_publication_operation_id(&req.operation_id).map_err(anyhow::Error::msg)?;
        if req.writer_epoch == 0 || req.lease_id == 0 || req.fenced_pointer_revision == 0 {
            anyhow::bail!(
                "sequencer commit requires positive writer_epoch, lease_id, and fenced_pointer_revision"
            );
        }

        // Step 2 & 3 & 4: Resolve operation records and committed manifest lineage.
        match self
            .resolve_publication_operation(
                &req.index_uuid,
                &req.operation_id,
                &req.request_fingerprint,
            )
            .await?
        {
            PublicationResolution::Committed(outcome) => {
                crate::metrics::record_remote_store_publication_attempt("replayed");
                Ok(outcome)
            }
            PublicationResolution::Conflict {
                operation_id,
                reason,
            } => {
                crate::metrics::record_remote_store_publication_attempt("conflict");
                Err(PublicationError::OperationConflict {
                    operation_id,
                    reason,
                }
                .into())
            }
            PublicationResolution::NotCommitted { intent, prepared } => {
                let Some(intent) = intent else {
                    return Err(PublicationError::NotCommitted {
                        operation_id: req.operation_id.clone(),
                        reason: "missing publication intent.json".into(),
                    }
                    .into());
                };
                let Some(prepared) = prepared else {
                    return Err(PublicationError::NotCommitted {
                        operation_id: req.operation_id.clone(),
                        reason: "missing publication prepared.json".into(),
                    }
                    .into());
                };
                if intent.request_fingerprint != req.request_fingerprint
                    || prepared.request_fingerprint != req.request_fingerprint
                {
                    crate::metrics::record_remote_store_publication_attempt("conflict");
                    return Err(PublicationError::OperationConflict {
                        operation_id: req.operation_id.clone(),
                        reason: "intent or prepared fingerprint does not match commit request"
                            .into(),
                    }
                    .into());
                }
                if prepared.schema_hash != req.schema_hash
                    || prepared.schema_generation != req.schema_generation
                {
                    anyhow::bail!(
                        "schema mismatch for operation '{}': prepared=({}, {}) request=({}, {})",
                        req.operation_id,
                        prepared.schema_generation,
                        prepared.schema_hash,
                        req.schema_generation,
                        req.schema_hash
                    );
                }

                // Step 5: Validate prepared artifact checksum, sizes, and bundle object.
                self.verify_prepared_split_bundle(&req.index_uuid, &prepared)
                    .await?;

                if self.take_publication_crash_stage_if_matches(
                    PublicationCrashStage::AfterPreparedBeforeManifestCreated,
                ) {
                    return Err(PublicationError::InjectedCrash {
                        operation_id: req.operation_id.clone(),
                        stage: PublicationCrashStage::AfterPreparedBeforeManifestCreated,
                    }
                    .into());
                }

                // Step 6: Read and validate current pointer and parent manifest.
                let Some((pointer, expected_token)) = self
                    .load_manifest_pointer_with_token(&req.index_uuid)
                    .await?
                else {
                    return Err(PublicationError::UnfencedPointer {
                        index_uuid: req.index_uuid.clone(),
                        expected_epoch: req.writer_epoch,
                        observed_epoch: 0,
                    }
                    .into());
                };
                if pointer.writer_epoch > req.writer_epoch {
                    return Err(PublicationError::StaleWriterEpoch {
                        index_uuid: req.index_uuid.clone(),
                        attempted_epoch: req.writer_epoch,
                        current_epoch: pointer.writer_epoch,
                    }
                    .into());
                }
                if pointer.writer_epoch < req.writer_epoch
                    || pointer.pointer_revision < req.fenced_pointer_revision
                {
                    return Err(PublicationError::UnfencedPointer {
                        index_uuid: req.index_uuid.clone(),
                        expected_epoch: req.writer_epoch,
                        observed_epoch: pointer.writer_epoch,
                    }
                    .into());
                }

                let (
                    parent_generation,
                    parent_manifest_path,
                    mut splits,
                    existing_settings,
                    mut published_watermarks,
                ) = if pointer.current_generation == 0 {
                    (
                        0u64,
                        None,
                        Vec::new(),
                        None,
                        std::collections::BTreeMap::new(),
                    )
                } else {
                    let parent = self
                        .load_manifest_for_pointer(
                            &req.index_uuid,
                            &pointer,
                            Some(&req.schema_hash),
                        )
                        .await?;
                    (
                        parent.generation,
                        Some(pointer.manifest_path.clone()),
                        parent.splits,
                        parent.settings,
                        parent.published_watermarks,
                    )
                };

                if splits
                    .iter()
                    .any(|existing| existing.split_id == prepared.split.split_id)
                {
                    anyhow::bail!(
                        "split_id '{}' already exists in parent manifest generation {} for index '{}'",
                        prepared.split.split_id,
                        parent_generation,
                        req.index_uuid
                    );
                }

                let candidate_generation = parent_generation + 1;
                let mut split_entry = prepared.split.clone();
                split_entry
                    .tags
                    .insert("operation_id".into(), req.operation_id.clone());
                split_entry
                    .tags
                    .insert("writer_epoch".into(), req.writer_epoch.to_string());
                split_entry.tags.insert(
                    "schema_generation".into(),
                    req.schema_generation.to_string(),
                );
                if let Some(range) = split_entry.seal_range()? {
                    advance_shard_watermark(
                        &mut published_watermarks,
                        &range,
                        &split_entry.split_id,
                        &req.operation_id,
                        candidate_generation,
                    )?;
                }
                splits.push(split_entry.clone());

                // Step 7: Create candidate immutable manifest at manifests/<gen>/<operation_id>.json
                // (or disambiguate with --epoch-<epoch> if a crashed earlier epoch left a candidate at the same generation).
                let mut candidate_manifest_key = manifest_operation_key(
                    &req.index_uuid,
                    candidate_generation,
                    &req.operation_id,
                );
                let candidate_manifest = RemoteStoreManifest {
                    version: REMOTE_MANIFEST_FORMAT_VERSION,
                    engine: crate::cluster::state::IndexEngine::RemoteStore,
                    index_uuid: req.index_uuid.clone(),
                    index_name: req.index_name.clone(),
                    schema_generation: req.schema_generation,
                    schema_hash: req.schema_hash.clone(),
                    generation: candidate_generation,
                    parent_generation,
                    parent_manifest_path: parent_manifest_path.clone(),
                    writer_epoch: req.writer_epoch,
                    operation_id: req.operation_id.clone(),
                    request_fingerprint: req.request_fingerprint.clone(),
                    published_at: prepared.prepared_at.clone(),
                    settings: req.settings.clone().or(existing_settings),
                    published_watermarks: published_watermarks.clone(),
                    splits,
                };
                candidate_manifest.validate_for_read(
                    &req.index_uuid,
                    candidate_generation,
                    Some(&req.schema_hash),
                )?;

                let candidate_bytes = serde_json::to_vec_pretty(&candidate_manifest)
                    .context("failed to serialize candidate manifest")?;
                let candidate_checksum = sha256_prefixed(&candidate_bytes);
                let candidate_path = self.object_path(&candidate_manifest_key)?;
                let create_outcome = self
                    .create_immutable(&candidate_path, candidate_bytes.clone())
                    .await?;
                if let super::CreateImmutableOutcome::AlreadyExists { ref existing } =
                    create_outcome
                    && !existing.content_matches
                {
                    let (existing_manifest, _) = self
                        .load_manifest_at_path(
                            &req.index_uuid,
                            &candidate_manifest_key,
                            candidate_generation,
                            Some(&req.schema_hash),
                        )
                        .await?;
                    if existing_manifest.operation_id == req.operation_id
                        && existing_manifest.request_fingerprint == req.request_fingerprint
                        && existing_manifest.writer_epoch < req.writer_epoch
                    {
                        candidate_manifest_key = manifest_operation_epoch_key(
                            &req.index_uuid,
                            candidate_generation,
                            &req.operation_id,
                            req.writer_epoch,
                        );
                        let epoch_candidate_path = self.object_path(&candidate_manifest_key)?;
                        let epoch_outcome = self
                            .create_immutable(&epoch_candidate_path, candidate_bytes)
                            .await?;
                        epoch_outcome.ensure_matching_content(&epoch_candidate_path)?;
                    } else {
                        create_outcome.ensure_matching_content(&candidate_path)?;
                    }
                } else {
                    create_outcome.ensure_matching_content(&candidate_path)?;
                }

                let next_pointer_revision = pointer.pointer_revision.saturating_add(1);
                let attempt_id = format!(
                    "epoch-{:012}-gen-{:012}-rev-{:012}-{}",
                    req.writer_epoch,
                    candidate_generation,
                    next_pointer_revision,
                    uuid::Uuid::new_v4().simple()
                );

                if self.take_publication_crash_stage_if_matches(
                    PublicationCrashStage::AfterManifestCreatedBeforePointerCas,
                ) {
                    return Err(PublicationError::InjectedCrash {
                        operation_id: req.operation_id.clone(),
                        stage: PublicationCrashStage::AfterManifestCreatedBeforePointerCas,
                    }
                    .into());
                }

                // Step 8: Conditionally update manifest.current.json.
                let new_pointer = RemoteManifestPointer {
                    version: REMOTE_MANIFEST_POINTER_FORMAT_VERSION,
                    index_uuid: req.index_uuid.clone(),
                    pointer_revision: next_pointer_revision,
                    writer_epoch: req.writer_epoch,
                    current_generation: candidate_generation,
                    manifest_path: candidate_manifest_key.clone(),
                    manifest_checksum: Some(candidate_checksum.clone()),
                    last_operation_id: Some(req.operation_id.clone()),
                    manifest_etag: None,
                };
                new_pointer.validate_for_index(&req.index_uuid)?;
                let new_pointer_bytes = serde_json::to_vec_pretty(&new_pointer)
                    .context("failed to serialize new manifest pointer")?;
                let pointer_path = self.object_path(&manifest_pointer_key(&req.index_uuid))?;

                let cas_result: std::result::Result<
                    CompareAndSwapOutcome,
                    ConditionalStorageError,
                > = if self.take_publication_crash_stage_if_matches(
                    PublicationCrashStage::ForcePointerCasIndeterminateBeforeCommit,
                ) {
                    Err(ConditionalStorageError::Indeterminate {
                        path: pointer_path.to_string(),
                        reason: "injected indeterminate pointer CAS before commit".into(),
                    })
                } else {
                    let actual = self
                        .compare_and_swap(&pointer_path, &expected_token, new_pointer_bytes)
                        .await;
                    if actual.is_ok()
                        && self.take_publication_crash_stage_if_matches(
                            PublicationCrashStage::ForcePointerCasIndeterminateAfterCommit,
                        )
                    {
                        Err(ConditionalStorageError::Indeterminate {
                            path: pointer_path.to_string(),
                            reason: "injected indeterminate pointer CAS after commit".into(),
                        })
                    } else {
                        actual
                    }
                };

                // Step 9 & 10: Handle CAS outcome and record committed/attempt audit records.
                match cas_result {
                    Ok(CompareAndSwapOutcome::Swapped { token }) => {
                        if self.take_publication_crash_stage_if_matches(
                            PublicationCrashStage::AfterPointerCommittedBeforeCommitRecord,
                        ) {
                            return Err(PublicationError::InjectedCrash {
                                operation_id: req.operation_id.clone(),
                                stage:
                                    PublicationCrashStage::AfterPointerCommittedBeforeCommitRecord,
                            }
                            .into());
                        }
                        let now_ms = super::now_epoch_ms();
                        let now_iso = chrono::Utc::now().to_rfc3339();
                        let committed_record = PublicationCommittedRecord {
                            format_version: PUBLICATION_RECORD_FORMAT_VERSION,
                            index_uuid: req.index_uuid.clone(),
                            index_name: req.index_name.clone(),
                            operation_id: req.operation_id.clone(),
                            request_fingerprint: req.request_fingerprint.clone(),
                            split_id: split_entry.split_id.clone(),
                            doc_count: split_entry.doc_count,
                            size_bytes: split_entry.size_bytes,
                            generation: candidate_generation,
                            parent_generation,
                            parent_manifest_path: parent_manifest_path.clone(),
                            manifest_path: candidate_manifest_key.clone(),
                            manifest_checksum: candidate_checksum.clone(),
                            writer_epoch: req.writer_epoch,
                            lease_id: req.lease_id,
                            sequencer_node_id: req.sequencer_node_id.clone(),
                            pointer_revision: next_pointer_revision,
                            observed_compare_token: token.as_str().to_string(),
                            committed_at: now_iso.clone(),
                            committed_at_epoch_ms: now_ms,
                            published_watermarks: published_watermarks.clone(),
                        };
                        let _ = self.write_publication_committed(&committed_record).await;
                        let _ = self
                            .record_publication_attempt(&PublicationAttemptRecord {
                                format_version: PUBLICATION_RECORD_FORMAT_VERSION,
                                index_uuid: req.index_uuid.clone(),
                                operation_id: req.operation_id.clone(),
                                attempt_id,
                                sequencer_node_id: req.sequencer_node_id.clone(),
                                writer_epoch: req.writer_epoch,
                                lease_id: req.lease_id,
                                parent_generation,
                                parent_manifest_path: parent_manifest_path.clone(),
                                candidate_generation,
                                candidate_manifest_path: candidate_manifest_key.clone(),
                                candidate_manifest_checksum: candidate_checksum.clone(),
                                outcome: PublicationAttemptOutcome::Committed,
                                recorded_at: now_iso,
                                recorded_at_epoch_ms: now_ms,
                                detail: None,
                            })
                            .await;

                        if self.take_publication_crash_stage_if_matches(
                            PublicationCrashStage::AfterCommitRecordBeforeResponse,
                        ) {
                            return Err(PublicationError::InjectedCrash {
                                operation_id: req.operation_id.clone(),
                                stage: PublicationCrashStage::AfterCommitRecordBeforeResponse,
                            }
                            .into());
                        }

                        Ok(PublicationCommitOutcome {
                            index_uuid: req.index_uuid.clone(),
                            index_name: req.index_name.clone(),
                            operation_id: req.operation_id.clone(),
                            request_fingerprint: req.request_fingerprint.clone(),
                            split_id: split_entry.split_id,
                            doc_count: split_entry.doc_count,
                            size_bytes: split_entry.size_bytes,
                            generation: candidate_generation,
                            parent_generation,
                            parent_manifest_path,
                            manifest_path: candidate_manifest_key,
                            manifest_checksum: candidate_checksum,
                            writer_epoch: req.writer_epoch,
                            pointer_revision: next_pointer_revision,
                            replayed: false,
                            published_watermarks,
                        })
                    }
                    Ok(CompareAndSwapOutcome::Conflict) => {
                        let now_ms = super::now_epoch_ms();
                        let _ = self
                            .record_publication_attempt(&PublicationAttemptRecord {
                                format_version: PUBLICATION_RECORD_FORMAT_VERSION,
                                index_uuid: req.index_uuid.clone(),
                                operation_id: req.operation_id.clone(),
                                attempt_id,
                                sequencer_node_id: req.sequencer_node_id.clone(),
                                writer_epoch: req.writer_epoch,
                                lease_id: req.lease_id,
                                parent_generation,
                                parent_manifest_path,
                                candidate_generation,
                                candidate_manifest_path: candidate_manifest_key,
                                candidate_manifest_checksum: candidate_checksum,
                                outcome: PublicationAttemptOutcome::Superseded,
                                recorded_at: chrono::Utc::now().to_rfc3339(),
                                recorded_at_epoch_ms: now_ms,
                                detail: Some("pointer CAS reported definite conflict".into()),
                            })
                            .await;

                        if let Some((reloaded, _)) = self
                            .load_manifest_pointer_with_token(&req.index_uuid)
                            .await?
                            && reloaded.writer_epoch > req.writer_epoch
                        {
                            return Err(PublicationError::StaleWriterEpoch {
                                index_uuid: req.index_uuid.clone(),
                                attempted_epoch: req.writer_epoch,
                                current_epoch: reloaded.writer_epoch,
                            }
                            .into());
                        }
                        Err(PublicationError::PointerConflict {
                            index_uuid: req.index_uuid.clone(),
                            reason: format!(
                                "candidate generation {} for operation '{}' lost pointer CAS",
                                candidate_generation, req.operation_id
                            ),
                        }
                        .into())
                    }
                    Err(ConditionalStorageError::Indeterminate { reason, .. }) => {
                        // ADR 0002 section 3.7: resolve against pointer/chain before returning indeterminate.
                        if let Ok(PublicationResolution::Committed(outcome)) = self
                            .resolve_publication_operation(
                                &req.index_uuid,
                                &req.operation_id,
                                &req.request_fingerprint,
                            )
                            .await
                        {
                            crate::metrics::record_remote_store_publication_attempt("replayed");
                            return Ok(outcome);
                        }
                        let now_ms = super::now_epoch_ms();
                        let _ = self
                            .record_publication_attempt(&PublicationAttemptRecord {
                                format_version: PUBLICATION_RECORD_FORMAT_VERSION,
                                index_uuid: req.index_uuid.clone(),
                                operation_id: req.operation_id.clone(),
                                attempt_id,
                                sequencer_node_id: req.sequencer_node_id.clone(),
                                writer_epoch: req.writer_epoch,
                                lease_id: req.lease_id,
                                parent_generation,
                                parent_manifest_path,
                                candidate_generation,
                                candidate_manifest_path: candidate_manifest_key,
                                candidate_manifest_checksum: candidate_checksum,
                                outcome: PublicationAttemptOutcome::Indeterminate,
                                recorded_at: chrono::Utc::now().to_rfc3339(),
                                recorded_at_epoch_ms: now_ms,
                                detail: Some(reason.clone()),
                            })
                            .await;
                        Err(PublicationError::Indeterminate {
                            operation_id: req.operation_id.clone(),
                            reason,
                        }
                        .into())
                    }
                    Err(other) => Err(anyhow::Error::from(other)),
                }
            }
        }
    }
}
