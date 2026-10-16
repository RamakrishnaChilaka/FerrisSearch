//! Versioned, persisted shard snapshot format (`FS-022`).
//!
//! Captures Tantivy segment files, committed WAL/sequence boundary, retained
//! document/version and canonical-history archives, vector index files/metadata,
//! schema hash, and per-file SHA-256 checksums, sealed with an atomic
//! `.complete` completion marker.

use super::PeerRecoveryFileMetadata;
use super::sequence::CommittedBoundaryRecord;
use crate::cluster::state::FieldMapping;
use crate::storage::FormatCapability;
use crate::wal::WalCursor;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

pub const SHARD_SNAPSHOT_FORMAT_VERSION: u32 = 1;
pub const SHARD_SNAPSHOT_MANIFEST_FILE: &str = "shard-snapshot.manifest.json";
pub const SHARD_SNAPSHOT_COMPLETE_FILE: &str = "shard-snapshot.complete";
pub const PERSISTED_SHARD_SNAPSHOT_DIR: &str = "snapshot.latest";

pub const SHARD_SNAPSHOT_FORMAT_CAPABILITY: FormatCapability = FormatCapability {
    component: "shard_snapshot_manifest",
    min_read_version: SHARD_SNAPSHOT_FORMAT_VERSION,
    max_read_version: SHARD_SNAPSHOT_FORMAT_VERSION,
    write_version: SHARD_SNAPSHOT_FORMAT_VERSION,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSnapshotVectorMetadata {
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<usize>,
    pub vector_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc_ids_file: Option<String>,
}

impl ShardSnapshotVectorMetadata {
    pub fn validate(&self, files: &[PeerRecoveryFileMetadata]) -> Result<()> {
        if !self.present {
            if self.dimensions.is_some()
                || self.vector_count > 0
                || self.index_file.is_some()
                || self.doc_ids_file.is_some()
            {
                anyhow::bail!(
                    "shard snapshot vector metadata is marked absent but contains vector fields"
                );
            }
            return Ok(());
        }
        let dimensions = self
            .dimensions
            .filter(|dimensions| *dimensions > 0)
            .ok_or_else(|| {
                anyhow::anyhow!("shard snapshot vector metadata requires non-zero dimensions")
            })?;
        let _ = dimensions;
        let index_file = self
            .index_file
            .as_deref()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("shard snapshot vector metadata is missing index_file")
            })?;
        let doc_ids_file = self
            .doc_ids_file
            .as_deref()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!("shard snapshot vector metadata is missing doc_ids_file")
            })?;
        let file_names: HashSet<&str> = files.iter().map(|file| file.name.as_str()).collect();
        if !file_names.contains(index_file) || !file_names.contains(doc_ids_file) {
            anyhow::bail!(
                "shard snapshot vector metadata references files ({index_file}, {doc_ids_file}) absent from the file manifest"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSnapshotCompletionMarker {
    pub format_version: u32,
    pub snapshot_id: String,
    pub manifest_checksum_sha256: String,
    pub completed_at_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSnapshotManifest {
    pub format_version: u32,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<u64>,
    pub primary_term: u64,
    pub created_at_epoch_ms: u64,
    pub schema_hash: String,
    pub committed_boundary: CommittedBoundaryRecord,
    pub snapshot_cursor: WalCursor,
    pub vector_state: ShardSnapshotVectorMetadata,
    pub files: Vec<PeerRecoveryFileMetadata>,
    pub manifest_checksum_sha256: String,
}

#[derive(Deserialize)]
struct VersionHeader {
    format_version: u32,
}

#[derive(Serialize)]
struct CanonicalManifestPayload<'a> {
    format_version: u32,
    snapshot_id: &'a str,
    index_uuid: Option<&'a str>,
    shard_id: Option<u32>,
    allocation_id: Option<u64>,
    primary_term: u64,
    created_at_epoch_ms: u64,
    schema_hash: &'a str,
    committed_boundary: &'a CommittedBoundaryRecord,
    snapshot_cursor: WalCursor,
    vector_state: &'a ShardSnapshotVectorMetadata,
    files: &'a [PeerRecoveryFileMetadata],
}

pub fn compute_shard_schema_hash(mappings: &HashMap<String, FieldMapping>) -> String {
    let ordered: BTreeMap<&str, (&str, Option<usize>)> = mappings
        .iter()
        .map(|(name, mapping)| {
            let kind = match mapping.field_type {
                crate::cluster::state::FieldType::Text => "text",
                crate::cluster::state::FieldType::Keyword => "keyword",
                crate::cluster::state::FieldType::Integer => "integer",
                crate::cluster::state::FieldType::Float => "float",
                crate::cluster::state::FieldType::Boolean => "boolean",
                crate::cluster::state::FieldType::Date => "date",
                crate::cluster::state::FieldType::KnnVector => "knn_vector",
            };
            (name.as_str(), (kind, mapping.dimension))
        })
        .collect();
    let bytes = serde_json::to_vec(&ordered).expect("canonical field mapping map must serialize");
    hex_sha256(&bytes)
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn hash_file_sha256(path: &Path) -> Result<(u64, String)> {
    let mut file = File::open(path).with_context(|| format!("open snapshot file {path:?}"))?;
    let length = file.metadata()?.len();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((length, digest))
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

impl ShardSnapshotManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot_id: impl Into<String>,
        index_uuid: Option<String>,
        shard_id: Option<u32>,
        allocation_id: Option<u64>,
        schema_hash: impl Into<String>,
        committed_boundary: CommittedBoundaryRecord,
        snapshot_cursor: WalCursor,
        vector_state: ShardSnapshotVectorMetadata,
        mut files: Vec<PeerRecoveryFileMetadata>,
    ) -> Result<Self> {
        files.sort_by(|left, right| left.name.cmp(&right.name));
        let primary_term = committed_boundary.term_sequence_state.current_term;
        let mut manifest = Self {
            format_version: SHARD_SNAPSHOT_FORMAT_VERSION,
            snapshot_id: snapshot_id.into(),
            index_uuid,
            shard_id,
            allocation_id,
            primary_term,
            created_at_epoch_ms: now_epoch_ms(),
            schema_hash: schema_hash.into(),
            committed_boundary,
            snapshot_cursor,
            vector_state,
            files,
            manifest_checksum_sha256: String::new(),
        };
        manifest.seal()?;
        Ok(manifest)
    }

    pub fn compute_checksum(&self) -> Result<String> {
        let mut sorted_files = self.files.clone();
        sorted_files.sort_by(|left, right| left.name.cmp(&right.name));
        let canonical = CanonicalManifestPayload {
            format_version: self.format_version,
            snapshot_id: &self.snapshot_id,
            index_uuid: self.index_uuid.as_deref(),
            shard_id: self.shard_id,
            allocation_id: self.allocation_id,
            primary_term: self.primary_term,
            created_at_epoch_ms: self.created_at_epoch_ms,
            schema_hash: &self.schema_hash,
            committed_boundary: &self.committed_boundary,
            snapshot_cursor: self.snapshot_cursor,
            vector_state: &self.vector_state,
            files: &sorted_files,
        };
        let bytes = serde_json::to_vec(&canonical)?;
        Ok(hex_sha256(&bytes))
    }

    pub fn seal(&mut self) -> Result<()> {
        self.files.sort_by(|left, right| left.name.cmp(&right.name));
        self.manifest_checksum_sha256 = self.compute_checksum()?;
        self.validate_structure()
    }

    pub fn validate_structure(&self) -> Result<()> {
        if !SHARD_SNAPSHOT_FORMAT_CAPABILITY.supports_read(self.format_version) {
            return Err(crate::common::unsupported_index_format(
                "shard snapshot manifest",
                format!(
                    "version {} is not supported; supported read range is {}..={}",
                    self.format_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.min_read_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        if self.snapshot_id.trim().is_empty() {
            anyhow::bail!("shard snapshot manifest has an empty snapshot_id");
        }
        if self
            .index_uuid
            .as_deref()
            .is_some_and(|uuid| uuid.trim().is_empty())
        {
            anyhow::bail!("shard snapshot manifest has an empty index_uuid");
        }
        if self.allocation_id == Some(0) {
            anyhow::bail!("shard snapshot manifest has a zero allocation_id");
        }
        if self.primary_term == 0 {
            anyhow::bail!("shard snapshot manifest has a zero primary_term");
        }
        if self.schema_hash.len() != 64
            || !self
                .schema_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            anyhow::bail!("shard snapshot manifest has an invalid schema_hash");
        }
        self.committed_boundary.validate()?;
        if self.committed_boundary.term_sequence_state.current_term != self.primary_term {
            anyhow::bail!(
                "shard snapshot manifest primary_term {} disagrees with committed boundary term {}",
                self.primary_term,
                self.committed_boundary.term_sequence_state.current_term
            );
        }
        if self.files.is_empty() {
            anyhow::bail!("shard snapshot manifest must contain at least one file");
        }
        let mut seen = HashSet::with_capacity(self.files.len());
        for file in &self.files {
            if file.name.is_empty()
                || file.name == SHARD_SNAPSHOT_MANIFEST_FILE
                || file.name == SHARD_SNAPSHOT_COMPLETE_FILE
            {
                anyhow::bail!("shard snapshot manifest contains an invalid file entry");
            }
            if !seen.insert(file.name.as_str()) {
                anyhow::bail!(
                    "shard snapshot manifest contains duplicate file entry '{}'",
                    file.name
                );
            }
            if file.sha256.len() != 64
                || !file
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                anyhow::bail!(
                    "shard snapshot manifest file '{}' has an invalid SHA-256 digest",
                    file.name
                );
            }
        }
        self.vector_state.validate(&self.files)?;
        let expected_checksum = self.compute_checksum()?;
        if self.manifest_checksum_sha256 != expected_checksum {
            anyhow::bail!(
                "shard snapshot manifest checksum mismatch: stored {}, computed {expected_checksum}",
                self.manifest_checksum_sha256
            );
        }
        Ok(())
    }

    pub fn persist_atomically(&self, snapshot_dir: &Path) -> Result<ShardSnapshotCompletionMarker> {
        self.validate_structure()?;
        std::fs::create_dir_all(snapshot_dir)?;

        let manifest_path = snapshot_dir.join(SHARD_SNAPSHOT_MANIFEST_FILE);
        let manifest_tmp = snapshot_dir.join(format!("{SHARD_SNAPSHOT_MANIFEST_FILE}.tmp"));
        let manifest_bytes = serde_json::to_vec_pretty(self)?;
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&manifest_tmp)?;
            file.write_all(&manifest_bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&manifest_tmp, &manifest_path)?;

        let marker = ShardSnapshotCompletionMarker {
            format_version: self.format_version,
            snapshot_id: self.snapshot_id.clone(),
            manifest_checksum_sha256: self.manifest_checksum_sha256.clone(),
            completed_at_epoch_ms: now_epoch_ms(),
        };
        let complete_path = snapshot_dir.join(SHARD_SNAPSHOT_COMPLETE_FILE);
        let complete_tmp = snapshot_dir.join(format!("{SHARD_SNAPSHOT_COMPLETE_FILE}.tmp"));
        let marker_bytes = serde_json::to_vec_pretty(&marker)?;
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&complete_tmp)?;
            file.write_all(&marker_bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&complete_tmp, &complete_path)?;
        File::open(snapshot_dir)?.sync_all()?;
        Ok(marker)
    }

    pub fn load_and_verify(snapshot_dir: &Path) -> Result<Self> {
        let complete_path = snapshot_dir.join(SHARD_SNAPSHOT_COMPLETE_FILE);
        if !complete_path.try_exists()? {
            anyhow::bail!(
                "shard snapshot at {snapshot_dir:?} is incomplete: missing {SHARD_SNAPSHOT_COMPLETE_FILE}"
            );
        }
        let complete_bytes = std::fs::read(&complete_path)
            .with_context(|| format!("read snapshot completion marker {complete_path:?}"))?;
        let complete_header: VersionHeader =
            serde_json::from_slice(&complete_bytes).map_err(|error| {
                anyhow::anyhow!("decode snapshot completion marker {complete_path:?}: {error}")
            })?;
        if !SHARD_SNAPSHOT_FORMAT_CAPABILITY.supports_read(complete_header.format_version) {
            return Err(crate::common::unsupported_index_format(
                "shard snapshot completion marker",
                format!(
                    "version {} is not supported; supported read range is {}..={}",
                    complete_header.format_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.min_read_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        let marker: ShardSnapshotCompletionMarker = serde_json::from_slice(&complete_bytes)
            .with_context(|| format!("decode snapshot completion marker {complete_path:?}"))?;

        let manifest_path = snapshot_dir.join(SHARD_SNAPSHOT_MANIFEST_FILE);
        let manifest_bytes = std::fs::read(&manifest_path)
            .with_context(|| format!("read shard snapshot manifest {manifest_path:?}"))?;
        let header: VersionHeader = serde_json::from_slice(&manifest_bytes).map_err(|error| {
            anyhow::anyhow!("decode shard snapshot manifest header {manifest_path:?}: {error}")
        })?;
        if !SHARD_SNAPSHOT_FORMAT_CAPABILITY.supports_read(header.format_version) {
            return Err(crate::common::unsupported_index_format(
                "shard snapshot manifest",
                format!(
                    "version {} is not supported; supported read range is {}..={}",
                    header.format_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.min_read_version,
                    SHARD_SNAPSHOT_FORMAT_CAPABILITY.max_read_version
                ),
            ));
        }
        let manifest: Self = serde_json::from_slice(&manifest_bytes)
            .with_context(|| format!("decode shard snapshot manifest {manifest_path:?}"))?;
        manifest.validate_structure()?;
        if marker.snapshot_id != manifest.snapshot_id
            || marker.manifest_checksum_sha256 != manifest.manifest_checksum_sha256
        {
            anyhow::bail!(
                "shard snapshot completion marker at {complete_path:?} does not match manifest {}",
                manifest.snapshot_id
            );
        }

        for file in &manifest.files {
            let file_path = snapshot_dir.join(&file.name);
            if !file_path.try_exists()? {
                anyhow::bail!(
                    "shard snapshot at {snapshot_dir:?} is missing declared file '{}'",
                    file.name
                );
            }
            let (actual_len, actual_sha256) = hash_file_sha256(&file_path)?;
            if actual_len != file.length {
                anyhow::bail!(
                    "shard snapshot file '{}' length mismatch: expected {}, found {actual_len}",
                    file.name,
                    file.length
                );
            }
            if actual_sha256 != file.sha256 {
                anyhow::bail!(
                    "shard snapshot file '{}' checksum mismatch: expected {}, found {actual_sha256}",
                    file.name,
                    file.sha256
                );
            }
        }
        Ok(manifest)
    }
}
