use super::{Backend, StorageManager, hex_lower};
use object_store::{
    ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const COMPARE_TOKEN_VERSION: &str = "fs1";
const COMPARE_TOKEN_DOMAIN_LOCAL: &str = "ferris-compare-token-v1:local";
const COMPARE_TOKEN_DOMAIN_S3: &str = "ferris-compare-token-v1:s3";
const LOCAL_STATE_FORMAT_VERSION: u32 = 1;
const CAPABILITY_NAME: &str = "conditional_put_and_cas";

/// Opaque compare token returned by [`StorageManager::read_with_token`],
/// [`StorageManager::create_immutable`], and [`StorageManager::compare_and_swap`].
///
/// Tokens bind backend-specific version identity and a cryptographic integrity
/// tag so malformed, tampered, or cross-backend tokens fail closed before any
/// mutation occurs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompareToken(String);

impl CompareToken {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn local(revision: u64, inode: u64, size_bytes: u64, sha256_hex: &str) -> Self {
        let mac = compute_token_mac(&format!(
            "{COMPARE_TOKEN_DOMAIN_LOCAL}:{revision}:{inode}:{size_bytes}:{sha256_hex}"
        ));
        Self(format!(
            "{COMPARE_TOKEN_VERSION}:local:{revision}:{inode}:{size_bytes}:{sha256_hex}:{mac}"
        ))
    }

    pub(crate) fn s3(e_tag: &str, version: Option<&str>) -> Self {
        let etag_hex = hex_lower(e_tag.as_bytes());
        let version_hex = match version {
            Some(v) if !v.is_empty() => hex_lower(v.as_bytes()),
            _ => "-".to_string(),
        };
        let mac = compute_token_mac(&format!(
            "{COMPARE_TOKEN_DOMAIN_S3}:{etag_hex}:{version_hex}"
        ));
        Self(format!(
            "{COMPARE_TOKEN_VERSION}:s3:{etag_hex}:{version_hex}:{mac}"
        ))
    }

    fn parse_local(&self, path: &ObjectPath) -> Result<LocalVersionSlot, ConditionalStorageError> {
        match self.parse_inner(path)? {
            ParsedCompareToken::Local(slot) => Ok(slot),
            ParsedCompareToken::S3 { .. } => Err(ConditionalStorageError::InvalidToken {
                path: path.to_string(),
                reason: "compare token is for an S3 backend, not a local filesystem backend"
                    .to_string(),
            }),
        }
    }

    fn parse_s3(
        &self,
        path: &ObjectPath,
    ) -> Result<(String, Option<String>), ConditionalStorageError> {
        match self.parse_inner(path)? {
            ParsedCompareToken::S3 { e_tag, version } => Ok((e_tag, version)),
            ParsedCompareToken::Local(_) => Err(ConditionalStorageError::InvalidToken {
                path: path.to_string(),
                reason: "compare token is for a local filesystem backend, not an S3 backend"
                    .to_string(),
            }),
        }
    }

    fn parse_inner(
        &self,
        path: &ObjectPath,
    ) -> Result<ParsedCompareToken, ConditionalStorageError> {
        let invalid = |reason: &str| ConditionalStorageError::InvalidToken {
            path: path.to_string(),
            reason: reason.to_string(),
        };
        if self.0.is_empty() {
            return Err(invalid("compare token must not be empty"));
        }
        let parts: Vec<&str> = self.0.split(':').collect();
        if parts.first().copied() != Some(COMPARE_TOKEN_VERSION) {
            return Err(invalid("unsupported or malformed compare token prefix"));
        }
        match parts.get(1).copied() {
            Some("local") => {
                if parts.len() != 7 {
                    return Err(invalid("malformed local compare token field count"));
                }
                let revision: u64 = parts[2]
                    .parse()
                    .map_err(|_| invalid("invalid local compare token revision"))?;
                if revision == 0 {
                    return Err(invalid("local compare token revision must be non-zero"));
                }
                let inode: u64 = parts[3]
                    .parse()
                    .map_err(|_| invalid("invalid local compare token inode"))?;
                let size_bytes: u64 = parts[4]
                    .parse()
                    .map_err(|_| invalid("invalid local compare token size"))?;
                let sha256 = parts[5];
                if !is_valid_sha256_hex(sha256) {
                    return Err(invalid("invalid local compare token sha256 digest"));
                }
                let mac = parts[6];
                let expected_mac = compute_token_mac(&format!(
                    "{COMPARE_TOKEN_DOMAIN_LOCAL}:{revision}:{inode}:{size_bytes}:{sha256}"
                ));
                if mac != expected_mac {
                    return Err(invalid("local compare token integrity check failed"));
                }
                Ok(ParsedCompareToken::Local(LocalVersionSlot {
                    revision,
                    inode,
                    size_bytes,
                    sha256: sha256.to_string(),
                }))
            }
            Some("s3") => {
                if parts.len() != 5 {
                    return Err(invalid("malformed S3 compare token field count"));
                }
                let etag_hex = parts[2];
                let version_hex = parts[3];
                let mac = parts[4];
                if etag_hex.is_empty() {
                    return Err(invalid("S3 compare token ETag must not be empty"));
                }
                let expected_mac = compute_token_mac(&format!(
                    "{COMPARE_TOKEN_DOMAIN_S3}:{etag_hex}:{version_hex}"
                ));
                if mac != expected_mac {
                    return Err(invalid("S3 compare token integrity check failed"));
                }
                let e_tag_bytes = decode_hex_lower(etag_hex)
                    .ok_or_else(|| invalid("invalid hex encoding in S3 compare token ETag"))?;
                let e_tag = String::from_utf8(e_tag_bytes)
                    .map_err(|_| invalid("non-UTF-8 S3 compare token ETag"))?;
                if e_tag.trim().is_empty() {
                    return Err(invalid("S3 compare token ETag must not be blank"));
                }
                let version = if version_hex == "-" {
                    None
                } else {
                    let version_bytes = decode_hex_lower(version_hex).ok_or_else(|| {
                        invalid("invalid hex encoding in S3 compare token version")
                    })?;
                    let version_str = String::from_utf8(version_bytes)
                        .map_err(|_| invalid("non-UTF-8 S3 compare token version"))?;
                    if version_str.is_empty() {
                        return Err(invalid("empty S3 compare token version"));
                    }
                    Some(version_str)
                };
                Ok(ParsedCompareToken::S3 { e_tag, version })
            }
            _ => Err(invalid("unknown compare token backend scheme")),
        }
    }
}

impl std::fmt::Display for CompareToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

enum ParsedCompareToken {
    Local(LocalVersionSlot),
    S3 {
        e_tag: String,
        version: Option<String>,
    },
}

/// Verified identity of an immutable object stored via [`StorageManager::create_immutable`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImmutableObjectIdentity {
    /// Content digest in `"sha256:<64 lowercase hex>"` format.
    pub sha256: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// Compare token observed for the stored object.
    pub token: CompareToken,
    /// True when the stored object is byte-identical to the caller's payload.
    pub content_matches: bool,
}

/// Result of [`StorageManager::create_immutable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateImmutableOutcome {
    /// The object did not previously exist and was atomically created.
    Created { identity: ImmutableObjectIdentity },
    /// An object already existed at the target path; its identity was read and verified.
    AlreadyExists { existing: ImmutableObjectIdentity },
}

impl CreateImmutableOutcome {
    pub fn identity(&self) -> &ImmutableObjectIdentity {
        match self {
            Self::Created { identity } => identity,
            Self::AlreadyExists { existing } => existing,
        }
    }

    /// Enforces ADR 0002's immutable-object rule: a duplicate create succeeds
    /// only when the existing object is byte-equivalent; different content is
    /// rejected as [`ConditionalStorageError::ImmutableConflict`].
    pub fn ensure_matching_content(
        &self,
        location: &ObjectPath,
    ) -> Result<&ImmutableObjectIdentity, ConditionalStorageError> {
        match self {
            Self::Created { identity } => Ok(identity),
            Self::AlreadyExists { existing } if existing.content_matches => Ok(existing),
            Self::AlreadyExists { existing } => Err(ConditionalStorageError::ImmutableConflict {
                path: location.to_string(),
                actual_sha256: existing.sha256.clone(),
                actual_size_bytes: existing.size_bytes,
            }),
        }
    }
}

/// Payload and opaque compare token returned by [`StorageManager::read_with_token`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionalRead {
    pub bytes: Vec<u8>,
    pub token: CompareToken,
}

/// Result of [`StorageManager::compare_and_swap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareAndSwapOutcome {
    /// The expected condition matched and the replacement is now durable and readable.
    Swapped { token: CompareToken },
    /// The object was modified (or missing/present contrary to expectation); no write occurred.
    Conflict,
}

/// Diagnostic report describing whether the storage backend supports
/// conditional manifest publication (`create_immutable` + `compare_and_swap`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationCapabilityStatus {
    pub supported: bool,
    pub capability: String,
    pub backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl PublicationCapabilityStatus {
    pub(crate) fn supported(backend: impl Into<String>) -> Self {
        Self {
            supported: true,
            capability: CAPABILITY_NAME.to_string(),
            backend: backend.into(),
            reason: None,
        }
    }

    pub(crate) fn unsupported(backend: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            supported: false,
            capability: CAPABILITY_NAME.to_string(),
            backend: backend.into(),
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CapabilityProbeState {
    Unprobed,
    Supported,
    Unsupported(String),
}

/// Errors produced by conditional object-store operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConditionalStorageError {
    #[error(
        "remote_store publication capability [{CAPABILITY_NAME}] is unavailable on backend [{backend}]: {reason}"
    )]
    UnsupportedBackend { backend: String, reason: String },

    #[error("invalid or unverifiable compare token for [{path}]: {reason}")]
    InvalidToken { path: String, reason: String },

    #[error("conditional compare-and-swap conflict at [{path}]: {reason}")]
    Conflict { path: String, reason: String },

    #[error(
        "immutable object [{path}] already exists with different content (existing sha256={actual_sha256}, size={actual_size_bytes})"
    )]
    ImmutableConflict {
        path: String,
        actual_sha256: String,
        actual_size_bytes: u64,
    },

    #[error("corrupt conditional storage state at [{path}]: {reason}")]
    CorruptState { path: String, reason: String },

    #[error("indeterminate conditional storage outcome at [{path}]: {reason}")]
    Indeterminate { path: String, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LocalVersionSlot {
    revision: u64,
    inode: u64,
    size_bytes: u64,
    sha256: String,
}

impl LocalVersionSlot {
    fn matches(&self, inode: u64, size_bytes: u64, sha256_hex: &str) -> bool {
        self.inode == inode && self.size_bytes == size_bytes && self.sha256 == sha256_hex
    }

    fn to_token(&self) -> CompareToken {
        CompareToken::local(self.revision, self.inode, self.size_bytes, &self.sha256)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct LocalCasState {
    max_revision: u64,
    committed: Option<LocalVersionSlot>,
    pending: Option<LocalVersionSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalStateEnvelope {
    version: u32,
    state: LocalCasState,
    checksum: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalCasCrashStage {
    LockAcquire,
    TempWriteAndSync,
    PendingStatePersist,
    TargetRenameAndSync,
}

fn compute_token_mac(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex_lower(&hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

fn is_valid_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn decode_hex_lower(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().as_chunks::<2>().0 {
        let hi = decode_nibble(chunk[0])?;
        let lo = decode_nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn payload_to_vec(payload: PutPayload) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.content_length());
    for chunk in &payload {
        out.extend_from_slice(chunk);
    }
    out
}

/// Companion paths for a target file on the local filesystem backend.
///
/// All companion files carry a `#0` suffix so `object_store::local::LocalFileSystem`
/// ignores them in `list` and `list_with_delimiter` (`is_valid_file_path` skips
/// `#<digits>` staging names).
struct LocalCompanionPaths {
    parent_dir: PathBuf,
    lock_path: PathBuf,
    state_path: PathBuf,
    state_tmp_path: PathBuf,
    cas_tmp_path: PathBuf,
}

fn companion_paths(
    target_path: &Path,
    location: &ObjectPath,
) -> Result<LocalCompanionPaths, ConditionalStorageError> {
    let parent_dir = target_path
        .parent()
        .ok_or_else(|| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!("target path {target_path:?} has no parent directory"),
        })?
        .to_path_buf();
    let raw_name = target_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!("target path {target_path:?} has no valid UTF-8 filename"),
        })?;
    let sanitized = raw_name.replace('#', "_");
    Ok(LocalCompanionPaths {
        lock_path: parent_dir.join(format!(".{sanitized}.ferris.lock#0")),
        state_path: parent_dir.join(format!(".{sanitized}.ferris.state#0")),
        state_tmp_path: parent_dir.join(format!(".{sanitized}.ferris.state.tmp#0")),
        cas_tmp_path: parent_dir.join(format!(".{sanitized}.ferris.cas.tmp#0")),
        parent_dir,
    })
}

fn sync_directory(dir: &Path, location: &ObjectPath) -> Result<(), ConditionalStorageError> {
    let dir_file = File::open(dir).map_err(|error| ConditionalStorageError::Indeterminate {
        path: location.to_string(),
        reason: format!("failed to open parent directory {dir:?} for fsync: {error}"),
    })?;
    dir_file
        .sync_all()
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!("failed to fsync parent directory {dir:?}: {error}"),
        })
}

fn compute_state_checksum(version: u32, state: &LocalCasState) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(version, state))
        .map_err(|error| format!("failed to serialize local CAS state for checksum: {error}"))?;
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

fn load_local_state(
    state_path: &Path,
    location: &ObjectPath,
) -> Result<LocalCasState, ConditionalStorageError> {
    let bytes = match std::fs::read(state_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LocalCasState::default());
        }
        Err(error) => {
            return Err(ConditionalStorageError::Indeterminate {
                path: location.to_string(),
                reason: format!("failed to read local CAS state {state_path:?}: {error}"),
            });
        }
    };
    let envelope: LocalStateEnvelope =
        serde_json::from_slice(&bytes).map_err(|error| ConditionalStorageError::CorruptState {
            path: location.to_string(),
            reason: format!("cannot decode local CAS state {state_path:?}: {error}"),
        })?;
    if envelope.version != LOCAL_STATE_FORMAT_VERSION {
        return Err(ConditionalStorageError::CorruptState {
            path: location.to_string(),
            reason: format!(
                "unsupported local CAS state version {} at {state_path:?}; expected {}",
                envelope.version, LOCAL_STATE_FORMAT_VERSION
            ),
        });
    }
    let expected_checksum =
        compute_state_checksum(envelope.version, &envelope.state).map_err(|reason| {
            ConditionalStorageError::CorruptState {
                path: location.to_string(),
                reason,
            }
        })?;
    if envelope.checksum != expected_checksum {
        return Err(ConditionalStorageError::CorruptState {
            path: location.to_string(),
            reason: format!(
                "local CAS state checksum mismatch at {state_path:?}: expected {expected_checksum}, found {}",
                envelope.checksum
            ),
        });
    }
    Ok(envelope.state)
}

fn persist_local_state(
    companions: &LocalCompanionPaths,
    state: &LocalCasState,
    location: &ObjectPath,
) -> Result<(), ConditionalStorageError> {
    let checksum = compute_state_checksum(LOCAL_STATE_FORMAT_VERSION, state).map_err(|reason| {
        ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason,
        }
    })?;
    let envelope = LocalStateEnvelope {
        version: LOCAL_STATE_FORMAT_VERSION,
        state: state.clone(),
        checksum,
    };
    let encoded =
        serde_json::to_vec(&envelope).map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!("failed to serialize local CAS state: {error}"),
        })?;
    let mut temp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&companions.state_tmp_path)
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to open local CAS state temp file {:?}: {error}",
                companions.state_tmp_path
            ),
        })?;
    temp_file
        .write_all(&encoded)
        .and_then(|()| temp_file.sync_all())
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to write and fsync local CAS state temp file {:?}: {error}",
                companions.state_tmp_path
            ),
        })?;
    drop(temp_file);
    std::fs::rename(&companions.state_tmp_path, &companions.state_path).map_err(|error| {
        ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to atomically replace local CAS state {:?}: {error}",
                companions.state_path
            ),
        }
    })?;
    sync_directory(&companions.parent_dir, location)
}

fn acquire_local_lock(
    companions: &LocalCompanionPaths,
    location: &ObjectPath,
) -> Result<File, ConditionalStorageError> {
    std::fs::create_dir_all(&companions.parent_dir).map_err(|error| {
        ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to create parent directory {:?}: {error}",
                companions.parent_dir
            ),
        }
    })?;
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&companions.lock_path)
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to open lock file {:?}: {error}",
                companions.lock_path
            ),
        })?;
    lock_file
        .lock()
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to acquire exclusive OS lock on {:?}: {error}",
                companions.lock_path
            ),
        })?;
    Ok(lock_file)
}

type ReconciledLocalTarget = (LocalCasState, Option<(Vec<u8>, LocalVersionSlot)>);

/// Reads the current target file (if present) and reconciles `.ferris.state#0`
/// while holding the exclusive OS lock.
fn read_and_reconcile_locked(
    target_path: &Path,
    companions: &LocalCompanionPaths,
    location: &ObjectPath,
) -> Result<ReconciledLocalTarget, ConditionalStorageError> {
    let mut state = load_local_state(&companions.state_path, location)?;
    let _ = std::fs::remove_file(&companions.cas_tmp_path);
    let _ = std::fs::remove_file(&companions.state_tmp_path);

    let mut target_file = match File::open(target_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if state.committed.is_some() || state.pending.is_some() {
                state.committed = None;
                state.pending = None;
                persist_local_state(companions, &state, location)?;
            }
            return Ok((state, None));
        }
        Err(error) => {
            return Err(ConditionalStorageError::Indeterminate {
                path: location.to_string(),
                reason: format!("failed to open target file {target_path:?}: {error}"),
            });
        }
    };

    let metadata =
        target_file
            .metadata()
            .map_err(|error| ConditionalStorageError::Indeterminate {
                path: location.to_string(),
                reason: format!("failed to read metadata for {target_path:?}: {error}"),
            })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    target_file.read_to_end(&mut bytes).map_err(|error| {
        ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!("failed to read target file {target_path:?}: {error}"),
        }
    })?;

    let actual_inode = metadata.ino();
    let actual_size = bytes.len() as u64;
    let actual_sha256 = sha256_hex(&bytes);

    let resolved_slot = if let Some(committed) = state.committed.as_ref()
        && committed.matches(actual_inode, actual_size, &actual_sha256)
    {
        let slot = committed.clone();
        if state.pending.is_some() {
            state.pending = None;
            persist_local_state(companions, &state, location)?;
        }
        slot
    } else if let Some(pending) = state.pending.as_ref()
        && pending.matches(actual_inode, actual_size, &actual_sha256)
    {
        let slot = pending.clone();
        state.max_revision = state.max_revision.max(slot.revision);
        state.committed = Some(slot.clone());
        state.pending = None;
        persist_local_state(companions, &state, location)?;
        slot
    } else {
        let next_revision = state.max_revision + 1;
        let slot = LocalVersionSlot {
            revision: next_revision,
            inode: actual_inode,
            size_bytes: actual_size,
            sha256: actual_sha256,
        };
        state.max_revision = next_revision;
        state.committed = Some(slot.clone());
        state.pending = None;
        persist_local_state(companions, &state, location)?;
        slot
    };

    Ok((state, Some((bytes, resolved_slot))))
}

/// Writes `bytes` to `cas_tmp_path`, fsyncs, records `pending` in `.ferris.state#0`,
/// atomically installs `target_path`, fsyncs `parent_dir`, and promotes `pending`
/// to `committed`.
fn write_and_commit_locked(
    target_path: &Path,
    companions: &LocalCompanionPaths,
    state: &mut LocalCasState,
    bytes: &[u8],
    create_only: bool,
    location: &ObjectPath,
    #[cfg(test)] crash_stage: Option<LocalCasCrashStage>,
) -> Result<LocalVersionSlot, ConditionalStorageError> {
    #[cfg(test)]
    if crash_stage == Some(LocalCasCrashStage::LockAcquire) {
        return Err(ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: "injected crash after lock acquisition".to_string(),
        });
    }

    let mut temp_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&companions.cas_tmp_path)
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to open staging file {:?}: {error}",
                companions.cas_tmp_path
            ),
        })?;
    temp_file
        .write_all(bytes)
        .and_then(|()| temp_file.sync_all())
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to write and fsync staging file {:?}: {error}",
                companions.cas_tmp_path
            ),
        })?;
    let temp_inode = temp_file
        .metadata()
        .map_err(|error| ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: format!(
                "failed to stat staging file {:?}: {error}",
                companions.cas_tmp_path
            ),
        })?
        .ino();
    drop(temp_file);

    #[cfg(test)]
    if crash_stage == Some(LocalCasCrashStage::TempWriteAndSync) {
        return Err(ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: "injected crash after staging write and fsync".to_string(),
        });
    }

    let next_revision = state.max_revision + 1;
    let new_slot = LocalVersionSlot {
        revision: next_revision,
        inode: temp_inode,
        size_bytes: bytes.len() as u64,
        sha256: sha256_hex(bytes),
    };
    state.max_revision = next_revision;
    state.pending = Some(new_slot.clone());
    #[cfg(test)]
    if crash_stage.is_some() {
        persist_local_state(companions, state, location)?;
    }

    #[cfg(test)]
    if crash_stage == Some(LocalCasCrashStage::PendingStatePersist) {
        return Err(ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: "injected crash after pending CAS state persist".to_string(),
        });
    }

    if create_only {
        match std::fs::hard_link(&companions.cas_tmp_path, target_path) {
            Ok(()) => {
                let _ = std::fs::remove_file(&companions.cas_tmp_path);
            }
            Err(error) => {
                let _ = std::fs::remove_file(&companions.cas_tmp_path);
                return Err(ConditionalStorageError::Indeterminate {
                    path: location.to_string(),
                    reason: format!(
                        "failed to link immutable staging file {:?} to {target_path:?}: {error}",
                        companions.cas_tmp_path
                    ),
                });
            }
        }
    } else {
        std::fs::rename(&companions.cas_tmp_path, target_path).map_err(|error| {
            let _ = std::fs::remove_file(&companions.cas_tmp_path);
            ConditionalStorageError::Indeterminate {
                path: location.to_string(),
                reason: format!(
                    "failed to atomically rename {:?} over {target_path:?}: {error}",
                    companions.cas_tmp_path
                ),
            }
        })?;
    }

    #[cfg(test)]
    if crash_stage == Some(LocalCasCrashStage::TargetRenameAndSync) {
        sync_directory(&companions.parent_dir, location)?;
        return Err(ConditionalStorageError::Indeterminate {
            path: location.to_string(),
            reason: "injected crash after target rename and directory fsync".to_string(),
        });
    }

    state.committed = state.pending.take();
    persist_local_state(companions, state, location)?;
    Ok(new_slot)
}

const UNCERTIFIED_NETWORK_FS_TYPES: &[&str] = &[
    "nfs",
    "nfs4",
    "cifs",
    "smb3",
    "smbfs",
    "ceph",
    "glusterfs",
    "fuse.glusterfs",
    "fuse.sshfs",
    "lustre",
    "gfs2",
    "ocfs2",
    "afs",
    "ncpfs",
    "9p",
];

pub(crate) fn is_uncertified_network_fstype(fs_type: &str) -> bool {
    let lower = fs_type.trim().to_ascii_lowercase();
    UNCERTIFIED_NETWORK_FS_TYPES.contains(&lower.as_str())
}

pub(crate) fn detect_local_mount_fstype(path: &Path) -> Option<String> {
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    detect_mount_fstype_from_proc_mounts(path, &mounts)
}

pub(crate) fn detect_mount_fstype_from_proc_mounts(path: &Path, mounts: &str) -> Option<String> {
    let mut best_match: Option<(usize, String)> = None;
    for line in mounts.lines() {
        let mut fields = line.split_whitespace();
        let _device = fields.next()?;
        let mount_point_raw = fields.next()?;
        let fs_type = fields.next()?;
        let mount_point = PathBuf::from(unescape_mount_path(mount_point_raw));
        if path.starts_with(&mount_point) {
            let depth = mount_point.as_os_str().len();
            if best_match
                .as_ref()
                .is_none_or(|(best_depth, _)| depth >= *best_depth)
            {
                best_match = Some((depth, fs_type.to_string()));
            }
        }
    }
    best_match.map(|(_, fs_type)| fs_type)
}

fn unescape_mount_path(raw: &str) -> String {
    raw.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

impl StorageManager {
    /// Returns the currently known conditional-publication capability status
    /// without performing network I/O.
    pub fn publication_capability_status(&self) -> PublicationCapabilityStatus {
        let guard = self
            .capability_state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match &*guard {
            CapabilityProbeState::Supported => PublicationCapabilityStatus::supported(self.uri()),
            CapabilityProbeState::Unsupported(reason) => {
                PublicationCapabilityStatus::unsupported(self.uri(), reason.clone())
            }
            CapabilityProbeState::Unprobed => PublicationCapabilityStatus::supported(self.uri()),
        }
    }

    /// Runs (or returns the cached result of) the isolated conditional-storage
    /// capability probe required by ADR 0002 section 3.6.
    pub async fn probe_publication_capability(&self) -> PublicationCapabilityStatus {
        match self.ensure_publication_supported().await {
            Ok(()) => PublicationCapabilityStatus::supported(self.uri()),
            Err(ConditionalStorageError::UnsupportedBackend { backend, reason }) => {
                PublicationCapabilityStatus::unsupported(backend, reason)
            }
            Err(other) => PublicationCapabilityStatus::unsupported(self.uri(), other.to_string()),
        }
    }

    /// Ensures the active backend satisfies the conditional-storage contract
    /// (`create_immutable`, `read_with_token`, and `compare_and_swap`).
    ///
    /// Fails closed with [`ConditionalStorageError::UnsupportedBackend`] when
    /// the backend is uncertified or fails the isolated capability probe.
    pub async fn ensure_publication_supported(&self) -> Result<(), ConditionalStorageError> {
        {
            let guard = self
                .capability_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match &*guard {
                CapabilityProbeState::Supported => return Ok(()),
                CapabilityProbeState::Unsupported(reason) => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: reason.clone(),
                    });
                }
                CapabilityProbeState::Unprobed => {}
            }
        }

        let _probe_guard = self.capability_probe_lock.lock().await;
        {
            let guard = self
                .capability_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match &*guard {
                CapabilityProbeState::Supported => return Ok(()),
                CapabilityProbeState::Unsupported(reason) => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: reason.clone(),
                    });
                }
                CapabilityProbeState::Unprobed => {}
            }
        }

        static LOCAL_FS_CAPABILITY_VERIFIED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        #[cfg(test)]
        let no_injected_crash = self
            .local_cas_crash_stage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none();
        #[cfg(not(test))]
        let no_injected_crash = true;

        if self.is_local()
            && no_injected_crash
            && LOCAL_FS_CAPABILITY_VERIFIED.load(std::sync::atomic::Ordering::Acquire)
        {
            let mut guard = self
                .capability_state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = CapabilityProbeState::Supported;
            return Ok(());
        }

        match self.run_isolated_capability_probe().await {
            Ok(()) => {
                if self.is_local() && no_injected_crash {
                    LOCAL_FS_CAPABILITY_VERIFIED.store(true, std::sync::atomic::Ordering::Release);
                }
                let mut guard = self
                    .capability_state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *guard = CapabilityProbeState::Supported;
                Ok(())
            }
            Err(ConditionalStorageError::UnsupportedBackend { backend, reason }) => {
                let mut guard = self
                    .capability_state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *guard = CapabilityProbeState::Unsupported(reason.clone());
                Err(ConditionalStorageError::UnsupportedBackend { backend, reason })
            }
            Err(other) => Err(other),
        }
    }

    async fn run_isolated_capability_probe(&self) -> Result<(), ConditionalStorageError> {
        let probe_id = Uuid::new_v4();
        let probe_key = format!("_ferris_capability_probe/{probe_id}/pointer.json");
        let probe_path = self.object_path(&probe_key).map_err(|error| {
            ConditionalStorageError::UnsupportedBackend {
                backend: self.uri(),
                reason: format!("invalid capability probe path: {error}"),
            }
        })?;

        let v1_bytes = b"{\"probe\":1}".to_vec();
        let v1_diff_bytes = b"{\"probe\":999}".to_vec();
        let v2_bytes = b"{\"probe\":2}".to_vec();
        let stale_bytes = b"{\"probe\":3}".to_vec();

        let probe_result = async {
            // 1. Atomic create-if-absent must succeed on a fresh key.
            let create_outcome = self
                .create_immutable_unprobed(&probe_path, v1_bytes.clone().into())
                .await?;
            let created_identity = match create_outcome {
                CreateImmutableOutcome::Created { identity } => identity,
                CreateImmutableOutcome::AlreadyExists { .. } => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: "capability probe create_immutable reported AlreadyExists on a fresh key"
                            .to_string(),
                    });
                }
            };

            // 2. Duplicate create-if-absent with identical bytes must report AlreadyExists(content_matches=true),
            // and with different bytes must report AlreadyExists(content_matches=false).
            match self
                .create_immutable_unprobed(&probe_path, v1_bytes.clone().into())
                .await?
            {
                CreateImmutableOutcome::AlreadyExists { existing } if existing.content_matches => {}
                other => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: format!(
                            "capability probe duplicate create_immutable with identical bytes returned {other:?}"
                        ),
                    });
                }
            }
            match self
                .create_immutable_unprobed(&probe_path, v1_diff_bytes.into())
                .await?
            {
                CreateImmutableOutcome::AlreadyExists { existing } if !existing.content_matches => {}
                other => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: format!(
                            "capability probe duplicate create_immutable with conflicting bytes did not reject overwrite: {other:?}"
                        ),
                    });
                }
            }

            // 3. Immediate readback must return v1 bytes and a verifiable compare token.
            let read_v1 = self
                .read_with_token_unprobed(&probe_path)
                .await?
                .ok_or_else(|| ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe read_with_token returned None immediately after create"
                        .to_string(),
                })?;
            if read_v1.bytes != v1_bytes {
                return Err(ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe readback bytes did not match created payload"
                        .to_string(),
                });
            }
            if read_v1.token != created_identity.token {
                return Err(ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe readback compare token did not match created token"
                        .to_string(),
                });
            }

            // 4. Conditional replacement with the observed token must succeed and advance the token.
            let swapped_token = match self
                .compare_and_swap_optional_unprobed(
                    &probe_path,
                    Some(&read_v1.token),
                    v2_bytes.clone().into(),
                )
                .await?
            {
                CompareAndSwapOutcome::Swapped { token } => token,
                CompareAndSwapOutcome::Conflict => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: "capability probe compare_and_swap conflicted with current token"
                            .to_string(),
                    });
                }
            };
            if swapped_token == read_v1.token {
                return Err(ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe compare token did not change after pointer replacement"
                        .to_string(),
                });
            }

            // 5. Stale-token CAS must conflict and leave v2 intact on immediate readback.
            match self
                .compare_and_swap_optional_unprobed(
                    &probe_path,
                    Some(&read_v1.token),
                    stale_bytes.into(),
                )
                .await?
            {
                CompareAndSwapOutcome::Conflict => {}
                CompareAndSwapOutcome::Swapped { .. } => {
                    return Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: "capability probe stale compare token overwrote newer pointer"
                            .to_string(),
                    });
                }
            }
            let read_v2 = self
                .read_with_token_unprobed(&probe_path)
                .await?
                .ok_or_else(|| ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe read_with_token returned None after CAS replacement"
                        .to_string(),
                })?;
            if read_v2.bytes != v2_bytes || read_v2.token != swapped_token {
                return Err(ConditionalStorageError::UnsupportedBackend {
                    backend: self.uri(),
                    reason: "capability probe readback after stale-token conflict did not preserve replacement"
                        .to_string(),
                });
            }

            Ok(())
        }
        .await;

        self.cleanup_capability_probe(&probe_path, &probe_id.to_string())
            .await;
        probe_result
    }

    async fn cleanup_capability_probe(&self, probe_path: &ObjectPath, probe_id: &str) {
        match &self.backend {
            Backend::Local { root, .. } => {
                let probe_root = root.join("_ferris_capability_probe");
                let probe_dir = probe_root.join(probe_id);
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = std::fs::remove_dir_all(&probe_dir);
                    let _ = std::fs::remove_dir(&probe_root);
                })
                .await;
            }
            Backend::Remote { store, .. } => {
                let _ = store.delete(probe_path).await;
            }
        }
    }

    /// Creates an immutable object with atomic create-if-absent semantics.
    ///
    /// If the object already exists, reads and returns its verified identity
    /// (`sha256`, `size_bytes`, `token`, and `content_matches`) without
    /// overwriting it.
    pub async fn create_immutable(
        &self,
        location: &ObjectPath,
        payload: impl Into<PutPayload>,
    ) -> Result<CreateImmutableOutcome, ConditionalStorageError> {
        if let Err(error) = self.ensure_publication_supported().await {
            crate::metrics::record_object_store_request("create_immutable", "unsupported");
            return Err(error);
        }
        let payload = payload.into();
        let content_length = payload.content_length();
        match self.create_immutable_unprobed(location, payload).await {
            Ok(outcome @ CreateImmutableOutcome::Created { .. }) => {
                crate::metrics::record_object_store_request("create_immutable", "created");
                crate::metrics::record_object_store_transfer("upload", content_length);
                Ok(outcome)
            }
            Ok(outcome @ CreateImmutableOutcome::AlreadyExists { .. }) => {
                crate::metrics::record_object_store_request("create_immutable", "already_exists");
                Ok(outcome)
            }
            Err(error @ ConditionalStorageError::UnsupportedBackend { .. }) => {
                crate::metrics::record_object_store_request("create_immutable", "unsupported");
                Err(error)
            }
            Err(error) => {
                crate::metrics::record_object_store_request("create_immutable", "error");
                Err(error)
            }
        }
    }

    /// Reads an object together with its opaque [`CompareToken`].
    ///
    /// Returns `Ok(None)` when the object does not exist. Fails closed if the
    /// backend returns unverifiable metadata or if local CAS state is corrupt.
    pub async fn read_with_token(
        &self,
        location: &ObjectPath,
    ) -> Result<Option<ConditionalRead>, ConditionalStorageError> {
        match self.read_with_token_unprobed(location).await {
            Ok(Some(read)) => {
                crate::metrics::record_object_store_request("read_with_token", "success");
                crate::metrics::record_object_store_transfer("download", read.bytes.len());
                Ok(Some(read))
            }
            Ok(None) => {
                crate::metrics::record_object_store_request("read_with_token", "not_found");
                Ok(None)
            }
            Err(error) => {
                crate::metrics::record_object_store_request("read_with_token", "error");
                Err(error)
            }
        }
    }

    /// Atomically replaces an existing object at `location` if and only if its
    /// current version matches `expected`.
    pub async fn compare_and_swap(
        &self,
        location: &ObjectPath,
        expected: &CompareToken,
        replacement: impl Into<PutPayload>,
    ) -> Result<CompareAndSwapOutcome, ConditionalStorageError> {
        self.compare_and_swap_optional(location, Some(expected), replacement)
            .await
    }

    /// Atomically creates (`expected = None`) or replaces (`expected = Some(token)`)
    /// the object at `location`.
    pub async fn compare_and_swap_optional(
        &self,
        location: &ObjectPath,
        expected: Option<&CompareToken>,
        replacement: impl Into<PutPayload>,
    ) -> Result<CompareAndSwapOutcome, ConditionalStorageError> {
        // Validate token syntax and backend match before capability or I/O work
        // so malformed or cross-backend tokens always fail closed immediately.
        if let Some(token) = expected {
            match &self.backend {
                Backend::Local { .. } => {
                    token.parse_local(location)?;
                }
                Backend::Remote { .. } => {
                    token.parse_s3(location)?;
                }
            }
        }
        if let Err(error) = self.ensure_publication_supported().await {
            crate::metrics::record_object_store_request("compare_and_swap", "unsupported");
            return Err(error);
        }
        let payload = replacement.into();
        let content_length = payload.content_length();
        match self
            .compare_and_swap_optional_unprobed(location, expected, payload)
            .await
        {
            Ok(outcome @ CompareAndSwapOutcome::Swapped { .. }) => {
                crate::metrics::record_object_store_request("compare_and_swap", "swapped");
                crate::metrics::record_object_store_transfer("upload", content_length);
                Ok(outcome)
            }
            Ok(outcome @ CompareAndSwapOutcome::Conflict) => {
                crate::metrics::record_object_store_request("compare_and_swap", "conflict");
                Ok(outcome)
            }
            Err(error @ ConditionalStorageError::InvalidToken { .. }) => {
                crate::metrics::record_object_store_request("compare_and_swap", "invalid_token");
                Err(error)
            }
            Err(error @ ConditionalStorageError::UnsupportedBackend { .. }) => {
                crate::metrics::record_object_store_request("compare_and_swap", "unsupported");
                Err(error)
            }
            Err(error) => {
                crate::metrics::record_object_store_request("compare_and_swap", "error");
                Err(error)
            }
        }
    }

    async fn create_immutable_unprobed(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
    ) -> Result<CreateImmutableOutcome, ConditionalStorageError> {
        let expected_bytes = payload_to_vec(payload.clone());
        let expected_size = expected_bytes.len() as u64;
        let expected_sha256_hex = sha256_hex(&expected_bytes);
        let expected_sha256 = format!("sha256:{expected_sha256_hex}");

        match &self.backend {
            Backend::Local { local, .. } => {
                let target_path = local.path_to_filesystem(location).map_err(|error| {
                    ConditionalStorageError::Indeterminate {
                        path: location.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                let location_owned = location.clone();
                #[cfg(test)]
                let crash_stage = self.take_local_cas_crash_stage_for_test();
                tokio::task::spawn_blocking(move || {
                    let companions = companion_paths(&target_path, &location_owned)?;
                    let _lock_guard = acquire_local_lock(&companions, &location_owned)?;
                    let (mut state, current) =
                        read_and_reconcile_locked(&target_path, &companions, &location_owned)?;
                    if let Some((existing_bytes, existing_slot)) = current {
                        let content_matches = existing_slot.size_bytes == expected_size
                            && existing_slot.sha256 == expected_sha256_hex
                            && existing_bytes == expected_bytes;
                        return Ok(CreateImmutableOutcome::AlreadyExists {
                            existing: ImmutableObjectIdentity {
                                sha256: format!("sha256:{}", existing_slot.sha256),
                                size_bytes: existing_slot.size_bytes,
                                token: existing_slot.to_token(),
                                content_matches,
                            },
                        });
                    }
                    let created_slot = write_and_commit_locked(
                        &target_path,
                        &companions,
                        &mut state,
                        &expected_bytes,
                        true,
                        &location_owned,
                        #[cfg(test)]
                        crash_stage,
                    )?;
                    Ok(CreateImmutableOutcome::Created {
                        identity: ImmutableObjectIdentity {
                            sha256: expected_sha256,
                            size_bytes: expected_size,
                            token: created_slot.to_token(),
                            content_matches: true,
                        },
                    })
                })
                .await
                .map_err(|error| ConditionalStorageError::Indeterminate {
                    path: location.to_string(),
                    reason: format!("local create_immutable worker panicked: {error}"),
                })?
            }
            Backend::Remote { store, uri } => {
                match store
                    .put_opts(location, payload, PutOptions::from(PutMode::Create))
                    .await
                {
                    Ok(put_result) => {
                        let Some(e_tag) = put_result.e_tag.as_deref() else {
                            return Err(ConditionalStorageError::UnsupportedBackend {
                                backend: uri.clone(),
                                reason: "remote store create_immutable succeeded without returning an ETag"
                                    .to_string(),
                            });
                        };
                        if e_tag.trim().is_empty() {
                            return Err(ConditionalStorageError::UnsupportedBackend {
                                backend: uri.clone(),
                                reason: "remote store create_immutable returned an empty ETag"
                                    .to_string(),
                            });
                        }
                        let token = CompareToken::s3(e_tag, put_result.version.as_deref());
                        Ok(CreateImmutableOutcome::Created {
                            identity: ImmutableObjectIdentity {
                                sha256: expected_sha256,
                                size_bytes: expected_size,
                                token,
                                content_matches: true,
                            },
                        })
                    }
                    Err(
                        object_store::Error::AlreadyExists { .. }
                        | object_store::Error::Precondition { .. },
                    ) => {
                        let existing_read = self
                            .read_with_token_unprobed(location)
                            .await?
                            .ok_or_else(|| ConditionalStorageError::Indeterminate {
                                path: location.to_string(),
                                reason: "remote create_immutable reported AlreadyExists, but subsequent readback found no object"
                                    .to_string(),
                            })?;
                        let actual_size = existing_read.bytes.len() as u64;
                        let actual_sha256_hex = sha256_hex(&existing_read.bytes);
                        let content_matches = actual_size == expected_size
                            && actual_sha256_hex == expected_sha256_hex
                            && existing_read.bytes == expected_bytes;
                        Ok(CreateImmutableOutcome::AlreadyExists {
                            existing: ImmutableObjectIdentity {
                                sha256: format!("sha256:{actual_sha256_hex}"),
                                size_bytes: actual_size,
                                token: existing_read.token,
                                content_matches,
                            },
                        })
                    }
                    Err(object_store::Error::NotImplemented { operation, .. }) => {
                        Err(ConditionalStorageError::UnsupportedBackend {
                            backend: uri.clone(),
                            reason: format!(
                                "backend does not implement conditional create ({operation})"
                            ),
                        })
                    }
                    Err(other) => Err(ConditionalStorageError::Indeterminate {
                        path: location.to_string(),
                        reason: other.to_string(),
                    }),
                }
            }
        }
    }

    async fn read_with_token_unprobed(
        &self,
        location: &ObjectPath,
    ) -> Result<Option<ConditionalRead>, ConditionalStorageError> {
        match &self.backend {
            Backend::Local { local, .. } => {
                let target_path = local.path_to_filesystem(location).map_err(|error| {
                    ConditionalStorageError::Indeterminate {
                        path: location.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                let location_owned = location.clone();
                tokio::task::spawn_blocking(move || {
                    let companions = companion_paths(&target_path, &location_owned)?;
                    if !companions.parent_dir.exists() {
                        return Ok(None);
                    }
                    if !target_path.exists()
                        && !companions.state_path.exists()
                        && !companions.lock_path.exists()
                    {
                        return Ok(None);
                    }
                    let _lock_guard = acquire_local_lock(&companions, &location_owned)?;
                    let (_state, current) =
                        read_and_reconcile_locked(&target_path, &companions, &location_owned)?;
                    Ok(current.map(|(bytes, slot)| ConditionalRead {
                        bytes,
                        token: slot.to_token(),
                    }))
                })
                .await
                .map_err(|error| ConditionalStorageError::Indeterminate {
                    path: location.to_string(),
                    reason: format!("local read_with_token worker panicked: {error}"),
                })?
            }
            Backend::Remote { store, .. } => match store.get(location).await {
                Ok(result) => {
                    let meta = result.meta.clone();
                    let bytes = result.bytes().await.map_err(|error| {
                        ConditionalStorageError::Indeterminate {
                            path: location.to_string(),
                            reason: format!("failed to read object payload: {error}"),
                        }
                    })?;
                    let Some(e_tag) = meta.e_tag.as_deref() else {
                        return Err(ConditionalStorageError::InvalidToken {
                            path: location.to_string(),
                            reason: "remote object metadata is missing an ETag".to_string(),
                        });
                    };
                    if e_tag.trim().is_empty() {
                        return Err(ConditionalStorageError::InvalidToken {
                            path: location.to_string(),
                            reason: "remote object metadata has an empty ETag".to_string(),
                        });
                    }
                    let token = CompareToken::s3(e_tag, meta.version.as_deref());
                    Ok(Some(ConditionalRead {
                        bytes: bytes.to_vec(),
                        token,
                    }))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(object_store::Error::NotImplemented { operation, .. }) => {
                    Err(ConditionalStorageError::UnsupportedBackend {
                        backend: self.uri(),
                        reason: format!("backend does not support conditional read ({operation})"),
                    })
                }
                Err(other) => Err(ConditionalStorageError::Indeterminate {
                    path: location.to_string(),
                    reason: other.to_string(),
                }),
            },
        }
    }

    async fn compare_and_swap_optional_unprobed(
        &self,
        location: &ObjectPath,
        expected: Option<&CompareToken>,
        replacement: PutPayload,
    ) -> Result<CompareAndSwapOutcome, ConditionalStorageError> {
        match &self.backend {
            Backend::Local { local, .. } => {
                let expected_slot = match expected {
                    Some(token) => Some(token.parse_local(location)?),
                    None => None,
                };
                let target_path = local.path_to_filesystem(location).map_err(|error| {
                    ConditionalStorageError::Indeterminate {
                        path: location.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                let replacement_bytes = payload_to_vec(replacement);
                let location_owned = location.clone();
                #[cfg(test)]
                let crash_stage = self.take_local_cas_crash_stage_for_test();
                tokio::task::spawn_blocking(move || {
                    let companions = companion_paths(&target_path, &location_owned)?;
                    let _lock_guard = acquire_local_lock(&companions, &location_owned)?;
                    let (mut state, current) =
                        read_and_reconcile_locked(&target_path, &companions, &location_owned)?;
                    match (&expected_slot, current.as_ref()) {
                        (None, None) => {}
                        (None, Some(_)) => return Ok(CompareAndSwapOutcome::Conflict),
                        (Some(_), None) => return Ok(CompareAndSwapOutcome::Conflict),
                        (Some(expected_slot), Some((actual_bytes, current_slot))) => {
                            let actual_sha256 = sha256_hex(actual_bytes);
                            if current_slot != expected_slot
                                || expected_slot.sha256 != actual_sha256
                                || expected_slot.size_bytes != actual_bytes.len() as u64
                            {
                                return Ok(CompareAndSwapOutcome::Conflict);
                            }
                        }
                    }
                    let new_slot = write_and_commit_locked(
                        &target_path,
                        &companions,
                        &mut state,
                        &replacement_bytes,
                        false,
                        &location_owned,
                        #[cfg(test)]
                        crash_stage,
                    )?;
                    Ok(CompareAndSwapOutcome::Swapped {
                        token: new_slot.to_token(),
                    })
                })
                .await
                .map_err(|error| ConditionalStorageError::Indeterminate {
                    path: location.to_string(),
                    reason: format!("local compare_and_swap worker panicked: {error}"),
                })?
            }
            Backend::Remote { store, uri } => {
                let put_mode = match expected {
                    None => PutMode::Create,
                    Some(token) => {
                        let (e_tag, version) = token.parse_s3(location)?;
                        PutMode::Update(UpdateVersion {
                            e_tag: Some(e_tag),
                            version,
                        })
                    }
                };
                match store
                    .put_opts(location, replacement, PutOptions::from(put_mode))
                    .await
                {
                    Ok(put_result) => {
                        let Some(new_etag) = put_result.e_tag.as_deref() else {
                            return Err(ConditionalStorageError::UnsupportedBackend {
                                backend: uri.clone(),
                                reason:
                                    "remote compare_and_swap succeeded without returning an ETag"
                                        .to_string(),
                            });
                        };
                        if new_etag.trim().is_empty() {
                            return Err(ConditionalStorageError::UnsupportedBackend {
                                backend: uri.clone(),
                                reason: "remote compare_and_swap returned an empty ETag"
                                    .to_string(),
                            });
                        }
                        let new_token = CompareToken::s3(new_etag, put_result.version.as_deref());
                        Ok(CompareAndSwapOutcome::Swapped { token: new_token })
                    }
                    Err(
                        object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. }
                        | object_store::Error::NotFound { .. },
                    ) => Ok(CompareAndSwapOutcome::Conflict),
                    Err(object_store::Error::NotImplemented { operation, .. }) => {
                        Err(ConditionalStorageError::UnsupportedBackend {
                            backend: uri.clone(),
                            reason: format!(
                                "backend does not implement conditional compare-and-swap ({operation})"
                            ),
                        })
                    }
                    Err(other) => Err(ConditionalStorageError::Indeterminate {
                        path: location.to_string(),
                        reason: other.to_string(),
                    }),
                }
            }
        }
    }
}

/// Shared conditional-storage contract and multi-writer race suite used by
/// both local filesystem tests and S3-compatible integration tests.
///
/// `make_manager` must construct a fresh `StorageManager` instance backed by
/// the same underlying storage root/bucket so concurrent tasks do NOT share
/// the process-local `publish_lock`.
pub async fn run_shared_conditional_race_and_contract_suite<F>(make_manager: F)
where
    F: Fn() -> StorageManager + Send + Sync + 'static,
{
    use crate::storage::{RemoteSplitManifest, RemoteSplitState};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let make_manager = Arc::new(make_manager);
    let primary = make_manager();

    // 0. Capability probe must succeed and report supported.
    let status = primary.probe_publication_capability().await;
    assert!(
        status.supported,
        "expected supported capability status, got {status:?}"
    );
    assert_eq!(status.capability, CAPABILITY_NAME);
    assert!(primary.publication_capability_status().supported);

    // 1. Atomic create_immutable race across 8 independent StorageManager instances.
    let immutable_path = primary
        .object_path("race-suite/immutable/manifest-0001.json")
        .expect("valid object path");
    let writers = 8usize;
    let barrier = Arc::new(Barrier::new(writers));
    let mut handles = Vec::with_capacity(writers);
    for writer_id in 0..writers {
        let manager = make_manager();
        let path = immutable_path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let payload = format!("{{\"writer\":{writer_id},\"epoch\":1}}").into_bytes();
            barrier.wait().await;
            let outcome = manager
                .create_immutable(&path, payload.clone())
                .await
                .expect("create_immutable should not error");
            (writer_id, payload, outcome)
        }));
    }

    let mut created_winners = Vec::new();
    let mut existing_losers = Vec::new();
    for handle in handles {
        let (writer_id, payload, outcome) = handle.await.expect("writer task");
        match &outcome {
            CreateImmutableOutcome::Created { identity } => {
                assert!(identity.content_matches);
                assert_eq!(identity.size_bytes, payload.len() as u64);
                assert!(
                    outcome.ensure_matching_content(&immutable_path).is_ok(),
                    "winner must pass ensure_matching_content"
                );
                created_winners.push((writer_id, payload, identity.clone()));
            }
            CreateImmutableOutcome::AlreadyExists { existing } => {
                assert!(
                    !existing.content_matches,
                    "distinct losing payload must report content_matches=false"
                );
                assert!(matches!(
                    outcome.ensure_matching_content(&immutable_path),
                    Err(ConditionalStorageError::ImmutableConflict { .. })
                ));
                existing_losers.push(existing.clone());
            }
        }
    }
    assert_eq!(
        created_winners.len(),
        1,
        "exactly one concurrent create_immutable writer must win Created"
    );
    assert_eq!(existing_losers.len(), writers - 1);
    let (_winner_id, winner_payload, winner_identity) = &created_winners[0];
    for loser in &existing_losers {
        assert_eq!(loser.sha256, winner_identity.sha256);
        assert_eq!(loser.size_bytes, winner_identity.size_bytes);
        assert_eq!(loser.token, winner_identity.token);
    }

    // Duplicate create_immutable with identical winning payload must report content_matches=true.
    let retry_same = primary
        .create_immutable(&immutable_path, winner_payload.clone())
        .await
        .expect("idempotent duplicate create_immutable");
    match &retry_same {
        CreateImmutableOutcome::AlreadyExists { existing } => {
            assert!(existing.content_matches);
            assert_eq!(existing.sha256, winner_identity.sha256);
            assert_eq!(existing.token, winner_identity.token);
        }
        other => panic!("expected AlreadyExists on duplicate create, got {other:?}"),
    }
    retry_same
        .ensure_matching_content(&immutable_path)
        .expect("idempotent duplicate create must pass ensure_matching_content");

    // Readback with token must return the exact winning payload and token.
    let readback = primary
        .read_with_token(&immutable_path)
        .await
        .expect("read_with_token")
        .expect("immutable object must exist");
    assert_eq!(&readback.bytes, winner_payload);
    assert_eq!(readback.token, winner_identity.token);

    // 2. ABA protection and token evolution on pointer replacement.
    let aba_path = primary
        .object_path("race-suite/pointers/aba-pointer.json")
        .expect("valid object path");
    assert!(
        primary
            .read_with_token(&aba_path)
            .await
            .expect("missing read_with_token")
            .is_none()
    );
    let token_a1 = match primary
        .compare_and_swap_optional(&aba_path, None, b"{\"state\":\"A\",\"rev\":1}".to_vec())
        .await
        .expect("initial CAS create")
    {
        CompareAndSwapOutcome::Swapped { token } => token,
        CompareAndSwapOutcome::Conflict => panic!("initial CAS on absent key must succeed"),
    };
    assert_eq!(
        primary
            .compare_and_swap_optional(&aba_path, None, b"{\"state\":\"dup\"}".to_vec())
            .await
            .expect("duplicate create CAS"),
        CompareAndSwapOutcome::Conflict
    );
    let token_b = match primary
        .compare_and_swap(
            &aba_path,
            &token_a1,
            b"{\"state\":\"B\",\"rev\":2}".to_vec(),
        )
        .await
        .expect("CAS A1 -> B")
    {
        CompareAndSwapOutcome::Swapped { token } => token,
        CompareAndSwapOutcome::Conflict => panic!("CAS A1 -> B must succeed"),
    };
    assert_ne!(token_a1, token_b);

    // Replace back with the exact same bytes as A1 (when on local backend, inode/revision still advances;
    // when on S3, we test distinct pointer revisions as required by ADR 0002).
    let a2_payload = if primary.is_local() {
        b"{\"state\":\"A\",\"rev\":1}".to_vec()
    } else {
        b"{\"state\":\"A\",\"rev\":3}".to_vec()
    };
    let token_a2 = match primary
        .compare_and_swap(&aba_path, &token_b, a2_payload.clone())
        .await
        .expect("CAS B -> A2")
    {
        CompareAndSwapOutcome::Swapped { token } => token,
        CompareAndSwapOutcome::Conflict => panic!("CAS B -> A2 must succeed"),
    };
    assert_ne!(token_b, token_a2);
    assert_ne!(token_a1, token_a2);

    // Stale token_a1 and token_b must both conflict and leave A2 untouched.
    for stale in [&token_a1, &token_b] {
        assert_eq!(
            primary
                .compare_and_swap(&aba_path, stale, b"{\"state\":\"STALE\"}".to_vec())
                .await
                .expect("stale CAS"),
            CompareAndSwapOutcome::Conflict
        );
    }
    let final_aba = primary
        .read_with_token(&aba_path)
        .await
        .expect("read final aba")
        .expect("aba pointer exists");
    assert_eq!(final_aba.bytes, a2_payload);
    assert_eq!(final_aba.token, token_a2);

    // 3. Multi-writer compare_and_swap race with retry-to-convergence across 8 independent managers.
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    struct CounterPointer {
        revision: u64,
        history: Vec<usize>,
    }

    let cas_race_path = primary
        .object_path("race-suite/pointers/concurrent-pointer.json")
        .expect("valid path");
    let barrier = Arc::new(Barrier::new(writers));
    let first_read_barrier = Arc::new(Barrier::new(writers));
    let mut handles = Vec::with_capacity(writers);
    for writer_id in 0..writers {
        let manager = make_manager();
        let path = cas_race_path.clone();
        let barrier = Arc::clone(&barrier);
        let first_read_barrier = Arc::clone(&first_read_barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut conflicts = 0usize;
            let mut first_attempt = true;
            loop {
                let current = manager
                    .read_with_token(&path)
                    .await
                    .expect("read_with_token in race");
                if first_attempt {
                    first_attempt = false;
                    first_read_barrier.wait().await;
                }
                let (next_state, expected_token) = match current {
                    None => (
                        CounterPointer {
                            revision: 1,
                            history: vec![writer_id],
                        },
                        None,
                    ),
                    Some(read) => {
                        let mut decoded: CounterPointer =
                            serde_json::from_slice(&read.bytes).expect("valid CounterPointer JSON");
                        decoded.revision += 1;
                        decoded.history.push(writer_id);
                        (decoded, Some(read.token))
                    }
                };
                let replacement = serde_json::to_vec(&next_state).unwrap();
                match manager
                    .compare_and_swap_optional(&path, expected_token.as_ref(), replacement)
                    .await
                    .expect("CAS in race")
                {
                    CompareAndSwapOutcome::Swapped { token } => {
                        return (writer_id, conflicts, next_state.revision, token);
                    }
                    CompareAndSwapOutcome::Conflict => {
                        conflicts += 1;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }

    let mut total_conflicts = 0usize;
    let mut observed_revisions = HashSet::new();
    let mut observed_tokens = HashSet::new();
    for handle in handles {
        let (_writer_id, conflicts, rev, token) = handle.await.expect("CAS writer task");
        total_conflicts += conflicts;
        assert!(
            observed_revisions.insert(rev),
            "duplicate committed revision {rev}"
        );
        assert!(
            observed_tokens.insert(token),
            "every successful pointer replacement must produce a distinct CompareToken"
        );
    }
    assert!(
        total_conflicts > 0,
        "concurrent writers racing on initial/subsequent CAS must observe conflicts"
    );
    assert_eq!(observed_revisions.len(), writers);

    let converged = primary
        .read_with_token(&cas_race_path)
        .await
        .expect("read converged pointer")
        .expect("converged pointer exists");
    let final_counter: CounterPointer = serde_json::from_slice(&converged.bytes).unwrap();
    assert_eq!(final_counter.revision, writers as u64);
    assert_eq!(final_counter.history.len(), writers);
    let unique_writers: HashSet<usize> = final_counter.history.iter().copied().collect();
    assert_eq!(unique_writers.len(), writers);

    // 4. Cross-instance append_split_and_publish race: independent StorageManagers
    // racing on the same index detect pointer/generation conflicts and never lose a split.
    let index_uuid = format!("race-idx-{}", Uuid::new_v4());
    let publish_writers = 4usize;
    let barrier = Arc::new(Barrier::new(publish_writers));
    let mut handles = Vec::with_capacity(publish_writers);
    for writer_id in 0..publish_writers {
        let manager = make_manager();
        let index_uuid = index_uuid.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let split = RemoteSplitManifest {
                split_id: format!("split-{writer_id}"),
                state: RemoteSplitState::Published,
                bundle_path: format!("{index_uuid}/splits/split-{writer_id}/bundle"),
                bundle_etag: None,
                hotcache_path: None,
                checksum: format!("sha256:{:064x}", writer_id + 1),
                doc_count: 10 + writer_id as u64,
                size_bytes: 128,
                uncompressed_bytes: 256,
                hotcache_bytes: None,
                time_range: None,
                tags: BTreeMap::new(),
                field_ranges: BTreeMap::new(),
                field_terms: BTreeMap::new(),
            };
            barrier.wait().await;
            let mut conflicts = 0usize;
            loop {
                match manager
                    .append_split_and_publish(
                        &index_uuid,
                        "events",
                        "sha256:v1",
                        None,
                        split.clone(),
                    )
                    .await
                {
                    Ok(manifest) => return (writer_id, conflicts, manifest.generation),
                    Err(error) => {
                        let is_conflict = matches!(
                            error.downcast_ref::<ConditionalStorageError>(),
                            Some(
                                ConditionalStorageError::Conflict { .. }
                                    | ConditionalStorageError::ImmutableConflict { .. }
                            )
                        );
                        assert!(
                            is_conflict,
                            "expected conditional conflict error during cross-instance publish race, got: {error:#}"
                        );
                        conflicts += 1;
                        tokio::task::yield_now().await;
                    }
                }
            }
        }));
    }

    let mut publish_generations = HashSet::new();
    let mut publish_conflicts = 0usize;
    for handle in handles {
        let (_writer_id, conflicts, generation) = handle.await.expect("publish writer");
        publish_conflicts += conflicts;
        assert!(publish_generations.insert(generation));
    }
    assert!(publish_conflicts > 0);
    assert_eq!(
        publish_generations,
        (1..=publish_writers as u64).collect::<HashSet<_>>()
    );

    let final_manifest = primary
        .load_current_manifest(&index_uuid, Some("sha256:v1"))
        .await
        .expect("load final manifest")
        .expect("final manifest exists");
    assert_eq!(final_manifest.generation, publish_writers as u64);
    let published_ids: HashSet<String> = final_manifest
        .published_splits()
        .map(|s| s.split_id.clone())
        .collect();
    let expected_ids: HashSet<String> = (0..publish_writers)
        .map(|id| format!("split-{id}"))
        .collect();
    assert_eq!(published_ids, expected_ids);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions,
    };
    use std::ops::Range;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn local_filesystem_passes_shared_conditional_race_and_contract_suite() {
        let temp_dir = TempDir::new().unwrap();
        let root_path = temp_dir.path().to_path_buf();
        run_shared_conditional_race_and_contract_suite(move || {
            StorageManager::new_in_path(&root_path).unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn remote_object_store_passes_shared_conditional_race_and_contract_suite() {
        let temp_dir = TempDir::new().unwrap();
        let shared_store: Arc<dyn ObjectStore> = Arc::new(FaultyObjectStore {
            inner: object_store::memory::InMemory::new(),
            mode: FaultyStoreMode::YieldingNormal,
            fail_transport: Arc::new(AtomicBool::new(false)),
        });
        let base_dir = temp_dir.path().to_path_buf();
        run_shared_conditional_race_and_contract_suite(move || {
            let workdir = base_dir.join(format!("work-{}", Uuid::new_v4()));
            StorageManager::new_with_object_store(
                "s3://test-bucket/shared-prefix".to_string(),
                Arc::clone(&shared_store),
                workdir,
                None,
            )
            .unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn local_cas_recovers_cleanly_across_every_crash_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager
            .object_path("idx-crash/manifest.current.json")
            .unwrap();

        // Seed revision 1.
        let token_1 = match manager
            .compare_and_swap_optional(&path, None, b"{\"gen\":1}".to_vec())
            .await
            .unwrap()
        {
            CompareAndSwapOutcome::Swapped { token } => token,
            CompareAndSwapOutcome::Conflict => panic!("initial create must succeed"),
        };

        // Pre-rename crash stages must preserve revision 1 and clean up staging artifacts.
        for stage in [
            LocalCasCrashStage::LockAcquire,
            LocalCasCrashStage::TempWriteAndSync,
            LocalCasCrashStage::PendingStatePersist,
        ] {
            manager.set_local_cas_crash_stage_for_test(Some(stage));
            let err = manager
                .compare_and_swap(&path, &token_1, b"{\"gen\":99}".to_vec())
                .await
                .unwrap_err();
            assert!(matches!(err, ConditionalStorageError::Indeterminate { .. }));

            // A fresh manager (simulating a new process after crash) must still see gen 1 and token_1.
            let fresh_manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
            let read = fresh_manager
                .read_with_token(&path)
                .await
                .unwrap()
                .expect("pointer must still exist");
            assert_eq!(read.bytes, b"{\"gen\":1}");
            assert_eq!(read.token, token_1);
        }

        // Crash AFTER atomic target rename (before pending state is promoted to committed)
        // must be reconciled on the next read/CAS so the committed swap is never lost.
        manager.set_local_cas_crash_stage_for_test(Some(LocalCasCrashStage::TargetRenameAndSync));
        let err = manager
            .compare_and_swap(&path, &token_1, b"{\"gen\":2}".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(err, ConditionalStorageError::Indeterminate { .. }));

        let fresh_manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let read_after = fresh_manager
            .read_with_token(&path)
            .await
            .unwrap()
            .expect("pointer must exist after post-rename crash");
        assert_eq!(read_after.bytes, b"{\"gen\":2}");
        assert_ne!(read_after.token, token_1);

        // Old token_1 must now conflict, while read_after.token advances cleanly to gen 3.
        assert_eq!(
            fresh_manager
                .compare_and_swap(&path, &token_1, b"{\"gen\":999}".to_vec())
                .await
                .unwrap(),
            CompareAndSwapOutcome::Conflict
        );
        match fresh_manager
            .compare_and_swap(&path, &read_after.token, b"{\"gen\":3}".to_vec())
            .await
            .unwrap()
        {
            CompareAndSwapOutcome::Swapped { token: token_3 } => {
                assert_ne!(token_3, read_after.token);
            }
            CompareAndSwapOutcome::Conflict => panic!("CAS with reconciled token must succeed"),
        }
    }

    #[tokio::test]
    async fn local_cas_multi_process_flock_race_preserves_every_update() {
        const CHILD_ENV: &str = "FERRIS_FS015_MULTI_PROCESS_DIR";
        const CHILD_ID_ENV: &str = "FERRIS_FS015_MULTI_PROCESS_ID";

        if let Ok(dir) = std::env::var(CHILD_ENV) {
            let worker_id: usize = std::env::var(CHILD_ID_ENV).unwrap().parse().unwrap();
            let manager = StorageManager::new_in_path(Path::new(&dir)).unwrap();
            let path = manager.object_path("mp-idx/manifest.current.json").unwrap();
            for step in 0..3usize {
                loop {
                    let current = manager.read_with_token(&path).await.unwrap();
                    let (mut list, token) = match current {
                        None => (Vec::<String>::new(), None),
                        Some(read) => (
                            serde_json::from_slice::<Vec<String>>(&read.bytes).unwrap(),
                            Some(read.token),
                        ),
                    };
                    list.push(format!("w{worker_id}-s{step}"));
                    let bytes = serde_json::to_vec(&list).unwrap();
                    if let CompareAndSwapOutcome::Swapped { .. } = manager
                        .compare_and_swap_optional(&path, token.as_ref(), bytes)
                        .await
                        .unwrap()
                    {
                        break;
                    }
                }
            }
            return;
        }

        let temp_dir = TempDir::new().unwrap();
        let exe = std::env::current_exe().unwrap();
        let num_processes = 4usize;
        let mut children = Vec::with_capacity(num_processes);
        for worker_id in 0..num_processes {
            let child = std::process::Command::new(&exe)
                .arg("--exact")
                .arg("storage::conditional::tests::local_cas_multi_process_flock_race_preserves_every_update")
                .arg("--nocapture")
                .env(CHILD_ENV, temp_dir.path())
                .env(CHILD_ID_ENV, worker_id.to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("spawn child process");
            children.push(child);
        }

        for child in children {
            let output = child.wait_with_output().expect("wait child process");
            assert!(
                output.status.success(),
                "child process failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager.object_path("mp-idx/manifest.current.json").unwrap();
        let final_read = manager
            .read_with_token(&path)
            .await
            .unwrap()
            .expect("multi-process pointer must exist");
        let entries: Vec<String> = serde_json::from_slice(&final_read.bytes).unwrap();
        assert_eq!(entries.len(), num_processes * 3);
        for worker_id in 0..num_processes {
            for step in 0..3usize {
                let key = format!("w{worker_id}-s{step}");
                assert!(entries.contains(&key), "missing entry {key} in {entries:?}");
            }
        }
    }

    #[tokio::test]
    async fn local_corrupt_cas_state_and_malformed_tokens_fail_closed() {
        let temp_dir = TempDir::new().unwrap();
        let manager = StorageManager::new_in_path(temp_dir.path()).unwrap();
        let path = manager.object_path("idx-1/manifest.current.json").unwrap();

        // Malformed / tampered / cross-backend tokens fail closed with InvalidToken.
        for bad_token in [
            CompareToken::new(""),
            CompareToken::new("invalid-prefix"),
            CompareToken::new(
                "fs1:local:0:1:1:0000000000000000000000000000000000000000000000000000000000000000:bad",
            ),
            CompareToken::new("fs1:local:1:2:3:not-hex:bad"),
            CompareToken::s3("\"etag-1\"", None),
        ] {
            let err = manager
                .compare_and_swap(&path, &bad_token, b"{}".to_vec())
                .await
                .unwrap_err();
            assert!(
                matches!(err, ConditionalStorageError::InvalidToken { .. }),
                "expected InvalidToken for {bad_token}, got {err:?}"
            );
        }

        let remote_manager = StorageManager::new_with_object_store(
            "s3://test-bucket/prefix".to_string(),
            Arc::new(object_store::memory::InMemory::new()),
            temp_dir.path().join("remote-work"),
            None,
        )
        .unwrap();
        let local_token = CompareToken::local(1, 42, 2, &sha256_hex(b"{}"));
        for bad_remote_token in [
            CompareToken::new(""),
            CompareToken::new("fs1:s3:::bad"),
            CompareToken::new("fs1:s3:zzzz:-:bad"),
            local_token,
        ] {
            let err = remote_manager
                .compare_and_swap(&path, &bad_remote_token, b"{}".to_vec())
                .await
                .unwrap_err();
            assert!(
                matches!(err, ConditionalStorageError::InvalidToken { .. }),
                "expected InvalidToken for remote {bad_remote_token}, got {err:?}"
            );
        }

        // Write valid pointer on local backend, then corrupt its `.ferris.state#0` file.
        let outcome = manager
            .compare_and_swap_optional(&path, None, b"{\"ok\":true}".to_vec())
            .await
            .unwrap();
        let token = match outcome {
            CompareAndSwapOutcome::Swapped { token } => token,
            CompareAndSwapOutcome::Conflict => panic!("expected Swapped"),
        };

        // Verify companion `#0` files are invisible to `ObjectStore::list`.
        let listed: Vec<_> = manager
            .object_store()
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, path);

        let target_fs_path = manager.filesystem_path(&path).unwrap();
        let companions = companion_paths(&target_fs_path, &path).unwrap();
        std::fs::write(&companions.state_path, b"corrupt-state-json").unwrap();

        assert!(matches!(
            manager.read_with_token(&path).await,
            Err(ConditionalStorageError::CorruptState { .. })
        ));
        assert!(matches!(
            manager
                .compare_and_swap(&path, &token, b"{\"ok\":false}".to_vec())
                .await,
            Err(ConditionalStorageError::CorruptState { .. })
        ));
        assert!(matches!(
            manager
                .create_immutable(&path, b"{\"ok\":true}".to_vec())
                .await,
            Err(ConditionalStorageError::CorruptState { .. })
        ));
    }

    #[test]
    fn network_filesystem_detection_rejects_shared_mounts_and_accepts_local_mounts() {
        let sample_mounts = "\
/dev/sda1 / ext4 rw,relatime 0 0\n\
server:/export /mnt/nfs_share nfs4 rw,relatime 0 0\n\
//fileserver/share /mnt/smb\\040share cifs rw 0 0\n\
tmpfs /mnt/nfs_share/local_tmp tmpfs rw 0 0\n";

        let root_fs = detect_mount_fstype_from_proc_mounts(Path::new("/var/data"), sample_mounts)
            .expect("root mount");
        assert_eq!(root_fs, "ext4");
        assert!(!is_uncertified_network_fstype(&root_fs));

        let nfs_fs = detect_mount_fstype_from_proc_mounts(
            Path::new("/mnt/nfs_share/ferris/_remote_store"),
            sample_mounts,
        )
        .expect("nfs mount");
        assert_eq!(nfs_fs, "nfs4");
        assert!(is_uncertified_network_fstype(&nfs_fs));

        let cifs_fs =
            detect_mount_fstype_from_proc_mounts(Path::new("/mnt/smb share/ferris"), sample_mounts)
                .expect("escaped cifs mount");
        assert_eq!(cifs_fs, "cifs");
        assert!(is_uncertified_network_fstype(&cifs_fs));

        let nested_tmpfs = detect_mount_fstype_from_proc_mounts(
            Path::new("/mnt/nfs_share/local_tmp/store"),
            sample_mounts,
        )
        .expect("nested local tmpfs mount");
        assert_eq!(nested_tmpfs, "tmpfs");
        assert!(!is_uncertified_network_fstype(&nested_tmpfs));
    }

    #[derive(Debug)]
    enum FaultyStoreMode {
        YieldingNormal,
        ConditionalPutNotImplemented,
        OverwriteOnCreate,
        IgnoreIfMatchOnUpdate,
        MissingETag,
        TransportErrorAfterProbe,
    }

    #[derive(Debug)]
    struct FaultyObjectStore {
        inner: object_store::memory::InMemory,
        mode: FaultyStoreMode,
        fail_transport: Arc<AtomicBool>,
    }

    impl std::fmt::Display for FaultyObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultyObjectStore({:?})", self.mode)
        }
    }

    #[tonic::async_trait]
    impl ObjectStore for FaultyObjectStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if matches!(self.mode, FaultyStoreMode::YieldingNormal) {
                tokio::task::yield_now().await;
            }
            if self.fail_transport.load(Ordering::Acquire) {
                return Err(object_store::Error::Generic {
                    store: "FaultyObjectStore",
                    source: "simulated transport timeout during PUT".into(),
                });
            }
            match self.mode {
                FaultyStoreMode::ConditionalPutNotImplemented
                    if !matches!(opts.mode, PutMode::Overwrite) =>
                {
                    Err(object_store::Error::NotImplemented {
                        operation: format!("put_opts({:?})", opts.mode),
                        implementer: self.to_string(),
                    })
                }
                FaultyStoreMode::OverwriteOnCreate => {
                    self.inner
                        .put_opts(location, payload, PutOptions::from(PutMode::Overwrite))
                        .await
                }
                FaultyStoreMode::IgnoreIfMatchOnUpdate => {
                    let effective = match opts.mode {
                        PutMode::Update(_) => PutOptions::from(PutMode::Overwrite),
                        other => PutOptions::from(other),
                    };
                    self.inner.put_opts(location, payload, effective).await
                }
                FaultyStoreMode::MissingETag => {
                    let mut res = self.inner.put_opts(location, payload, opts).await?;
                    res.e_tag = None;
                    Ok(res)
                }
                _ => self.inner.put_opts(location, payload, opts).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if matches!(self.mode, FaultyStoreMode::YieldingNormal) {
                tokio::task::yield_now().await;
            }
            if self.fail_transport.load(Ordering::Acquire) {
                return Err(object_store::Error::Generic {
                    store: "FaultyObjectStore",
                    source: "simulated transport timeout during GET".into(),
                });
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<axum::body::Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn capability_probe_and_publication_fail_closed_on_broken_or_unsupported_backends() {
        use crate::storage::{
            REMOTE_MANIFEST_FORMAT_VERSION, RemoteSplitManifest, RemoteSplitState,
            RemoteStoreManifest,
        };
        use std::collections::BTreeMap;

        let temp_dir = TempDir::new().unwrap();

        for mode in [
            FaultyStoreMode::ConditionalPutNotImplemented,
            FaultyStoreMode::OverwriteOnCreate,
            FaultyStoreMode::IgnoreIfMatchOnUpdate,
            FaultyStoreMode::MissingETag,
        ] {
            let label = format!("{mode:?}");
            let store = Arc::new(FaultyObjectStore {
                inner: object_store::memory::InMemory::new(),
                mode,
                fail_transport: Arc::new(AtomicBool::new(false)),
            });
            let manager = StorageManager::new_with_object_store(
                format!("s3://broken-bucket/{label}"),
                store.clone(),
                temp_dir.path().join(&label),
                None,
            )
            .unwrap();

            let report = manager.probe_publication_capability().await;
            assert!(
                !report.supported,
                "backend {label} must fail capability probe: {report:?}"
            );
            assert!(report.reason.is_some());
            assert!(!manager.publication_capability_status().supported);

            let manifest = RemoteStoreManifest {
                version: REMOTE_MANIFEST_FORMAT_VERSION,
                engine: crate::cluster::state::IndexEngine::RemoteStore,
                index_uuid: "idx-fail-closed".into(),
                index_name: "events".into(),
                schema_generation: 1,
                schema_hash: "sha256:v1".into(),
                generation: 1,
                parent_generation: 0,
                parent_manifest_path: None,
                writer_epoch: 1,
                operation_id: "op-1".into(),
                request_fingerprint: format!("sha256:{}", "1".repeat(64)),
                published_at: "2026-10-16T00:00:00Z".into(),
                settings: None,
                published_watermarks: BTreeMap::new(),
                compaction_lineage: Vec::new(),
                splits: vec![RemoteSplitManifest {
                    split_id: "split-1".into(),
                    state: RemoteSplitState::Published,
                    bundle_path: "idx-fail-closed/splits/split-1/bundle".into(),
                    bundle_etag: None,
                    hotcache_path: None,
                    checksum: "sha256:a".into(),
                    doc_count: 1,
                    size_bytes: 10,
                    uncompressed_bytes: 10,
                    hotcache_bytes: None,
                    time_range: None,
                    tags: BTreeMap::new(),
                    field_ranges: BTreeMap::new(),
                    field_terms: BTreeMap::new(),
                }],
            };

            let publish_err = manager.publish_manifest(&manifest).await.unwrap_err();
            assert!(
                matches!(
                    publish_err.downcast_ref::<ConditionalStorageError>(),
                    Some(ConditionalStorageError::UnsupportedBackend { .. })
                ),
                "publish_manifest on {label} must fail closed with UnsupportedBackend, got: {publish_err:#}"
            );

            // Verify no manifest or pointer was written on the uncertified backend.
            assert!(
                manager
                    .load_manifest_pointer("idx-fail-closed")
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        // Verify ambiguous transport errors after probe remain Indeterminate (not Conflict).
        let fail_flag = Arc::new(AtomicBool::new(false));
        let store = Arc::new(FaultyObjectStore {
            inner: object_store::memory::InMemory::new(),
            mode: FaultyStoreMode::TransportErrorAfterProbe,
            fail_transport: Arc::clone(&fail_flag),
        });
        let manager = StorageManager::new_with_object_store(
            "s3://bucket/transport-error".to_string(),
            store,
            temp_dir.path().join("transport-error"),
            None,
        )
        .unwrap();
        let path = manager.object_path("idx-1/manifest.current.json").unwrap();
        let token = match manager
            .compare_and_swap_optional(&path, None, b"{\"v\":1}".to_vec())
            .await
            .unwrap()
        {
            CompareAndSwapOutcome::Swapped { token } => token,
            CompareAndSwapOutcome::Conflict => panic!("initial CAS must succeed"),
        };

        fail_flag.store(true, Ordering::Release);
        assert!(matches!(
            manager
                .compare_and_swap(&path, &token, b"{\"v\":2}".to_vec())
                .await,
            Err(ConditionalStorageError::Indeterminate { .. })
        ));
        assert!(matches!(
            manager.read_with_token(&path).await,
            Err(ConditionalStorageError::Indeterminate { .. })
        ));
    }
}
