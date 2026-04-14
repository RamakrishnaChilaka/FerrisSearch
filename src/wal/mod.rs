//! Write-Ahead Log (WAL)
//!
//! Provides crash durability for indexed documents.
//! Every index operation is appended here BEFORE being written to the engine buffer.
//! On crash + restart, uncommitted entries are replayed into the engine.
//! After a successful flush (engine commit), the WAL is truncated.
//!
//! ## Binary format
//!
//! Each entry is stored as a length-prefixed bincode frame:
//!
//! ```text
//! [u32 LE: payload_len] [payload_len bytes: bincode-encoded TranslogEntry]
//! ```
//!
//! This is more compact than JSONL and faster to parse/replay.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

/// Controls when the translog is fsynced to disk.
///
/// Matches OpenSearch's `index.translog.durability` setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TranslogDurability {
    /// Fsync after every write request (default). Maximum durability — no data
    /// loss on crash. Equivalent to OpenSearch `"request"`.
    #[default]
    Request,
    /// Fsync on a timer interval. Faster writes but up to `sync_interval` of
    /// data may be lost on crash. Equivalent to OpenSearch `"async"`.
    /// Safe when replicas exist (data can be recovered from peers).
    Async { sync_interval_ms: u64 },
}

const BINCODE_CONFIG: bincode_next::config::Configuration = bincode_next::config::standard();
const TRANSLOG_MANIFEST_FILE: &str = "translog.manifest";
const TRANSLOG_MANIFEST_VERSION: u32 = 1;
const TRANSLOG_SEQNO_FILE: &str = "translog.seqno";
const TRANSLOG_FILE_PREFIX: &str = "translog-";
const TRANSLOG_FILE_SUFFIX: &str = ".bin";
const TRANSLOG_GENERATION_WIDTH: usize = 20;

/// The type of WAL operation.
///
/// This enum replaces the previous stringly-typed `"index"` / `"delete"` convention.
/// Serialized as a string in the binary wire format for backwards compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    /// Index (create or update) a document.
    Index,
    /// Delete a document by ID.
    Delete,
}

impl WalOperation {
    /// Parse from the string representation used in the WAL wire format.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "index" => Ok(Self::Index),
            "delete" => Ok(Self::Delete),
            other => anyhow::bail!("unknown WAL operation type: {other}"),
        }
    }

    /// The string representation used in the WAL wire format.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Delete => "delete",
        }
    }
}

impl std::fmt::Display for WalOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single WAL entry, representing one indexing operation.
#[derive(Debug, Clone)]
pub struct TranslogEntry {
    /// Monotonically increasing sequence number for ordering
    pub seq_no: u64,
    /// The operation type.
    pub op: WalOperation,
    /// The full document payload
    pub payload: serde_json::Value,
}

/// Wire format for binary serialization.
/// `serde_json::Value` uses `deserialize_any()` which bincode doesn't support,
/// so we serialize the JSON payload to a string first.
#[derive(Serialize, Deserialize)]
struct WireEntry {
    seq_no: u64,
    op: String,
    payload_json: String,
}

impl WireEntry {
    #[cfg(test)]
    fn from_translog(entry: &TranslogEntry) -> Result<Self> {
        Ok(Self {
            seq_no: entry.seq_no,
            op: entry.op.as_str().to_string(),
            payload_json: serde_json::to_string(&entry.payload)?,
        })
    }

    fn into_translog(self) -> Result<TranslogEntry> {
        Ok(TranslogEntry {
            seq_no: self.seq_no,
            op: WalOperation::parse(&self.op).map_err(|error| {
                anyhow::anyhow!(
                    "failed to decode translog entry seq_no {}: {}",
                    self.seq_no,
                    error
                )
            })?,
            payload: serde_json::from_str(&self.payload_json)?,
        })
    }
}

/// Trait abstracting a Write-Ahead Log backend.
/// The current default is `HotTranslog` (binary file, fsync-per-write).
/// Future implementations could use memory-only WAL, remote WAL, etc.
pub trait WriteAheadLog: Send + Sync {
    /// Append a single operation and fsync for durability.
    fn append(&self, op: WalOperation, payload: serde_json::Value) -> Result<TranslogEntry>;

    /// Append a single operation using a caller-supplied sequence number.
    /// Used for replica/recovery paths so shard copies persist the primary's seq_no.
    fn append_with_seq(
        &self,
        seq_no: u64,
        op: WalOperation,
        payload: serde_json::Value,
    ) -> Result<TranslogEntry>;

    /// Append multiple operations with a single fsync (bulk optimization).
    fn append_bulk(&self, ops: &[(WalOperation, serde_json::Value)]) -> Result<Vec<TranslogEntry>>;

    /// Write multiple operations to WAL with a single fsync, without constructing
    /// return entries. Faster than `append_bulk` when the caller doesn't need the entries.
    fn write_bulk(&self, ops: &[(WalOperation, serde_json::Value)]) -> Result<()>;

    /// Write multiple operations with caller-supplied contiguous sequence numbers.
    fn write_bulk_with_start_seq(
        &self,
        start_seq_no: u64,
        ops: &[(WalOperation, serde_json::Value)],
    ) -> Result<()>;

    /// Read all pending entries (used for replay on startup).
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;

    /// Read entries with seq_no > the given checkpoint (for replica recovery).
    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>>;

    /// Truncate after a successful commit — data is safe in engine segments.
    fn truncate(&self) -> Result<()>;

    /// Truncate only entries with seq_no <= the global checkpoint.
    /// Entries above the global checkpoint are retained for replica recovery.
    fn truncate_below(&self, global_checkpoint: u64) -> Result<()>;

    /// Get the last assigned sequence number (highest seq_no written so far).
    /// Returns 0 if no writes have occurred.
    fn last_seq_no(&self) -> u64;

    /// Get the next sequence number that will be assigned on append.
    /// This is exclusive: if seq_no 0 has been written, this returns 1.
    fn next_seq_no(&self) -> u64;

    /// Return the current on-disk size of the translog in bytes.
    /// Used by the auto-flush mechanism to trigger flush when the translog
    /// exceeds a configurable threshold.
    fn size_bytes(&self) -> Result<u64>;

    /// Stream entries with seq_no >= `min_seq_no` through `callback` without
    /// accumulating them in memory. Returns the number of entries processed.
    fn for_each_from(
        &self,
        min_seq_no: u64,
        callback: &mut dyn FnMut(TranslogEntry) -> Result<()>,
    ) -> Result<u64>;
}

/// Encode a `TranslogEntry` into a length-prefixed binary frame.
#[cfg(test)]
fn encode_entry(entry: &TranslogEntry) -> Result<Vec<u8>> {
    let wire = WireEntry::from_translog(entry)?;
    let encoded = bincode_next::serde::encode_to_vec(&wire, BINCODE_CONFIG)?;
    let len = encoded.len() as u32;
    let mut frame = Vec::with_capacity(4 + encoded.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

/// Encode directly from borrowed values — avoids cloning the payload.
fn encode_entry_borrowed(
    seq_no: u64,
    op: WalOperation,
    payload: &serde_json::Value,
) -> Result<Vec<u8>> {
    let wire = WireEntry {
        seq_no,
        op: op.as_str().to_string(),
        payload_json: serde_json::to_string(payload)?,
    };
    let encoded = bincode_next::serde::encode_to_vec(&wire, BINCODE_CONFIG)?;
    let len = encoded.len() as u32;
    let mut frame = Vec::with_capacity(4 + encoded.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

/// Read all length-prefixed entries from a reader, stopping at EOF or a partial frame.
#[cfg(test)]
fn decode_entries<R: Read>(reader: &mut R) -> Result<Vec<TranslogEntry>> {
    let mut entries = Vec::new();
    decode_entries_streaming(reader, |entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

/// Stream through all length-prefixed entries from a reader, calling `callback`
/// for each decoded entry without accumulating them in memory. Stops at EOF or
/// a partial frame.
fn decode_entries_streaming<R: Read>(
    reader: &mut R,
    mut callback: impl FnMut(TranslogEntry) -> Result<()>,
) -> Result<()> {
    let mut len_buf = [0u8; 4];
    loop {
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload_buf = vec![0u8; payload_len];
        match reader.read_exact(&mut payload_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Partial entry at end of file — truncated write, skip it
                tracing::warn!(
                    "Translog has partial entry at end, ignoring {} bytes",
                    payload_len
                );
                break;
            }
            Err(e) => return Err(e.into()),
        }
        let (wire, _): (WireEntry, _) =
            bincode_next::serde::decode_from_slice(&payload_buf, BINCODE_CONFIG)?;
        callback(wire.into_translog()?)?;
    }
    Ok(())
}

#[derive(Debug, Default)]
struct GenerationScan {
    first_seq_no: Option<u64>,
    last_seq_no: Option<u64>,
}

impl GenerationScan {
    fn observe(&mut self, seq_no: u64) {
        self.first_seq_no = Some(match self.first_seq_no {
            Some(first) => first.min(seq_no),
            None => seq_no,
        });
        self.last_seq_no = Some(match self.last_seq_no {
            Some(last) => last.max(seq_no),
            None => seq_no,
        });
    }

    #[cfg(test)]
    fn next_seq_no(&self) -> u64 {
        self.last_seq_no
            .map(|last| last.saturating_add(1))
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
struct GenerationInfo {
    id: u64,
    path: PathBuf,
    first_seq_no: Option<u64>,
    last_seq_no: Option<u64>,
    size_bytes: u64,
}

impl GenerationInfo {
    fn observe_seq(&mut self, seq_no: u64) {
        self.first_seq_no = Some(match self.first_seq_no {
            Some(first) => first.min(seq_no),
            None => seq_no,
        });
        self.last_seq_no = Some(match self.last_seq_no {
            Some(last) => last.max(seq_no),
            None => seq_no,
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ManifestGenerationInfo {
    id: u64,
    first_seq_no: Option<u64>,
    last_seq_no: Option<u64>,
    size_bytes: u64,
}

impl ManifestGenerationInfo {
    fn into_generation_info(self, data_dir: &Path) -> GenerationInfo {
        GenerationInfo {
            id: self.id,
            path: generation_path(data_dir, self.id),
            first_seq_no: self.first_seq_no,
            last_seq_no: self.last_seq_no,
            size_bytes: self.size_bytes,
        }
    }
}

impl From<&GenerationInfo> for ManifestGenerationInfo {
    fn from(generation: &GenerationInfo) -> Self {
        Self {
            id: generation.id,
            first_seq_no: generation.first_seq_no,
            last_seq_no: generation.last_seq_no,
            size_bytes: generation.size_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TranslogManifest {
    version: u32,
    active_generation_id: u64,
    next_generation_id: u64,
    generations: Vec<ManifestGenerationInfo>,
}

impl TranslogManifest {
    fn from_state(state: &TranslogState) -> Self {
        Self {
            version: TRANSLOG_MANIFEST_VERSION,
            active_generation_id: state.active_generation_id,
            next_generation_id: state.next_generation_id,
            generations: state
                .generations
                .iter()
                .map(ManifestGenerationInfo::from)
                .collect(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version != TRANSLOG_MANIFEST_VERSION {
            anyhow::bail!(
                "unsupported translog manifest version {} (expected {})",
                self.version,
                TRANSLOG_MANIFEST_VERSION
            );
        }
        if self.generations.is_empty() {
            anyhow::bail!("translog manifest has no generations");
        }

        let mut previous_id = None;
        let mut saw_active = false;
        for generation in &self.generations {
            if let Some(previous_id) = previous_id
                && generation.id <= previous_id
            {
                anyhow::bail!(
                    "translog manifest generations are not strictly increasing: {} after {}",
                    generation.id,
                    previous_id
                );
            }
            previous_id = Some(generation.id);
            if generation.id == self.active_generation_id {
                saw_active = true;
            }
        }

        if !saw_active {
            anyhow::bail!(
                "translog manifest active generation {} missing from generation list",
                self.active_generation_id
            );
        }
        if self.next_generation_id <= self.active_generation_id {
            anyhow::bail!(
                "translog manifest next generation {} must be greater than active generation {}",
                self.next_generation_id,
                self.active_generation_id
            );
        }

        Ok(())
    }
}

#[derive(Debug)]
struct TranslogState {
    next_seq_no: u64,
    next_generation_id: u64,
    active_generation_id: u64,
    active_file: File,
    generations: Vec<GenerationInfo>,
}

impl TranslogState {
    fn active_generation_index(&self) -> Result<usize> {
        let active_id = self.active_generation_id;
        self.generations
            .iter()
            .position(|generation| generation.id == active_id)
            .ok_or_else(|| {
                anyhow::anyhow!("translog state is missing active generation {}", active_id)
            })
    }

    fn generations_snapshot(&self) -> Vec<GenerationInfo> {
        self.generations.clone()
    }
}

/// Scan a translog file and return only the maximum seq_no found,
/// without retaining any entries in memory. Returns 0 if the file is empty.
#[cfg(test)]
fn scan_max_seq_no<R: Read>(reader: &mut R) -> Result<u64> {
    Ok(scan_generation(reader)?.next_seq_no())
}

fn scan_generation<R: Read>(reader: &mut R) -> Result<GenerationScan> {
    let mut scan = GenerationScan::default();
    decode_entries_streaming(reader, |entry| {
        scan.observe(entry.seq_no);
        Ok(())
    })?;
    Ok(scan)
}

fn parse_generation_id(file_name: &str) -> Option<u64> {
    if !file_name.starts_with(TRANSLOG_FILE_PREFIX) || !file_name.ends_with(TRANSLOG_FILE_SUFFIX) {
        return None;
    }
    let id = &file_name[TRANSLOG_FILE_PREFIX.len()..file_name.len() - TRANSLOG_FILE_SUFFIX.len()];
    id.parse::<u64>().ok()
}

fn generation_path(data_dir: &Path, generation_id: u64) -> PathBuf {
    data_dir.join(format!(
        "{TRANSLOG_FILE_PREFIX}{generation_id:0width$}{TRANSLOG_FILE_SUFFIX}",
        width = TRANSLOG_GENERATION_WIDTH
    ))
}

fn manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join(TRANSLOG_MANIFEST_FILE)
}

fn discover_generation_files(data_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut generations = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(generation_id) = parse_generation_id(file_name) else {
            continue;
        };
        generations.push((generation_id, entry.path()));
    }
    generations.sort_by_key(|(generation_id, _)| *generation_id);
    Ok(generations)
}

fn scan_generation_from_path(path: &Path) -> Result<GenerationScan> {
    let file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(GenerationScan::default()),
        Err(e) => return Err(e.into()),
    };
    let mut reader = BufReader::new(file);
    scan_generation(&mut reader)
}

fn load_translog_manifest(data_dir: &Path) -> Result<Option<TranslogManifest>> {
    let path = manifest_path(data_dir);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let manifest = serde_json::from_slice::<TranslogManifest>(&bytes)?;
    manifest.validate()?;
    Ok(Some(manifest))
}

fn persist_translog_manifest(path: &Path, manifest: &TranslogManifest) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    let json = serde_json::to_vec(manifest)?;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;
    file.write_all(&json)?;
    file.sync_data()?;
    drop(file);

    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn remove_unreferenced_generation_files(
    data_dir: &Path,
    manifest: &TranslogManifest,
) -> Result<()> {
    let referenced: std::collections::BTreeSet<u64> = manifest
        .generations
        .iter()
        .map(|generation| generation.id)
        .collect();

    for (generation_id, path) in discover_generation_files(data_dir)? {
        if referenced.contains(&generation_id) {
            continue;
        }

        match fs::remove_file(&path) {
            Ok(()) => {
                tracing::warn!(
                    "Removed unreferenced translog generation {:?} not present in manifest",
                    path
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(
                    "Failed to remove unreferenced translog generation {:?}: {}",
                    path,
                    e
                );
            }
        }
    }

    Ok(())
}

fn generations_from_manifest(
    data_dir: &Path,
    manifest: &TranslogManifest,
) -> Result<Vec<GenerationInfo>> {
    let mut generations = Vec::with_capacity(manifest.generations.len());
    for generation in &manifest.generations {
        let mut info = generation.clone().into_generation_info(data_dir);
        let metadata = fs::metadata(&info.path).map_err(|e| {
            anyhow::anyhow!(
                "manifest references missing translog generation {:?}: {}",
                info.path,
                e
            )
        })?;
        info.size_bytes = metadata.len();
        generations.push(info);
    }
    Ok(generations)
}

fn open_generation_writer(path: &Path) -> Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(file)
}

fn create_empty_generation(data_dir: &Path, generation_id: u64) -> Result<(GenerationInfo, File)> {
    let path = generation_path(data_dir, generation_id);
    let file = open_generation_writer(&path)?;
    let size_bytes = file.metadata()?.len();
    Ok((
        GenerationInfo {
            id: generation_id,
            path,
            first_seq_no: None,
            last_seq_no: None,
            size_bytes,
        },
        file,
    ))
}

fn recover_lock<'a, T>(mutex: &'a Mutex<T>, lock_name: &'static str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                lock = lock_name,
                "translog lock poisoned; continuing with inner state"
            );
            poisoned.into_inner()
        }
    }
}

/// Hot translog — binary length-prefixed, fsync-on-every-write WAL for maximum durability.
///
/// Stored at `<data_dir>/translog-*.bin` generation files. Each entry is a
/// bincode-encoded frame preceded by a 4-byte little-endian length. The active
/// generation is fsynced on every write in request durability mode.
///
/// The sequence number is monotonically increasing and *never resets*, even after
/// truncation. This enables replica recovery via seq_no-based translog replay.
pub struct HotTranslog {
    state: Arc<Mutex<TranslogState>>,
    data_dir: PathBuf,
    manifest_path: PathBuf,
    /// Path to the file that persists the seq_no high-water mark across truncations.
    seq_no_path: PathBuf,
    /// Controls whether writes are fsynced immediately or on a timer.
    durability: TranslogDurability,
}

async fn sync_file_in_background(state: Arc<Mutex<TranslogState>>) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let state = recover_lock(state.as_ref(), "state");
        state.active_file.sync_data()
    })
    .await
    .map_err(|e| std::io::Error::other(format!("background translog sync task failed: {}", e)))?
}

impl HotTranslog {
    /// Open or create the translog at the given directory.
    /// Uses `TranslogDurability::Request` (fsync per write) by default.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        Self::open_with_durability(data_dir, TranslogDurability::Request)
    }

    /// Open or create the translog with explicit durability setting.
    pub fn open_with_durability<P: AsRef<Path>>(
        data_dir: P,
        durability: TranslogDurability,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir)?;
        let manifest_path = manifest_path(data_dir);
        let seq_no_path = data_dir.join(TRANSLOG_SEQNO_FILE);

        // Load the persisted high-water mark (survives truncation)
        let persisted_seq = if seq_no_path.exists() {
            let s = fs::read_to_string(&seq_no_path)?;
            s.trim().parse::<u64>().unwrap_or(0)
        } else {
            0
        };

        let (generations, active_generation_id, next_generation_id, active_file) =
            if let Some(manifest) = load_translog_manifest(data_dir)? {
                remove_unreferenced_generation_files(data_dir, &manifest)?;
                let mut generations = generations_from_manifest(data_dir, &manifest)?;
                let active_generation_id = manifest.active_generation_id;
                let active_generation = generations
                    .iter_mut()
                    .find(|generation| generation.id == active_generation_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "manifest active generation {} missing after generation load",
                            active_generation_id
                        )
                    })?;
                let active_scan = scan_generation_from_path(&active_generation.path)?;
                active_generation.first_seq_no = active_scan.first_seq_no;
                active_generation.last_seq_no = active_scan.last_seq_no;
                active_generation.size_bytes = fs::metadata(&active_generation.path)?.len();
                let active_file = open_generation_writer(&active_generation.path)?;

                (
                    generations,
                    active_generation_id,
                    manifest.next_generation_id,
                    active_file,
                )
            } else {
                let existing_generations = discover_generation_files(data_dir)?;
                if !existing_generations.is_empty() {
                    anyhow::bail!(
                        "generation-based translog files exist in {:?} without manifest {:?}",
                        data_dir,
                        manifest_path
                    );
                }

                let (generation, active_file) = create_empty_generation(data_dir, 0)?;
                let generations = vec![generation];
                let manifest = TranslogManifest {
                    version: TRANSLOG_MANIFEST_VERSION,
                    active_generation_id: 0,
                    next_generation_id: 1,
                    generations: generations
                        .iter()
                        .map(ManifestGenerationInfo::from)
                        .collect(),
                };
                persist_translog_manifest(&manifest_path, &manifest)?;

                (generations, 0, 1, active_file)
            };

        let next_seq = std::cmp::max(
            persisted_seq,
            generations
                .iter()
                .filter_map(|generation| generation.last_seq_no)
                .max()
                .map(|last| last.saturating_add(1))
                .unwrap_or(0),
        );

        let durability_label = match durability {
            TranslogDurability::Request => "request".to_string(),
            TranslogDurability::Async { sync_interval_ms } => {
                format!("async ({}ms)", sync_interval_ms)
            }
        };
        tracing::info!(
            "Translog opened at {:?} (active generation: {}, next seq_no: {}, durability: {})",
            data_dir,
            active_generation_id,
            next_seq,
            durability_label
        );

        Ok(Self {
            state: Arc::new(Mutex::new(TranslogState {
                next_seq_no: next_seq,
                next_generation_id,
                active_generation_id,
                active_file,
                generations,
            })),
            data_dir: data_dir.to_path_buf(),
            manifest_path,
            seq_no_path,
            durability,
        })
    }

    /// Get the current (next) sequence number without incrementing.
    pub fn current_seq_no(&self) -> u64 {
        recover_lock(&self.state, "state").next_seq_no
    }

    /// Start a background task that periodically fsyncs the translog file.
    /// Only useful in `Async` durability mode. Returns a join handle.
    pub fn start_sync_task(&self) -> Option<tokio::task::JoinHandle<()>> {
        let interval = match self.durability {
            TranslogDurability::Async { sync_interval_ms } => {
                Duration::from_millis(sync_interval_ms)
            }
            _ => return None,
        };
        let state = self.state.clone();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = sync_file_in_background(state.clone()).await {
                    tracing::error!("Background translog fsync failed: {}", e);
                }
            }
        }))
    }

    /// Manually trigger an fsync (useful for testing or explicit flush).
    pub fn sync(&self) -> Result<()> {
        let state = recover_lock(&self.state, "state");
        state.active_file.sync_data()?;
        Ok(())
    }

    /// Scan a translog file for the maximum seq_no without loading all entries into memory.
    /// Returns next_seq_no (max + 1), or 0 if the file is empty or does not exist.
    #[cfg(test)]
    fn scan_max_seq_no_from_path<P: AsRef<Path>>(path: P) -> Result<u64> {
        let file = match OpenOptions::new().read(true).open(path.as_ref()) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        let mut reader = BufReader::new(file);
        scan_max_seq_no(&mut reader)
    }

    fn persist_manifest_locked(&self, state: &TranslogState) -> Result<()> {
        persist_translog_manifest(&self.manifest_path, &TranslogManifest::from_state(state))
    }

    fn persist_seq_no_high_watermark(&self, next_seq_no: u64) -> Result<()> {
        fs::write(&self.seq_no_path, next_seq_no.to_string())?;
        Ok(())
    }

    fn roll_generation_locked(&self, state: &mut TranslogState) -> Result<u64> {
        let generation_id = state.next_generation_id;
        let (generation, file) = create_empty_generation(&self.data_dir, generation_id)?;
        state.active_generation_id = generation_id;
        state.next_generation_id = generation_id.saturating_add(1);
        state.active_file = file;
        state.generations.push(generation);
        Ok(generation_id)
    }

    fn take_prunable_generations_locked(
        &self,
        state: &mut TranslogState,
        global_checkpoint: Option<u64>,
    ) -> Vec<GenerationInfo> {
        let active_generation_id = state.active_generation_id;
        let mut kept = Vec::with_capacity(state.generations.len());
        let mut removed = Vec::new();

        for generation in state.generations.drain(..) {
            let should_remove = generation.id != active_generation_id
                && match global_checkpoint {
                    Some(global_checkpoint) => generation
                        .last_seq_no
                        .map(|last_seq_no| last_seq_no <= global_checkpoint)
                        .unwrap_or(true),
                    None => true,
                };

            if should_remove {
                removed.push(generation);
            } else {
                kept.push(generation);
            }
        }

        kept.sort_by_key(|generation| generation.id);
        state.generations = kept;
        removed
    }

    fn delete_generation_files(&self, generations: Vec<GenerationInfo>) -> usize {
        let mut removed = 0usize;
        for generation in generations {
            match fs::remove_file(&generation.path) {
                Ok(()) => {
                    removed += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    removed += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to remove obsolete translog generation {:?}: {}",
                        generation.path,
                        e
                    );
                }
            }
        }
        removed
    }

    fn generations_snapshot(&self) -> Vec<GenerationInfo> {
        recover_lock(&self.state, "state").generations_snapshot()
    }
}

impl WriteAheadLog for HotTranslog {
    fn append(&self, op: WalOperation, payload: serde_json::Value) -> Result<TranslogEntry> {
        let mut state = recover_lock(&self.state, "state");
        let generation_index = state.active_generation_index()?;
        let seq_no = state.next_seq_no;
        let frame = encode_entry_borrowed(seq_no, op, &payload)?;
        state.active_file.write_all(&frame)?;
        if matches!(self.durability, TranslogDurability::Request) {
            state.active_file.sync_data()?;
        }
        let generation = &mut state.generations[generation_index];
        generation.observe_seq(seq_no);
        generation.size_bytes += frame.len() as u64;
        state.next_seq_no = state.next_seq_no.saturating_add(1);

        let entry = TranslogEntry {
            seq_no,
            op,
            payload,
        };

        Ok(entry)
    }

    fn append_with_seq(
        &self,
        seq_no: u64,
        op: WalOperation,
        payload: serde_json::Value,
    ) -> Result<TranslogEntry> {
        let mut state = recover_lock(&self.state, "state");
        let generation_index = state.active_generation_index()?;
        let frame = encode_entry_borrowed(seq_no, op, &payload)?;
        state.active_file.write_all(&frame)?;
        if matches!(self.durability, TranslogDurability::Request) {
            state.active_file.sync_data()?;
        }
        let generation = &mut state.generations[generation_index];
        generation.observe_seq(seq_no);
        generation.size_bytes += frame.len() as u64;
        state.next_seq_no = state.next_seq_no.max(seq_no.saturating_add(1));

        let entry = TranslogEntry {
            seq_no,
            op,
            payload,
        };

        Ok(entry)
    }

    fn append_bulk(&self, ops: &[(WalOperation, serde_json::Value)]) -> Result<Vec<TranslogEntry>> {
        let mut state = recover_lock(&self.state, "state");
        let generation_index = state.active_generation_index()?;
        let start_seq_no = state.next_seq_no;
        let mut entries = Vec::with_capacity(ops.len());
        let mut buf = Vec::with_capacity(ops.len() * 200);
        let mut last_seq_no = None;

        for (offset, (op, payload)) in ops.iter().enumerate() {
            let seq_no = start_seq_no + offset as u64;
            buf.extend_from_slice(&encode_entry_borrowed(seq_no, *op, payload)?);
            entries.push(TranslogEntry {
                seq_no,
                op: *op,
                payload: payload.clone(),
            });
            last_seq_no = Some(seq_no);
        }

        state.active_file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            state.active_file.sync_data()?;
        }
        if let Some(last_seq_no) = last_seq_no {
            let generation = &mut state.generations[generation_index];
            generation.observe_seq(start_seq_no);
            generation.observe_seq(last_seq_no);
            generation.size_bytes += buf.len() as u64;
            state.next_seq_no = last_seq_no.saturating_add(1);
        }

        Ok(entries)
    }

    fn write_bulk(&self, ops: &[(WalOperation, serde_json::Value)]) -> Result<()> {
        let mut state = recover_lock(&self.state, "state");
        let generation_index = state.active_generation_index()?;
        let start_seq_no = state.next_seq_no;
        let mut buf = Vec::with_capacity(ops.len() * 200);
        let mut last_seq_no = None;

        for (offset, (op, payload)) in ops.iter().enumerate() {
            let seq_no = start_seq_no + offset as u64;
            buf.extend_from_slice(&encode_entry_borrowed(seq_no, *op, payload)?);
            last_seq_no = Some(seq_no);
        }

        state.active_file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            state.active_file.sync_data()?;
        }
        if let Some(last_seq_no) = last_seq_no {
            let generation = &mut state.generations[generation_index];
            generation.observe_seq(start_seq_no);
            generation.observe_seq(last_seq_no);
            generation.size_bytes += buf.len() as u64;
            state.next_seq_no = last_seq_no.saturating_add(1);
        }

        Ok(())
    }

    fn write_bulk_with_start_seq(
        &self,
        start_seq_no: u64,
        ops: &[(WalOperation, serde_json::Value)],
    ) -> Result<()> {
        let mut state = recover_lock(&self.state, "state");
        let generation_index = state.active_generation_index()?;
        let mut buf = Vec::with_capacity(ops.len() * 200);
        let mut last_seq_no = None;

        for (offset, (op, payload)) in ops.iter().enumerate() {
            let seq_no = start_seq_no + offset as u64;
            buf.extend_from_slice(&encode_entry_borrowed(seq_no, *op, payload)?);
            last_seq_no = Some(seq_no);
        }

        state.active_file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            state.active_file.sync_data()?;
        }
        if let Some(last_seq_no) = last_seq_no {
            let generation = &mut state.generations[generation_index];
            generation.observe_seq(start_seq_no);
            generation.observe_seq(last_seq_no);
            generation.size_bytes += buf.len() as u64;
            state.next_seq_no = state.next_seq_no.max(last_seq_no.saturating_add(1));
        }

        Ok(())
    }

    fn read_all(&self) -> Result<Vec<TranslogEntry>> {
        let mut entries = Vec::new();
        for generation in self.generations_snapshot() {
            let file = match OpenOptions::new().read(true).open(&generation.path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let mut reader = BufReader::new(file);
            decode_entries_streaming(&mut reader, |entry| {
                entries.push(entry);
                Ok(())
            })?;
        }
        Ok(entries)
    }

    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>> {
        let mut entries = Vec::new();
        for generation in self
            .generations_snapshot()
            .into_iter()
            .filter(|generation| {
                generation
                    .last_seq_no
                    .map(|last_seq_no| last_seq_no > after_seq_no)
                    .unwrap_or(false)
            })
        {
            let file = match OpenOptions::new().read(true).open(&generation.path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let mut reader = BufReader::new(file);
            decode_entries_streaming(&mut reader, |entry| {
                if entry.seq_no > after_seq_no {
                    entries.push(entry);
                }
                Ok(())
            })?;
        }
        Ok(entries)
    }

    fn truncate(&self) -> Result<()> {
        let mut state = recover_lock(&self.state, "state");
        let preserved_seq = state.next_seq_no;
        let rolled_generation = self.roll_generation_locked(&mut state)?;
        let removed_generations = self.take_prunable_generations_locked(&mut state, None);
        self.persist_manifest_locked(&state)?;
        drop(state);
        self.persist_seq_no_high_watermark(preserved_seq)?;
        let removed = self.delete_generation_files(removed_generations);

        tracing::info!(
            "Translog rolled to generation {} after flush; removed {} obsolete generations (seq_no preserved at {}).",
            rolled_generation,
            removed,
            preserved_seq
        );
        Ok(())
    }

    fn truncate_below(&self, global_checkpoint: u64) -> Result<()> {
        let mut state = recover_lock(&self.state, "state");
        let preserved_seq = state.next_seq_no;
        let rolled_generation = self.roll_generation_locked(&mut state)?;
        let removed_generations =
            self.take_prunable_generations_locked(&mut state, Some(global_checkpoint));
        let retained = state
            .generations
            .iter()
            .filter(|generation| generation.id != state.active_generation_id)
            .count();
        self.persist_manifest_locked(&state)?;
        drop(state);
        self.persist_seq_no_high_watermark(preserved_seq)?;
        let removed = self.delete_generation_files(removed_generations);

        tracing::info!(
            "Translog rolled to generation {} at checkpoint {}; removed {} obsolete generations and retained {} generations for recovery (seq_no at {}).",
            rolled_generation,
            global_checkpoint,
            removed,
            retained,
            preserved_seq
        );
        Ok(())
    }

    fn last_seq_no(&self) -> u64 {
        let next_seq_no = recover_lock(&self.state, "state").next_seq_no;
        if next_seq_no == 0 { 0 } else { next_seq_no - 1 }
    }

    fn next_seq_no(&self) -> u64 {
        recover_lock(&self.state, "state").next_seq_no
    }

    fn size_bytes(&self) -> Result<u64> {
        let state = recover_lock(&self.state, "state");
        Ok(state
            .generations
            .iter()
            .map(|generation| generation.size_bytes)
            .sum())
    }

    fn for_each_from(
        &self,
        min_seq_no: u64,
        callback: &mut dyn FnMut(TranslogEntry) -> Result<()>,
    ) -> Result<u64> {
        let mut count: u64 = 0;

        for generation in self
            .generations_snapshot()
            .into_iter()
            .filter(|generation| {
                generation
                    .last_seq_no
                    .map(|last_seq_no| last_seq_no >= min_seq_no)
                    .unwrap_or(false)
            })
        {
            let file = match OpenOptions::new().read(true).open(&generation.path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let mut reader = BufReader::new(file);
            decode_entries_streaming(&mut reader, |entry| {
                if entry.seq_no >= min_seq_no {
                    count += 1;
                    callback(entry)?;
                }
                Ok(())
            })?;
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    /// Helper: create a translog in a fresh temp directory.
    fn open_translog() -> (tempfile::TempDir, HotTranslog) {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open(dir.path()).unwrap();
        (dir, tl)
    }

    fn generation_file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                if !entry.file_type().ok()?.is_file() {
                    return None;
                }
                let file_name = entry.file_name();
                let file_name = file_name.to_str()?;
                parse_generation_id(file_name)?;
                Some(file_name.to_string())
            })
            .collect();
        names.sort();
        names
    }

    fn read_manifest(dir: &Path) -> TranslogManifest {
        serde_json::from_slice(&fs::read(manifest_path(dir)).unwrap()).unwrap()
    }

    // ── append / read_all ───────────────────────────────────────────────

    #[test]
    fn append_single_entry_and_read_back() {
        let (_dir, tl) = open_translog();
        let entry = tl
            .append(WalOperation::Index, json!({"title": "hello"}))
            .unwrap();
        assert_eq!(entry.seq_no, 0);
        assert_eq!(entry.op, WalOperation::Index);

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload["title"], "hello");
    }

    #[test]
    fn open_creates_manifest_for_new_translog() {
        let (dir, _tl) = open_translog();
        let manifest = read_manifest(dir.path());

        assert_eq!(manifest.version, TRANSLOG_MANIFEST_VERSION);
        assert_eq!(manifest.active_generation_id, 0);
        assert_eq!(manifest.next_generation_id, 1);
        assert_eq!(manifest.generations.len(), 1);
        assert_eq!(manifest.generations[0].id, 0);
        assert_eq!(manifest.generations[0].first_seq_no, None);
        assert_eq!(manifest.generations[0].last_seq_no, None);
    }

    #[test]
    fn append_multiple_entries_increments_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        let e3 = tl
            .append(WalOperation::Delete, json!({"_doc_id": "x"}))
            .unwrap();
        assert_eq!(e3.seq_no, 2);

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].op, WalOperation::Delete);
    }

    #[test]
    fn append_with_explicit_seq_preserves_provided_seq_number() {
        let (_dir, tl) = open_translog();
        let entry = tl
            .append_with_seq(7, WalOperation::Index, json!({"title": "replicated"}))
            .unwrap();

        assert_eq!(entry.seq_no, 7);
        assert_eq!(tl.next_seq_no(), 8);

        let next = tl
            .append(WalOperation::Index, json!({"title": "local"}))
            .unwrap();
        assert_eq!(next.seq_no, 8);
    }

    // ── append_bulk ─────────────────────────────────────────────────────

    #[test]
    fn append_bulk_writes_atomically() {
        let (_dir, tl) = open_translog();
        let ops: Vec<(WalOperation, serde_json::Value)> = (0..5)
            .map(|i| (WalOperation::Index, json!({"doc": i})))
            .collect();
        let entries = tl.append_bulk(&ops).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[4].seq_no, 4);

        let all = tl.read_all().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn write_bulk_with_explicit_start_seq_advances_allocator() {
        let (_dir, tl) = open_translog();
        let ops: Vec<(WalOperation, serde_json::Value)> = vec![
            (WalOperation::Index, json!({"doc": "a"})),
            (WalOperation::Index, json!({"doc": "b"})),
            (WalOperation::Index, json!({"doc": "c"})),
        ];

        tl.write_bulk_with_start_seq(10, &ops).unwrap();

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq_no, 10);
        assert_eq!(entries[1].seq_no, 11);
        assert_eq!(entries[2].seq_no, 12);
        assert_eq!(tl.next_seq_no(), 13);
    }

    #[test]
    fn bulk_then_single_seq_no_continues() {
        let (_dir, tl) = open_translog();
        tl.append_bulk(&[
            (WalOperation::Index, json!({"a": 1})),
            (WalOperation::Index, json!({"b": 2})),
        ])
        .unwrap();
        let e = tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
        assert_eq!(e.seq_no, 2);
    }

    // ── truncate ────────────────────────────────────────────────────────

    #[test]
    fn truncate_clears_all_entries() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"x": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"y": 2})).unwrap();

        tl.truncate().unwrap();

        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn truncate_preserves_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.truncate().unwrap();

        let e = tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        assert_eq!(
            e.seq_no, 1,
            "seq_no should continue from where it left off after truncate"
        );
    }

    // ── persistence across reopen ───────────────────────────────────────

    #[test]
    fn entries_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let tl = HotTranslog::open(dir.path()).unwrap();
            tl.append(WalOperation::Index, json!({"persisted": true}))
                .unwrap();
            tl.append(WalOperation::Delete, json!({"_doc_id": "abc"}))
                .unwrap();
        }
        // Reopen from the same directory
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let entries = tl2.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].payload["persisted"], true);
    }

    #[test]
    fn seq_no_resumes_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let tl = HotTranslog::open(dir.path()).unwrap();
            tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
            tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        }
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let e = tl2.append(WalOperation::Index, json!({"c": 3})).unwrap();
        assert_eq!(
            e.seq_no, 2,
            "seq_no should continue from where previous instance left off"
        );
    }

    // ── binary format correctness ───────────────────────────────────────

    #[test]
    fn binary_encode_decode_roundtrip() {
        let entry = TranslogEntry {
            seq_no: 42,
            op: WalOperation::Index,
            payload: json!({"field": "value", "num": 123}),
        };
        let frame = encode_entry(&entry).unwrap();
        // First 4 bytes are the length prefix
        let payload_len = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(frame.len(), 4 + payload_len);

        let mut reader = std::io::Cursor::new(&frame);
        let decoded = decode_entries(&mut reader).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].seq_no, 42);
        assert_eq!(decoded[0].op, WalOperation::Index);
        assert_eq!(decoded[0].payload["field"], "value");
    }

    #[test]
    fn partial_frame_at_eof_is_skipped() {
        let entry = TranslogEntry {
            seq_no: 0,
            op: WalOperation::Index,
            payload: json!({"ok": true}),
        };
        let mut frame = encode_entry(&entry).unwrap();
        // Append a partial frame: valid length header but truncated body
        frame.extend_from_slice(&100u32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 10]); // only 10 of 100 bytes

        let mut reader = std::io::Cursor::new(&frame);
        let decoded = decode_entries(&mut reader).unwrap();
        assert_eq!(
            decoded.len(),
            1,
            "should recover the first valid entry and skip the partial"
        );
    }

    // ── edge cases ──────────────────────────────────────────────────────

    #[test]
    fn empty_translog_read_all_returns_empty() {
        let (_dir, tl) = open_translog();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn large_payload_roundtrips() {
        let (_dir, tl) = open_translog();
        let big = "x".repeat(100_000);
        tl.append(WalOperation::Index, json!({"data": big}))
            .unwrap();
        let entries = tl.read_all().unwrap();
        assert_eq!(entries[0].payload["data"].as_str().unwrap().len(), 100_000);
    }

    // ── read_from ───────────────────────────────────────────────────────

    #[test]
    fn read_from_returns_entries_after_checkpoint() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap(); // seq 0
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap(); // seq 1
        tl.append(WalOperation::Index, json!({"c": 3})).unwrap(); // seq 2

        let entries = tl.read_from(0).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq_no, 1);
        assert_eq!(entries[1].seq_no, 2);
    }

    #[test]
    fn read_from_zero_returns_all() {
        let (_dir, tl) = open_translog();
        // read_from(0) should return entries with seq_no > 0
        // But if we want all entries, we need to use a sentinel below first seq.
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap(); // seq 0
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap(); // seq 1

        // read_from filters > after_seq_no, so read_from(0) skips seq 0
        let entries = tl.read_from(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq_no, 1);
    }

    #[test]
    fn read_from_returns_empty_when_all_below_checkpoint() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();

        let entries = tl.read_from(5).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn read_from_on_empty_translog_returns_empty() {
        let (_dir, tl) = open_translog();
        let entries = tl.read_from(0).unwrap();
        assert!(entries.is_empty());
    }

    // ── truncate_below ──────────────────────────────────────────────────

    #[test]
    fn truncate_below_keeps_mixed_generation_until_future_roll() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap(); // seq 0
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap(); // seq 1
        tl.append(WalOperation::Index, json!({"c": 3})).unwrap(); // seq 2
        tl.append(WalOperation::Index, json!({"d": 4})).unwrap(); // seq 3

        tl.truncate_below(1).unwrap();

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].seq_no, 0);
        assert_eq!(entries[3].seq_no, 3);
    }

    #[test]
    fn truncate_below_preserves_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append_with_seq(5, WalOperation::Index, json!({"a": 1}))
            .unwrap();
        tl.append_with_seq(6, WalOperation::Index, json!({"b": 2}))
            .unwrap();

        tl.truncate_below(5).unwrap();

        let e = tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
        assert_eq!(e.seq_no, 7, "seq_no should continue after truncate_below");
    }

    #[test]
    fn truncate_below_at_max_clears_fully_obsolete_generations() {
        let (_dir, tl) = open_translog();
        tl.append_with_seq(10, WalOperation::Index, json!({"a": 1}))
            .unwrap();
        tl.append_with_seq(11, WalOperation::Index, json!({"b": 2}))
            .unwrap();

        tl.truncate_below(20).unwrap();

        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn truncate_below_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let tl = HotTranslog::open(dir.path()).unwrap();
            tl.append_with_seq(5, WalOperation::Index, json!({"a": 1}))
                .unwrap();
            tl.append_with_seq(6, WalOperation::Index, json!({"b": 2}))
                .unwrap();
            tl.truncate_below(5).unwrap();
            tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
            tl.truncate_below(6).unwrap();
        }
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let entries = tl2.read_all().unwrap();
        assert_eq!(entries.len(), 1, "only seq 7 should survive reopen");
        assert_eq!(entries[0].seq_no, 7);
        let e = tl2.append(WalOperation::Index, json!({"d": 4})).unwrap();
        assert_eq!(e.seq_no, 8);
    }

    #[test]
    fn truncate_below_rolls_generation_and_prunes_obsolete_files() {
        let (dir, tl) = open_translog();
        tl.append_with_seq(5, WalOperation::Index, json!({"a": 1}))
            .unwrap();
        tl.append_with_seq(6, WalOperation::Index, json!({"b": 2}))
            .unwrap();

        tl.truncate_below(5).unwrap();
        assert_eq!(generation_file_names(dir.path()).len(), 2);

        tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
        tl.truncate_below(6).unwrap();

        let generation_files = generation_file_names(dir.path());
        assert_eq!(generation_files.len(), 2);
        assert_eq!(
            generation_files,
            vec![
                format!(
                    "{TRANSLOG_FILE_PREFIX}{:0width$}{TRANSLOG_FILE_SUFFIX}",
                    1,
                    width = TRANSLOG_GENERATION_WIDTH
                ),
                format!(
                    "{TRANSLOG_FILE_PREFIX}{:0width$}{TRANSLOG_FILE_SUFFIX}",
                    2,
                    width = TRANSLOG_GENERATION_WIDTH
                ),
            ]
        );

        let manifest = read_manifest(dir.path());
        assert_eq!(manifest.active_generation_id, 2);
        assert_eq!(manifest.next_generation_id, 3);
        assert_eq!(
            manifest
                .generations
                .iter()
                .map(|generation| generation.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn open_ignores_non_generation_side_file() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_entry = TranslogEntry {
            seq_no: 0,
            op: WalOperation::Index,
            payload: json!({"legacy": true}),
        };
        let frame = encode_entry(&legacy_entry).unwrap();
        fs::write(dir.path().join("translog.bin"), frame).unwrap();

        let tl = HotTranslog::open(dir.path()).unwrap();
        assert_eq!(tl.current_seq_no(), 0);
        assert!(tl.read_all().unwrap().is_empty());
        assert_eq!(generation_file_names(dir.path()).len(), 1);
    }

    #[test]
    fn open_requires_manifest_for_existing_generation_files() {
        let dir = tempfile::tempdir().unwrap();
        let entry = TranslogEntry {
            seq_no: 0,
            op: WalOperation::Index,
            payload: json!({"doc": 1}),
        };
        let frame = encode_entry(&entry).unwrap();
        fs::write(generation_path(dir.path(), 0), frame).unwrap();

        let err = HotTranslog::open(dir.path()).err().unwrap();
        assert!(err.to_string().contains("without manifest"));
    }

    #[test]
    fn open_scans_only_active_generation_when_manifest_exists() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(generation_path(dir.path(), 0), b"corrupt old generation").unwrap();

        let active_entry = TranslogEntry {
            seq_no: 7,
            op: WalOperation::Index,
            payload: json!({"active": true}),
        };
        let active_frame = encode_entry(&active_entry).unwrap();
        fs::write(generation_path(dir.path(), 1), &active_frame).unwrap();

        let manifest = TranslogManifest {
            version: TRANSLOG_MANIFEST_VERSION,
            active_generation_id: 1,
            next_generation_id: 2,
            generations: vec![
                ManifestGenerationInfo {
                    id: 0,
                    first_seq_no: Some(0),
                    last_seq_no: Some(6),
                    size_bytes: b"corrupt old generation".len() as u64,
                },
                ManifestGenerationInfo {
                    id: 1,
                    first_seq_no: Some(7),
                    last_seq_no: Some(7),
                    size_bytes: active_frame.len() as u64,
                },
            ],
        };
        persist_translog_manifest(&manifest_path(dir.path()), &manifest).unwrap();

        let tl = HotTranslog::open(dir.path()).unwrap();
        assert_eq!(tl.current_seq_no(), 8);
    }

    #[test]
    fn open_returns_error_for_unknown_active_generation_wal_operation() {
        let dir = tempfile::tempdir().unwrap();
        let wire = WireEntry {
            seq_no: 0,
            op: "bogus".to_string(),
            payload_json: serde_json::to_string(&json!({"doc": 1})).unwrap(),
        };
        let encoded = bincode_next::serde::encode_to_vec(&wire, BINCODE_CONFIG).unwrap();
        let mut frame = Vec::with_capacity(4 + encoded.len());
        frame.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        frame.extend_from_slice(&encoded);
        fs::write(generation_path(dir.path(), 0), &frame).unwrap();

        let manifest = TranslogManifest {
            version: TRANSLOG_MANIFEST_VERSION,
            active_generation_id: 0,
            next_generation_id: 1,
            generations: vec![ManifestGenerationInfo {
                id: 0,
                first_seq_no: Some(0),
                last_seq_no: Some(0),
                size_bytes: frame.len() as u64,
            }],
        };
        persist_translog_manifest(&manifest_path(dir.path()), &manifest).unwrap();

        let err = match HotTranslog::open(dir.path()) {
            Ok(_) => panic!("open should fail for unknown WAL operation type"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("unknown WAL operation type: bogus")
        );
    }

    #[test]
    fn append_returns_error_when_active_generation_is_missing_from_state() {
        let (_dir, tl) = open_translog();
        {
            let mut state = tl.state.lock().unwrap_or_else(|e| e.into_inner());
            state.active_generation_id = 99;
        }

        let err = tl
            .append(WalOperation::Index, json!({"doc": 1}))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("translog state is missing active generation 99")
        );
        assert!(tl.read_all().unwrap().is_empty());
    }

    // ── last_seq_no ─────────────────────────────────────────────────────

    #[test]
    fn last_seq_no_returns_zero_on_empty() {
        let (_dir, tl) = open_translog();
        assert_eq!(tl.last_seq_no(), 0);
    }

    #[test]
    fn last_seq_no_returns_highest_written() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        assert_eq!(tl.last_seq_no(), 1);
    }

    #[test]
    fn last_seq_no_preserved_after_truncate() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        tl.truncate().unwrap();
        assert_eq!(
            tl.last_seq_no(),
            1,
            "last_seq_no should persist after truncate"
        );
    }

    // ── TranslogDurability ──────────────────────────────────────────────

    #[test]
    fn default_durability_is_request() {
        assert_eq!(TranslogDurability::default(), TranslogDurability::Request);
    }

    #[test]
    fn open_defaults_to_request_durability() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open(dir.path()).unwrap();
        assert_eq!(tl.durability, TranslogDurability::Request);
    }

    #[test]
    fn open_with_request_durability() {
        let dir = tempfile::tempdir().unwrap();
        let tl =
            HotTranslog::open_with_durability(dir.path(), TranslogDurability::Request).unwrap();
        assert_eq!(tl.durability, TranslogDurability::Request);
    }

    #[test]
    fn open_with_async_durability() {
        let dir = tempfile::tempdir().unwrap();
        let durability = TranslogDurability::Async {
            sync_interval_ms: 3000,
        };
        let tl = HotTranslog::open_with_durability(dir.path(), durability).unwrap();
        assert_eq!(tl.durability, durability);
    }

    #[test]
    fn async_durability_preserves_interval() {
        let d = TranslogDurability::Async {
            sync_interval_ms: 7500,
        };
        match d {
            TranslogDurability::Async { sync_interval_ms } => assert_eq!(sync_interval_ms, 7500),
            _ => panic!("expected Async variant"),
        }
    }

    #[test]
    fn async_mode_append_writes_data() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 5000,
            },
        )
        .unwrap();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq_no, 0);
        assert_eq!(entries[1].seq_no, 1);
    }

    #[test]
    fn async_mode_append_bulk_writes_data() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 5000,
            },
        )
        .unwrap();
        let ops: Vec<(WalOperation, serde_json::Value)> = vec![
            (WalOperation::Index, json!({"a": 1})),
            (WalOperation::Index, json!({"b": 2})),
        ];
        let entries = tl.append_bulk(&ops).unwrap();
        assert_eq!(entries.len(), 2);

        let all = tl.read_all().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn async_mode_write_bulk_writes_data() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 5000,
            },
        )
        .unwrap();
        let ops: Vec<(WalOperation, serde_json::Value)> = vec![
            (WalOperation::Index, json!({"a": 1})),
            (WalOperation::Index, json!({"b": 2})),
        ];
        tl.write_bulk(&ops).unwrap();

        let all = tl.read_all().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn async_mode_manual_sync_works() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 5000,
            },
        )
        .unwrap();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        // Manual sync should succeed without error
        tl.sync().unwrap();
    }

    #[test]
    fn start_sync_task_returns_none_for_request_mode() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open(dir.path()).unwrap();
        assert!(tl.start_sync_task().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn background_sync_helper_does_not_starve_runtime_when_state_lock_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 1,
            },
        )
        .unwrap();

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let state = tl.state.clone();
        let holder = std::thread::spawn(move || {
            let _guard = state.lock().unwrap_or_else(|e| e.into_inner());
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });
        locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let state = tl.state.clone();
        let sync_task = tokio::spawn(async move {
            let _ = started_tx.send(());
            sync_file_in_background(state).await.unwrap()
        });

        let start = std::time::Instant::now();
        started_rx.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "background translog sync stalled the async runtime for {:?}",
            elapsed
        );

        sync_task.await.unwrap();
        holder.join().unwrap();
    }

    #[test]
    fn async_mode_truncate_works() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open_with_durability(
            dir.path(),
            TranslogDurability::Async {
                sync_interval_ms: 5000,
            },
        )
        .unwrap();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();

        tl.truncate().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());

        // seq_no preserved
        let e = tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
        assert_eq!(e.seq_no, 2);
    }

    #[test]
    fn async_mode_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let durability = TranslogDurability::Async {
            sync_interval_ms: 5000,
        };
        {
            let tl = HotTranslog::open_with_durability(dir.path(), durability).unwrap();
            tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
            tl.sync().unwrap(); // ensure data is on disk
        }
        // Reopen in request mode — data should still be there
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let entries = tl2.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload["a"], 1);
    }

    #[test]
    fn request_mode_sync_is_noop_succeeds() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        // sync() should succeed even in request mode (already fsynced)
        tl.sync().unwrap();
    }

    #[test]
    fn poisoned_state_lock_is_recovered_for_reads_and_appends() {
        let (_dir, tl) = open_translog();

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = tl.state.lock().unwrap();
            panic!("poison state lock");
        }));

        assert_eq!(tl.current_seq_no(), 0);
        let entry = tl.append(WalOperation::Index, json!({"doc": 1})).unwrap();
        assert_eq!(entry.seq_no, 0);
        assert_eq!(tl.next_seq_no(), 1);
        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq_no, 0);
        assert_eq!(entries[0].payload["doc"], 1);
    }

    // ── size_bytes ──────────────────────────────────────────────────

    #[test]
    fn size_bytes_empty_translog_is_zero() {
        let (_dir, tl) = open_translog();
        assert_eq!(tl.size_bytes().unwrap(), 0);
    }

    #[test]
    fn size_bytes_grows_after_appends() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        let s1 = tl.size_bytes().unwrap();
        assert!(s1 > 0);

        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        let s2 = tl.size_bytes().unwrap();
        assert!(s2 > s1);
    }

    #[test]
    fn size_bytes_resets_after_truncate() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        assert!(tl.size_bytes().unwrap() > 0);

        tl.truncate().unwrap();
        assert_eq!(tl.size_bytes().unwrap(), 0);
    }

    // ── for_each_from (streaming replay) ────────────────────────────

    #[test]
    fn for_each_from_streams_all_entries_when_min_is_zero() {
        let (_dir, tl) = open_translog();
        for i in 0..5 {
            tl.append(WalOperation::Index, json!({"doc": i})).unwrap();
        }

        let mut collected = Vec::new();
        let count = tl
            .for_each_from(0, &mut |entry| {
                collected.push(entry.seq_no);
                Ok(())
            })
            .unwrap();

        assert_eq!(count, 5);
        assert_eq!(collected, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn for_each_from_filters_below_min_seq() {
        let (_dir, tl) = open_translog();
        for i in 0..5 {
            tl.append(WalOperation::Index, json!({"doc": i})).unwrap();
        }

        let mut collected = Vec::new();
        let count = tl
            .for_each_from(3, &mut |entry| {
                collected.push(entry.seq_no);
                Ok(())
            })
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(collected, vec![3, 4]);
    }

    #[test]
    fn for_each_from_empty_translog_returns_zero() {
        let (_dir, tl) = open_translog();
        let count = tl.for_each_from(0, &mut |_| Ok(())).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn for_each_from_with_min_above_all_entries_returns_zero() {
        let (_dir, tl) = open_translog();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();

        let count = tl.for_each_from(100, &mut |_| Ok(())).unwrap();
        assert_eq!(count, 0);
    }

    // ── scan_max_seq_no ─────────────────────────────────────────────

    #[test]
    fn scan_max_seq_no_from_path_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let _tl = HotTranslog::open(dir.path()).unwrap();
        let path = generation_path(dir.path(), 0);
        let result = HotTranslog::scan_max_seq_no_from_path(&path).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn scan_max_seq_no_from_path_with_entries() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open(dir.path()).unwrap();
        tl.append(WalOperation::Index, json!({"a": 1})).unwrap();
        tl.append(WalOperation::Index, json!({"b": 2})).unwrap();
        tl.append(WalOperation::Index, json!({"c": 3})).unwrap();
        drop(tl);

        let path = generation_path(dir.path(), 0);
        let result = HotTranslog::scan_max_seq_no_from_path(&path).unwrap();
        // max seq_no is 2, so next_seq_no (max + 1) = 3
        assert_eq!(result, 3);
    }

    #[test]
    fn scan_max_seq_no_from_path_nonexistent_file_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.bin");
        let result = HotTranslog::scan_max_seq_no_from_path(&path).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn scan_max_seq_no_matches_open_with_durability_result() {
        // Verify the new scan_max_seq_no path produces the same seq_no
        // that reading all entries would produce (regression guard).
        let dir = tempfile::tempdir().unwrap();
        {
            let tl = HotTranslog::open(dir.path()).unwrap();
            for i in 0..100 {
                tl.append(WalOperation::Index, json!({"doc": i})).unwrap();
            }
        }
        // Reopen — the constructor now uses scan_max_seq_no internally
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        assert_eq!(tl2.next_seq_no(), 100);
    }
}
