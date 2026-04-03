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
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
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

/// A single WAL entry, representing one indexing operation.
#[derive(Debug, Clone)]
pub struct TranslogEntry {
    /// Monotonically increasing sequence number for ordering
    pub seq_no: u64,
    /// The operation type (currently only "index")
    pub op: String,
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
    fn from_translog(entry: &TranslogEntry) -> Result<Self> {
        Ok(Self {
            seq_no: entry.seq_no,
            op: entry.op.clone(),
            payload_json: serde_json::to_string(&entry.payload)?,
        })
    }

    fn into_translog(self) -> Result<TranslogEntry> {
        Ok(TranslogEntry {
            seq_no: self.seq_no,
            op: self.op,
            payload: serde_json::from_str(&self.payload_json)?,
        })
    }
}

/// Trait abstracting a Write-Ahead Log backend.
/// The current default is `HotTranslog` (binary file, fsync-per-write).
/// Future implementations could use memory-only WAL, remote WAL, etc.
pub trait WriteAheadLog: Send + Sync {
    /// Append a single operation and fsync for durability.
    fn append(&self, op: &str, payload: serde_json::Value) -> Result<TranslogEntry>;

    /// Append a single operation using a caller-supplied sequence number.
    /// Used for replica/recovery paths so shard copies persist the primary's seq_no.
    fn append_with_seq(
        &self,
        seq_no: u64,
        op: &str,
        payload: serde_json::Value,
    ) -> Result<TranslogEntry>;

    /// Append multiple operations with a single fsync (bulk optimization).
    fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>>;

    /// Write multiple operations to WAL with a single fsync, without constructing
    /// return entries. Faster than `append_bulk` when the caller doesn't need the entries.
    fn write_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<()>;

    /// Write multiple operations with caller-supplied contiguous sequence numbers.
    fn write_bulk_with_start_seq(
        &self,
        start_seq_no: u64,
        ops: &[(&str, serde_json::Value)],
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
}

/// Encode a `TranslogEntry` into a length-prefixed binary frame.
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
fn encode_entry_borrowed(seq_no: u64, op: &str, payload: &serde_json::Value) -> Result<Vec<u8>> {
    let wire = WireEntry {
        seq_no,
        op: op.to_string(),
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
fn decode_entries<R: Read>(reader: &mut R) -> Result<Vec<TranslogEntry>> {
    let mut entries = Vec::new();
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
        entries.push(wire.into_translog()?);
    }
    Ok(entries)
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
/// Stored at `<data_dir>/translog.bin`. Each entry is a bincode-encoded frame
/// preceded by a 4-byte little-endian length. The file is fsynced on every write
/// (request-level durability) to guarantee no ops are lost on crash.
///
/// The sequence number is monotonically increasing and *never resets*, even after
/// truncation. This enables replica recovery via seq_no-based translog replay.
pub struct HotTranslog {
    file: Arc<Mutex<File>>,
    seq_no: Mutex<u64>,
    /// Path to the file that persists the seq_no high-water mark across truncations.
    seq_no_path: std::path::PathBuf,
    /// Controls whether writes are fsynced immediately or on a timer.
    durability: TranslogDurability,
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
        let path = data_dir.join("translog.bin");
        let seq_no_path = data_dir.join("translog.seqno");

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        // Load the persisted high-water mark (survives truncation)
        let persisted_seq = if seq_no_path.exists() {
            let s = std::fs::read_to_string(&seq_no_path)?;
            s.trim().parse::<u64>().unwrap_or(0)
        } else {
            0
        };

        // Check translog entries — take the max of persisted and on-disk entries
        let existing = Self::read_entries_from_path(&path)?;
        let entry_max = existing.last().map(|e| e.seq_no + 1).unwrap_or(0);
        let next_seq = std::cmp::max(persisted_seq, entry_max);

        // Seek to end for subsequent appends
        let mut f = file;
        f.seek(SeekFrom::End(0))?;

        let durability_label = match durability {
            TranslogDurability::Request => "request".to_string(),
            TranslogDurability::Async { sync_interval_ms } => {
                format!("async ({}ms)", sync_interval_ms)
            }
        };
        tracing::info!(
            "Translog opened at {:?} ({} pending entries, next seq_no: {}, durability: {})",
            path,
            existing.len(),
            next_seq,
            durability_label
        );

        Ok(Self {
            file: Arc::new(Mutex::new(f)),
            seq_no: Mutex::new(next_seq),
            seq_no_path,
            durability,
        })
    }

    /// Get the current (next) sequence number without incrementing.
    pub fn current_seq_no(&self) -> u64 {
        *recover_lock(&self.seq_no, "seq_no")
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
        let file = self.file.clone();
        Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let f = recover_lock(file.as_ref(), "file");
                if let Err(e) = f.sync_data() {
                    tracing::error!("Background translog fsync failed: {}", e);
                }
            }
        }))
    }

    /// Manually trigger an fsync (useful for testing or explicit flush).
    pub fn sync(&self) -> Result<()> {
        let f = recover_lock(self.file.as_ref(), "file");
        f.sync_data()?;
        Ok(())
    }

    /// Helper to read entries directly from a path (used during open to check existing state)
    fn read_entries_from_path<P: AsRef<Path>>(path: P) -> Result<Vec<TranslogEntry>> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let mut reader = BufReader::new(file);
        decode_entries(&mut reader)
    }
}

impl WriteAheadLog for HotTranslog {
    fn append(&self, op: &str, payload: serde_json::Value) -> Result<TranslogEntry> {
        let mut seq = recover_lock(&self.seq_no, "seq_no");
        let entry = TranslogEntry {
            seq_no: *seq,
            op: op.to_string(),
            payload,
        };
        *seq += 1;

        let frame = encode_entry(&entry)?;

        let mut file = recover_lock(self.file.as_ref(), "file");
        file.write_all(&frame)?;
        if matches!(self.durability, TranslogDurability::Request) {
            file.sync_data()?;
        }

        Ok(entry)
    }

    fn append_with_seq(
        &self,
        seq_no: u64,
        op: &str,
        payload: serde_json::Value,
    ) -> Result<TranslogEntry> {
        let entry = TranslogEntry {
            seq_no,
            op: op.to_string(),
            payload,
        };

        let frame = encode_entry(&entry)?;

        let mut file = recover_lock(self.file.as_ref(), "file");
        file.write_all(&frame)?;
        if matches!(self.durability, TranslogDurability::Request) {
            file.sync_data()?;
        }
        drop(file);

        let mut next_seq = recover_lock(&self.seq_no, "seq_no");
        *next_seq = (*next_seq).max(seq_no.saturating_add(1));

        Ok(entry)
    }

    fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>> {
        let mut seq = recover_lock(&self.seq_no, "seq_no");
        let mut entries = Vec::with_capacity(ops.len());
        // Pre-estimate buffer size: ~200 bytes per entry is a reasonable guess
        let mut buf = Vec::with_capacity(ops.len() * 200);

        for (op, payload) in ops {
            buf.extend_from_slice(&encode_entry_borrowed(*seq, op, payload)?);
            entries.push(TranslogEntry {
                seq_no: *seq,
                op: op.to_string(),
                payload: payload.clone(),
            });
            *seq += 1;
        }

        let mut file = recover_lock(self.file.as_ref(), "file");
        file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            file.sync_data()?;
        }

        Ok(entries)
    }

    fn write_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<()> {
        let mut seq = recover_lock(&self.seq_no, "seq_no");
        let mut buf = Vec::with_capacity(ops.len() * 200);

        for (op, payload) in ops {
            buf.extend_from_slice(&encode_entry_borrowed(*seq, op, payload)?);
            *seq += 1;
        }

        let mut file = recover_lock(self.file.as_ref(), "file");
        file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            file.sync_data()?;
        }

        Ok(())
    }

    fn write_bulk_with_start_seq(
        &self,
        start_seq_no: u64,
        ops: &[(&str, serde_json::Value)],
    ) -> Result<()> {
        let mut buf = Vec::with_capacity(ops.len() * 200);

        for (offset, (op, payload)) in ops.iter().enumerate() {
            let seq_no = start_seq_no + offset as u64;
            buf.extend_from_slice(&encode_entry_borrowed(seq_no, op, payload)?);
        }

        let mut file = recover_lock(self.file.as_ref(), "file");
        file.write_all(&buf)?;
        if matches!(self.durability, TranslogDurability::Request) {
            file.sync_data()?;
        }
        drop(file);

        let mut next_seq = recover_lock(&self.seq_no, "seq_no");
        *next_seq = (*next_seq).max(start_seq_no.saturating_add(ops.len() as u64));

        Ok(())
    }

    fn read_all(&self) -> Result<Vec<TranslogEntry>> {
        let mut file = recover_lock(self.file.as_ref(), "file");
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&*file);
        decode_entries(&mut reader)
    }

    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>> {
        let all = self.read_all()?;
        Ok(all
            .into_iter()
            .filter(|e| e.seq_no > after_seq_no)
            .collect())
    }

    fn truncate(&self) -> Result<()> {
        let mut file = recover_lock(self.file.as_ref(), "file");
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.sync_data()?;
        drop(file);

        // Persist the current seq_no so it survives truncation.
        // This ensures seq_no never goes backward after flush.
        let seq = recover_lock(&self.seq_no, "seq_no");
        std::fs::write(&self.seq_no_path, seq.to_string())?;

        tracing::info!(
            "Translog truncated after flush (seq_no preserved at {}).",
            *seq
        );
        Ok(())
    }

    fn truncate_below(&self, global_checkpoint: u64) -> Result<()> {
        // Read all entries, keep only those above the global checkpoint
        let entries = self.read_all()?;
        let retained: Vec<&TranslogEntry> = entries
            .iter()
            .filter(|e| e.seq_no > global_checkpoint)
            .collect();

        let retained_count = retained.len();

        // Rewrite the file with only retained entries
        let mut file = recover_lock(self.file.as_ref(), "file");
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;

        let mut buf = Vec::new();
        for entry in &retained {
            buf.extend_from_slice(&encode_entry(entry)?);
        }
        if !buf.is_empty() {
            file.write_all(&buf)?;
        }
        file.sync_data()?;
        drop(file);

        // Persist seq_no high-water mark
        let seq = recover_lock(&self.seq_no, "seq_no");
        std::fs::write(&self.seq_no_path, seq.to_string())?;

        tracing::info!(
            "Translog compacted: removed entries <= seq_no {}, retained {} entries (seq_no at {}).",
            global_checkpoint,
            retained_count,
            *seq
        );
        Ok(())
    }

    fn last_seq_no(&self) -> u64 {
        let seq = recover_lock(&self.seq_no, "seq_no");
        if *seq == 0 { 0 } else { *seq - 1 }
    }

    fn next_seq_no(&self) -> u64 {
        *recover_lock(&self.seq_no, "seq_no")
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

    // ── append / read_all ───────────────────────────────────────────────

    #[test]
    fn append_single_entry_and_read_back() {
        let (_dir, tl) = open_translog();
        let entry = tl.append("index", json!({"title": "hello"})).unwrap();
        assert_eq!(entry.seq_no, 0);
        assert_eq!(entry.op, "index");

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload["title"], "hello");
    }

    #[test]
    fn append_multiple_entries_increments_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();
        let e3 = tl.append("delete", json!({"_doc_id": "x"})).unwrap();
        assert_eq!(e3.seq_no, 2);

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].op, "delete");
    }

    #[test]
    fn append_with_explicit_seq_preserves_provided_seq_number() {
        let (_dir, tl) = open_translog();
        let entry = tl
            .append_with_seq(7, "index", json!({"title": "replicated"}))
            .unwrap();

        assert_eq!(entry.seq_no, 7);
        assert_eq!(tl.next_seq_no(), 8);

        let next = tl.append("index", json!({"title": "local"})).unwrap();
        assert_eq!(next.seq_no, 8);
    }

    // ── append_bulk ─────────────────────────────────────────────────────

    #[test]
    fn append_bulk_writes_atomically() {
        let (_dir, tl) = open_translog();
        let ops: Vec<(&str, serde_json::Value)> =
            (0..5).map(|i| ("index", json!({"doc": i}))).collect();
        let entries = tl.append_bulk(&ops).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[4].seq_no, 4);

        let all = tl.read_all().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn write_bulk_with_explicit_start_seq_advances_allocator() {
        let (_dir, tl) = open_translog();
        let ops: Vec<(&str, serde_json::Value)> = vec![
            ("index", json!({"doc": "a"})),
            ("index", json!({"doc": "b"})),
            ("index", json!({"doc": "c"})),
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
        tl.append_bulk(&[("index", json!({"a": 1})), ("index", json!({"b": 2}))])
            .unwrap();
        let e = tl.append("index", json!({"c": 3})).unwrap();
        assert_eq!(e.seq_no, 2);
    }

    // ── truncate ────────────────────────────────────────────────────────

    #[test]
    fn truncate_clears_all_entries() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"x": 1})).unwrap();
        tl.append("index", json!({"y": 2})).unwrap();

        tl.truncate().unwrap();

        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn truncate_preserves_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.truncate().unwrap();

        let e = tl.append("index", json!({"b": 2})).unwrap();
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
            tl.append("index", json!({"persisted": true})).unwrap();
            tl.append("delete", json!({"_doc_id": "abc"})).unwrap();
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
            tl.append("index", json!({"a": 1})).unwrap();
            tl.append("index", json!({"b": 2})).unwrap();
        }
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let e = tl2.append("index", json!({"c": 3})).unwrap();
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
            op: "index".into(),
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
        assert_eq!(decoded[0].op, "index");
        assert_eq!(decoded[0].payload["field"], "value");
    }

    #[test]
    fn partial_frame_at_eof_is_skipped() {
        let entry = TranslogEntry {
            seq_no: 0,
            op: "index".into(),
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
        tl.append("index", json!({"data": big})).unwrap();
        let entries = tl.read_all().unwrap();
        assert_eq!(entries[0].payload["data"].as_str().unwrap().len(), 100_000);
    }

    // ── read_from ───────────────────────────────────────────────────────

    #[test]
    fn read_from_returns_entries_after_checkpoint() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap(); // seq 0
        tl.append("index", json!({"b": 2})).unwrap(); // seq 1
        tl.append("index", json!({"c": 3})).unwrap(); // seq 2

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
        tl.append("index", json!({"a": 1})).unwrap(); // seq 0
        tl.append("index", json!({"b": 2})).unwrap(); // seq 1

        // read_from filters > after_seq_no, so read_from(0) skips seq 0
        let entries = tl.read_from(0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq_no, 1);
    }

    #[test]
    fn read_from_returns_empty_when_all_below_checkpoint() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();

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
    fn truncate_below_retains_entries_above_checkpoint() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap(); // seq 0
        tl.append("index", json!({"b": 2})).unwrap(); // seq 1
        tl.append("index", json!({"c": 3})).unwrap(); // seq 2
        tl.append("index", json!({"d": 4})).unwrap(); // seq 3

        tl.truncate_below(1).unwrap();

        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq_no, 2);
        assert_eq!(entries[1].seq_no, 3);
    }

    #[test]
    fn truncate_below_preserves_seq_no() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();

        tl.truncate_below(0).unwrap();

        let e = tl.append("index", json!({"c": 3})).unwrap();
        assert_eq!(e.seq_no, 2, "seq_no should continue after truncate_below");
    }

    #[test]
    fn truncate_below_at_max_clears_all() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();

        tl.truncate_below(10).unwrap();

        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn truncate_below_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let tl = HotTranslog::open(dir.path()).unwrap();
            tl.append("index", json!({"a": 1})).unwrap();
            tl.append("index", json!({"b": 2})).unwrap();
            tl.append("index", json!({"c": 3})).unwrap();
            tl.truncate_below(1).unwrap();
        }
        let tl2 = HotTranslog::open(dir.path()).unwrap();
        let entries = tl2.read_all().unwrap();
        assert_eq!(entries.len(), 1, "only seq 2 should survive reopen");
        assert_eq!(entries[0].seq_no, 2);
        // seq_no should continue from 3
        let e = tl2.append("index", json!({"d": 4})).unwrap();
        assert_eq!(e.seq_no, 3);
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
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();
        assert_eq!(tl.last_seq_no(), 1);
    }

    #[test]
    fn last_seq_no_preserved_after_truncate() {
        let (_dir, tl) = open_translog();
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();
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
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();

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
        let ops: Vec<(&str, serde_json::Value)> =
            vec![("index", json!({"a": 1})), ("index", json!({"b": 2}))];
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
        let ops: Vec<(&str, serde_json::Value)> =
            vec![("index", json!({"a": 1})), ("index", json!({"b": 2}))];
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
        tl.append("index", json!({"a": 1})).unwrap();
        // Manual sync should succeed without error
        tl.sync().unwrap();
    }

    #[test]
    fn start_sync_task_returns_none_for_request_mode() {
        let dir = tempfile::tempdir().unwrap();
        let tl = HotTranslog::open(dir.path()).unwrap();
        assert!(tl.start_sync_task().is_none());
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
        tl.append("index", json!({"a": 1})).unwrap();
        tl.append("index", json!({"b": 2})).unwrap();

        tl.truncate().unwrap();
        let entries = tl.read_all().unwrap();
        assert!(entries.is_empty());

        // seq_no preserved
        let e = tl.append("index", json!({"c": 3})).unwrap();
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
            tl.append("index", json!({"a": 1})).unwrap();
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
        tl.append("index", json!({"a": 1})).unwrap();
        // sync() should succeed even in request mode (already fsynced)
        tl.sync().unwrap();
    }

    #[test]
    fn poisoned_seq_lock_is_recovered_for_reads_and_appends() {
        let (_dir, tl) = open_translog();

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = tl.seq_no.lock().unwrap();
            panic!("poison seq lock");
        }));

        assert_eq!(tl.current_seq_no(), 0);

        let entry = tl.append("index", json!({"recovered": true})).unwrap();
        assert_eq!(entry.seq_no, 0);
        assert_eq!(tl.next_seq_no(), 1);
    }

    #[test]
    fn poisoned_file_lock_is_recovered_for_subsequent_io() {
        let (_dir, tl) = open_translog();

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = tl.file.lock().unwrap();
            panic!("poison file lock");
        }));

        tl.append("index", json!({"doc": 1})).unwrap();
        let entries = tl.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].payload["doc"], 1);
    }
}
