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
use std::sync::Mutex;

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

    /// Append multiple operations with a single fsync (bulk optimization).
    fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>>;

    /// Read all pending entries (used for replay on startup).
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;

    /// Truncate after a successful commit — data is safe in engine segments.
    fn truncate(&self) -> Result<()>;

    /// Get the last assigned sequence number (highest seq_no written so far).
    /// Returns 0 if no writes have occurred.
    fn last_seq_no(&self) -> u64;
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
                tracing::warn!("Translog has partial entry at end, ignoring {} bytes", payload_len);
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

/// Hot translog — binary length-prefixed, fsync-on-every-write WAL for maximum durability.
///
/// Stored at `<data_dir>/translog.bin`. Each entry is a bincode-encoded frame
/// preceded by a 4-byte little-endian length. The file is fsynced on every write
/// (request-level durability) to guarantee no ops are lost on crash.
///
/// The sequence number is monotonically increasing and *never resets*, even after
/// truncation. This enables replica recovery via seq_no-based translog replay.
pub struct HotTranslog {
    file: Mutex<File>,
    seq_no: Mutex<u64>,
    /// Path to the file that persists the seq_no high-water mark across truncations.
    seq_no_path: std::path::PathBuf,
}

impl HotTranslog {
    /// Open or create the translog at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let path = data_dir.join("translog.bin");
        let seq_no_path = data_dir.join("translog.seqno");

        let file = OpenOptions::new()
            .create(true)
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

        tracing::info!(
            "Translog opened at {:?} ({} pending entries, next seq_no: {})",
            path,
            existing.len(),
            next_seq
        );

        Ok(Self {
            file: Mutex::new(f),
            seq_no: Mutex::new(next_seq),
            seq_no_path,
        })
    }

    /// Get the current (next) sequence number without incrementing.
    pub fn current_seq_no(&self) -> u64 {
        *self.seq_no.lock().unwrap()
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
        let mut seq = self.seq_no.lock().unwrap();
        let entry = TranslogEntry {
            seq_no: *seq,
            op: op.to_string(),
            payload,
        };
        *seq += 1;

        let frame = encode_entry(&entry)?;

        let mut file = self.file.lock().unwrap();
        file.write_all(&frame)?;
        file.sync_data()?;

        Ok(entry)
    }

    fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>> {
        let mut seq = self.seq_no.lock().unwrap();
        let mut entries = Vec::with_capacity(ops.len());
        let mut buf = Vec::new();

        for (op, payload) in ops {
            let entry = TranslogEntry {
                seq_no: *seq,
                op: op.to_string(),
                payload: payload.clone(),
            };
            *seq += 1;
            buf.extend_from_slice(&encode_entry(&entry)?);
            entries.push(entry);
        }

        let mut file = self.file.lock().unwrap();
        file.write_all(&buf)?;
        file.sync_data()?;

        Ok(entries)
    }

    fn read_all(&self) -> Result<Vec<TranslogEntry>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&*file);
        decode_entries(&mut reader)
    }

    fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.sync_data()?;
        drop(file);

        // Persist the current seq_no so it survives truncation.
        // This ensures seq_no never goes backward after flush.
        let seq = self.seq_no.lock().unwrap();
        std::fs::write(&self.seq_no_path, seq.to_string())?;

        tracing::info!("Translog truncated after flush (seq_no preserved at {}).", *seq);
        Ok(())
    }

    fn last_seq_no(&self) -> u64 {
        let seq = self.seq_no.lock().unwrap();
        if *seq == 0 { 0 } else { *seq - 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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

    // ── append_bulk ─────────────────────────────────────────────────────

    #[test]
    fn append_bulk_writes_atomically() {
        let (_dir, tl) = open_translog();
        let ops: Vec<(&str, serde_json::Value)> = (0..5)
            .map(|i| ("index", json!({"doc": i})))
            .collect();
        let entries = tl.append_bulk(&ops).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[4].seq_no, 4);

        let all = tl.read_all().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn bulk_then_single_seq_no_continues() {
        let (_dir, tl) = open_translog();
        tl.append_bulk(&[("index", json!({"a": 1})), ("index", json!({"b": 2}))]).unwrap();
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
        assert_eq!(e.seq_no, 1, "seq_no should continue from where it left off after truncate");
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
        assert_eq!(e.seq_no, 2, "seq_no should continue from where previous instance left off");
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
        assert_eq!(decoded.len(), 1, "should recover the first valid entry and skip the partial");
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
}
