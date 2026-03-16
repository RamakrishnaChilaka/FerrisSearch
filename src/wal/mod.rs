//! Write-Ahead Log (WAL)
//!
//! Provides crash durability for indexed documents.
//! Every index operation is appended here BEFORE being written to the engine buffer.
//! On crash + restart, uncommitted entries are replayed into the engine.
//! After a successful flush (engine commit), the WAL is truncated.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

/// A single WAL entry, representing one indexing operation.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TranslogEntry {
    /// Monotonically increasing sequence number for ordering
    pub seq_no: u64,
    /// The operation type (currently only "index")
    pub op: String,
    /// The full document payload
    pub payload: serde_json::Value,
}

/// Trait abstracting a Write-Ahead Log backend.
/// The current default is `HotTranslog` (JSONL file, fsync-per-write).
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
}

/// Hot translog — JSONL-backed, fsync-on-every-write WAL for maximum durability.
///
/// Stored as newline-delimited JSON (JSONL) at `<data_dir>/translog.jsonl`.
/// Each line is a serialized `TranslogEntry`. The file is fsynced on every write
/// (request-level durability) to guarantee no ops are lost on crash.
pub struct HotTranslog {
    file: Mutex<File>,
    seq_no: Mutex<u64>,
}

impl HotTranslog {
    /// Open or create the translog at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let path = data_dir.as_ref().join("translog.jsonl");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Figure out the highest sequence number already on disk
        let existing = Self::read_entries_from_path(&path)?;
        let next_seq = existing.last().map(|e| e.seq_no + 1).unwrap_or(0);

        tracing::info!(
            "Translog opened at {:?} ({} pending entries, next seq_no: {})",
            path,
            existing.len(),
            next_seq
        );

        Ok(Self {
            file: Mutex::new(file),
            seq_no: Mutex::new(next_seq),
        })
    }

    /// Helper to read entries directly from a path (used during open to check existing state)
    fn read_entries_from_path<P: AsRef<Path>>(path: P) -> Result<Vec<TranslogEntry>> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<TranslogEntry>(&line) {
                entries.push(entry);
            }
        }
        Ok(entries)
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

        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');

        let mut file = self.file.lock().unwrap();
        file.write_all(line.as_bytes())?;
        file.sync_data()?;

        Ok(entry)
    }

    fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>> {
        let mut seq = self.seq_no.lock().unwrap();
        let mut entries = Vec::with_capacity(ops.len());
        let mut buf = String::new();

        for (op, payload) in ops {
            let entry = TranslogEntry {
                seq_no: *seq,
                op: op.to_string(),
                payload: payload.clone(),
            };
            *seq += 1;
            buf.push_str(&serde_json::to_string(&entry)?);
            buf.push('\n');
            entries.push(entry);
        }

        let mut file = self.file.lock().unwrap();
        file.write_all(buf.as_bytes())?;
        file.sync_data()?;

        Ok(entries)
    }

    fn read_all(&self) -> Result<Vec<TranslogEntry>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let reader = BufReader::new(&*file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<TranslogEntry>(&line) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.sync_data()?;
        drop(file);
        let mut seq = self.seq_no.lock().unwrap();
        *seq = 0;
        tracing::info!("Translog truncated after flush.");
        Ok(())
    }
}
