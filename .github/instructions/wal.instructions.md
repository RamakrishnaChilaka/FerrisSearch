---
description: "Use for the generation-based translog, sequence allocation, durability, truncation, corruption handling, and replay."
applyTo: "src/wal/**"
---

# WAL Module — src/wal/mod.rs

## TranslogDurability
```rust
pub enum TranslogDurability {
    Request,                           // fsync per write (default, no data loss)
    Async { sync_interval_ms: u64 },   // background fsync timer (faster, up to sync_interval data loss)
}
```

## TranslogEntry
```rust
pub struct TranslogEntry {
    pub seq_no: u64,        // monotonic, survives truncation (persisted in .seqno file)
    pub op: WalOperation,   // Index or Delete
    pub payload: Value,     // document JSON
}
```

## WriteAheadLog Trait
```rust
pub trait WriteAheadLog: Send + Sync {
    fn append(&self, op: WalOperation, payload: Value) -> Result<TranslogEntry>;
    fn append_with_seq(&self, seq_no: u64, op: WalOperation, payload: Value) -> Result<TranslogEntry>;
    fn append_bulk(&self, ops: &[(WalOperation, Value)]) -> Result<Vec<TranslogEntry>>;
    fn write_bulk(&self, ops: &[(WalOperation, Value)]) -> Result<()>;
    fn write_bulk_with_receipt(&self, ops: &[(WalOperation, Value)]) -> Result<Option<u64>>;
    fn write_bulk_with_start_seq(&self, start_seq_no: u64, ops: &[(WalOperation, Value)]) -> Result<()>;
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;
    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>>;  // replica recovery
    fn truncate(&self) -> Result<()>;
    fn truncate_below(&self, global_checkpoint: u64) -> Result<()>;  // retain above for recovery
    fn last_seq_no(&self) -> u64;
    fn next_seq_no(&self) -> u64;
    fn size_bytes(&self) -> Result<u64>;  // auto-flush threshold check
    fn for_each_from(&self, min_seq_no: u64, callback: &mut dyn FnMut(TranslogEntry) -> Result<()>) -> Result<u64>;  // streaming replay
    fn register_retention_pin(&self, min_seq_no: u64) -> Result<u64>;
    fn release_retention_pin(&self, pin_id: u64) -> Result<()>;
    fn min_retention_pin(&self) -> Option<u64>;
}
```

## HotTranslog (Generation-Based Binary Implementation)
### Wire Format
`[u32 LE: payload_len][bincode(WireEntry { seq_no, op, payload_json })]`
- Length-prefixed frames for efficient sequential reading
- Handles partial writes at EOF gracefully (skips/truncates corrupted tail)
- Seq numbers are monotonically increasing, persisted in `.seqno` sidecar file

### Files on Disk (per shard)
- `{data_dir}/{index_uuid}/shard_{id}/translog-<generation>.bin` — ordered WAL generation files (`00000000000000000000`, `00000000000000000001`, ...)
- `{data_dir}/{index_uuid}/shard_{id}/translog.manifest` — authoritative generation metadata (active generation, next generation id, seq ranges, sizes)
- `{data_dir}/{index_uuid}/shard_{id}/translog.seqno` — last assigned sequence number
- `{data_dir}/{index_uuid}/shard_{id}/translog.committed` — exclusive committed seq_no used to skip already committed entries on restart

## Key Behaviors
- `append()` returns the assigned seq_no in the TranslogEntry
- `write_bulk_with_receipt()` returns the first sequence reserved under the WAL
  lock; the input length determines its contiguous range. Empty input returns
  `None` without allocating a sequence. `write_bulk()` is the discard-receipt
  compatibility wrapper.
- Primary sequence exhaustion and overflowing explicit bulk ranges must fail
  before any bytes are written; never wrap or reuse a saturated allocator value.
- `MAX_WAL_FRAME_BYTES` is 32 MiB including the four-byte length prefix. Every
  single, delete, primary-bulk item, and explicit-sequence replica/recovery item
  is fully encoded and checked before any WAL bytes or sequence state change.
  The effective `_source` limit is slightly smaller because the encoded frame
  also contains `_doc_id`, `_source`, operation, sequence, and bincode metadata.
- `append_with_seq()` persists a caller-supplied seq_no and advances the local allocator past it
- `write_bulk_with_start_seq()` persists contiguous caller-supplied seq_nos for replica/recovery bulk apply
- `read_from(seq_no)` scans all generations in order and returns entries with seq_no > the given value (used for replica recovery)
- `for_each_from(seq_no, callback)` streams entries with seq_no >= the given value without loading the whole WAL into memory (used by startup replay)
- `size_bytes()` returns the summed size of all retained generations so the engine can trigger checkpoint-aware auto-flush
- `truncate_below(global_checkpoint)` rolls to a new empty generation and deletes only generations whose max seq_no is ≤ the checkpoint; it does NOT rewrite mixed generations in place
- `truncate()` rolls to a new empty generation and deletes all older generations
- Recovery retention pins protect every operation at or above their exclusive
  boundary. Both `truncate()` and `truncate_below()` prune only below the
  lowest active pin; a pin at zero prevents history pruning.
- `read_bounded_range()` reads an inclusive/exclusive sequence window without
  opening another append writer and reports whether the bounded response
  reached the captured head. Recovery reads use the live generation list under
  the translog state lock, not a potentially lagging on-disk manifest.
- The lock protects only capture and validation of the exclusive head and
  generation-list clone. File scanning runs after releasing it. Recovery scans
  use the same 32 MiB total-frame cap, fully decode a complete frame that reaches
  the captured head, and use relative seeks to skip bounded pre-cursor frames
  after decoding only their sequence prefix. A partial frame whose decoded
  sequence is at or beyond the captured head is a concurrent append and ends
  the scan cleanly; a frame below the head that extends past EOF is corruption,
  while an incomplete sequence prefix is treated as EOF and cannot report
  completion until every pre-head operation was read.
- `initialize_empty_at()` creates the empty target WAL/high-water state at a
  file snapshot's exclusive boundary.
- `next_seq_no()` returns the exclusive next seq_no; this is what gets persisted on commit paths
- Async durability: background task fsyncs every `sync_interval_ms` via Tokio's blocking pool — never call `File::sync_data()` inline on an async worker
- Reopen requires `translog.manifest`; it trusts persisted metadata for old generations, removes stray generation files not listed in the manifest, ignores unrelated non-generation side files, and scans only the active generation file to recover the allocator high-water mark
- Unknown operation tags in persisted entries are corruption errors: reopen/replay must return `Err`, not panic
- Persist the manifest before deleting obsolete generation files during `truncate()` / `truncate_below()` so crashes never leave startup without authoritative generation metadata
- `translog.committed` should be persisted after each intermediate replay batch commit so replay remains idempotent across repeated crash recovery

## Seq Ownership Invariant
- Primary-originated writes use `append()` / `append_bulk()` and allocate new seq_nos locally
- Replica apply and recovery replay MUST use `append_with_seq()` / `write_bulk_with_start_seq()` so all shard copies persist the primary's seq space
- Never let a replica invent fresh WAL seq_nos for a replicated operation — this breaks failover and `read_from()` semantics
- Carry primary-assigned receipts through the engine and transport layers.
  Reading the allocator/checkpoint again after releasing the write lock cannot
  recover the identity of an earlier operation.
