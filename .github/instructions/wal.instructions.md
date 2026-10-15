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
    pub seq_no: u64,        // primary-assigned identity
    pub primary_term: u64,
    pub op: WalOperation,   // Index, Delete, or NoOp
    pub payload: Value,     // document JSON
}
```

## WriteAheadLog Trait
```rust
pub trait WriteAheadLog: Send + Sync {
    fn append(&self, primary_term: u64, op: WalOperation, payload: Value) -> Result<TranslogEntry>;
    fn append_with_seq(&self, seq_no: u64, primary_term: u64, op: WalOperation, payload: Value) -> Result<TranslogEntry>;
    fn append_bulk(&self, primary_term: u64, ops: &[(WalOperation, Value)]) -> Result<Vec<TranslogEntry>>;
    fn write_bulk(&self, primary_term: u64, ops: &[(WalOperation, Value)]) -> Result<()>;
    fn write_bulk_with_receipt(&self, primary_term: u64, ops: &[(WalOperation, Value)]) -> Result<Option<u64>>;
    fn write_bulk_with_start_seq(&self, start_seq_no: u64, primary_term: u64, ops: &[(WalOperation, Value)]) -> Result<()>;
    fn append_batch_with_seq(&self, entries: &[SequencedWalEntry]) -> Result<Vec<TranslogEntry>>;
    fn write_document_with_receipt(&self, primary_term: u64, operation: WalDocumentOperation<'_>) -> Result<u64>;
    fn write_document_bulk_with_receipt(&self, primary_term: u64, operations: &[WalDocumentOperation<'_>]) -> Result<Option<u64>>;
    fn write_document_batch_with_seq(&self, entries: &[BorrowedSequencedWalEntry<'_>]) -> Result<()>;
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;
    fn find_entry(&self, seq_no: u64) -> Result<Option<TranslogEntry>>;
    fn find_entry_position(&self, seq_no: u64, primary_term: u64) -> Result<Option<WalCursor>>;
    fn entry_positions(&self, start: WalCursor, count: usize) -> Result<Vec<WalCursor>>;
    fn read_entry_at(&self, position: WalCursor) -> Result<Option<TranslogEntry>>;
    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>>;  // replica recovery
    fn truncate(&self) -> Result<()>;
    fn truncate_below(&self, global_checkpoint: u64) -> Result<()>;  // retain above for recovery
    fn last_seq_no(&self) -> u64;
    fn next_seq_no(&self) -> u64;
    fn size_bytes(&self) -> Result<u64>;  // auto-flush threshold check
    fn for_each_from(&self, min_seq_no: u64, callback: &mut dyn FnMut(TranslogEntry) -> Result<()>) -> Result<u64>;  // streaming replay
    fn for_each_from_at(&self, min_seq_no: u64, callback: &mut dyn FnMut(WalCursor, TranslogEntry) -> Result<()>) -> Result<u64>;
    fn register_retention_pin(&self, min_seq_no: u64) -> Result<u64>;
    fn recovery_read_snapshot(&self) -> Result<TranslogReadSnapshot>;
    fn release_retention_pin(&self, pin_id: u64) -> Result<()>;
    fn min_retention_pin(&self) -> Option<u64>;
}
```

## HotTranslog (Generation-Based Binary Implementation)
### Wire Format
`[u32 LE: payload_len][bincode(WireEntryV4 { format_version, seq_no, primary_term, op, payload_json })][SHA-256 of encoded entry]`
- The length includes the checksum; the 32 MiB ceiling includes the prefix.
- On open, truncates only an incomplete, unprotected active-generation tail.
  Protected-range loss/mismatch and incomplete retained frames fail closed.
- Primary allocation is contiguous, but explicit replica/recovery entries may
  arrive out of numeric order; generation metadata stores min/max ranges
- Index/delete payloads require the primary-assigned nonzero `_version`; NoOp
  payloads have no document version. Missing or zero document versions are WAL
  corruption and fail closed.

### Files on Disk (per shard)
- `{data_dir}/{index_uuid}/shard_{id}/translog-<generation>.bin` — ordered WAL generation files (`00000000000000000000`, `00000000000000000001`, ...)
- `{data_dir}/{index_uuid}/shard_{id}/translog.manifest` — checksummed v3
  protected descriptor: generations/ranges/physical lengths/hashes, exclusive
  next sequence, and exact durable intervals. No sequence sidecar is written.
- `{data_dir}/{index_uuid}/shard_{id}/translog.committed` — versioned JSON
  committed boundary with processed/persisted checkpoints, max sequence, term,
  and current-term interval state

## Key Behaviors
- `append()` returns the assigned seq_no in the TranslogEntry
- `write_bulk_with_receipt()` returns the first sequence reserved under the WAL
  lock; the input length determines its contiguous range. Empty input returns
  `None` without allocating a sequence. `write_bulk()` is the discard-receipt
  convenience wrapper.
- Primary sequence exhaustion and overflowing explicit bulk ranges must fail
  before any bytes are written; never wrap or reuse a saturated allocator value.
- Request-durable receipts follow WAL sync and atomic protected-manifest
  publication including directory fsync. A WAL write/sync or publication error
  closes the shared failure cell immediately. Issued permits and later WAL,
  replay, mutation, and recovery operations observe the same first failure.
  Do not infer durability from read-back or a later successful sync.
- `MAX_WAL_FRAME_BYTES` is 32 MiB including the four-byte length prefix. Every
  single, delete, primary-bulk item, and explicit-sequence replica/recovery item
  is fully encoded and checked before any WAL bytes or sequence state change.
  The effective `_source` limit is slightly smaller because the encoded frame
  also contains `_doc_id`, `_source`, operation, sequence, and bincode metadata.
- Restart scan, replay, and recovery enforce the same 32 MiB frame ceiling as
  new writes.
- `append_with_seq()` persists a caller-supplied `(primary_term, seq_no)` and
  advances the local allocator past it.
- `append_batch_with_seq()` preserves arbitrary physical input order for live
  replica apply and peer recovery; do not sort it by sequence.
- Checkpoint-aware and full flushes retain the WAL generation containing the
  oldest live delete tombstone. Restart and primary activation rebuild retained
  committed document versions before replaying the uncommitted suffix. Once
  tombstone retention prunes that metadata, later flushes may reclaim the WAL.
- Receipt-only document writes serialize borrowed `WalDocumentOperation`
  envelopes without constructing a deep JSON copy or unused return entries.
  They share the existing append mutation logic, frame limits, sequence
  overflow checks, fsync behavior, and test hooks. Borrowed and owned encoders
  must produce byte-identical frames, including document-envelope key order.
- `write_document_batch_with_seq()` preserves arbitrary physical input order
  and explicit primary term/sequence identity, like `append_batch_with_seq()`.
- `write_bulk_with_start_seq()` is only the contiguous explicit-sequence helper.
- `read_from(seq_no)` is a test-only sequence filter, not a peer-recovery
  pagination cursor.
- `for_each_from(seq_no, callback)` streams entries with seq_no >= the given value without loading the whole WAL into memory (used by startup replay)
- `for_each_from_at()` also returns the exact generation/byte offset before
  each decoded frame. Filtering never changes those physical positions.
  Realtime GET seeks directly with `read_entry_at()`; an unretained generation
  returns `None`, while I/O or malformed frames fail explicitly.
  Engine callers hold the translog mutex through lookup/read and publish a
  covering reader before truncation. `entry_positions()` scans only frame
  headers after a contiguous append. Do not serialize sources again to
  calculate offsets or rescan the WAL per replayed document.
  Track the checked byte offset locally and use buffered relative skips;
  do not discard the read buffer or query the file position per frame.
- `size_bytes()` returns the summed size of all retained generations so the engine can trigger checkpoint-aware auto-flush
- `truncate_below(global_checkpoint)` rolls to a new empty generation and deletes only generations whose max seq_no is ≤ the checkpoint; it does NOT rewrite mixed generations in place
- `truncate()` rolls to a new empty generation and deletes all older generations
- Recovery retention pins protect every operation at or above their exclusive
  boundary. Both `truncate()` and `truncate_below()` prune only below the
  lowest active pin; a pin at zero prevents history pruning.
- `read_bounded_cursor()` paginates by generation and byte offset in physical
  order. Sequence filtering never determines the next cursor, so a physically
  later lower sequence cannot be skipped.
- Recovery may use `read_bounded_cursor_while()` to stop immediately before the
  first frame the source has not processed. The returned cursor remains at that
  frame so source replay can make it eligible without restarting the session.
- The lock protects only capture and validation of the exclusive head and
  generation-list clone. File scanning runs after releasing it. Recovery scans
  enforce the 32 MiB frame ceiling and use relative seeks for
  bounded pre-cursor frames after decoding only their sequence prefix. An
  incomplete frame is a concurrent append only in the final captured generation
  when it starts at or beyond that generation's captured `size_bytes`; it ends
  the scan cleanly only after all pre-head operations are accounted for.
  Incomplete frames elsewhere and torn-then-appended frames are corruption.
- `initialize_empty_at()` initializes the target allocator at source
  `max_seq_no + 1`; the exact committed checkpoint state is installed
  separately from the source boundary record.
- `next_seq_no()` returns the exclusive next seq_no; this is what gets persisted on commit paths
- `translog.committed` may advance only from a successful Tantivy commit
  boundary. Flush and checkpoint-aware truncation validate that the persisted
  boundary equals the current WAL head before deleting history; a failed commit
  must leave both the checkpoint and WAL intact.
- Any failed Tantivy commit invalidates its writer. Before a later write appends
  a new operation, or before blocking maintenance/snapshot commit continues,
  writer reconstruction replays `[translog.committed, next_seq_no)` with the
  same idempotent replay logic used at startup. Best-effort try-flush may defer
  instead. Reconstruction is permitted only while the shared copy state is
  healthy; it cannot clear WAL/post-WAL fail-stop. A failed write-triggered
  reconstruction fail-stops the copy instead of consuming an Apply retry budget.
- WAL document interpretation is shared by startup/runtime replay and peer
  recovery. Every operation requires `_doc_id`;
  index operations additionally require `_source`. Missing fields are typed
  corruption. Replay deletes the ID for every operation and adds a document
  back only for `Index`.
- ID-bearing writes store `_client_operation` in the same WAL document
  envelope as the mutation. The record contains the validated client ID,
  request fingerprint, and original result. Restore the bounded deduplication
  state from exact WAL entries before replay; never synthesize an operation
  record from checkpoints or current document state.
- Keep the latest 4,096 shard sequence operations that carry client IDs from
  truncation even after their mutations are committed. This is a bounded retry
  window, not an unbounded exactly-once ledger.
- Writer reconstruction holds the translog lock for the entire suffix so no new
  append can race recovery. This blocks writes to that shard and may scan a
  large suffix when refresh is disabled.
- Async durability: background task fsyncs every `sync_interval_ms` via Tokio's blocking pool — never call `File::sync_data()` inline on an async worker
- Reopen requires a valid protected `translog.manifest` for existing data and
  verifies every protected generation's physical bytes/hash and durable
  sequence evidence. Missing descriptor/generation or protected data loss fails
  closed; never reinterpret an existing copy as a fresh WAL.
- On open, an incomplete trailing frame in the active generation is truncated
  wholly beyond the protected frontier is truncated to the last complete,
  fully decoded boundary. The generation file and parent
  directory are fsynced before opening the append writer, and the discarded
  byte count is logged. Complete malformed frames, corrupt middle frames, and
  incomplete frames in retained non-active generations fail closed.
- `HotTranslog::open*()` is a mutating, exclusive startup operation: it may
  remove unreferenced generations and truncate an incomplete active tail.
  Never call it against a shard with a live engine/writer. Runtime recovery and
  diagnostics must read through the live engine's captured generation state.
- Unknown operation tags in persisted entries are corruption errors: reopen/replay must return `Err`, not panic
- Manifest, frame, operation-tag, payload, sequence-state, and other
  persisted WAL decode/validation failures carry a typed corruption cause so
  shard lifecycle can fail the exact allocation immediately. Ordinary I/O
  errors retain their source. Ordinary assigned-open I/O may use bounded
  retry/backoff; live WAL/post-WAL mutation uncertainty fails immediately.
- Persist the manifest before deleting obsolete generation files during `truncate()` / `truncate_below()` so crashes never leave startup without authoritative generation metadata
- `translog.committed` should be persisted after each intermediate replay batch commit so replay remains idempotent across repeated crash recovery

## Seq Ownership Invariant
- Primary-originated writes use `append()` / `append_bulk()` and allocate new seq_nos locally
- Replica apply and recovery replay MUST use explicit-sequence append APIs so
  all shard copies persist the primary's term/sequence identity.
- Never let a replica invent fresh WAL identities for a replicated operation;
  this breaks failover, redelivery, and recovery semantics.
- Carry primary-assigned receipts through the engine and transport layers.
  Reading the allocator/checkpoint again after releasing the write lock cannot
  recover the identity of an earlier operation.

## Write-Failure And Restart Boundary

A failed `write_all`, `sync_data`, or protected-descriptor publication prevents
later mutation in that process. Restart verifies retained protected bytes;
unsealed valid records remain evidence for fresh canonical reconciliation.
Only a proven unprotected torn tail may be discarded. Tail repair cannot
restore an old primary epoch, and an acknowledged above-gap operation is not
discardable merely because a contiguous checkpoint lags.
