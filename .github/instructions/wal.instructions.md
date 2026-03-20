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
    pub op: String,         // "index" or "delete"
    pub payload: Value,     // document JSON
}
```

## WriteAheadLog Trait
```rust
pub trait WriteAheadLog: Send + Sync {
    fn append(&self, op: &str, payload: Value) -> Result<TranslogEntry>;
    fn append_with_seq(&self, seq_no: u64, op: &str, payload: Value) -> Result<TranslogEntry>;
    fn append_bulk(&self, ops: &[(&str, Value)]) -> Result<Vec<TranslogEntry>>;
    fn write_bulk(&self, ops: &[(&str, Value)]) -> Result<()>;
    fn write_bulk_with_start_seq(&self, start_seq_no: u64, ops: &[(&str, Value)]) -> Result<()>;
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;
    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>>;  // replica recovery
    fn truncate(&self) -> Result<()>;
    fn truncate_below(&self, global_checkpoint: u64) -> Result<()>;  // retain above for recovery
    fn last_seq_no(&self) -> u64;
    fn next_seq_no(&self) -> u64;
}
```

## HotTranslog (Binary Implementation)
### Wire Format
`[u32 LE: payload_len][bincode(WireEntry { seq_no, op, payload_json })]`
- Length-prefixed frames for efficient sequential reading
- Handles partial writes at EOF gracefully (skips/truncates corrupted tail)
- Seq numbers are monotonically increasing, persisted in `.seqno` sidecar file

### Files on Disk (per shard)
- `{data_dir}/{index}/shard_{id}/translog.bin` — the WAL file
- `{data_dir}/{index}/shard_{id}/translog.seqno` — last assigned sequence number
- `{data_dir}/{index}/shard_{id}/translog.committed` — exclusive committed seq_no used to skip already committed entries on restart

## Key Behaviors
- `append()` returns the assigned seq_no in the TranslogEntry
- `append_with_seq()` persists a caller-supplied seq_no and advances the local allocator past it
- `write_bulk_with_start_seq()` persists contiguous caller-supplied seq_nos for replica/recovery bulk apply
- `read_from(seq_no)` reads all entries with seq_no > the given value (used for replica recovery)
- `truncate_below(global_checkpoint)` removes entries ≤ global_checkpoint, keeps entries above for replica recovery
- `truncate()` clears entire log (used on full flush)
- `next_seq_no()` returns the exclusive next seq_no; this is what gets persisted on commit paths
- Async durability: background task fsyncs every `sync_interval_ms` — safe when replicas provide durability

## Seq Ownership Invariant
- Primary-originated writes use `append()` / `append_bulk()` and allocate new seq_nos locally
- Replica apply and recovery replay MUST use `append_with_seq()` / `write_bulk_with_start_seq()` so all shard copies persist the primary's seq space
- Never let a replica invent fresh WAL seq_nos for a replicated operation — this breaks failover and `read_from()` semantics
