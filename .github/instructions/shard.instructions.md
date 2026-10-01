---
description: "Use for shard ownership, UUID-backed data paths, open and close scheduling, settings application, and ISR state."
applyTo: "src/shard/**"
---

# Shard Module — src/shard/mod.rs

## ShardManager
```rust
pub struct ShardManager {
    data_dir: PathBuf,
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    settings_managers: RwLock<HashMap<String, Arc<SettingsManager>>>,  // per-index
    index_uuids: RwLock<HashMap<String, String>>,  // index_name → UUID for on-disk dirs
    copy_identities: RwLock<HashMap<ShardKey, ShardCopyIdentity>>,
    pub isr_tracker: IsrTracker,
    durability: TranslogDurability,
}
```

### Key Methods
- `open_shard(index, shard_id)` — creates CompositeEngine with a generated per-index UUID for local/test helpers
- `open_shard_with_mappings(index, shard_id, mappings)` — with field type info, reuses the same generated per-index UUID for local/test helpers
- `open_shard_with_settings(index, shard_id, mappings, settings, index_uuid)` — with UUID, SettingsManager + reactive refresh loop + vector rebuild
- `open_shard_with_settings_blocking(index, shard_id, mappings, settings, index_uuid)` — async-safe Tokio wrapper for shard open/recovery work
- `open_assigned_shard_with_settings*()` — authoritative open that requires the
  expected allocation ID and term and permits empty creation only under G1
- `open_shard_with_settings_strict*()` — recovery install open that never invokes the schema-mismatch wipe fallback
- `prepare_peer_recovery_target_blocking()` / `finalize_peer_recovery_target_blocking()` — close and wipe one out-of-sync copy, persist the marker, initialize WAL state, verify the commit files, and publish the opened engine
- `get_shard(index, shard_id) -> Option<Arc<dyn SearchEngine>>`
- `get_index_shards(index) -> Vec<(u32, Arc<dyn SearchEngine>)>`
- `all_shards() -> Vec<(ShardKey, Arc<dyn SearchEngine>)>`
- `close_index_shards(index)` — remove engines, clean ISR, delete UUID-based directory
- `close_index_shards_with_reason(index, reason)` — same as above, but emits the delete reason in logs for destructive paths
- `close_index_shards_blocking(index)` — async-safe Tokio wrapper for shard shutdown + directory deletion
- `close_index_shards_blocking_with_reason(index, reason)` — async-safe Tokio wrapper for reason-tagged destructive delete paths
- `apply_settings(index, new_settings)` — push to SettingsManager watch channels
- `register_index_uuid(index, uuid)` — store the UUID mapping for an index
- `index_uuid(index) -> Option<String>` — get registered UUID
- `shard_data_dir(index, shard_id) -> Option<PathBuf>` — on-disk path using UUID
- `cleanup_orphaned_data(known_uuids)` — delete dirs not matching any authoritative known UUID
- `cleanup_orphaned_data_blocking(known_uuids)` — async-safe Tokio wrapper for orphan cleanup
- `raise_copy_fence_blocking(...)` — atomically persist a monotonic replica fence
- `apply_replica_operation(...)` — serialize identity/gate/fence validation with replica mutation
- `quarantine_shard_copy_blocking(...)` — stop serving an invalid copy without deleting evidence
- `restore_peer_recovery_awaiting_membership(...)` — validate an exact durable
  pending marker, restore its in-memory gate, and reopen the finalized existing
  copy before recovery scheduling
- `reset_peer_recovery_target_for_retry_blocking(...)` — remove a controlled
  partial install without manufacturing a failed-copy report

### Durable Copy Identity
- OCC/create conflicts are normal pre-WAL rejections, not storage failures.
  They must not consume Apply retry budget or quarantine the copy.
- Every served assigned copy has `<data_dir>/<uuid>/shard_<id>/SHARD_COPY_IDENTITY.json`.
- The versioned JSON contains index UUID, allocation ID, and durable replica
  fence plus an allocation-bound collision-quarantine flag. Updates use temp
  write, file fsync, rename, and directory fsync.
- Assigned opens load and validate the file before publishing an engine.
  Missing, malformed, or mismatched identity fails closed.
- Identity, marker, WAL, and Tantivy decode/validation failures are definitive.
  Sequence-state corruption, including disagreement between the durable
  identity fence maximum and the committed term-start maximum, is also
  definitive and must fail the exact copy instead of consuming the I/O retry
  window.
  Other filesystem/engine I/O uses a shared per-copy retry budget: exponential
  1–5 second backoff, at least three failed attempts, and a 60-second minimum
  window before persistent-I/O escalation.
- Retry state is keyed by operation. Apply-level WAL/fsync/engine failures,
  including a writer left unavailable by failed force-merge replacement, use
  the Apply key. A write first attempts one failed-writer rebuild before adding
  a new WAL entry; transient replacement failure can self-heal, while persistent
  rebuild I/O consumes the same Apply budget. Successful Apply clears that key.
  Apply escalation does not quarantine or reopen the copy; reads remain
  available. A writer-invalidating commit failure rebuilds and replays the
  retained WAL suffix before the next write or blocking maintenance/snapshot
  commit. A separate engine-apply failure after WAL append has an unknown
  outcome. In production it means the Tantivy writer was killed, so the next
  commit fails and rebuild or restart replay applies the entry on this copy.
  On a primary, replicas never receive it, so copies can diverge; peer
  recovery from this copy can ship the retained entry to a new copy. When a
  later write exposes that missing sequence, every affected replica normally
  reaches the fixed gap deadline (about 60 seconds), is removed, and is
  peer-recovered. This is intentionally conservative until D10 adds targeted
  repair.
  Definitive and open-level failures may quarantine only after the report
  throttle admits the attempt, except sequence/version collisions, which
  atomically persist collision quarantine before the engine is evicted and are
  also reported by the primary. If marker persistence fails, retain the marked
  identity in memory, keep the engine evicted, and return a reportable
  persistent-storage failure. Later collision handling may retry the atomic
  marker write, but assigned open and replica apply must reject the cached
  marker until persistence succeeds or the process exits.
- Only an uninitialized CreateIndex primary allocation may create a fresh empty
  copy. Initial and later out-of-sync replicas receive identity through
  verified recovery install.
- Pre-1.0 or unknown copy identity versions are never adopted or upgraded.
  They use the shared unsupported-format error and require index recreation.
- A collision-quarantined identity cannot open or accept replica apply for the
  same allocation. The marker remains until routing removes that allocation
  and peer recovery installs a fresh identity for a new allocation.
- Assigned open checks cached and durable collision quarantine under the
  shard-open lock before consulting I/O retry backoff. Definitive errors,
  including active collision quarantine, never arm or retain copy-I/O retry
  state.
- `fence_max_seq_no` is captured and persisted only when a copy fence advances
  (or when peer recovery creates a new identity). Ordinary assigned-copy open
  validates and reconciles that value but never rewrites it.
- A stale exact `SHARD_COPY_IDENTITY.json.tmp` is removed before the
  initial-primary empty-directory check. Local/test helpers load and preserve
  an existing durable identity rather than overwriting it with allocation `1`.

### UUID-Based Data Directories
- On-disk path: `<data_dir>/<uuid>/shard_<id>` (NOT `<data_dir>/<index_name>/shard_<id>`)
- The UUID comes from `IndexMetadata.uuid` and is passed to `open_shard_with_settings()`
- `open_shard_with_settings()` must reject empty `index_uuid` values; only `open_shard()` / `open_shard_with_mappings()` synthesize test/local UUIDs, and those helpers must keep one stable generated UUID per index
- `close_index_shards()` looks up the stored UUID to find and delete the correct directory
- `cleanup_orphaned_data(known_uuids)` is called on startup only after authoritative index UUIDs are available; empty pre-catch-up state or missing expected UUID directories must skip cleanup to avoid deleting live shard data
- Destructive delete paths must log an explicit reason so operators can distinguish API delete-index, transport delete-index, and orphan-cleanup removals in logs
- **NEVER** construct shard paths using the index name — always go through UUID

### Shard Opening Sequence
1. Register UUID mapping via `register_index_uuid()` or `get_or_generate_uuid()`
2. Create SettingsManager (one per index) with watch channels
3. Create directory at `<data_dir>/<uuid>/shard_<id>`
4. Start `CompositeEngine::start_refresh_loop_reactive()` — responds to setting changes
5. Call `engine.rebuild_vectors()` only when `mappings` contains `KnnVector`
   fields. Rebuild scans every live Tantivy document in bounded batches; skip
   that scan entirely for non-vector indices.
6. Handle schema mismatch by wiping orphaned directories and retrying

Peer-recovery install is the exception to step 6: while
`PEER_RECOVERY_IN_PROGRESS` exists, ordinary open/search/replica-apply paths
must fail closed. Finalization opens under the per-shard lock with schema reset
disabled, verifies the exact committed file set, and removes the marker only
after the engine is ready to publish.

After CompleteFinalize is sent, `PEER_RECOVERY_AWAITING_MEMBERSHIP` preserves
the caught-up copy across target restart. This marker permits open and live
replica apply. Reconcile removes it without closing the engine when the node is
in-sync or promoted with the same allocation ID; missing/different allocation
identity, a different primary, or a strictly newer observed term is definitive
rejection and closes the engine and restores `PEER_RECOVERY_IN_PROGRESS`.
Lifecycle restoration of an exact marker happens before recovery candidate
selection. Target begin and preparation recheck the marker under the per-shard
lock before any engine eviction or directory removal.
If marker rename succeeds but directory fsync fails, publish the in-memory
pending state before returning the error; retry cleanup also restores that state
from a matching marker. Delayed abort checks the registered UUID and must not
recreate storage for a deleted/recreated index incarnation.

`ShardManager::reopen_shard()` and async index-close wrappers invoke the
registered source-session cleanup hook before replacing engines. Cleanup must
drop the session's engine `Arc` and WAL pin before Tantivy reopen.
StartPeerRecovery, reopen, and close also share the UUID/shard lifecycle lock;
the cleanup-to-removal interval is not open to a new source session.
Reopen is replacement-only: after acquiring that lifecycle lock and again
under the per-shard open lock, the registered UUID must still match and the
exact shard engine/directory must still exist. A detached reopen must never
register an old UUID, recreate a deleted directory, or create a missing engine.
Its existing-only engine open also requires `index/meta.json`. When mappings
contain vector fields, rebuild and persist the complete vector index on the
blocking pool before starting maintenance or publishing the replacement engine.
Async index deletion acquires every lifecycle lock registered for the UUID and
every per-shard open lock registered for the index, including shards temporarily
absent from the engine map during reopen, before removing engines or storage.

The in-progress marker must be checked both before and after the per-shard open
lock. A marker created while open is waiting always wins and keeps the shard
closed.

### Async Scheduling Rule
- `open_shard_with_settings()`, `close_index_shards()`, and `cleanup_orphaned_data()` are synchronous helpers for already-blocking contexts and tests.
- Any Tokio call site must use the `*_blocking()` wrappers so shard startup/rebuild/delete work does not stall unrelated async tasks.
- Request or restart reopen paths must not silently invent a new UUID for an existing cluster index. If the authoritative UUID is missing, fail closed and surface the mismatch.

## ShardKey
```rust
pub struct ShardKey {
    pub index: String,
    pub shard_id: u32,
}
```

## Replica Checkpoint Tracking
```rust
pub struct IsrTracker {
    replicas: RwLock<HashMap<ShardKey, HashMap<String, ReplicaCheckpoint>>>,
    max_lag: u64,  // default: 1000
}

pub struct ReplicaCheckpoint {
    pub index_uuid: String,
    pub allocation_id: u64,
    pub primary_term: u64,
    pub processed_checkpoint: Option<u64>,
    pub persisted_checkpoint: Option<u64>,
    pub last_updated: Instant,
}
```

### Key Methods
- `update_replica_checkpoint(...)` / `update_replica_checkpoints(...)` take
  exact-allocation typed checkpoint responses plus the captured primary prefix
- `with_updated_replica_checkpoints_at(...)` updates observations and computes
  from their monotonic view under the same node-wide lock. Its consumer must
  not read engine sequence state or perform blocking I/O.
- `in_sync_replicas(index, shard_id, primary_checkpoint) -> Vec<String>`
  - Returns a lag-based diagnostic view only; it does not grant
    authoritative in-sync membership
- `replica_checkpoints(index, shard_id) -> Vec<(String, u64)>`
- `remove_shard(index, shard_id)`, `remove_index(index)`

### How Checkpoint Observations Are Used
1. Primary writes to WAL + engine → replicates to the Raft-authoritative
   `ShardRoutingEntry.in_sync_replicas`
2. Each replica proves the exact operation processed and returns optional
   contiguous processed/persisted checkpoints
3. Primary updates a monotonic maximum per index UUID, allocation ID, and
   primary term; reordered lower responses cannot regress it. An identity
   change resets processed and persisted observations, except that reports
   from an older primary term for the same UUID are ignored. Ignoring such a
   report also preserves gap identity, target, deadline, and progress. A
   different UUID may reset at a lower term; allocation changes reset within
   the same or a newer term.
4. A leader that also hosts the primary may use `replica_checkpoints()` to
   prefer the highest observed candidate within the authoritative in-sync set;
   otherwise it chooses a live in-sync cluster member without checkpoint
   ranking

Checkpoint observations are contiguous-prefix proofs, not maximum sequence
numbers. `ReplicaGapObservation` fixes its target at first observation and the
lifecycle performs an exact-allocation sequence-state probe before removal.
The primary also uses scoped persisted observations to compute the global
checkpoint against fresh Raft-authoritative in-sync membership. A missing
current-identity observation holds progress back. The tracker never grants
membership.
