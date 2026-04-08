# Shard Module — src/shard/mod.rs

## ShardManager
```rust
pub struct ShardManager {
    data_dir: PathBuf,
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    settings_managers: RwLock<HashMap<String, Arc<SettingsManager>>>,  // per-index
    index_uuids: RwLock<HashMap<String, String>>,  // index_name → UUID for on-disk dirs
    pub isr_tracker: IsrTracker,
    durability: TranslogDurability,
}
```

### Key Methods
- `open_shard(index, shard_id)` — creates CompositeEngine with a generated per-index UUID for local/test helpers
- `open_shard_with_mappings(index, shard_id, mappings)` — with field type info, reuses the same generated per-index UUID for local/test helpers
- `open_shard_with_settings(index, shard_id, mappings, settings, index_uuid)` — with UUID, SettingsManager + reactive refresh loop + vector rebuild
- `open_shard_with_settings_blocking(index, shard_id, mappings, settings, index_uuid)` — async-safe Tokio wrapper for shard open/recovery work
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

### UUID-Based Data Directories
- On-disk path: `<data_dir>/<uuid>/shard_<id>` (NOT `<data_dir>/<index_name>/shard_<id>`)
- The UUID comes from `IndexMetadata.uuid` and is passed to `open_shard_with_settings()`
- `open_shard_with_settings()` must reject empty `index_uuid` values; only `open_shard()` / `open_shard_with_mappings()` synthesize test/local UUIDs, and those helpers must keep one stable generated UUID per index
- `close_index_shards()` looks up the stored UUID to find and delete the correct directory
- `cleanup_orphaned_data(known_uuids)` is called on startup only after authoritative index UUIDs are available; empty pre-catch-up state or missing expected UUID directories must skip cleanup to avoid deleting live shard data
- Destructive delete paths must log an explicit reason so operators can distinguish API delete-index, transport delete-index, legacy non-Raft `PublishState`, and orphan-cleanup removals in logs
- **NEVER** construct shard paths using the index name — always go through UUID

### Shard Opening Sequence
1. Register UUID mapping via `register_index_uuid()` or `get_or_generate_uuid()`
2. Create SettingsManager (one per index) with watch channels
3. Create directory at `<data_dir>/<uuid>/shard_<id>`
4. Start `CompositeEngine::start_refresh_loop_reactive()` — responds to setting changes
5. Call `engine.rebuild_vectors()` only when `mappings` contains `KnnVector` fields — skip the expensive 100K-doc MatchAll query for non-vector indices to prevent OOM during multi-shard restart
6. Handle schema mismatch by wiping orphaned directories and retrying

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

## ISR Tracking (In-Sync Replica)
```rust
pub struct IsrTracker {
    replicas: RwLock<HashMap<ShardKey, HashMap<String, ReplicaCheckpoint>>>,
    max_lag: u64,  // default: 1000
}

pub struct ReplicaCheckpoint {
    pub checkpoint: u64,
    pub last_updated: Instant,
}
```

### Key Methods
- `update_replica_checkpoint(index, shard_id, replica_node_id, checkpoint)`
- `update_replica_checkpoints(index, shard_id, checkpoints: &[(String, u64)])`
- `in_sync_replicas(index, shard_id, primary_checkpoint) -> Vec<String>`
  - A replica is "in-sync" if `primary_checkpoint - replica_checkpoint <= max_lag`
- `replica_checkpoints(index, shard_id) -> Vec<(String, u64)>`
- `remove_shard(index, shard_id)`, `remove_index(index)`

### How ISR is Used
1. Primary writes to WAL + engine → replicates to all replicas
2. Each replica returns its `local_checkpoint` after applying
3. Primary calls `update_replica_checkpoints()` with returned values
4. Leader uses `replica_checkpoints()` during shard failover to pick best replica (highest checkpoint)
