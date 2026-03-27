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
- `open_shard(index, shard_id)` — creates CompositeEngine with auto-generated UUID
- `open_shard_with_mappings(index, shard_id, mappings)` — with field type info, auto-UUID
- `open_shard_with_settings(index, shard_id, mappings, settings, index_uuid)` — with UUID, SettingsManager + reactive refresh loop + vector rebuild
- `get_shard(index, shard_id) -> Option<Arc<dyn SearchEngine>>`
- `get_index_shards(index) -> Vec<(u32, Arc<dyn SearchEngine>)>`
- `all_shards() -> Vec<(ShardKey, Arc<dyn SearchEngine>)>`
- `close_index_shards(index)` — remove engines, clean ISR, delete UUID-based directory
- `apply_settings(index, new_settings)` — push to SettingsManager watch channels
- `register_index_uuid(index, uuid)` — store the UUID mapping for an index
- `index_uuid(index) -> Option<String>` — get registered UUID
- `shard_data_dir(index, shard_id) -> Option<PathBuf>` — on-disk path using UUID
- `cleanup_orphaned_data(known_uuids)` — delete dirs not matching any known UUID

### UUID-Based Data Directories
- On-disk path: `<data_dir>/<uuid>/shard_<id>` (NOT `<data_dir>/<index_name>/shard_<id>`)
- The UUID comes from `IndexMetadata.uuid` and is passed to `open_shard_with_settings()`
- If `index_uuid` is empty, `get_or_generate_uuid()` creates a random one (test/test convenience)
- `close_index_shards()` looks up the stored UUID to find and delete the correct directory
- `cleanup_orphaned_data(known_uuids)` is called on startup to delete stale data from deleted indices
- **NEVER** construct shard paths using the index name — always go through UUID

### Shard Opening Sequence
1. Register UUID mapping via `register_index_uuid()` or `get_or_generate_uuid()`
2. Create SettingsManager (one per index) with watch channels
3. Create directory at `<data_dir>/<uuid>/shard_<id>`
4. Start `CompositeEngine::start_refresh_loop_reactive()` — responds to setting changes
5. Call `engine.rebuild_vectors()` — recover USearch index from persisted docs (crash recovery)
6. Handle schema mismatch by wiping orphaned directories and retrying

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
