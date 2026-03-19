# Shard Module — src/shard/mod.rs

## ShardManager
```rust
pub struct ShardManager {
    data_dir: PathBuf,
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    settings_managers: RwLock<HashMap<String, Arc<SettingsManager>>>,  // per-index
    pub isr_tracker: IsrTracker,
    durability: TranslogDurability,
}
```

### Key Methods
- `open_shard(index, shard_id)` — creates CompositeEngine with default settings
- `open_shard_with_mappings(index, shard_id, mappings)` — with field type info
- `open_shard_with_settings(index, shard_id, mappings, settings)` — with SettingsManager + reactive refresh loop + vector rebuild
- `get_shard(index, shard_id) -> Option<Arc<dyn SearchEngine>>`
- `get_index_shards(index) -> Vec<(u32, Arc<dyn SearchEngine>)>`
- `all_shards() -> Vec<(ShardKey, Arc<dyn SearchEngine>)>`
- `close_index_shards(index)` — remove engines, clean ISR, delete directory
- `apply_settings(index, new_settings)` — push to SettingsManager watch channels

### Shard Opening Sequence
1. Create SettingsManager (one per index) with watch channels
2. Start `CompositeEngine::start_refresh_loop_reactive()` — responds to setting changes
3. Call `engine.rebuild_vectors()` — recover USearch index from persisted docs (crash recovery)
4. Handle schema mismatch by wiping orphaned directories and retrying

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
