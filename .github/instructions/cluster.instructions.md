---
description: "Use for cluster metadata, index engines and settings, shard routing, UUID identity, security records, and reactive settings."
applyTo: "src/cluster/**"
---

# Cluster Module — src/cluster/

## ClusterState (src/cluster/state.rs)

### Enums
- `NodeRole` — `Master`, `Data`, `Client`
- `ShardState` — `Started` (active), `Unassigned` (needs a node)
- `FieldType` — `Text`, `Keyword`, `Integer`, `Float`, `Boolean`, `Date`, `KnnVector`
- `IndexEngine` — `LocalShards` (mutable shard-owned engine), `RemoteStore` (shardless immutable-split read engine)
- `DynamicMapping` — `True`, `False`, `Strict`

### Structs
```
NodeInfo { id, name, host, transport_port, http_port, roles, raft_node_id }
FieldMapping { field_type, dimension }  // dimension for knn_vector only
RemoteStoreSettings { object_store_uri, manifest_path, manifest_generation, manifest_checksum, manifest_refresh_ms, hotcache_bytes, split_cache_bytes }
IndexSettings { engine: IndexEngine, refresh_interval_ms: Option<u64>, flush_threshold_bytes: Option<u64>, remote_store: Option<RemoteStoreSettings> }  // engine defaults to LocalShards
ShardCopy { node_id: Option<NodeId>, state: ShardState }
ShardRoutingEntry { primary, replicas, unassigned_replicas }
IndexMetadata { name, uuid, number_of_shards, number_of_replicas, shard_routing, mappings, dynamic, settings }
SecurityApiKeyRecord { id, name, hash_sha256, roles, indices, created_at_millis }   // hash only, never plaintext
SecurityRoleDefinition { name, cluster, indices, index_privileges }                 // custom role
ClusterState { cluster_name, version, master_node, nodes, indices, last_seen, api_keys, roles }
```

### Control-Plane Config Fields (snapshotted for free)
- `api_keys: HashMap<String, SecurityApiKeyRecord>` (key_id → record) and `roles: HashMap<String, SecurityRoleDefinition>` (role_name → def) hold the dynamic security control plane. Both are `#[serde(default)]` so old snapshots restore, and both are initialized in `ClusterState::new()`.
- These are mutated only via the `PutApiKey`/`DeleteApiKey`/`PutRole`/`DeleteRole` `ClusterCommand`s — never written directly. See `control-plane.instructions.md` for the recipe and `security.instructions.md` for the security model.
- Any new small, globally-consistent cluster config belongs here as a `#[serde(default)]` field too. Do NOT store such config as Tantivy docs/shards/WAL.
- `SecurityApiKeyRecord`/`SecurityRoleDefinition` live in `cluster::state` (not `security` or `consensus`) so `ClusterState` can embed them and `consensus::types` can import them without a circular dependency. They derive `Serialize, Deserialize, Clone, Debug, PartialEq, Eq`.

### Engine Selection
- `IndexSettings.engine` is a create-time selector persisted in cluster state, surfaced by `GET /{index}/_settings`, `SHOW TABLES`, and `SHOW CREATE TABLE`
- `engine` is immutable after creation — `PUT /{index}/_settings` must reject attempts to change it
- `local_shards` supports ordinary document CRUD through shard routing, WAL, and replication
- `remote_store` can be created and queried through manifest-backed split execution; it is shardless and ordinary document CRUD returns `501`
- `remote_store` data is added through the dedicated publish endpoint, which is currently a manually invoked, single-writer-oriented path rather than near-real-time ingest
- The shared `AppConfig.storage_uri` selects the process object-store backend; do not treat per-index `object_store_uri` metadata as an independently wired backend without verifying source

### Index UUID
- Every `IndexMetadata` has a non-empty `uuid: IndexUuid` value; production
  creation generates UUID v4 values, while transport and legacy fixtures may
  preserve any non-empty identifier
- Missing or empty UUIDs fail deserialization; startup must not synthesize
  identity for an existing index
- The UUID determines the on-disk data directory: `<data_dir>/<uuid>/shard_<id>`
- Delete + re-create with the same index name gets a new UUID — stale data never collides
- `build_shard_routing()` auto-generates a UUID; `auto_create_index()` generates one explicitly

### Key ClusterState Methods
- `add_node(node)`, `remove_node(node_id) -> Option<NodeInfo>`
- `ping_node(node_id)` — update `last_seen` timestamp
- `add_index(metadata)`, `delete_index(name) -> Option<IndexMetadata>`
- `last_seen` is `#[serde(skip)]` — transient, not replicated by Raft

### Key IndexMetadata Methods
```rust
// Routing queries
fn primary_node(&self, shard_id: u32) -> Option<&NodeId>
fn replica_nodes(&self, shard_id: u32) -> Vec<&NodeId>
fn unassigned_replica_count(&self) -> u32

// Construction
fn build_shard_routing(name, num_shards, num_replicas, data_nodes) -> Self  // round-robin

// Node removal & failover
fn remove_node(&mut self, node_id: &NodeId) -> Vec<u32>  // removes replicas, increments each shard's lost replica slots, returns orphaned primary shard IDs
fn promote_replica(&mut self, shard_id: u32) -> bool   // promote first available replica
fn promote_replica_to(&mut self, shard_id: u32, new_primary: &str) -> bool  // targeted promotion

// Replica management
fn update_number_of_replicas(&mut self, new_count: u32) -> Vec<(u32, String)>  // returns deleted slots
fn allocate_unassigned_replicas(&mut self, data_nodes: &[String]) -> bool
```

## ClusterManager (src/cluster/manager.rs)
```rust
pub struct ClusterManager { state: Arc<RwLock<ClusterState>> }
```
- `new(cluster_name)` / `with_shared_state(state)` — Raft SM shares the same `Arc<RwLock<ClusterState>>`
- `get_state() -> ClusterState` — cloned snapshot (read lock)
- `add_node(node)`, `ping_node(node_id)`
- `update_state(new_state)` — full overwrite, preserves `last_seen`
- **WARNING**: `update_state()` should never replace Raft-managed state.

## SettingsManager (src/cluster/settings.rs)
```rust
pub struct SettingsManager {
    refresh_interval_tx: watch::Sender<Duration>,
    flush_threshold_tx: watch::Sender<u64>,
}

impl SettingsManager {
    pub fn new(initial: &IndexSettings) -> Self
    pub fn watch_refresh_interval(&self) -> watch::Receiver<Duration>  // subscribe
    pub fn refresh_interval(&self) -> Duration                         // current value
    pub fn watch_flush_threshold(&self) -> watch::Receiver<u64>        // subscribe
    pub fn flush_threshold(&self) -> u64                               // current value
    pub fn update(&self, new_values: &IndexSettings)                   // push to channels
    pub fn current(&self) -> IndexSettings                             // snapshot
}
```

### Reactive Settings Flow
1. `PUT /{index}/_settings` API call
2. Non-leader forwards to master via gRPC `UpdateSettings`
3. Leader issues `UpdateIndex` Raft command with new settings
4. Raft replication updates `ClusterState` on every node
5. The HTTP receiver and leader gRPC handler currently call
   `ShardManager::apply_settings()` for their local engines
6. `SettingsManager::update()` pushes to local `watch::Sender`s
7. Local `watch::Receiver`s (engine refresh / auto-flush loops) react

The Raft state machine does not own `ShardManager`, so a follower that neither
received nor led the request does not currently notify already-open engines
when the committed metadata changes. Do not claim cluster-wide reactive
application. A fix must apply committed settings to every node's local engines
and include a multi-node regression.

### Adding a New Reactive Setting
1. Add field to `IndexSettings`
2. Add `watch::Sender<T>` + `watch::Receiver<T>` to `SettingsManager`
3. Detect change in `SettingsManager::update()`
4. Subscribe in consumer (e.g., engine refresh loop)

### Auto-Flush Setting
- `flush_threshold_bytes` drives background WAL auto-flush in `CompositeEngine`
- Default: 512 MB; `0` disables auto-flush
- The consumer must skip auto-flush while `global_checkpoint == 0`, because `flush_with_global_checkpoint(0)` degrades to full WAL truncation and can discard replica recovery history
- Background auto-flush should be best-effort: if the shard is already ingesting or persisting vectors, defer the tick instead of blocking foreground writes
