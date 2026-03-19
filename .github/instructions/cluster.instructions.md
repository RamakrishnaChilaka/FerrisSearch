# Cluster Module — src/cluster/

## ClusterState (src/cluster/state.rs)

### Enums
- `NodeRole` — `Master`, `Data`, `Client`
- `ShardState` — `Started` (active), `Unassigned` (needs a node)
- `FieldType` — `Text`, `Keyword`, `Integer`, `Float`, `Boolean`, `KnnVector`

### Structs
```
NodeInfo { id, name, host, transport_port, http_port, roles, raft_node_id }
FieldMapping { field_type, dimension }  // dimension for knn_vector only
IndexSettings { refresh_interval_ms: Option<u64> }  // None = cluster default 5000ms
ShardCopy { node_id: Option<NodeId>, state: ShardState }
ShardRoutingEntry { primary, replicas, unassigned_replicas }
IndexMetadata { name, number_of_shards, number_of_replicas, shard_routing, mappings, settings }
ClusterState { cluster_name, version, master_node, nodes, indices, last_seen }
```

### Key ClusterState Methods
- `add_node(node)`, `remove_node(node_id) -> Option<NodeInfo>`
- `ping_node(node_id)` — update `last_seen` timestamp
- `add_index(metadata)`, `delete_index(name) -> Option<IndexMetadata>`
- `last_seen` is `#[serde(skip)]` — transient, not replicated by Raft

### Key IndexMetadata Methods
```rust
// Routing queries
fn primary_node(&self, shard_id: u32) -> Option<&NodeId>
fn replica_nodes(&self, shard_id: u32) -> Vec<&str>
fn unassigned_replica_count(&self) -> u32

// Construction
fn build_shard_routing(name, num_shards, num_replicas, data_nodes) -> Self  // round-robin

// Node removal & failover
fn remove_node(&self, node_id: &str) -> Vec<u32>  // returns orphaned primary shard IDs
fn promote_replica(&self, shard_id: u32) -> bool   // promote first available replica
fn promote_replica_to(&self, shard_id: u32, new_primary: &str) -> bool  // targeted promotion

// Replica management
fn update_number_of_replicas(&self, new_count: u32) -> Vec<(u32, String)>  // returns deleted slots
fn allocate_unassigned_replicas(&self, data_nodes: &[&str]) -> bool
```

## ClusterManager (src/cluster/manager.rs)
```rust
pub struct ClusterManager { state: Arc<RwLock<ClusterState>> }
```
- `new(cluster_name)` / `with_shared_state(state)` — Raft SM shares the same `Arc<RwLock<ClusterState>>`
- `get_state() -> ClusterState` — cloned snapshot (read lock)
- `add_node(node)`, `ping_node(node_id)`
- `update_state(new_state)` — full overwrite, preserves `last_seen`
- **WARNING**: `update_state()` should never replace Raft-managed state

## SettingsManager (src/cluster/settings.rs)
```rust
pub struct SettingsManager {
    refresh_interval_tx: watch::Sender<Duration>,
}

impl SettingsManager {
    pub fn new(initial: &IndexSettings) -> Self
    pub fn watch_refresh_interval(&self) -> watch::Receiver<Duration>  // subscribe
    pub fn refresh_interval(&self) -> Duration                         // current value
    pub fn update(&self, new_values: &IndexSettings)                   // push to channels
    pub fn current(&self) -> IndexSettings                             // snapshot
}
```

### Reactive Settings Flow
1. `PUT /{index}/_settings` API call
2. Non-leader forwards to master via gRPC `UpdateSettings`
3. Leader issues `UpdateIndex` Raft command with new settings
4. Raft replicates → all nodes apply in state machine
5. `ShardManager::apply_settings(index, new_settings)` called
6. `SettingsManager::update()` pushes to `watch::Sender`
7. All `watch::Receiver`s (engine refresh loops) react immediately

### Adding a New Reactive Setting
1. Add field to `IndexSettings`
2. Add `watch::Sender<T>` + `watch::Receiver<T>` to `SettingsManager`
3. Detect change in `SettingsManager::update()`
4. Subscribe in consumer (e.g., engine refresh loop)
