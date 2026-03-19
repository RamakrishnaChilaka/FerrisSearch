# Node Module — src/node/mod.rs

## Node Struct
```rust
pub struct Node {
    pub config: AppConfig,
    pub cluster_manager: Arc<ClusterManager>,
    pub transport_client: TransportClient,
    pub shard_manager: Arc<ShardManager>,
    pub raft: Option<Arc<RaftInstance>>,
}
```

## Startup Sequence (`Node::new()` → `Node::start()`)
1. `Node::new(config)` — creates Raft instance via `create_raft_instance()` (persisted to `raft.db`)
2. `Node::start()` spawns THREE concurrent tasks via `tokio::select!`:
   - **gRPC Transport Server** (port 9300) — Raft RPCs + shard ops + replication
   - **HTTP API Server** (port 9200) — REST endpoints via Axum
   - **Cluster Lifecycle Loop** — runs every 5 seconds

## Cluster Lifecycle Loop
### First Node (no reachable seeds)
1. Filters `seed_hosts` to exclude self
2. Bootstraps single-node Raft via `bootstrap_single_node()`
3. Registers self via `raft.client_write(AddNode)` + `raft.client_write(SetMaster)`
4. Opens shards for any indices assigned to this node

### Joining Node (seed reachable)
1. Sends `JoinCluster` gRPC to seed hosts (includes `raft_node_id`)
2. Leader handles: `AddNode` Raft command → `add_learner()` → `change_membership()`
3. Raft log replication propagates state (joiner does NOT call `update_state`)
4. After joining, opens shards assigned to this node

### Leader Duties (every 5s tick)
1. `SetMaster` if not already set
2. Dead node scan (skip first 20s after becoming leader — grace period):
   - Nodes not seen for 15s → dead
   - Call `remove_node()` on each dead node
   - Shard failover for orphaned primaries (see shard failover section)
3. Reopen any locally assigned shards that are still not open
4. Allocate unassigned replicas to available data nodes

### Follower Duties (every 5s tick)
1. Ping master node for liveness check
2. Reopen any locally assigned shards that are still not open
3. Request translog-based replica recovery for replica shards once opened

## Shard Failover Algorithm (leader only)
1. `IndexMetadata::remove_node(dead_node)` → returns orphaned primary shard IDs
2. For each orphaned primary:
   - Query `isr_tracker.replica_checkpoints(index, shard_id)` for all replicas
   - Find replica with **highest checkpoint** (most up-to-date data)
   - Call `IndexMetadata::promote_replica_to(shard_id, best_replica_node)`
   - Increment `unassigned_replicas` for the lost replica slot
3. For replicas on the dead node: increment `unassigned_replicas`
4. Issue `UpdateIndex` Raft command to persist routing changes

## AppState (shared across all API handlers)
```rust
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    pub local_node_id: String,
    pub raft: Option<Arc<RaftInstance>>,
}
```
