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

## Seed Hosts Configuration
- `seed_hosts` must include ALL node transport addresses (e.g., `["127.0.0.1:9300", "127.0.0.1:9301", "127.0.0.1:9302"]`).
- `remote_seed_hosts()` filters out the current node's own transport port to prevent self-join.
- **Critical**: If `seed_hosts` only contains the first node's address (e.g., `["127.0.0.1:9300"]`), starting nodes in any order other than 1→2→3 fails — each node bootstraps its own isolated Raft cluster.
- The `dev_cluster.sh` / `dev_cluster_release.sh` scripts set `FERRISSEARCH_SEED_HOSTS` to all three local ports.

## Cluster Lifecycle Loop
### First Node (no reachable seeds)
1. Filters `seed_hosts` to exclude self
2. Tries `try_join_cluster()` with 5 retries and exponential backoff (500ms → 1s → 2s → 4s → 5s cap)
3. If no seed responds, bootstraps single-node Raft via `bootstrap_single_node()`
4. Registers self via `raft.client_write(AddNode)` + `raft.client_write(SetMaster)`
5. Opens shards for any indices assigned to this node
6. Cleans up orphaned data directories via `cleanup_orphaned_data(known_uuids)`

### Joining Node (seed reachable)
1. Tries `try_join_cluster()` with 5 retries and exponential backoff
2. Sends `JoinCluster` gRPC to seed hosts (includes `raft_node_id`)
3. Leader handles: `AddNode` Raft command → `add_learner()` → `change_membership()`
4. Raft log replication propagates state (joiner does NOT call `update_state`)
5. After joining, opens shards assigned to this node

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
4. Replay recovered operations using the seq_no carried by the primary so the replica WAL stays in the shared shard seq space

## Recovery Invariant
- `apply_recovery_ops()` must use the explicit-seq engine write methods for both index and delete operations
- Recovery replay must not allocate fresh local WAL seq_nos on the recovering replica

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
