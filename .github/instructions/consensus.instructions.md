# Consensus Module — src/consensus/

## Raft Type Configuration (types.rs)
```rust
openraft::declare_raft_types!(
    pub TypeConfig: D = ClusterCommand, R = ClusterResponse, Node = BasicNode
);
type RaftInstance = openraft::Raft<TypeConfig, ClusterStateMachine>;
```

## ClusterCommand (Raft log entries)
- `AddNode { node: NodeInfo }` — register/update a node
- `RemoveNode { node_id: String }` — remove node from cluster + Raft membership
- `CreateIndex { metadata: IndexMetadata }` — create index with shard routing
- `DeleteIndex { index_name: String }` — delete index and all metadata
- `SetMaster { node_id: String }` — set cluster master
- `UpdateIndex { metadata: IndexMetadata }` — update shard routing (failover, replicas, settings)
- `AddMappings { index_name, new_fields, dynamic }` — merge auto-detected field mappings into an existing index (dynamic mapping)

## ClusterResponse
- `Ok` — command applied successfully
- `Error(String)` — application error

## State Machine (state_machine.rs)
```rust
pub struct ClusterStateMachine {
    state: Arc<RwLock<ClusterState>>,  // shared with ClusterManager
    last_applied: Option<LogId>,
    last_membership: StoredMembership,
}
```
### Apply behavior per command
| Command | Action |
|---------|--------|
| `AddNode` | `state.add_node()` |
| `RemoveNode` | `state.remove_node()` |
| `CreateIndex` | `state.add_index()` |
| `DeleteIndex` | remove from `state.indices` |
| `SetMaster` | set `state.master_node` |
| `UpdateIndex` | replace `shard_routing` in `state.indices` |

### Snapshot
- Format: JSON-serialized `ClusterState`
- ID: `snap-{last_applied_index}`

## Raft Config
- heartbeat_interval: 1000ms
- election_timeout_min: 3000ms, election_timeout_max: 6000ms

## Log Store
### DiskLogStore (src/consensus/disk_store.rs) — production
- Backed by `redb` (embedded key-value store)
- Persists to `{data_dir}/raft.db`
- Survives process restarts
- redb transactions are blocking; any async openraft storage method that touches the database must offload through a Tokio blocking-pool helper such as `run_blocking_io()` rather than lock/read/write inline on an async worker
- Keep Raft heartbeats, vote handling, and other control-plane futures on Tokio; do not move them to rayon to compensate for blocking disk I/O

### MemLogStore (src/consensus/store.rs) — tests only
- In-memory `BTreeMap`
- `get_log_reader()` must return a shared-state handle (not a clone) — SM worker holds reader permanently

## Module Functions
- `create_raft_instance(node_id, cluster_name, data_dir)` — persistent disk store
- `create_raft_instance_mem(node_id, cluster_name)` — in-memory (tests only)
- `bootstrap_single_node(raft, node_id, addr)` — initialize single-node cluster

## openraft 0.10.0-alpha.17 API Gotchas
- `Vote::new(term: u64, node_id: u64)` — NOT `Vote::new(LeaderId, bool)`
- `LeaderId` is at `openraft::impls::leader_id_adv::LeaderId` with public fields `term`, `node_id`
- `IOFlushed::new()` is `pub(crate)` — use `IOFlushed::noop()` in tests
- `raft.add_learner(node_id, BasicNode { addr }, blocking)` then `raft.change_membership(voter_set, false)` to add nodes
