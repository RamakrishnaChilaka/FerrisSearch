---
description: "Use for openraft configuration, commands, the cluster state machine, snapshots, and persistent or in-memory Raft storage."
applyTo: "src/consensus/**"
---

# Consensus Module — src/consensus/

> Adding a new command? Follow `control-plane.instructions.md` — the canonical
> end-to-end recipe (ClusterCommand → Display → apply arm + `version` bump → proto
> → transport server/client → coordinator API handler → strict `ClusterState`
> field). Copy `AddMappings` or the security commands verbatim.

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
- `MarkReplicaInSync { index_name, index_uuid, shard_id, replica, allocation_id, primary, primary_term }` — conditionally admit the exact recovered assignment
- `ActivatePrimary { index_name, index_uuid, shard_id, primary, allocation_id, expected_term }` — conditionally bump the term and monotonically mark the shard initialized
- `MarkPrimaryUnavailable { index_name, index_uuid, shard_id, primary, allocation_id }` — set status-only unavailability for the exact initialized primary allocation
- `MarkPrimaryAvailable { index_name, index_uuid, shard_id, primary, allocation_id, primary_term }` — clear status-only unavailability only for the exact current primary allocation and term, without changing the term
- `FailShardCopy { index_name, index_uuid, shard_id, node, allocation_id, expected_primary_term, promote_only, promotion_candidate }` — conditionally remove an unopenable replica or promote the leader-selected in-sync survivor; primary reports must be promote-only and the expected term must be nonzero.
- `AddMappings { index_name, new_fields, dynamic }` — merge auto-detected field mappings into an existing index (dynamic mapping)
- `PutApiKey { record: SecurityApiKeyRecord }` — upsert a dynamic API key (stores only the hash) into `ClusterState.api_keys`
- `DeleteApiKey { key_id: String }` — remove a dynamic API key
- `PutRole { role: SecurityRoleDefinition }` — upsert a custom role into `ClusterState.roles`
- `DeleteRole { name: String }` — remove a custom role

Every variant has a `Display` arm (used in logs — never print secrets) with a serde JSON
roundtrip test in `types.rs`, and an `apply_command` arm in `state_machine.rs`.

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
| `CreateIndex` | assign initial copy IDs from the committed log index; replicas start out of sync |
| `DeleteIndex` | remove from `state.indices` |
| `SetMaster` | set `state.master_node` |
| `UpdateIndex` | preserve existing copy IDs (including an unchanged red shard's absent primary ID), assign the current log index to new copies, clear removed IDs, intersect in-sync membership, and reject out-of-sync promotion |
| `MarkReplicaInSync` | add one assigned replica only when the shard is initialized, a primary allocation exists, and UUID, allocation ID, primary, and term match |
| `ActivatePrimary` | increment the term and set `primary_initialized` only when UUID, allocation ID, primary, and expected term match |
| `MarkPrimaryUnavailable` | set the status-only unavailable flag for the exact initialized primary allocation; reject an already-set flag |
| `MarkPrimaryAvailable` | clear an already-set unavailable flag only when UUID, primary, allocation, and primary term match; never change the term |
| `FailShardCopy` | after initialization, remove only an exact failed replica allocation whose nonzero `expected_primary_term` matches. A primary command must be promote-only and promotes an in-sync candidate with a term bump, otherwise it is rejected without clearing the primary allocation |
| `AddMappings` | merge `new_fields` into `state.indices[name].mappings` via `.entry().or_insert()` |
| `PutApiKey` / `DeleteApiKey` | `insert` / `remove` on `state.api_keys` |
| `PutRole` / `DeleteRole` | `insert` / `remove` on `state.roles` |

Successful apply arms bump `state.version += 1`; conditional-command rejection
returns `ClusterResponse::Error` without partial mutation or a version bump.
Unconditional apply arms bump on idempotent upserts and deletes
of absent keys (mirrors `DeleteIndex` / `RemoveNode`). `AddNode`/`CreateIndex` bump version
inside the `state.*` helper they call.

### Snapshot
- Format: JSON-serialized `ClusterState`
- ID: `snap-{last_applied_index}`
- Unsupported or unreadable snapshot state uses
  `UnsupportedRaftFormatError` and tells the operator to "wipe the node data
  directories and recreate the cluster". Never use the index-data remedy for
  Raft state.

## Raft Config
- heartbeat_interval: 1000ms
- election_timeout_min: 3000ms, election_timeout_max: 6000ms

## Log Store
### DiskLogStore (src/consensus/disk_store.rs) — production
- Backed by `redb` (embedded key-value store)
- Persists to `{data_dir}/raft.db`
- Survives process restarts
- Unsupported or unreadable log entries use `UnsupportedRaftFormatError` with
  the same wipe-and-recreate-cluster remedy as snapshots.
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
