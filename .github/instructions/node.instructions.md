---
description: "Use for node construction, bootstrap and join, lifecycle reconciliation, server startup, and recovery scheduling."
applyTo: "src/node/**"
---

# Node Module — src/node/mod.rs

## Node Struct
```rust
pub struct Node {
    pub config: AppConfig,
    pub cluster_manager: Arc<ClusterManager>,
    pub transport_client: TransportClient,
    pub shard_manager: Arc<ShardManager>,
    pub raft: Arc<RaftInstance>,
    pub task_manager: Arc<TaskManager>,
    pub storage_manager: Arc<StorageManager>,
    pub security_manager: Arc<SecurityManager>,
    pub remote_store_reader_cache: Arc<RemoteSplitReaderCache>,
}
```

## Startup Sequence (`Node::new()` → `Node::start()`)
1. `Node::new(config)` — resolves the shared column-cache budget on Tokio's
   blocking pool, then creates the Raft instance via `create_raft_instance()`
   (persisted to `raft.db`)
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
6. Cleans up orphaned data directories only after authoritative index UUIDs are available (bootstrap state or a `JoinCluster` response snapshot)

### Joining Node (seed reachable)
1. Tries `try_join_cluster()` with 5 retries and exponential backoff
2. Sends `JoinCluster` gRPC to seed hosts (includes `raft_node_id`)
3. Leader handles: validate node identity → `add_learner()` for non-voters → `AddNode` Raft command → `change_membership()`; if promotion fails, roll back the `AddNode`
4. Raft log replication propagates state (joiner does NOT call `update_state`)
5. `JoinCluster` returns an authoritative cluster-state snapshot; use it for initial local shard reopen and orphan-cleanup decisions before Raft catch-up finishes

### Leader Duties (every 5s tick)
1. `SetMaster` if not already set
2. Dead node scan (skip first 20s after becoming leader — grace period):
   - Nodes not seen for 15s → dead
   - Remove from Raft membership before cluster state; if membership removal fails or would empty the voter set, leave the node registered
   - Shard failover for orphaned primaries (see shard failover section)
3. Reopen only authoritative local copies whose durable UUID/allocation
   identity matches; only an initial CreateIndex primary may be created empty
   before the shard's first activation
4. Report corruption-class decode/validation failures immediately. Retry other
   engine/filesystem/fence/apply I/O with operation-specific per-copy state and
   shared exponential backoff. Escalation requires at least three failed
   attempts spanning the configured window, 60 seconds by default. Replica
   reports remove the exact allocation; primary reports are promote-only and
   are submitted only with an in-sync candidate. Duplicate reports are
   throttled per allocation. Definitive/open-level quarantine occurs only
   after that throttle except for sequence/version collisions, which
   persist allocation-bound collision quarantine immediately; Apply escalation
   leaves the copy open for reads. Lifecycle reopen keeps rediscovering and
   reporting a collision marker until Raft removes the exact allocation.
5. Proactively invoke the shared primary-activation path for each local primary
   after startup or promotion. The activation cache is keyed by
   UUID/shard/allocation/term so lifecycle ticks and request handlers do not
   issue repeated Raft term bumps. `primary_unavailable` alone does not bypass
   the cache. Repaired quarantined storage requires fresh activation; a
   successful write on an Apply-failed copy clears status at the same term.
6. Allocate unassigned replicas only for shards with a live allocated primary.
   The current allocator may select the same faulty node again; bounded failed-
   allocation exclusion is deferred.
7. When applied routing removes a local shard copy, remove its engine from the
   serving map without deleting its on-disk evidence. This closes copies removed
   after Apply-level escalation instead of retaining an unreachable open engine.

### Follower Duties (every 5s tick)
1. Ping master node for liveness check
2. Reopen any locally assigned shards that are still not open
3. Retry `JoinCluster` whenever the authoritative cluster state does not contain the local node, even if local Raft state is already initialized from disk
4. If the master ping is rejected because the target no longer recognizes this node in cluster state, immediately retry `JoinCluster` through the seed hosts so a removed or stale follower can re-register itself. Transient ping failures (timeouts, connection errors, missing local master info) should only log and retry on the next lifecycle tick — they must not trigger a rejoin by themselves. Repeated follower-side join retries should be rate-limited so a permanently rejected or partitioned node does not issue `JoinCluster` on every 5-second lifecycle tick.

### Async Scheduling Rule
- The lifecycle loop itself stays on Tokio because it coordinates Raft/control-plane work, but shard reopen and orphan cleanup perform blocking filesystem/Tantivy recovery work.
- On Tokio call sites, use `open_local_assigned_shards_blocking()` and `cleanup_orphaned_data_if_authoritative_blocking()` so the actual shard-manager work runs on Tokio's blocking pool.
- Do NOT move Raft heartbeats or master pings onto rayon; keep control-plane futures on Tokio and offload only the blocking shard work.
- Recovered-node startup assignments must fail closed when durable
  UUID/allocation identity is missing, malformed, or mismatched. The sole
  exception is an uninitialized allocation created by CreateIndex, before any
  write can be acknowledged.
- The guarded startup-assignment set must come from the node's pre-join recovered state, not the later authoritative join snapshot. Otherwise fresh assignments learned during rejoin can be permanently misclassified as guarded startup shards and stay stuck in `INITIALIZING`.
- Do not clear the recovered startup-assignment guard after bootstrap or rejoin. Authoritative cluster state confirms shard ownership, not the continued existence of the local shard data; only assignments that were never part of the recovered local state may create fresh UUID directories later in the lifecycle loop.
- Later out-of-sync replica assignments do not create empty engines; verified
  peer recovery installs their identity and data.

## Peer Recovery Driver
- The lifecycle loop schedules recovery on every node for each local
  `local_shards` replica assignment absent from `in_sync_replicas`.
- `max_concurrent_peer_recoveries` bounds target sessions per node (default 2,
  `0` disables); failures back off from 5 seconds to 60 seconds.
- File download, fsync, shard close/open, vector rebuild, and recovery apply run
  through Tokio's blocking facilities rather than the fixed search/write pools.
- Source snapshot preparation can rebuild a failed Tantivy writer and replay
  the retained WAL suffix while the shard is idle. A transient source commit
  failure must not leave replica recovery dependent on a later client write.
- If catch-up reaches a WAL frame the source has not processed, the physical
  cursor remains at that frame and the target advances to finalization. The
  exclusive finalize barrier rebuilds/replays the source writer and serves the
  remaining suffix without discarding the session's transferred snapshot.
- A failed target retains `PEER_RECOVERY_IN_PROGRESS` and stays unavailable.
  An inactive marker whose embedded allocation ID matches the current
  out-of-sync assignment is reported through `FailShardCopy`; an active target,
  a fresh target with no marker, or a stale marker from another allocation is
  not reported.
  Successful finalization clears the in-memory target gate only after the
  primary reports settled admission.
- Completion timeout is not rejection. A caught-up target persists a
  finalized-awaiting-membership marker, accepts live replication, and is
  excluded from new recovery scheduling until local ordered state says
  admitted/promoted or definitively rejected. This state is reconstructed when
  the target restarts, before recovery candidates are selected.
- Pending observation checks admission first. With the same current allocation,
  a strictly newer observed term or a different primary is definitive
  rejection; an older view or the same primary/term remains unknown.
- A matching durable pending marker blocks target begin and target preparation.
  Controlled retryable transfer failures remove their partial install and
  retry the same allocation; an inactive matching install marker represents an
  interrupted/crashed install and remains a definitive failure report.
- If pending-marker publication reaches rename but directory fsync fails,
  preserve or reconstruct `FinalizedAwaitingMembership` in memory; retry
  cleanup must not leave a permanent `Recovering` gate.
- A delayed abort checks the currently registered index UUID before touching
  storage and never recreates a deleted/recreated index's old UUID directory.
- An abandoned finalize session is made definitive by a source-side
  `ActivatePrimary` term bump. If no admission command was submitted, release
  the barrier first and bump asynchronously; after submission, keep the barrier
  until admission or the newer term is observed.
- Recovery start, source session, install, pending marker, admission, and target
  observation retain one exact allocation ID. Same-node remove/re-add is a
  definitive mismatch rather than an ABA-ambiguous `Unknown`.
- The G1 empty-primary exception assumes the local applied Raft view is
  monotonic. Normal startup replays persistent `raft.db`; losing that database
  and rejoining under the same node name does not justify treating retained
  shard storage as a fresh pre-activation copy.

## Shard Failover Algorithm (leader only)
1. `IndexMetadata::remove_node(dead_node)` removes the dead node from every
   replica list, increments `unassigned_replicas` on each affected shard, and
   returns orphaned primary shard IDs. Mixed primary/replica roles are accounted
   independently per shard, never gated by aggregate index state.
2. For each orphaned primary:
   - Restrict candidates to the Raft-authoritative
     `ShardRoutingEntry.in_sync_replicas` set.
   - If this leader also hosts the primary, prefer the eligible candidate with
     the highest locally observed ISR checkpoint. Otherwise no local checkpoint
     ranking is available, so use the first live in-sync replica in routing
     order.
   - Call `IndexMetadata::promote_replica_to()`; it independently rejects
     out-of-sync candidates.
   - Increment `unassigned_replicas` for the promoted replica's old slot.
   - If no in-sync copy survives, log the index/shard explicitly, do not
     promote, and leave the missing primary assignment unchanged so health is
     red and the original primary can return with its data.
3. Issue `UpdateIndex` through Raft when any replica slot was removed or any
   primary was promoted.
4. Re-read committed cluster state before processing another dead node so
   sequential removals do not reuse stale routing or double-count slots.
5. If any conditional routing update is rejected, do not remove that node from
   cluster state in the same tick; retry from fresh state.

Promotion changes increment the state-machine-owned shard term. A promoted or
restarted primary still activates once per process before its first write.
Activation persists the durable fence, replays retained WAL state, fills every
missing local sequence through the fenced maximum with durable current-term
NoOps, and only then enables writes. Best-effort NoOp replication may leave a
bounded replica gap for normal probe/removal handling; it must not weaken local
activation. Lifecycle ticks retry failed promotion NoOp batches for the same
active UUID/allocation/term until fan-out succeeds or the copy is invalidated.
These promotion NoOps fill gaps on the promoted copy only; they do not repair
replicas that missed a real post-WAL primary operation. Such copies follow the
gap deadline and peer-recovery path until D10 exists.

Earlier shard schemas, committed boundaries, manifests, WAL entries, and copy
identities never migrate in place. Every role fails closed through the same
typed unsupported-format error and directs the operator to recreate the index.
Whole-index `UpdateIndex` races beyond the enforced routing rules remain future
work.

## AppState (shared across all API handlers)
```rust
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    pub local_node_id: NodeId,
    pub raft: Arc<RaftInstance>,
    pub worker_pools: WorkerPools,
    pub task_manager: Arc<TaskManager>,
    pub storage_manager: Arc<StorageManager>,
    pub security_manager: Arc<SecurityManager>,
    pub remote_store_reader_cache: Arc<RemoteSplitReaderCache>,
    pub sql_group_by_scan_limit: usize,
    pub sql_approximate_top_k: bool,
}
```

`Node.raft` and `AppState.raft` are mandatory. The optional Raft field in
`TransportService` exists for isolated transport tests and must not be copied
back into production node or API state.

## Shared ClusterState wiring (control-plane consumers)
The startup `state_handle: Arc<RwLock<ClusterState>>` is created once and **moved** into
`ClusterManager::with_shared_state(state_handle)`. Any consumer that needs to read
control-plane config on a hot path (e.g. `SecurityManager` reading dynamic API keys/roles)
must hold a `.clone()` of that same `Arc` — capture the clone **before** the move:

```rust
let state_handle = Arc::new(RwLock::new(ClusterState::new(...)));
let security_manager = SecurityManager::with_cluster_state(
    config.security.clone(), state_handle.clone(),   // clone BEFORE the move below
)?;
let cluster_manager = ClusterManager::with_shared_state(state_handle); // moves original
```

Both now observe the exact same Raft-applied state under a short read-lock — no extra I/O,
no polling. Keep a `None`/static constructor (`SecurityManager::new`) so unit tests without
a cluster state still compile. See `control-plane.instructions.md`.
