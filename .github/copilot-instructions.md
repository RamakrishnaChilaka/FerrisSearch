# FerrisSearch — Copilot Context

## Project Overview
Distributed search engine in Rust, inspired by OpenSearch/Elasticsearch.
Uses **Tantivy** for full-text search and **openraft 0.10.0-alpha.17** for Raft consensus.

## Tech Stack
- Rust 1.94.0, edition 2024
- openraft 0.10.0-alpha.17 (features: serde, tokio-rt)
- Tantivy (search engine)
- Axum (HTTP API)
- Tonic/gRPC (inter-node transport)
- Protobuf (proto/transport.proto)

## Architecture
- **Raft consensus** manages cluster state (leader election, node membership, index metadata).
- **ClusterState** is the Raft state machine's data — all mutations go through `raft.client_write(ClusterCommand)`.
- **ClusterManager** wraps `Arc<RwLock<ClusterState>>` shared with the Raft state machine for reads.
- **TransportService** (gRPC) handles inter-node RPCs including Raft vote/append/snapshot.
- **ShardManager** manages local Tantivy index shards.
- Data replication (document-level) still uses gossip/gRPC, separate from Raft.

## Key Modules
- `src/consensus/` — Raft: types.rs, store.rs (MemLogStore with Arc<Mutex> shared state), state_machine.rs, network.rs, mod.rs
- `src/cluster/` — ClusterManager + ClusterState
- `src/node/mod.rs` — Node startup, Raft bootstrap, lifecycle loop
- `src/transport/` — gRPC server (with Raft RPCs) and client
- `src/api/` — Axum HTTP handlers (index CRUD routes through Raft)
- `src/engine/` — Tantivy search engine abstraction
- `src/shard/` — Shard management

## openraft 0.10.0-alpha.17 API Gotchas
- `Vote::new(term: u64, node_id: u64)` — NOT `Vote::new(LeaderId, bool)`
- `LeaderId` is at `openraft::impls::leader_id_adv::LeaderId` with public fields `term`, `node_id`
- `IOFlushed::new()` is `pub(crate)` — use `IOFlushed::noop()` in tests
- `MemLogStore::get_log_reader()` must return a shared-state handle (not a clone) because the SM worker holds the reader permanently
- `raft.add_learner(node_id, BasicNode { addr }, blocking)` then `raft.change_membership(voter_set, false)` to add nodes

## Cluster Commands (Raft log entries)
- `ClusterCommand::AddNode { node }` / `RemoveNode { node_id }`
- `ClusterCommand::CreateIndex { metadata }` / `DeleteIndex { index_name }`
- `ClusterCommand::SetMaster { node_id }`

## Test Suite
- 119 unit tests + 14 consensus integration + 11 replication integration = 144 total
- Run with: `cargo test`
- Dev cluster: `./dev_cluster.sh 1`, `./dev_cluster.sh 2`, `./dev_cluster.sh 3` (sets unique RAFT_NODE_ID per node)

## Node Lifecycle (Raft-driven)
- First node: filters self from seed_hosts → bootstraps single-node Raft → `AddNode` + `SetMaster` via client_write
- Joining node: sends JoinCluster gRPC (with raft_node_id) → leader does `AddNode` + `add_learner` + `change_membership`
- Joining node does NOT call `update_state` — Raft log replication propagates state
- Leader lifecycle loop: SetMaster if needed, dead node scan (15s timeout, 20s grace after becoming leader)
- Follower lifecycle loop: pings the master for liveness

## Important Design Decisions
- `ClusterManager::update_state()` is a full overwrite — never use it to replace Raft-managed state
- `last_seen` is `#[serde(skip)]` — transient, not replicated by Raft. Populated by `add_node()` and `ping_node()`
- New leader gets a 20s grace period (`leader_since`) before scanning for dead nodes to avoid false positives
- `raft_node_id` field on NodeInfo is critical for Raft membership changes — must be non-zero for Raft-managed nodes

## Config
- `config/ferrissearch.yml` for defaults
- `FERRISSEARCH_*` env vars override (e.g., FERRISSEARCH_RAFT_NODE_ID, FERRISSEARCH_NODE_NAME)

## Development Workflow
When implementing any feature or fix:
1. **Read first** — understand existing code before changing it
2. **Implement** — make the code changes
3. **Unit tests** — cover every code path/branch (empty inputs, edge cases, error paths)
4. **Integration tests** — if the feature involves Raft, gRPC, or multi-component interaction
5. **Live test** — spin up a node, exercise the feature via curl, verify output
6. **Fix bugs found in live test** — add a test for each bug discovered
7. **Coverage audit** — check every branch in new code has a test; add missing ones
8. **Update README** — examples, roadmap checkmarks, test counts
9. **Update copilot-instructions.md** — if architecture or conventions changed
