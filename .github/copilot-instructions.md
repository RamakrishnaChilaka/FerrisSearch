# ROpenSearch — Copilot Context

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
- 116 unit tests + 11 consensus integration + 11 replication integration = 138 total
- Run with: `cargo test`
- Dev cluster: `./dev_cluster.sh 1` and `./dev_cluster.sh 2` (sets unique RAFT_NODE_ID per node)

## Config
- `config/ropensearch.yml` for defaults
- `ROPENSEARCH_*` env vars override (e.g., ROPENSEARCH_RAFT_NODE_ID, ROPENSEARCH_NODE_NAME)
