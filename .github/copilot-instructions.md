# FerrisSearch — Copilot Context

## Project Overview
Distributed search engine in Rust, inspired by OpenSearch/Elasticsearch.
Uses **Tantivy** for full-text search and **openraft 0.10.0-alpha.17** for Raft consensus.

## Tech Stack
- Rust 1.94.0, edition 2024
- openraft 0.10.0-alpha.17 (features: serde, tokio-rt)
- Tantivy 0.25.0 (search engine)
- DataFusion 53 / sqlparser 0.61 (SQL layer)
- Axum (HTTP API)
- Tonic 0.13/gRPC (inter-node transport)
- Protobuf (proto/transport.proto)
- redb 3 (persistent Raft log storage — v3 file format, ~15% faster bulk writes, smaller files)
- jemalloc (global allocator via tikv-jemallocator — reduces post-workload RSS retention vs glibc malloc)
- rayon (dedicated thread pools for search/write workload isolation)

## Architecture
- **Raft consensus** manages cluster state (leader election, node membership, index metadata).
- **ClusterState** is the Raft state machine's data — all mutations go through `raft.client_write(ClusterCommand)`.
- **ClusterManager** wraps `Arc<RwLock<ClusterState>>` shared with the Raft state machine for reads.
- **TransportService** (gRPC) handles inter-node RPCs including Raft vote/append/snapshot.
- **ShardManager** manages local Tantivy index shards.
- Data replication (document-level) still uses gossip/gRPC, separate from Raft.

## Key Modules
- `src/consensus/` — Raft: types.rs (TypeConfig, ClusterCommand, ClusterResponse), store.rs (MemLogStore), disk_store.rs, state_machine.rs, network.rs, mod.rs
- `src/cluster/` — ClusterManager (manager.rs) + ClusterState (state.rs) + SettingsManager (settings.rs: reactive pub/sub via watch channels)
- `src/node/mod.rs` — Node struct, startup, Raft bootstrap, lifecycle loop, AppState
- `src/transport/` — gRPC server (server.rs: Raft RPCs + shard ops + replication) and client (client.rs: forwarding methods)
- `src/api/` — Axum HTTP handlers: index.rs (index CRUD, doc ops), search.rs (query-string search), cat.rs (catalog), cluster.rs (health, state, transfer_master), mod.rs (router)
- `src/engine/` — SearchEngine trait (mod.rs), CompositeEngine (composite.rs: Tantivy + USearch), HotEngine (tantivy.rs), VectorIndex (vector.rs: HNSW), routing.rs (Murmur3 shard routing)
- `src/shard/` — ShardManager, ShardKey, IsrTracker, ReplicaCheckpoint
- `src/search/` — SearchRequest, QueryClause, BoolQuery, aggregations, sort, k-NN
- `src/wal/` — HotTranslog (binary length-prefixed WAL), TranslogDurability, WriteAheadLog trait
- `src/worker.rs` — WorkerPools: dedicated rayon thread pools for search/write isolation
- `src/replication/` — replicate_write, replicate_bulk (sync to ISR replicas)
- `src/common/` — `Result<T>` type alias (anyhow), `validate_index_name()`
- `src/config/` — AppConfig (YAML + env var loading)
- `src/storage/` — StorageManager placeholder (future: blob storage abstraction)
- `proto/transport.proto` — gRPC service definition, all message types

## openraft 0.10.0-alpha.17 API Gotchas
- `Vote::new(term: u64, node_id: u64)` — NOT `Vote::new(LeaderId, bool)`
- `LeaderId` is at `openraft::impls::leader_id_adv::LeaderId` with public fields `term`, `node_id`
- `IOFlushed::new()` is `pub(crate)` — use `IOFlushed::noop()` in tests
- `MemLogStore::get_log_reader()` must return a shared-state handle (not a clone) because the SM worker holds the reader permanently
- `raft.add_learner(node_id, BasicNode { addr }, blocking)` then `raft.change_membership(voter_set, false)` to add nodes

## Tantivy Field Schema Flags
- `_id` field uses `(STRING | STORED).set_fast(None)` — enables fast-field columnar access for SQL queries without loading stored docs
- Numeric fields (Integer, Float) use INDEXED | STORED | FAST (mirrors OpenSearch default doc_values: true)
- Keyword and Boolean fields use STRING | STORED + set_fast(None) for dictionary-encoded columnar
- FAST enables columnar storage - critical for range queries, sorting, and aggregations
- Without FAST, range queries scan the inverted index (orders of magnitude slower on high-cardinality fields)

## Fast-Field Aggregations (Single-Pass Collector)
- Aggregations run in the same Tantivy pass as hit collection via custom `AggCollector` (implements `tantivy::collector::Collector`)
- Hit-returning queries combine collectors as `(TopDocs, Option<AggCollector>, Count)`; agg-only `size=0` queries skip `TopDocs` entirely and run `(Option<AggCollector>, Count)`
- `AggSegmentCollector::collect(doc, score)` reads fast-field columns and accumulates stats/terms/histogram per segment; string `terms` aggs count ords per segment and resolve ord→string once in `harvest()`
- `merge_fruits()` merges per-segment data, returns `HashMap<String, PartialAggResult>` per shard
- Per-shard partial results are serialized into the gRPC `partial_aggs_json` bytes field with `bincode-next`, then merged at coordinator via `merge_aggregations()`
- O(matching_docs) with minimal memory; agg-only queries avoid hit materialization and still run in a single query execution pass
- **Shard-level top-K pruning**: When ORDER BY + LIMIT are present on grouped partials, each shard keeps only `(offset + limit) * 3 + 10` buckets (sorted by the ORDER BY metric) before shipping to the coordinator. Uses `select_nth_unstable_by` (O(N) average) on flat-array ordinals BEFORE resolving strings, avoiding ord→string resolution for 99%+ of groups. `ShardTopK { limit, sort_by, sort_function, descending }` on `GroupedMetricsAggParams` carries the hint. This is approximate — the 3× multiplier makes missed global top-K groups extremely unlikely. Disabled when HAVING is present (pruned groups could satisfy HAVING only after cross-shard merge), when ORDER BY has multiple columns (secondary keys not evaluated during pruning), or when ORDER BY references a group column instead of a metric.
- **StringArena**: Batch ordinal→string resolution uses a contiguous `Vec<u8>` arena instead of N individual heap-allocated Strings. Each resolved string is `(offset, len)` into the arena; only the final `serde_json::Value::String` conversion allocates per-group.

## Tantivy Type Safety Gotchas
- **NEVER** create `Term` objects directly with `Term::from_field_text/i64/f64` in query building — always use `self.typed_term(field, value)` which checks the schema field type
- JSON integer `10` on a float field: `serde_json::Number::as_i64()` succeeds before `as_f64()`, creating an `i64` term on an `f64` field → silent 0-hit results. `typed_term()` prevents this.
- `build_tantivy_doc_inner()` takes `&Schema` to check field types before adding numeric values — prevents indexing `i64` into `f64` fields
- Tantivy does NOT error on type-mismatched terms — it silently returns 0 results. Always validate.

## Arrow Bridge Type Safety
- `build_record_batch_with_hints(column_store, type_hints)` uses schema-derived type hints to set correct Arrow column types
- Without type hints, `infer_column_kind()` scans data values and defaults to `Utf8` for empty/all-null columns — this breaks aggregation functions like `avg()`, `sum()` on zero-result queries
- The fast-field path (`sql_record_batch`) MUST populate `type_hints` from `SqlFieldReader` variants (F64/I64 → Float64, Str → Utf8) or the Tantivy schema when no segments exist
- The fast-field path skips `searcher.doc()` entirely when all requested columns have fast-field readers — reads `_id` from its fast-field column instead of loading stored docs
- String fast-field SQL paths use a shared `StringFastFieldReader` (`StrColumn` + ordinal `Column<u64>`) so `_id`, selective arrays, and streaming batches read ordinals directly instead of per-doc `term_ords()` iterators
- In the flat `sql_record_batch()` fast path, `_id` now uses the same per-segment array/take/reorder flow as other string fast fields instead of a separate top-doc-order decode/clone loop
- When the SQL query does not reference `_id` or `_score`, those columns are filled with empty/zero values and fast-field reads for `_id` are skipped entirely (`needs_id`/`needs_score` flags on `QueryPlan`)
- Zero-column SQL queries such as `SELECT 1 FROM ...` keep one row per hit directly in `sql_record_batch()`; do not overload `needs_score` just to preserve batch cardinality
- Ungrouped aggregates (`SELECT count(*), avg(price) FROM ...` without GROUP BY) use the grouped partial path with zero group-by columns — no row materialization needed
- The `materialized_hits_fallback` path uses plain `build_record_batch()` (no hints, data-driven inference only)
- SELECT aliases must be excluded from `required_columns` — aliases (e.g. `total` from `count(*) AS total`) are not real schema fields and cause `SourceFallback` → Null → Utf8 misclassification

## Cluster Commands (Raft log entries)
- `ClusterCommand::AddNode { node: NodeInfo }` — register/update a node in cluster state
- `ClusterCommand::RemoveNode { node_id: String }` — remove node from cluster + Raft membership
- `ClusterCommand::CreateIndex { metadata: IndexMetadata }` — create index with shard routing
- `ClusterCommand::DeleteIndex { index_name: String }` — delete index and all metadata
- `ClusterCommand::SetMaster { node_id: String }` — set cluster master (Raft leader)
- `ClusterCommand::UpdateIndex { metadata: IndexMetadata }` — update shard routing (failover, replica changes, settings)

## ClusterResponse
- `ClusterResponse::Ok` — command applied successfully
- `ClusterResponse::Error(String)` — application error

## Test Suite
- 792 unit tests + 61 CLI tests + 33 consensus integration + 40 replication integration + 24 REST API integration + 1 SQL correctness harness (sqllogictest, 163 assertions) = 951 total
- Run with: `cargo test`
- Feature-gated transport TLS integration coverage: `cargo test --test replication_integration --features transport-tls`
- Dev cluster: `./dev_cluster.sh 1`, `./dev_cluster.sh 2`, `./dev_cluster.sh 3` (sets unique RAFT_NODE_ID per node)
- SQL console: `cargo run --bin ferris-cli` (interactive with `Tab` completion, `\watch`, history, and NDJSON `/_sql/stream` consumption) or `cargo run --bin ferris-cli -- -c "SHOW TABLES"` (single command)

## Node Lifecycle (Raft-driven)
- First node: filters self from seed_hosts → bootstraps single-node Raft → `AddNode` + `SetMaster` via client_write
- Joining node: sends JoinCluster gRPC (with raft_node_id) → leader serializes concurrent joins, validates identity, does `add_learner` for non-voters, applies `AddNode`, then recomputes the latest voter set for `change_membership` (rolling back `AddNode` if promotion fails)
- Joining node does NOT call `update_state` — Raft log replication propagates state, but `JoinCluster` returns an authoritative snapshot for initial shard reopen/cleanup decisions
- Nodes reopen locally assigned shards after startup using the authoritative bootstrap/join snapshot when available, reconcile unopened assignments in the lifecycle loop, and skip orphan cleanup until index UUIDs are known
- Leader lifecycle loop: SetMaster if needed, dead node scan (15s timeout, 20s grace after becoming leader), shard failover (promote best ISR replica to primary for orphaned shards)
- Follower lifecycle loop: pings the master for liveness

## Important Design Decisions
- **Coordinator pattern**: see dedicated section below — NEVER return "not the leader" or "send to master" errors
- `ClusterManager::update_state()` is a full overwrite — never use it to replace Raft-managed state
- `last_seen` is `#[serde(skip)]` — transient, not replicated by Raft. Populated by `add_node()` and `ping_node()`
- New leader gets a 20s grace period (`leader_since`) before scanning for dead nodes to avoid false positives
- Dead node handling: leader removes node from Raft + cluster only after successful membership change, promotes best ISR replica for orphaned primary shards (highest checkpoint wins), increments `unassigned_replicas` for lost replica slots, and refuses removals that would empty the voter set
- `promote_replica_to(shard_id, node_name)` for targeted promotion; `promote_replica()` as fallback (first available)
- Shard failover uses existing `UpdateIndex` Raft command — no new command variant needed
- `raft_node_id` field on NodeInfo is critical for Raft membership changes — must be non-zero for Raft-managed nodes
- `raft_node_id` must remain unique across node IDs; JoinCluster must reject conflicting identity reuse
- Transport `ClusterState` snapshots are authoritative startup inputs — proto/domain roundtrips must preserve `raft_node_id`, `unassigned_replicas`, index `mappings`, index `settings`, and index `uuid` exactly, and must reject unknown field types instead of coercing them

## Dynamic Settings
- `PUT /{index}/_settings` and `GET /{index}/_settings` API endpoints
- `IndexSettings` struct on `IndexMetadata` holds `refresh_interval_ms: Option<u64>`
- `SettingsManager` (src/cluster/settings.rs) uses `tokio::sync::watch` channels for reactive pub/sub
- Consumers subscribe via `watch_refresh_interval()` and react in `tokio::select!` loops
- Non-leader nodes forward settings updates to master via gRPC `UpdateSettings` RPC
- Settings changes go through Raft (`UpdateIndex` command), then `ShardManager::apply_settings()` pushes to `SettingsManager::update()`
- To add a new reactive setting: add field to `IndexSettings`, add `watch::Sender<T>` to `SettingsManager`, detect changes in `update()`, subscribe in consumer

## AppState (shared across all API handlers)
```rust
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    pub local_node_id: String,
    pub raft: Option<Arc<RaftInstance>>,
    pub worker_pools: WorkerPools,
    pub sql_group_by_scan_limit: usize,
}
```

## Core Data Structures (src/cluster/state.rs)

### Enums
- `NodeRole` — `Master`, `Data`, `Client`
- `ShardState` — `Started` (active), `Unassigned` (needs a node)
- `FieldType` — `Text`, `Keyword`, `Integer`, `Float`, `Boolean`, `KnnVector`

### Structs
```
NodeInfo { id: NodeId, name: String, host: String, transport_port: u16, http_port: u16, roles: Vec<NodeRole>, raft_node_id: u64 }
FieldMapping { field_type: FieldType, dimension: Option<usize> }  // dimension for knn_vector only
IndexSettings { refresh_interval_ms: Option<u64> }  // None = cluster default 5000ms
ShardCopy { node_id: Option<NodeId>, state: ShardState }
ShardRoutingEntry { primary: NodeId, replicas: Vec<NodeId>, unassigned_replicas: u32 }
IndexMetadata { name, uuid, number_of_shards, number_of_replicas, shard_routing: HashMap<u32, ShardRoutingEntry>, mappings: HashMap<String, FieldMapping>, settings: IndexSettings }
ClusterState { cluster_name, version: u64, master_node: Option<NodeId>, nodes: HashMap<NodeId, NodeInfo>, indices: HashMap<String, IndexMetadata>, last_seen: HashMap<NodeId, Instant> }
```

### Key Methods
- `IndexMetadata::build_shard_routing(name, num_shards, num_replicas, data_nodes)` — round-robin distribution
- `IndexMetadata::promote_replica(shard_id)` / `promote_replica_to(shard_id, new_primary)` — shard failover
- `IndexMetadata::remove_node(node_id) -> Vec<u32>` — returns orphaned primary shard IDs
- `IndexMetadata::update_number_of_replicas(new_replicas) -> Vec<(u32, String)>` — returns deleted replica slots
- `IndexMetadata::allocate_unassigned_replicas(data_nodes) -> bool`
- `ClusterState::add_node(node)`, `remove_node(node_id)`, `ping_node(node_id)`, `add_index(metadata)`

## ClusterManager (src/cluster/manager.rs)
```rust
pub struct ClusterManager { state: Arc<RwLock<ClusterState>> }
```
- `new(cluster_name)` / `with_shared_state(state: Arc<RwLock<ClusterState>>)` — Raft SM shares state
- `get_state() -> ClusterState` — cloned snapshot (read lock)
- `add_node(node)`, `ping_node(node_id)`, `update_state(new_state)` — full overwrite, preserves `last_seen`

## Config
```rust
pub struct AppConfig {
    pub node_name: String,              // default: "node-1"
    pub cluster_name: String,           // default: "ferrissearch"
    pub http_port: u16,                 // default: 9200
    pub transport_port: u16,            // default: 9300
    pub data_dir: String,               // default: "./data"
    pub seed_hosts: Vec<String>,        // default: ["127.0.0.1:9300"]
    pub raft_node_id: u64,              // default: 1
    pub translog_durability: String,    // "request" (default) or "async"
    pub translog_sync_interval_ms: Option<u64>,  // default: None (5000 if async)
    pub sql_group_by_scan_limit: usize,          // default: 1_000_000 (0 = unlimited)
    pub transport_tls_enabled: bool,             // default: false (requires transport-tls feature)
    pub transport_tls_cert_file: Option<String>, // PEM cert for gRPC server
    pub transport_tls_key_file: Option<String>,  // PEM key for gRPC server
    pub transport_tls_ca_file: Option<String>,   // PEM CA for client verification
}
```
- Load order: defaults → `config/ferrissearch.yml` → `FERRISSEARCH_*` env vars
- `translog_durability: "request"` = fsync per write (no data loss); `"async"` = background fsync timer
- `transport_tls_enabled: true` requires building with `--features transport-tls`; startup must fail on missing CA/cert/key paths or missing feature instead of silently downgrading to plaintext
- The transport TLS helpers must install the rustls ring crypto provider before building server/client TLS config; otherwise feature builds can panic at runtime.

## Coordinator Pattern (CRITICAL — read before writing any API handler)

**Every node is a coordinator.** Any HTTP request landing on any node MUST be transparently routed to the correct node. The client should never need to know which node is the leader or which node owns a shard.

### Rules
1. **NEVER** return errors like "not the leader", "send to master", "SERVICE_UNAVAILABLE/master_not_discovered" from any API handler.
2. If the handler needs the Raft leader (index CRUD, settings updates, cluster ops), check `raft.is_leader()`:
   - If leader → execute locally via `raft.client_write(cmd)`
   - If NOT leader → look up master from `cluster_state.master_node`, forward via gRPC, return the response
3. If the handler needs a specific shard primary (document index/get/delete), use `calculate_shard()` → look up primary node → forward via gRPC `forward_*_to_shard()`
4. If the handler is read-only from cluster state (health, cat, get settings) → serve locally, no forwarding needed

### Forwarding implementation pattern
```rust
// In the API handler:
if let Some(ref raft) = state.raft {
    if !raft.is_leader() {
        let cs = state.cluster_manager.get_state();
        let master_id = cs.master_node.as_ref().ok_or(/* SERVICE_UNAVAILABLE only if no master exists at all */)?;
        let master_node = cs.nodes.get(master_id).ok_or(...)?;
        match state.transport_client.forward_<operation>(&master_node, ...).await {
            Ok(resp) => return (StatusCode::OK, Json(resp)),
            Err(e) => return error_response(INTERNAL_SERVER_ERROR, "forward_exception", ...),
        }
    }
    // Leader path: execute via Raft
    raft.client_write(cmd).await?;
}
```

### Existing forwarding gRPC RPCs (proto/transport.proto)
| RPC | Purpose | Client method |
|-----|---------|---------------|
| `CreateIndex` | Forward index creation to leader | `forward_create_index()` |
| `DeleteIndex` | Forward index deletion to leader | `forward_delete_index()` |
| `UpdateSettings` | Forward settings update to leader | `forward_update_settings()` |
| `TransferMaster` | Forward leadership transfer to leader | `forward_transfer_master()` |
| `IndexDoc` | Route doc write to shard primary | `forward_index_to_shard()` |
| `DeleteDoc` | Route doc delete to shard primary | `forward_delete_to_shard()` |
| `GetDoc` | Route doc get to shard primary | `forward_get_to_shard()` |
| `BulkIndex` | Route bulk write to shard primary | `forward_bulk_to_shard()` |
| `SearchShard` | Scatter search to remote shards | `forward_search_to_shard()` |
| `SearchShardDsl` | Scatter DSL search to remote shards | `forward_search_dsl_to_shard()` |
| `SqlRecordBatch` | Scatter SQL fast-field batch to remote shards (Arrow IPC) | `forward_sql_batch_to_shard()` |
| `SqlRecordBatchStream` | Stream multiple SQL fast-field batches from a remote shard (Arrow IPC) | `forward_sql_batch_stream_to_shard()` |

### Adding a new Raft-write API endpoint (checklist)
1. Add proto messages (`<Op>Request` / `<Op>Response`) to `proto/transport.proto`
2. Add the RPC to the `InternalTransport` service definition
3. Add server handler in `src/transport/server.rs` (must check `is_leader()`, execute via Raft)
4. Add client forwarding method `forward_<op>()` in `src/transport/client.rs`
5. In the API handler: if not leader → call `forward_<op>()`, else → `raft.client_write()`
6. **NEVER** return a "not the leader" error — always forward

## API Endpoints

### Cluster & Catalog (read-only, serve locally)
| HTTP | Path | Handler | Purpose |
|------|------|---------|--------
| GET | `/` | `handle_root()` | Node info |
| GET | `/_cluster/health` | `get_health()` | Cluster health (green/yellow/red) |
| GET | `/_cluster/state` | `get_state()` | Full cluster state JSON |
| GET | `/_cat/nodes` | `cat_nodes()` | Tabular node listing (`?v` for headers) |
| GET | `/_cat/shards` | `cat_shards()` | Shard allocation (prirep=p/r, state, docs, node) |
| GET | `/_cat/indices` | `cat_indices()` | Index listing (health, shards, docs) |
| GET | `/_cat/master` | `cat_master()` | Current master node |

### Index Management (Raft writes → forward to leader)
| HTTP | Path | Handler | Purpose |
|------|------|---------|--------|
| HEAD | `/{index}` | `index_exists()` | Check if index exists (204/404) |
| PUT | `/{index}` | `create_index()` | Create index with settings/mappings |
| DELETE | `/{index}` | `delete_index()` | Delete index and all shards |
| GET | `/{index}/_settings` | `get_index_settings()` | Get index settings (read-only, local) |
| PUT | `/{index}/_settings` | `update_index_settings()` | Update settings (forwarded to leader) |
| POST | `/_cluster/transfer_master` | `transfer_master()` | Leadership transfer (forwarded) |

### Document Operations (routed to shard primary)
| HTTP | Path | Handler | Purpose |
|------|------|---------|--------|
| POST | `/{index}/_doc` | `index_document()` | Index doc (auto-generate ID) |
| PUT | `/{index}/_doc/{id}` | `index_document_with_id()` | Index doc with explicit ID |
| GET | `/{index}/_doc/{id}` | `get_document()` | Retrieve document by ID |
| DELETE | `/{index}/_doc/{id}` | `delete_document()` | Delete document by ID |
| POST | `/{index}/_update/{id}` | `update_document()` | Partial document update |
| POST | `/_bulk` | `bulk_index_global()` | Bulk indexing (no index in path) |
| POST | `/{index}/_bulk` | `bulk_index()` | Bulk indexing (index in path) |

### Search
| HTTP | Path | Handler | Purpose |
|------|------|---------|--------|
| GET | `/{index}/_search` | `search_documents()` | Query-string search (q=, size, from) |
| POST | `/{index}/_search` | `search_documents_dsl()` | DSL search (SearchRequest body) |
| GET/POST | `/{index}/_count` | `count_documents()` | Document count (match_all fast path or query) |
| POST | `/{index}/_sql` | `search_sql()` | SQL over matched docs (search-aware planning) |
| POST | `/{index}/_sql/stream` | `search_sql_stream()` | NDJSON SQL stream (`meta` frame then `rows` frames) |
| POST | `/{index}/_sql/explain` | `explain_sql()` | Explain SQL plan without executing |
| POST | `/_sql` | `global_sql()` | Global SQL: SHOW TABLES, DESCRIBE, SHOW CREATE TABLE, SELECT (auto-extracts index from FROM) |
| POST | `/_sql/stream` | `global_sql_stream()` | Global NDJSON SQL stream endpoint |

### Maintenance
| HTTP | Path | Handler | Purpose |
|------|------|---------|--------|
| POST/GET | `/{index}/_refresh` | `refresh_index()` | Refresh all local shards |
| POST/GET | `/{index}/_flush` | `flush_index()` | Flush shards + truncate WAL |
| GET | `/_metrics` | `handle_metrics()` | Prometheus metrics (text exposition format) |

### Create Index Body Format
```json
{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1, "refresh_interval_ms": 5000 },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "status": { "type": "keyword" },
      "embedding": { "type": "knn_vector", "dimension": 768 }
    }
  }
}
```

## Search & Query DSL (src/search/mod.rs)

### SearchRequest
```rust
pub struct SearchRequest {
    pub query: QueryClause,                         // default: MatchAll
    pub size: usize,                                // default: 10
    pub from: usize,                                // default: 0
    pub knn: Option<KnnQuery>,                      // optional k-NN
    pub sort: Vec<SortClause>,                      // default: sort by _score desc
    pub aggs: HashMap<String, AggregationRequest>,  // aggregations
}
```

### QueryClause Variants
- `MatchAll(Value)` — match all documents
- `Match(HashMap<String, Value>)` — full-text match on a field
- `Term(HashMap<String, Value>)` — exact term match
- `Wildcard(HashMap<String, Value>)` — wildcard pattern (`*` any, `?` single)
- `Prefix(HashMap<String, Value>)` — prefix match
- `Fuzzy(HashMap<String, FuzzyParams>)` — fuzzy match (edit distance 0-2, default 1)
- `Range(HashMap<String, RangeCondition>)` — range: `{ "gt", "gte", "lt", "lte" }`
- `Bool(BoolQuery)` — `{ must: [], should: [], must_not: [], filter: [] }`

### k-NN Search
```rust
KnnQuery { fields: HashMap<String, KnnParams> }
KnnParams { vector: Vec<f32>, k: usize, filter: Option<QueryClause> }  // optional pre-filter
```

### Aggregations
- `Terms { field, size }` — top-N buckets by value (default size 10)
- `Stats { field }` — min, max, sum, count, avg
- `Min/Max/Avg/Sum/ValueCount { field }` — single metric
- `Histogram { field, interval }` — fixed-interval numeric buckets
- Per-shard: Tantivy `AggCollector` produces partial results → coordinator: `merge_aggregations()`

### Sort
- `SortClause::Simple(String)` — `"_score"` or field name
- `SortClause::Field(HashMap<String, SortOrder>)` — `{ "year": "desc" }`

## Engine Layer

### SearchEngine Trait (src/engine/mod.rs)
```rust
pub trait SearchEngine: Send + Sync {
    fn add_document(&self, doc_id: &str, payload: Value) -> Result<String>;
    fn add_document_with_seq(&self, doc_id: &str, payload: Value, seq_no: u64) -> Result<String>;
    fn bulk_add_documents(&self, docs: Vec<(String, Value)>) -> Result<Vec<String>>;
    fn bulk_add_documents_with_start_seq(&self, docs: Vec<(String, Value)>, start_seq_no: u64) -> Result<Vec<String>>;
    fn delete_document(&self, doc_id: &str) -> Result<u64>;
    fn delete_document_with_seq(&self, doc_id: &str, seq_no: u64) -> Result<u64>;
    fn get_document(&self, doc_id: &str) -> Result<Option<Value>>;
    fn refresh(&self) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn flush_with_global_checkpoint(&self) -> Result<()>;
    fn search(&self, query_str: &str) -> Result<Vec<Value>>;
    fn search_query(&self, req: &SearchRequest) -> Result<(Vec<Value>, usize, HashMap<String, PartialAggResult>)>;
    fn sql_record_batch(&self, req: &SearchRequest, columns: &[String], needs_id: bool, needs_score: bool) -> Result<Option<SqlBatchResult>>;
    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<Value>>;
    fn search_knn_filtered(&self, field: &str, vector: &[f32], k: usize, filter: Option<&QueryClause>) -> Result<Vec<Value>>;
    fn doc_count(&self) -> u64;
    fn local_checkpoint(&self) -> u64;
    fn update_local_checkpoint(&self, seq_no: u64);
    fn global_checkpoint(&self) -> u64;
    fn update_global_checkpoint(&self, checkpoint: u64);
}
```

### CompositeEngine (src/engine/composite.rs)
- Combines HotEngine (Tantivy, full-text) + VectorIndex (USearch, HNSW)
- `new_with_mappings(data_dir, refresh_interval, mappings, durability)` — creates both engines
- `start_refresh_loop_reactive(engine, refresh_rx)` — reactive to settings changes via watch channel
- Auto-detects vectors in payloads and indexes to USearch
- `rebuild_vectors()` — recovers vector index from persisted Tantivy docs on startup

### HotEngine (src/engine/tantivy.rs)
- `field_registry: RwLock<FieldRegistry>` — maps field names to Tantivy Field handles
- Dynamic field creation on first document encounter
- `"body"` field as catch-all for textual content
- `matching_doc_ids(clause)` — returns doc ID set for k-NN pre-filtering
- `replay_translog()` — crash recovery from WAL, replaying only entries at or above the persisted committed checkpoint

### VectorIndex (src/engine/vector.rs)
- USearch HNSW wrapper (connectivity=16, expansion_add=128, expansion_search=64)
- `add_with_doc_id(doc_id, vector)`, `search(query, k) -> (keys, distances)`
- Binary persistence: `save(path)` / `open(path, dimensions, metric)`
- Doc ID ↔ numeric key mapping via `HashMap` + bincode serialization

### Routing (src/engine/routing.rs)
- `calculate_shard(doc_id, num_shards) -> u32` — Murmur3 hash modulo
- `route_document(doc_id, metadata) -> Option<NodeId>` — returns primary node for doc

## Shard Management (src/shard/mod.rs)

### ShardManager
```rust
pub struct ShardManager {
    data_dir: PathBuf,
    shards: RwLock<HashMap<ShardKey, Arc<dyn SearchEngine>>>,
    settings_managers: RwLock<HashMap<String, Arc<SettingsManager>>>,  // per-index
    index_uuids: RwLock<HashMap<String, String>>,  // index_name → UUID for on-disk dirs
    pub isr_tracker: IsrTracker,
    durability: TranslogDurability,
}
```

### UUID-Based Data Directories
- On-disk path: `<data_dir>/<uuid>/shard_<id>` (NOT `<data_dir>/<index_name>/shard_<id>`)
- `IndexMetadata.uuid` is a UUID v4 string, auto-generated on index creation
- Passed to `open_shard_with_settings(index, shard_id, mappings, settings, index_uuid)`
- `close_index_shards()` uses stored UUID mapping to find the directory to delete
- `cleanup_orphaned_data(known_uuids)` deletes directories not matching any authoritative known index UUID; startup must skip cleanup until index UUIDs are available
- **NEVER** construct shard paths from index names — always use the UUID

### Key Methods
- `open_shard_with_settings(index, shard_id, mappings, settings, index_uuid)` — creates CompositeEngine, starts reactive refresh, rebuilds vectors
- `get_shard(index, shard_id)`, `get_index_shards(index)`, `all_shards()`
- `close_index_shards(index)` — remove engines, clean ISR, delete directory
- `apply_settings(index, new_settings)` — notify consumers via watch channels

### ISR Tracking
```rust
IsrTracker { replicas: HashMap<ShardKey, HashMap<String, ReplicaCheckpoint>>, max_lag: u64 }
ReplicaCheckpoint { checkpoint: u64, last_updated: Instant }
```
- `update_replica_checkpoint(index, shard_id, node_id, checkpoint)`
- `in_sync_replicas(index, shard_id, primary_checkpoint) -> Vec<String>` — replicas within max_lag

## WAL — Write-Ahead Log (src/wal/mod.rs)

### TranslogDurability
- `Request` — fsync per write (default, no data loss on crash)
- `Async { sync_interval_ms }` — background fsync timer (up to sync_interval_ms data loss)

### TranslogEntry & WriteAheadLog Trait
```rust
TranslogEntry { seq_no: u64, op: String /* "index"|"delete" */, payload: Value }

trait WriteAheadLog: Send + Sync {
    fn append(&self, op: &str, payload: Value) -> Result<TranslogEntry>;
    fn append_bulk(&self, ops: &[(&str, Value)]) -> Result<Vec<TranslogEntry>>;
    fn read_all(&self) -> Result<Vec<TranslogEntry>>;
    fn read_from(&self, after_seq_no: u64) -> Result<Vec<TranslogEntry>>;  // replica recovery
    fn truncate(&self) -> Result<()>;       // clear after commit
    fn truncate_below(&self, global_checkpoint: u64) -> Result<()>;
    fn last_seq_no(&self) -> u64;
    fn next_seq_no(&self) -> u64;
}
```

### HotTranslog (Binary Format)
- Length-prefixed: `[u32 LE: payload_len][bincode(WireEntry { seq_no, op, payload_json })]`
- Seq numbers are monotonically increasing, survive truncation (persisted in `.seqno` file)
- `translog.committed` stores the exclusive committed seq_no so restart replay skips already committed entries
- Handles partial writes at EOF gracefully (skips/truncates)

## Replication Protocol (src/replication/mod.rs)
- `replicate_write(transport_client, cluster_state, index, shard_id, doc_id, payload, op, seq_no)` — sync single write to all replicas
- `replicate_bulk(transport_client, cluster_state, index, shard_id, docs, start_seq_no)` — batch replication
- Synchronous: primary waits for ALL ISR replicas before acknowledging client
- Uses gRPC: `replicate_to_shard()` / `replicate_bulk_to_shard()`
- Each replica returns `local_checkpoint` after applying
- Replica recovery: `RecoverReplica` RPC fetches missed ops from primary's WAL via `read_from(checkpoint)`

## Consensus Module (src/consensus/)

### Raft Type Configuration (types.rs)
```rust
openraft::declare_raft_types!(
    pub TypeConfig: D = ClusterCommand, R = ClusterResponse, Node = BasicNode
);
type RaftInstance = openraft::Raft<TypeConfig, ClusterStateMachine>;
```

### State Machine (state_machine.rs)
```rust
ClusterStateMachine { state: Arc<RwLock<ClusterState>>, last_applied: Option<LogId>, last_membership: StoredMembership }
```
- Apply: AddNode → `state.add_node()`, RemoveNode → `state.remove_node()`, CreateIndex → `state.add_index()`, DeleteIndex → remove from indices, SetMaster → set master_node, UpdateIndex → replace shard_routing
- Snapshot format: JSON-serialized ClusterState, ID: `snap-{last_applied_index}`

### Raft Config
- heartbeat_interval: 1000ms, election_timeout_min: 3000ms, election_timeout_max: 6000ms

### Module Functions
- `create_raft_instance(node_id, cluster_name, data_dir)` — persistent disk store (production)
- `create_raft_instance_mem(node_id, cluster_name)` — in-memory (tests only)
- `bootstrap_single_node(raft, node_id, addr)` — initialize single-node Raft cluster

## gRPC Transport (proto/transport.proto)

### InternalTransport Service — All RPCs
```
// Cluster coordination
JoinCluster(JoinRequest) → JoinResponse
PublishState(PublishStateRequest) → Empty
Ping(PingRequest) → Empty

// Shard document operations
IndexDoc(ShardDocRequest) → ShardDocResponse
BulkIndex(ShardBulkRequest) → ShardBulkResponse
DeleteDoc(ShardDeleteRequest) → ShardDeleteResponse
GetDoc(ShardGetRequest) → ShardGetResponse

// Shard search
SearchShard(ShardSearchRequest) → ShardSearchResponse
SearchShardDsl(ShardSearchDslRequest) → ShardSearchResponse

// Distributed SQL (Arrow IPC)
SqlRecordBatch(SqlRecordBatchRequest) → SqlRecordBatchResponse
SqlRecordBatchStream(SqlRecordBatchRequest) → stream SqlRecordBatchResponse

// Replication
ReplicateDoc(ReplicateDocRequest) → ReplicateDocResponse
ReplicateBulk(ReplicateBulkRequest) → ReplicateBulkResponse
RecoverReplica(RecoverReplicaRequest) → RecoverReplicaResponse

// Settings + index management (forwarded to leader)
UpdateSettings(UpdateSettingsRequest) → UpdateSettingsResponse
CreateIndex(CreateIndexRequest) → CreateIndexResponse
DeleteIndex(DeleteIndexRequest) → DeleteIndexResponse
TransferMaster(TransferMasterRequest) → TransferMasterResponse

// Raft consensus (opaque JSON)
RaftVote(RaftRequest) → RaftReply
RaftAppendEntries(RaftRequest) → RaftReply
RaftSnapshot(RaftRequest) → RaftReply
```

## Validation & Common (src/common/mod.rs)
- `type Result<T> = std::result::Result<T, anyhow::Error>`
- `validate_index_name(name)` — not empty, max 255 chars, cannot start with `.` or `_`, lowercase alphanumeric + hyphens + underscores only

## Document Indexing Flow
1. API handler receives JSON → generate/extract `_id`
2. `calculate_shard(doc_id, num_shards)` → determine target shard
3. If shard primary is local → index directly via `engine.add_document()`
4. If shard primary is remote → forward via gRPC `forward_index_to_shard()`
5. Primary writes to WAL → indexes in Tantivy + USearch → replicates to all ISR replicas
6. Return `{ "_id", "_version", "result": "created|updated" }`

## Search Flow (Scatter-Gather)
1. Coordinator receives `POST /{index}/_search` with SearchRequest body
2. Look up all shards for the index from cluster state
3. For local shards → `engine.search_query(req)` directly
4. For remote shards → scatter via gRPC `forward_search_dsl_to_shard()`
5. Gather results, merge hits by score, merge aggregations, apply from/size
6. Return unified `{ "_shards": {...}, "hits": { "total": {...}, "hits": [...] }, "aggregations": {...} }`

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

## Hybrid Search + SQL Guidance

If you add a hybrid execution path that mixes full-text search with SQL-style projection, sorting, or aggregation, keep responsibilities separated.

## Fast-Field Access For Hybrid Execution

- Do NOT change Tantivy's fast-field on-disk format to implement search-aware planning, distributed partial execution, or grouped analytics over matched docs.
- Read fast fields directly from Rust through Tantivy's segment readers:
    - numeric: `segment_reader.fast_fields().f64(name)` / `.i64(name)`
    - string/keyword: `segment_reader.fast_fields().str(name)` plus `term_ords(doc)` and `ord_to_str(ord, buf)`
- For shard-local partial execution, prefer segment-local collectors and column readers over `_source` materialization.
- For grouped analytics, compute shard-local partials from fast fields, ship compact partial states, and merge at the coordinator. Do not ship full matched rows unless the query needs expressions that cannot run from fast fields.
- Runtime SQL reporting should keep `execution_mode` at the authoritative high-level path; if a local fast-field fallback uses bitset streaming internally, expose that separately via `streaming_used` instead of inventing a new execution mode.
- Grouped-partial wire formats must preserve numeric group-key type fidelity across shards. Do not coerce integer group keys into `f64`, or coordinator merge can split logically identical buckets like `0` and `0.0` between local and remote shards.
- Only introduce new storage or sidecar column formats if a required SQL feature cannot be served by Tantivy fast fields or stored fields. Planning and partial aggregation alone are not sufficient reasons.

### Responsibility Split
- Tantivy handles text matching, ranking, and returns `(doc_id, score)`
- Arrow holds in-memory columnar batches for matched docs or merged partial states
- Prefer reading Tantivy fast fields directly for structured SQL columns instead of materializing `_source` into temporary row objects when all needed fields are available columnarly
- DataFusion handles residual relational work on already-filtered matched docs or merged partial states
- DataFusion must not become the text-search engine

### What Stays In Tantivy
- Query planning pushdown for `text_match(...)` predicates (including multiple top-level `AND`ed matches), exact filters, and range filters
- Ranking and hit collection
- Fast-field reads for numeric and keyword columns
- Search-native shard-local partial aggregation over matched docs
- Compact per-shard partial state production for distributed grouped analytics

### What Stays In DataFusion
- SQL projection semantics
- Alias handling and expression evaluation after pushdown
- Residual predicates that cannot be pushed into Tantivy safely
- Final `GROUP BY`, `ORDER BY`, and aggregate execution when the query cannot be fully answered from shard-local partial states
- Final tabular shaping of Arrow batches into SQL result rows

### What This Must NOT Become
- Do not describe or implement the feature as "SQL over hits".
- Do not make row-materialized post-processing the normal execution model.
- Do not treat DataFusion as the default engine for matched documents once a query has already been narrowed by search-aware planning.
- The target architecture is a true hybrid planner: Tantivy executes search-native work first, shard-local partials are produced where possible, and DataFusion finishes only the remaining relational semantics.

### What To Move Next
- Keep DataFusion as the residual/final relational executor after Tantivy pushdown, fast-field reads, and shard-local partial execution
- Reduce fallback to materialized hits to only the cases that require unsupported expressions, wildcard projection, or unavailable columnar data
- Extend aggregate pushdown beyond the current grouped partial path (`count`, `min`, `max`, `sum`, `avg` for more eligible shapes)
- Treat `materialized_hits_fallback` as a compatibility path, not the target architecture

### Critical Invariants
- `doc_id` must equal the row index in any columnar representation used for hybrid execution
- Direct lookup should remain `column[doc_id as usize]`; avoid extra maps and indirection unless there is a proven need
- `_score` must be represented as a normal Arrow/DataFusion column so SQL can sort or aggregate over it
- Simple structured predicates (`=`, `>`, `>=`, `<`, `<=`) should be pushed into Tantivy `Term`/`Range` queries before DataFusion sees the rows

### Required Comments To Anchor Generation
When creating new hybrid-search modules, add explicit comments like these near the core flow:

```rust
// CRITICAL DESIGN:
// doc_id is the row index in all columnar arrays
// Do not introduce mappings or indirection
// Access pattern must be: column[doc_id]
```

```rust
// IMPORTANT:
// Treat '_score' as a normal column in Arrow
// so that SQL can sort and filter using it.
```

```rust
// Execution pipeline:
// 1. Run Tantivy search -> Vec<(doc_id, score)>
// 2. Fetch column values using doc_id
// 3. Build Arrow arrays
// 4. Run DataFusion for aggregation/sorting
```

```rust
// IMPORTANT:
// DataFusion is only used for aggregation and sorting
// It should NOT handle text search
```

```rust
// IMPORTANT:
// Prefer shard-local partial states over shipping matched rows
// to the coordinator for grouped analytics.
```

```rust
// IMPORTANT:
// The goal is not SQL over hits.
// The goal is search-aware planning with residual SQL execution.
```

### Suggested Implementation Order
1. Tantivy search executor returning `Vec<(u32, f32)>`
2. Column store with direct `doc_id -> row index` semantics
3. Arrow `RecordBatch` bridge that includes `_score`
4. DataFusion execution for projection, sort, `avg`, and `count`
5. Query planner that splits `text_match(...)` from SQL-style operations
6. Shard-local partial aggregation on fast fields for `GROUP BY` / `COUNT` / `SUM` / `MIN` / `MAX` / `AVG`
7. Coordinator merge of compact partial states before any fallback to row materialization

### Copilot Review Standard
Generated code is acceptable only if it preserves the invariants above and avoids unnecessary copying. Prefer slices, iterators, builders, and cache-friendly access patterns over collecting intermediate vectors unless materialization is required by the API boundary.

## Vector Search Plan (0.1.0)
Uses USearch (C++ with Rust bindings) for HNSW-based approximate nearest neighbor search.

Architecture per shard:
- Tantivy index — full-text (inverted index, BM25)
- USearch index — vector (HNSW graph, cosine/L2/IP)
- WAL — crash recovery for both

Implementation phases:
1. **Foundation** — Add usearch dep, create VectorIndex wrapper, knn_vector field type in mappings, index/search vectors on single shard
2. **Distribution** — Wire into shard manager, scatter-gather for knn across shards, gRPC forwarding for vector queries
3. **Hybrid search** — Combine BM25 + vector similarity scores in one query, from/size, pre-filtering with bool/range

API (OpenSearch k-NN compatible):
- Index: `PUT /my-index/_doc/1` with `{"embedding": [0.1, 0.2, ...], "title": "..."}`
- Search: `POST /my-index/_search` with `{"knn": {"embedding": {"vector": [0.1, ...], "k": 10}}}`
- Hybrid: `{"query": {"match": ...}, "knn": {"embedding": {...}}}`
