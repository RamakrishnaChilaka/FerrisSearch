# Transport Module — src/transport/

## gRPC Service Definition (proto/transport.proto)

### InternalTransport Service — All RPCs
```
// Cluster coordination
JoinCluster(JoinRequest) → JoinResponse
PublishState(PublishStateRequest) → Empty
Ping(PingRequest) → Empty

// Document operations (routed to shard primary)
IndexDoc(ShardDocRequest) → ShardDocResponse
BulkIndex(ShardBulkRequest) → ShardBulkResponse
DeleteDoc(ShardDeleteRequest) → ShardDeleteResponse
GetDoc(ShardGetRequest) → ShardGetResponse

// Search (scatter to remote shards)
SearchShard(ShardSearchRequest) → ShardSearchResponse
SearchShardDsl(ShardSearchDslRequest) → ShardSearchResponse

// Distributed SQL (scatter Arrow IPC batches from remote shards)
SqlRecordBatch(SqlRecordBatchRequest) → SqlRecordBatchResponse
SqlRecordBatchStream(SqlRecordBatchRequest) → stream SqlRecordBatchResponse

// Replication (primary → replica)
ReplicateDoc(ReplicateDocRequest) → ReplicateDocResponse
ReplicateBulk(ReplicateBulkRequest) → ReplicateBulkResponse
RecoverReplica(RecoverReplicaRequest) → RecoverReplicaResponse

// Forwarded to leader
UpdateSettings(UpdateSettingsRequest) → UpdateSettingsResponse
CreateIndex(CreateIndexRequest) → CreateIndexResponse
DeleteIndex(DeleteIndexRequest) → DeleteIndexResponse
TransferMaster(TransferMasterRequest) → TransferMasterResponse

// Shard stats (for _cat endpoints)
GetShardStats(ShardStatsRequest) → ShardStatsResponse

// Index maintenance (fan-out from coordinator)
RefreshIndex(IndexMaintenanceRequest) → IndexMaintenanceResponse
FlushIndex(IndexMaintenanceRequest) → IndexMaintenanceResponse

// Raft consensus (opaque JSON payloads)
RaftVote(RaftRequest) → RaftReply
RaftAppendEntries(RaftRequest) → RaftReply
RaftSnapshot(RaftRequest) → RaftReply
```

## TransportService (src/transport/server.rs)
```rust
pub struct TransportService {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    pub transport_client: TransportClient,
    pub raft: Option<Arc<RaftInstance>>,
    pub local_node_id: String,  // filters locally-assigned shards
}
```
Implements `InternalTransport` trait. All RPC handlers check Raft leadership or route to the correct shard.

### Shard Stats & Maintenance
- `get_shard_stats` only reports on **already-open** shards via `all_shards()` — it does NOT reopen shards from disk
- `refresh_index` / `flush_index` use `run_maintenance_on_assigned_shards()` which checks the routing table and only operates on shards where `primary == local_node_id` or the node is in `replicas` — orphaned shards are skipped
- The `local_node_id` field is required by the constructors: `create_transport_service(cm, sm, tc, local_node_id)` and `create_transport_service_with_raft(cm, sm, tc, raft, local_node_id)`

### Key Handler Patterns
- **join_cluster**: If leader → serialize concurrent joins, validate `node_id` / `raft_node_id`, register the transport address with `add_learner()` for non-voters, apply `AddNode`, then recompute the latest full voter set before `change_membership()`. If promotion fails, roll back the `AddNode`. If follower → **forwards to leader** via gRPC. NEVER mutate cluster state locally on a follower.
- **index_doc / bulk_index / delete_doc**: Look up shard in ShardManager, execute engine operation, replicate to all replicas. **Returns `success: false` if replication fails** — write is only acknowledged after all ISR replicas confirm (synchronous replication contract).
- **replicate_doc / replicate_bulk**: Apply to local replica engine using the seq_no supplied by the primary, persist that same seq_no in the replica WAL, return checkpoint
- **recover_replica**: Read WAL entries via `read_from()`, return operations
- **search_shard / search_shard_dsl**: Execute local shard search, return results
- **sql_record_batch / sql_record_batch_stream**: Execute local shard SQL fast-field reads and return Arrow IPC batches. `SqlRecordBatchStream` may emit multiple batches for the same shard; `batch_size = 0` means use the engine default. Stream responses must carry `total_hits`, `collected_rows`, and actual `streaming_used` metadata on every batch so the coordinator can build accurate `meta` / truncation decisions before draining the rest of the stream. The coordinator-facing live path should prefer `open_sql_batch_stream_to_shard()` so only the first response is read eagerly.
- **raft_vote / raft_append_entries / raft_snapshot**: Deserialize JSON, forward to Raft instance
- **create_index / delete_index**: Must be leader; execute via `raft.client_write()`. `create_index` must preserve index settings from the forwarded JSON body, including `refresh_interval_ms` and `flush_threshold_bytes`.
- **update_settings**: Must be leader; apply via `UpdateIndex` Raft command. Preserve `flush_threshold_bytes` exactly, including `null` resets and `0` as a valid disable value.

### Critical Invariants
- **join_cluster MUST forward on followers**: A follower receiving a JoinCluster RPC must forward it to the Raft leader. It must NEVER fall through to `cluster_manager.add_node()` when Raft is active, as this would add the node to local state without Raft membership.
- **Join identity MUST be unique and stable**: `raft_node_id` cannot be reused by a different logical node, and an existing `node_id` cannot silently switch to a different `raft_node_id`. Reject the join instead of mutating membership.
- **Shard writes MUST fail on replication failure**: The `index_doc`, `bulk_index`, and `delete_doc` handlers must return `success: false` when `replicate_write()` / `replicate_bulk()` returns `Err`. Logging the error and returning `success: true` violates the synchronous replication contract.
- **Replica apply MUST preserve primary seq_nos**: `replicate_doc`, `replicate_bulk`, and recovery replay must call the explicit-seq engine methods. Do not route replicated writes through local seq allocation APIs.
- **Write-side shard reopen MUST validate metadata**: `get_or_open_shard()` must return `NOT_FOUND` when the index or shard is absent from cluster state. It must NEVER create a shard with empty mappings/default settings on write or replication paths.
- **Shard reopen on gRPC paths MUST be async-safe**: `get_or_open_shard()` / `get_or_open_search_shard()` are async helpers and must use `open_shard_with_settings_blocking()` so shard recovery/open does not block tonic's async tasks. Likewise, leader-side delete/publish-state cleanup must use `close_index_shards_blocking()`.
- **Read-side shard reopen MUST fail closed on UUID mismatch**: `get_or_open_search_shard()` must reject empty UUIDs and missing expected UUID directories instead of creating a fresh shard on a read path.
- **Transport serialization must fail loudly**: gRPC handlers and clients must not use `unwrap_or_default()` for protocol payloads (`source_json`, `payload_json`, `partial_aggs_json`, Raft snapshot fields). Serialization or decode failures must surface as RPC errors, not empty payloads or silently dropped hits.
- **Raft snapshot RPCs must require all fields**: missing `vote`, `meta`, or `data` in `RaftSnapshot` is `INVALID_ARGUMENT`, not a defaulted empty snapshot.
- **ClusterState transport snapshots must be lossless**: startup consumes `JoinCluster` snapshots for shard reopen and orphan cleanup, so proto/domain conversion must preserve `raft_node_id`, `unassigned_replicas`, index `mappings`, index `settings`, and `uuid` exactly. Never synthesize defaults or new UUIDs during roundtrip, and reject unknown field types instead of coercing them.

## TransportClient (src/transport/client.rs)
```rust
pub struct TransportClient {
    connections: RwLock<HashMap<String, InternalTransportClient<Channel>>>,
}
```
- **Connection pooling**: reuses gRPC channels per node address
- `connect(host, port)` — lazy connection establishment (public, used by server for join forwarding)

### Forwarding Methods
| Method | Purpose |
|--------|--------|
| `forward_create_index()` | Forward index creation to leader |
| `forward_delete_index()` | Forward index deletion to leader |
| `forward_update_settings()` | Forward settings update to leader |
| `forward_transfer_master()` | Forward leadership transfer |
| `forward_index_to_shard()` | Route doc write to shard primary — returns `Err` on shard failure |
| `forward_delete_to_shard()` | Route doc delete to shard primary — returns `Err` on shard failure |
| `forward_get_to_shard()` | Route doc get to shard primary |
| `forward_bulk_to_shard()` | Route bulk write to shard primary — returns `Err` on shard failure |
| `forward_search_to_shard()` | Scatter search to remote shard |
| `forward_search_dsl_to_shard()` | Scatter DSL search to remote shard |
| `forward_sql_batch_to_shard()` | Scatter SQL RecordBatch to remote shard (Arrow IPC) |
| `open_sql_batch_stream_to_shard()` | Open a live remote SQL batch stream, eagerly decode only the first batch + metadata, then keep the remaining gRPC stream live for `StreamingTable` partitions |
| `forward_sql_batch_stream_to_shard()` | Stream multiple SQL RecordBatches from a remote shard (Arrow IPC) and report whether the shard actually used streaming |
| `get_shard_stats()` | Collect shard doc counts from remote node |
| `forward_refresh()` | Fan out refresh to remote node |
| `forward_flush()` | Fan out flush to remote node |
| `replicate_to_shard()` | Primary → replica single write |
| `replicate_bulk_to_shard()` | Primary → replica batch write |
| `recover_replica()` | Request missed ops from primary's WAL |

### Critical Invariant: Shard Forwarding Must Propagate Errors
`forward_index_to_shard()` and `forward_bulk_to_shard()` MUST return `Err(...)` when the shard RPC returns `success: false`. Never wrap a shard failure in `Ok(json!({"error": ...}))` — this hides failures from API handlers, causing them to return HTTP 201 for failed writes.

### Critical Invariant: Remote Search Decode Must Not Drop Data
- `forward_search_to_shard()` and `forward_search_dsl_to_shard()` must fail if a remote hit payload cannot be decoded.
- Do not use `filter_map(...ok())` on transport hits — returning partial results as success hides wire-format and compatibility bugs.
- Partial aggregation decode failures must also return `Err(...)`, not an empty aggregation map.

### Critical Invariant: Stream Metadata Must Stay Stable Across Batches
- `SqlBatchStream` must treat `total_hits`, `collected_rows`, and `streaming_used` as shard-level invariants captured from the first response.
- If any later `SqlRecordBatchStream` response disagrees with those first-batch values, decoding must fail instead of letting the coordinator emit a misleading NDJSON `meta` frame.
- The server-side `SqlRecordBatchStream` path should consume `SqlStreamingBatchHandle` lazily on the search pool so remote shards do not prebuild all Arrow batches in memory before tonic starts draining them.

### gRPC Limits
- **Max message size**: 64MB (`max_decoding_message_size` / `max_encoding_message_size`) on both client and server. Default tonic limit is 4MB, insufficient for high-cardinality GROUP BY partial results (~7MB for 200K groups).
- **Request timeout**: 30s (set on the tonic `Endpoint`). Full-table GROUP BY on large shards can take 10s+.
- **Connect timeout**: 5s (separate from request timeout).
- Both limits are set in `TransportClient::connect()` (client-side) and `create_transport_service*()` (server-side).

### Worker Pool Integration
All blocking engine calls in `TransportService` handlers are dispatched to dedicated rayon thread pools via `self.worker_pools.spawn_search()` / `self.worker_pools.spawn_write()`:
- **Search pool** (`search-N` threads): `get_doc`, `search_shard`, `search_shard_dsl`, `sql_record_batch`, `sql_record_batch_stream`, `recover_replica` (WAL I/O)
- **Write pool** (`write-N` threads): `index_doc`, `bulk_index`, `delete_doc`, `replicate_doc`, `replicate_bulk`, `refresh_index`, `flush_index`
- The `TransportService` struct holds `worker_pools: WorkerPools` initialized in `create_transport_service*()` constructors.
- Shard open/close/reopen are separate from engine work: use Tokio blocking-pool wrappers for those filesystem/Tantivy recovery steps before dispatching the steady-state engine call onto rayon.

### Transport TLS (optional, feature-gated)
Inter-node gRPC can be encrypted via the `transport-tls` Cargo feature flag. Disabled by default.
```bash
cargo build --features transport-tls
```

**Config** (`config/ferrissearch.yml` or `FERRISSEARCH_*` env vars):
```yaml
transport_tls_enabled: true
transport_tls_cert_file: /path/to/node.pem
transport_tls_key_file: /path/to/node-key.pem
transport_tls_ca_file: /path/to/ca.pem
```

**Architecture:**
- `TlsConnector` trait in `client.rs` — abstracts TLS endpoint configuration, no `#[cfg]` on the struct or `connect()` method.
- `TonicTlsConnector` in `transport/mod.rs` — concrete implementation behind `#[cfg(feature = "transport-tls")]`, applies `ClientTlsConfig` with CA verification.
- `transport/mod.rs` installs the rustls ring crypto provider once before building server/client TLS config so feature builds do not panic at runtime.
- `load_server_tls_config()` in `transport/mod.rs` — loads PEM cert+key into `tonic::transport::ServerTlsConfig`.
- `TransportClient::with_tls_connector(Arc<dyn TlsConnector>)` — factory that sets up https:// scheme and TLS for all connections.
- Server-side TLS (`node/mod.rs`): `Server::builder().tls_config(config)` when feature + config are both active.
- `node/mod.rs` validates TLS config up front: `transport_tls_ca_file`, `transport_tls_cert_file`, and `transport_tls_key_file` are required when TLS is enabled.
- Enabling `transport_tls_enabled: true` without compiling `--features transport-tls` must return a startup error. Never silently downgrade to plaintext transport.
- When `transport_tls_enabled: false` (default), all behavior is identical to pre-TLS code — zero overhead.
- Integration coverage for the encrypted path lives in `tests/replication_integration.rs` and should be run with `cargo test --test replication_integration --features transport-tls`.
