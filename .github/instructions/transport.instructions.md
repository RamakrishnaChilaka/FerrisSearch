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
- **join_cluster**: If leader → registers node via Raft (`AddNode` + `add_learner` + `change_membership`). If follower → **forwards to leader** via gRPC. NEVER mutate cluster state locally on a follower.
- **index_doc / bulk_index / delete_doc**: Look up shard in ShardManager, execute engine operation, replicate to all replicas. **Returns `success: false` if replication fails** — write is only acknowledged after all ISR replicas confirm (synchronous replication contract).
- **replicate_doc / replicate_bulk**: Apply to local replica engine using the seq_no supplied by the primary, persist that same seq_no in the replica WAL, return checkpoint
- **recover_replica**: Read WAL entries via `read_from()`, return operations
- **search_shard / search_shard_dsl**: Execute local shard search, return results
- **raft_vote / raft_append_entries / raft_snapshot**: Deserialize JSON, forward to Raft instance
- **create_index / delete_index**: Must be leader; execute via `raft.client_write()`
- **update_settings**: Must be leader; apply via `UpdateIndex` Raft command

### Critical Invariants
- **join_cluster MUST forward on followers**: A follower receiving a JoinCluster RPC must forward it to the Raft leader. It must NEVER fall through to `cluster_manager.add_node()` when Raft is active, as this would add the node to local state without Raft membership.
- **Shard writes MUST fail on replication failure**: The `index_doc`, `bulk_index`, and `delete_doc` handlers must return `success: false` when `replicate_write()` / `replicate_bulk()` returns `Err`. Logging the error and returning `success: true` violates the synchronous replication contract.
- **Replica apply MUST preserve primary seq_nos**: `replicate_doc`, `replicate_bulk`, and recovery replay must call the explicit-seq engine methods. Do not route replicated writes through local seq allocation APIs.

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
| `get_shard_stats()` | Collect shard doc counts from remote node |
| `forward_refresh()` | Fan out refresh to remote node |
| `forward_flush()` | Fan out flush to remote node |
| `replicate_to_shard()` | Primary → replica single write |
| `replicate_bulk_to_shard()` | Primary → replica batch write |
| `recover_replica()` | Request missed ops from primary's WAL |

### Critical Invariant: Shard Forwarding Must Propagate Errors
`forward_index_to_shard()` and `forward_bulk_to_shard()` MUST return `Err(...)` when the shard RPC returns `success: false`. Never wrap a shard failure in `Ok(json!({"error": ...}))` — this hides failures from API handlers, causing them to return HTTP 201 for failed writes.

### gRPC Limits
- **Max message size**: 64MB (`max_decoding_message_size` / `max_encoding_message_size`) on both client and server. Default tonic limit is 4MB, insufficient for high-cardinality GROUP BY partial results (~7MB for 200K groups).
- **Request timeout**: 30s (set on the tonic `Endpoint`). Full-table GROUP BY on large shards can take 10s+.
- **Connect timeout**: 5s (separate from request timeout).
- Both limits are set in `TransportClient::connect()` (client-side) and `create_transport_service*()` (server-side).

### Worker Pool Integration
All blocking engine calls in `TransportService` handlers are dispatched to dedicated rayon thread pools via `self.worker_pools.spawn_search()` / `self.worker_pools.spawn_write()`:
- **Search pool** (`search-N` threads): `get_doc`, `search_shard`, `search_shard_dsl`, `sql_record_batch`, `recover_replica` (WAL I/O)
- **Write pool** (`write-N` threads): `index_doc`, `bulk_index`, `delete_doc`, `replicate_doc`, `replicate_bulk`, `refresh_index`, `flush_index`
- The `TransportService` struct holds `worker_pools: WorkerPools` initialized in `create_transport_service*()` constructors.
