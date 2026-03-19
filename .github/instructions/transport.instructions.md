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

// Replication (primary → replica)
ReplicateDoc(ReplicateDocRequest) → ReplicateDocResponse
ReplicateBulk(ReplicateBulkRequest) → ReplicateBulkResponse
RecoverReplica(RecoverReplicaRequest) → RecoverReplicaResponse

// Forwarded to leader
UpdateSettings(UpdateSettingsRequest) → UpdateSettingsResponse
CreateIndex(CreateIndexRequest) → CreateIndexResponse
DeleteIndex(DeleteIndexRequest) → DeleteIndexResponse
TransferMaster(TransferMasterRequest) → TransferMasterResponse

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
}
```
Implements `InternalTransport` trait. All RPC handlers check Raft leadership or route to the correct shard.

### Key Handler Patterns
- **join_cluster**: Leader registers node via Raft (`AddNode` + `add_learner` + `change_membership`)
- **index_doc / delete_doc / get_doc**: Look up shard in ShardManager, execute engine operation
- **replicate_doc / replicate_bulk**: Apply to local replica engine, return checkpoint
- **recover_replica**: Read WAL entries via `read_from()`, return operations
- **search_shard / search_shard_dsl**: Execute local shard search, return results
- **raft_vote / raft_append_entries / raft_snapshot**: Deserialize JSON, forward to Raft instance
- **create_index / delete_index**: Must be leader; execute via `raft.client_write()`
- **update_settings**: Must be leader; apply via `UpdateIndex` Raft command

## TransportClient (src/transport/client.rs)
```rust
pub struct TransportClient {
    connections: RwLock<HashMap<String, InternalTransportClient<Channel>>>,
}
```
- **Connection pooling**: reuses gRPC channels per node address
- `get_or_connect(addr)` — lazy connection establishment

### Forwarding Methods
| Method | Purpose |
|--------|---------|
| `forward_create_index()` | Forward index creation to leader |
| `forward_delete_index()` | Forward index deletion to leader |
| `forward_update_settings()` | Forward settings update to leader |
| `forward_transfer_master()` | Forward leadership transfer |
| `forward_index_to_shard()` | Route doc write to shard primary |
| `forward_delete_to_shard()` | Route doc delete to shard primary |
| `forward_get_to_shard()` | Route doc get to shard primary |
| `forward_bulk_to_shard()` | Route bulk write to shard primary |
| `forward_search_to_shard()` | Scatter search to remote shard |
| `forward_search_dsl_to_shard()` | Scatter DSL search to remote shard |
| `replicate_to_shard()` | Primary → replica single write |
| `replicate_bulk_to_shard()` | Primary → replica batch write |
| `recover_replica()` | Request missed ops from primary's WAL |
