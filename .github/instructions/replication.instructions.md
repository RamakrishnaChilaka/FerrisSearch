# Replication Module — src/replication/mod.rs

## Replication Functions
```rust
pub async fn replicate_write(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    doc_id: &str,
    payload: &Value,
    op: &str,          // "index" or "delete"
    seq_no: u64,       // from primary's WAL
) -> Result<Vec<(String, u64)>, Vec<String>>
// Ok: [(replica_node_id, replica_checkpoint), ...]
// Err: list of error messages

pub async fn replicate_bulk(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    docs: &[(String, Value)],
    start_seq_no: u64,
) -> Result<Vec<(String, u64)>, Vec<String>>
```

## Replication Flow (Primary → Replicas)
1. Client writes to primary shard
2. Primary writes to WAL → assigns monotonic `seq_no`
3. Primary indexes in Tantivy + USearch, updates local checkpoint
4. Primary calls `replicate_write()` / `replicate_bulk()`
5. gRPC sends to ALL replicas concurrently via `tokio::spawn` + `join_all` (fan-out)
6. Each replica: applies the write using the primary-provided seq_no, persists that exact seq_no in its WAL, updates its local checkpoint, returns checkpoint
7. Primary updates ISR tracker with returned checkpoints
8. Primary computes global checkpoint (min of all replica checkpoints)
9. Write acknowledged to client **only after all replicas confirm**

## Replica Recovery Flow
1. Recovering replica sends `RecoverReplica` gRPC with its `local_checkpoint` (e.g., 100)
2. Primary calls `WAL.read_from(100)` → returns all entries with seq_no > 100
3. Primary sends entries in `RecoverReplicaResponse.operations`
4. Replica replays operations sequentially using the seq_no carried in each recovered op, updating its local checkpoint in the same shared seq space
5. After replay, replica is caught up and joins ISR

## gRPC RPCs Used
| RPC | Purpose |
|-----|---------|
| `ReplicateDoc` | Single document replication to replica |
| `ReplicateBulk` | Batch document replication to replica |
| `RecoverReplica` | Fetch missed operations from primary's WAL |

## Key Design Decisions
- **Synchronous replication**: primary waits for ALL ISR replicas before ACK
- **Concurrent fan-out**: replicas are contacted in parallel via `tokio::spawn` + `join_all` — write latency = max(replica RTTs), not sum
- Replicas are identified by node_id in `ShardRoutingEntry.replicas`
- Failed replication returns `Err(Vec<String>)` with per-replica error messages
- ISR tracking is on the primary via `ShardManager.isr_tracker`
- Primary shard handlers (`index_doc`, `bulk_index`, `delete_doc`) MUST return `success: false` when replication fails — never swallow replication errors
- **Primary owns seq numbers**: replica WAL entries must preserve the seq_no assigned by the primary; never allocate replica-local seq_nos for replicated or recovered operations
