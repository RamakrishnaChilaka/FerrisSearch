---
description: "Use for primary-to-replica fan-out, sequence ownership, checkpoints, ISR tracking, and replica recovery."
applyTo: "src/replication/**"
---

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
2. Primary writes to WAL → assigns monotonic `seq_no` or contiguous bulk range
3. Primary indexes in Tantivy + USearch, updates local checkpoint
4. The engine returns an operation-owned receipt; the primary calls
   `replicate_write()` / `replicate_bulk()` with those exact values
5. gRPC sends only to replicas in the Raft-authoritative
   `ShardRoutingEntry.in_sync_replicas` set, concurrently via `tokio::spawn` +
   `join_all` (fan-out)
6. Each replica: applies the write using the primary-provided seq_no, persists that exact seq_no in its WAL, updates its local checkpoint, returns checkpoint
7. Primary updates ISR tracker with returned checkpoints
8. Primary computes global checkpoint (min of all replica checkpoints)
9. Write acknowledged to client **only after every in-sync replica confirms**

## Current Recovery Limit
- Newly allocated replicas are assigned but out of sync. They receive no live
  writes, do not participate in acknowledgements, and are not promotable.
- The follower lifecycle no longer invokes partial WAL-suffix recovery. A
  retained suffix cannot reconstruct files truncated by flush, and replay onto
  a live copy can race newer writes.
- `RecoverReplica` still exposes bounded test/transport behavior, but no
  production lifecycle path uses it or treats its checkpoint as admission.
- File snapshot transfer, WAL suffix catch-up, a final write barrier, and
  conditional in-sync admission are deferred recovery work. Until then,
  increasing replicas does not restore durable redundancy.

## gRPC RPCs Used
| RPC | Purpose |
|-----|---------|
| `ReplicateDoc` | Single document replication to replica |
| `ReplicateBulk` | Batch document replication to replica |
| `RecoverReplica` | Fetch missed operations from primary's WAL |

## Key Design Decisions
- **Synchronous replication**: primary waits for every authoritative in-sync replica before ACK
- **Concurrent fan-out**: replicas are contacted in parallel via `tokio::spawn` + `join_all` — write latency = max(replica RTTs), not sum
- Assigned replicas are in `ShardRoutingEntry.replicas`; required
  acknowledgement targets are in `ShardRoutingEntry.in_sync_replicas`
- Failed replication returns `Err(Vec<String>)` with per-replica error messages
- `ShardManager.isr_tracker` stores checkpoint observations only. It can rank
  authoritative candidates but cannot grant membership.
- Primary shard handlers (`index_doc`, `bulk_index`, `delete_doc`) MUST return `success: false` when replication fails — never swallow replication errors
- **Primary owns seq numbers**: replica WAL entries must preserve the seq_no assigned by the primary; never allocate replica-local seq_nos for replicated or recovered operations
- Never derive an operation's sequence from `last_seq_no()` or a checkpoint after
  releasing the primary write lock. Concurrent writes can advance both before
  replication begins.
- Current local/global checkpoints are monotonic high-water marks. They are not
  a gap-free applied-prefix protocol and do not add idempotent retry handling,
  primary-epoch fencing, or a new failover ordering model.

## Proposed Recovery Work

For recovery-protocol changes, read
[`docs/recovery-protocol.md`](../../docs/recovery-protocol.md) and its
[`acceptance matrix`](../../docs/recovery-acceptance-matrix.md). They are proposed
contracts, not implemented behavior. Keep changes scoped to one dependency-aware
package and do not describe partial fencing/checkpoint/recovery work as parity.
