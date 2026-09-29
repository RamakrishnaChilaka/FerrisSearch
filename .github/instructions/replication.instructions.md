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
    primary_term: u64,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>>
// Err: list of error messages

pub async fn replicate_bulk(
    transport_client: &TransportClient,
    cluster_state: &ClusterState,
    index_name: &str,
    shard_id: u32,
    docs: &[(String, Value)],
    start_seq_no: u64,
    primary_term: u64,
) -> Result<Vec<ReplicaCheckpointUpdate>, Vec<ReplicaReplicationFailure>>
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
6. Each request carries index UUID, sender primary term, and the target's exact
   allocation ID. The replica validates UUID, allocation, recovery gate, and
   `term >= max(applied_view_term, durable_fence)` before mutation.
7. A higher accepted term is atomically persisted in the local copy identity
   before the WAL/engine operation. Bulk validates the common envelope first
   and advances the fence once.
8. Each replica applies through the sequence-aware planner and returns optional
   processed/persisted checkpoints plus proof that the exact operation was processed
9. Primary stores monotonic processed observations per exact allocation and
   creates a fixed-target gap observation when the contiguous prefix lags
10. Primary advances the global checkpoint only from the minimum persisted
   checkpoint across every authoritative copy
11. Write acknowledged to client **only after every in-sync replica confirms**

## File-Based Peer Recovery
- Every node drives recovery for assigned local replicas absent from
  `in_sync_replicas`; metadata-leader role does not disable the driver.
- `StartPeerRecovery` carries the target-observed allocation ID. The source
  rejects snapshot setup until its own current assignment has the exact same ID.
- The primary commits under the translog lock, captures the exact committed
  boundary plus physical WAL end, requires a gap-free processed prefix, pins
  from `processed_checkpoint + 1`, and hard-links the committed Tantivy files.
- The target wipes only its out-of-sync copy, persists
  `PEER_RECOVERY_IN_PROGRESS`, validates bounded chunks and SHA-256 hashes,
  initializes an empty WAL allocator at source `max_seq_no + 1`, installs the
  exact committed boundary, and opens with schema-wipe fallback disabled.
- After sending completion, the target persists
  `PEER_RECOVERY_AWAITING_MEMBERSHIP`, keeps the caught-up engine open, and
  accepts live replica apply while local membership is unresolved. Reconcile
  clears this state on admission/promotion and only writes the destructive
  in-progress marker after definitive rejection.
- Catch-up paginates by `(generation_id, byte_offset)` in physical file order.
  It stops before the first source-unprocessed WAL frame rather than advancing
  past it; finalization rebuilds the source writer and resumes from that exact
  cursor.
  A final exclusive shard write barrier captures a physical end and processed
  checkpoint; the target must match both, then the
  primary submits `MarkReplicaInSync(allocation_id, primary, term)` and observes local
  membership before releasing writes.
- Admission uncertainty remains write-blocking until membership is observed or
  `ActivatePrimary` commits a term bump that makes the stale admission
  impossible.

## gRPC RPCs Used
| RPC | Purpose |
|-----|---------|
| `ReplicateDoc` | Single document replication to replica |
| `ReplicateBulk` | Batch document replication to replica |
| `StartPeerRecovery` / `FetchRecoveryFileChunk` | Create and transfer the pinned file snapshot |
| `FetchRecoveryOps` | Fetch bounded ordered WAL suffix batches |
| `PrepareFinalizeRecovery` / `CompleteFinalizeRecovery` | Establish the final barrier and conditionally admit the target |

## Key Design Decisions
- **Synchronous replication**: primary waits for every authoritative in-sync replica before ACK
- **Concurrent fan-out**: replicas are contacted in parallel via `tokio::spawn` + `join_all` — write latency = max(replica RTTs), not sum
- Assigned replicas are in `ShardRoutingEntry.replicas`; required
  acknowledgement targets are in `ShardRoutingEntry.in_sync_replicas`
- Primary write handlers hold the shard's shared write-barrier guard from
  before engine mutation through synchronous replication. Finalization holds
  the exclusive guard.
- After acquiring the shared guard, handlers revalidate local primary and the
  activated index UUID and term, then mutate and replicate using that exact
  cluster-state snapshot. Never re-read a newer acknowledgement set after
  mutation.
- Dynamic-mapping reopen and async index close abort safe pre-finalize source
  sessions and await pin/snapshot/engine-Arc cleanup before replacing or
  deleting the primary engine. Encountering an admitting/settling source
  session on the shared-write path is a logic error and must fail the operation.
- Start/reopen/delete share a per-shard lifecycle lock, so no new source session
  can capture the old engine between cleanup and engine replacement.
- Each replicated operation must fit the same 32 MiB encoded WAL-frame limit as
  a primary operation. Oversized explicit-sequence single or bulk writes fail
  validation before replica WAL mutation.
- Pending-target reconciliation admits only the same allocation when in sync or
  after promotion. Admission is checked first; otherwise missing/different
  allocation identity, a different primary, or a strictly newer observed term
  is definitive rejection. An older view or the same primary/term remains
  unknown.
- Restart restores an exact matching durable pending marker and opens that
  finalized copy before scheduling recovery. Target begin/prepare and source
  status polling refuse to reattach once finalization/admission/settlement has
  begun.
- Controlled retryable recovery failures remove the partial target install and
  retry the same allocation. Definitive pending rejection intentionally writes
  the failed-install marker, causing allocation-bound replica failure and a
  fresh recovery allocation; a crash-left inactive matching marker follows the
  same path.
- Corrupt storage is definitive immediately. Other local I/O uses shared
  per-copy count/time retry state and exponential backoff. Persistent replica
  I/O eventually fails the allocation; persistent primary I/O can only request
  promote-only failover when an in-sync replacement exists.
- Apply-level escalation leaves the open engine readable and does not trigger
  runtime WAL replay. A single-copy primary is marked unavailable without
  changing authority; the first later successful local write conditionally
  clears that status at the same term. Definitive and open-level failures may
  quarantine and require fresh activation after repair.
- Failed replication returns typed per-replica failures retaining node,
  allocation, message, and definitive status. A primary receiving a definitive
  `DATA_LOSS` failure conditionally removes that exact allocation at the
  captured term before returning the write failure.
- Request durability requires every replica response to prove the exact
  operation persisted; async durability requires processed proof and advances
  persisted checkpoints only after fsync or commit.
- Replica background auto-flush uses its own contiguous persisted prefix when
  no primary global checkpoint exists. It may prune through that committed
  prefix but never through a gap; promotion clears the replica-only bound.
- `ShardManager.isr_tracker` stores checkpoint observations only. It can rank
  authoritative candidates only when the reporting leader hosts the primary;
  otherwise candidate selection falls back to a live in-sync cluster member.
  Checkpoint observations cannot grant membership.
- Primary shard handlers (`index_doc`, `bulk_index`, `delete_doc`) MUST return `success: false` when replication fails — never swallow replication errors
- **Primary owns seq numbers**: replica WAL entries must preserve the seq_no assigned by the primary; never allocate replica-local seq_nos for replicated or recovered operations
- Never derive an operation's sequence from `last_seq_no()` or a checkpoint after
  releasing the primary write lock. Concurrent writes can advance both before
  replication begins.
- Processed and persisted checkpoints are gap-aware contiguous prefixes.
  Internal redelivery is term/sequence aware, promotion fills local gaps with
  NoOps, and sustained gaps are probed before exact-allocation removal. This is
  still not general D10 rollback/resync or client retry-token support.
- Promotion NoOps are replicated in bounded homogeneous bulk batches, preserving
  each explicit non-contiguous sequence number. A batch transport failure
  remains best-effort and creates the same replica gap observation as the
  former single-operation path.
- A primary engine failure after WAL append but before replication leaves an
  operation that no replica received. After local rebuild/replay advances the
  primary prefix, later replica responses expose the permanent gap; each
  affected replica is normally removed after the approximately 60-second gap
  deadline and peer-recovered. Promotion NoOps do not repair this live-primary
  divergence; targeted gap repair is deferred to D10.

## Recovery Protocol Work

For recovery-protocol changes, read
[`docs/recovery-protocol.md`](../../docs/recovery-protocol.md) and its
[`acceptance matrix`](../../docs/recovery-acceptance-matrix.md). D1 foundations
in those documents are implemented; later RP/D10 sections remain proposed.
Keep changes scoped to one dependency-aware package and do not describe the
implemented subset as production parity.
