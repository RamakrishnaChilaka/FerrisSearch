---
description: "Use for primary-to-replica fan-out, sequence ownership, checkpoints, ISR tracking, and replica recovery."
applyTo: "src/replication/**"
---

# Replication Module — src/replication/mod.rs

## Replication Functions
`replicate_write()` and `replicate_bulk()` return `ReplicationResult`.
Failures retain both typed per-target causes and positive proofs from other
targets. Never discard those proofs or convert missing proofs into success.
Inputs preserve operation-owned sequences/versions/client records separately
from the current canonical sending authority and captured configuration.

## Replication Flow (Primary → Replicas)
1. Client writes to primary shard
2. Primary writes to WAL → assigns monotonic `seq_no` or contiguous bulk range
3. Primary indexes in Tantivy + USearch, updates local checkpoint
4. The engine returns an operation-owned receipt; the primary calls
   `replicate_write()` / `replicate_bulk()` with its exact sequence, primary
   term, and per-document version values
5. gRPC sends only to replicas in the Raft-authoritative
   `ShardRoutingEntry.in_sync_replicas` set, concurrently via `tokio::spawn` +
   `join_all` (fan-out)
6. Each request carries exact current ACTIVE source authority/certificate,
   index UUID, and target allocation. The operation's original primary term
   may be older after an idempotent resend. Validate current sending authority,
   durable certificate/fence, target allocation, and recovery gate before apply
   and again before returning the exact-operation proof.
7. A higher accepted term is atomically persisted in the local copy identity
   before the WAL/engine operation. Bulk validates the common envelope first
   and advances the fence once.
8. Each replica applies through the sequence-aware planner and returns optional
   processed/persisted checkpoints plus proof that the exact operation was processed
9. Primary stores monotonic observations per index UUID, allocation ID, and
   primary term and creates a fixed-target gap observation when the contiguous
   processed prefix lags
10. Primary samples its persisted checkpoint after replication, before taking
    the tracker lock. It then updates observations and computes the minimum
    with every current authoritative in-sync replica under one tracker lock
11. The primary certifies every captured copy's exact-operation proof or
    observed, settled conditional exclusion, the configured copy floor, and
    its still-valid permit. Prefixes govern retention, not above-gap ACKs.

## Post-Write Visibility

Explicit `refresh=true` runs a separate primary-owned phase after mutation
replication acknowledges and before the response. Single writes refresh the
primary and the same captured Raft `in_sync_replicas` used for acknowledgement.
REST bulk runs every mutation unit with refresh off, then requests one fenced
`RefreshShardWrites` round per mutated shard. That phase activates the primary
before taking the shared recovery guard, then captures the current validated
in-sync set. Never use the coordinator's view or the diagnostic ISR tracker.
Keep the shared guard through the bounded copy waits. Concurrent
`RefreshShardCopy` maintenance RPCs preserve UUID/allocation/primary/term
identity and use already-open engines on the blocking pool. Local and remote
waits are capped at five seconds and below the caller's remaining deadline;
timeout releases the guard even if blocking publication finishes later.

Refresh-only errors, including refresh RPC failures, remain attributable
`_shards` failures without changing write status, receipts, or bulk item
`errors`. Never convert them to `ReplicaReplicationFailure` or remove a
copy solely for a refresh error. This deliberately differs from OpenSearch's
whole-request failure on primary refresh error and replica-failure handling
on replica refresh error: data acknowledgement is already established.
Data-phase transport ambiguity is unchanged. No refresh/false, detected
single-update no-ops, and empty/all-error/no-op-only bulk add no refresh work.
A no-op alongside a mutation shares that bulk shard's report. See
[ADR 0001 D8](../../docs/adr/0001-write-consistency-and-retry-contract.md#d8-visibility).

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
  cursor. If finalization requests another catch-up and that fetch stalls empty
  at the same cursor, the target re-enters finalization instead of destroying
  the installed snapshot.
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
| `RefreshShardCopy` | Post-acknowledgement publication on an exact authoritative copy |
| `StartPeerRecovery` / `FetchRecoveryFileChunk` | Create and transfer the pinned file snapshot |
| `FetchRecoveryOps` | Fetch bounded ordered WAL suffix batches |
| `PrepareFinalizeRecovery` / `CompleteFinalizeRecovery` | Establish the final barrier and conditionally admit the target |

## Key Design Decisions
- Conditions and create intent are primary-only. Replicas receive full index
  replacements or deletes with the assigned sequence/term/version, including
  updates coordinated through realtime GET plus CAS. Replica and recovery
  decoders reject missing/zero index/delete versions and reject a version on
  NoOp; they never derive a document version from sequence state. Every applied index records its
  local physical WAL cursor for realtime reads; never copy a primary cursor
  into replica storage.
  Complete-map misses and tombstones take only the separate version-map read
  lock, not the apply-state or translog mutex. Keep map write sections brief;
  acquire apply state before the map whenever both are needed. Index
  hits still serialize cursor reads with WAL truncation. Replay marks the map
  incomplete until it publishes the full suffix; post-WAL apply and replay
  failures make realtime reads fail rather than report stale state. Conditional writes still finish
  replay before evaluating presence or versions.
- **Synchronous replication**: primary waits for every authoritative in-sync replica before ACK
- **Concurrent fan-out**: replicas are contacted in parallel via `tokio::spawn` + `join_all` — write latency = max(replica RTTs), not sum
- Bulk replication resolves the captured routing term and authoritative targets
  before cloning document IDs or serializing payloads. A zero-target bulk
  returns immediately. Non-empty fan-out serializes each payload once and
  shares the immutable operation slice across replica tasks; each gRPC request
  performs only its required owned protobuf copy.
- Single-operation fan-out also serializes the borrowed source once, after a
  valid target is found, and shares the encoded result across replica tasks.
  Do not deep-clone the JSON source or serialize it separately for each target;
  retain per-target serialization-error reporting and protocol-trace events.
- Assigned replicas are in `ShardRoutingEntry.replicas`; required
  acknowledgement targets are in `ShardRoutingEntry.in_sync_replicas`
- Primary write handlers hold the shard's shared write-barrier guard from
  before engine mutation through synchronous replication and any requested
  post-write copy refresh. Finalization holds the exclusive guard.
- Every caller of `record_replica_checkpoints` must hold the shared recovery
  write guard through the fresh-state snapshot and calculation. Snapshot
  freshness depends on excluding in-sync admission during that interval.
- Promotion-NoOp retries acquire the per-copy `noop_replication` mutex before
  the shared recovery guard and retain both through fan-out and recording.
  Retry callers must not already hold a recovery guard. Client handlers
  activate before acquiring their guard; local activation releases its
  exclusive guard before retrying. Reversing this order can deadlock behind
  Tokio's queued exclusive recovery writer.
- After acquiring the shared guard, handlers revalidate local primary and the
  activated index UUID and term, then mutate and replicate using that exact
  cluster-state snapshot. Never re-read a newer acknowledgement set after
  mutation.
- Global-checkpoint computation uses a fresh applied Raft snapshot, not the
  write's captured acknowledgement set. Revalidate the activated primary's
  UUID, allocation, and term before accepting reports. Count only current
  `in_sync_replicas` with matching allocation IDs. A missing current-identity
  report holds progress back; a removed copy no longer holds it back.
- Sample the primary's persisted prefix after replication and before taking
  the node-wide tracker lock. Engine sequence-state reads can wait behind
  bulk fsync and apply; never perform them under that lock. Update replica
  observations and compute from their monotonic maxima in one critical section.
  Request durability persists each operation on the primary before fan-out,
  so the last gap-closing round can reach the persisted minimum without sampling
  inside the lock. The highest-sequence round covers the no-replica case.
  Do not use the pre-replication primary snapshot or only the current round's
  replica values. Reset an observation when its UUID, allocation, or primary
  term changes, except that older-term reports for the same UUID must not
  replace newer checkpoint or gap state. A different UUID can restart at a
  lower term. Never move the global checkpoint backward.
- Dynamic-mapping reopen and async index close abort safe pre-finalize source
  sessions and await pin/snapshot/engine-Arc cleanup before replacing or
  deleting the primary engine. Encountering an admitting/settling source
  session on the shared-write path is a logic error and must fail the operation.
- Start/reopen/delete share a per-shard lifecycle lock, so no new source session
  can capture the old engine between cleanup and engine replacement.
- Whole-index retirement additionally excludes opens/reopen/install through an
  index read/write lifecycle lock. Applied UUID removal/replacement prompts
  the source reaper to run existing settlement without waiting for its deadline;
  this is definitive impossibility, not permission to release unknown admission.
  Cleanup drops the old session engine/WAL pin and never retargets a recreated
  name's new UUID.
- Each replicated operation must fit the same 32 MiB encoded WAL-frame limit as
  a primary operation. Oversized explicit-sequence single or bulk writes fail
  validation before replica WAL mutation.
- Replica transport decodes each index payload exactly once and reuses the
  validated JSON value when constructing the sequenced operation. Bulk decodes
  and source-validates every index payload before shard open, then completes
  envelope and operation validation before fence advancement or engine
  mutation.
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
- WAL/post-WAL failures immediately fail-stop the copy and its permits.
  `primary_unavailable` blocks admission; only fresh canonical activation can
  clear it. A later successful write is not a same-term healing mechanism.
- Failed replication returns typed per-replica failures retaining node,
  allocation, message, and definitive status. A primary receiving a definitive
  failure synchronously closes exclusion admission, durably journals exact
  source/target authority, and proposes conditional Raft exclusion. An unresolved
  or stale result remains a request failure; only observed settlement can
  contribute to certification. The replica persists an
  allocation-bound collision marker before closing; lifecycle failure reports
  retry until the exact allocation is removed.
- Request durability requires every replica response to prove the exact
  operation persisted; async durability requires processed proof and advances
  persisted checkpoints only after fsync or commit.
- Async background fsync does not send a new checkpoint report or recompute
  the primary's global checkpoint. After idle async writes, the global
  checkpoint can lag the true persisted minimum until another write reports
  progress. Checkpoint tracking does not add background synchronization.
- Replica background auto-flush may use its own contiguous persisted prefix,
  but retention is additionally capped by the matching committed canonical
  certificate. Preserve all post-certificate history; promotion clears the
  replica-only bound. Prepared, uncommitted certificates never advance GC.
- `ShardManager.isr_tracker` stores checkpoint proofs, not membership. The
  primary uses identity- and term-scoped persisted proofs for WAL retention.
  It can rank authoritative candidates only when the reporting leader hosts the primary;
  otherwise candidate selection falls back to a live in-sync cluster member.
  Checkpoint observations cannot grant membership.
- Uncertified single/batch writes return typed failures; mixed bulk preserves
  per-item outcomes. Successful per-copy reports can contain explicit failed
  excluded replicas, never an unresolved missing proof. The RPC envelope does
  not override item errors.
- **Primary owns seq numbers**: replica WAL entries must preserve the seq_no assigned by the primary; never allocate replica-local seq_nos for replicated or recovered operations
- Never derive an operation's sequence from `last_seq_no()` or a checkpoint after
  releasing the primary write lock. Concurrent writes can advance both before
  replication begins.
- Processed and persisted checkpoints are gap-aware contiguous prefixes.
  Internal redelivery is term/sequence aware, promotion fills local gaps with
  NoOps, and sustained gaps are probed before exact-allocation removal.
  Bounded canonical activation adopts/resyncs verified history; general D10
  rollback and unrestricted divergent-history recovery remain outside it.
- Replicated and recovered operations preserve the primary's client-operation
  record exactly. Replica apply rejects reuse of one client ID with a different
  fingerprint and never allocates replacement metadata.
- File-based peer recovery snapshots include exact retained ID-bearing WAL
  entries at or below the committed checkpoint for the latest 4,096 sequence
  operations. Install validates sequence bounds, operation/result agreement,
  unique IDs, and collisions with retained delete tombstones before appending
  them to the empty target WAL. Do not replace the original mutation with a
  synthetic NoOp; later same-sequence delivery must compare against the exact
  operation.
- Production canonical activation freezes authoritative histories under a fresh
  PROMOTING reservation, seals matching copies, and conditionally finishes
  ACTIVE before admission. Peer conflicts/unavailability cannot be hidden by
  best-effort NoOp fan-out. Legacy no-Raft D1 trace helpers remain a separate
  historical relation, not an alternate production activation path.
- A primary failure after WAL append can retain an unknown operation. Its copy
  stops immediately; a verified restart/promotion canonicalizes protected
  history before new writes. Failed responses promise no rollback.

## Recovery Protocol Work

For recovery-protocol changes, read
[`docs/recovery-protocol.md`](../../docs/recovery-protocol.md) and its
[`acceptance matrix`](../../docs/recovery-acceptance-matrix.md). D1 foundations
in those documents are implemented; later RP/D10 sections remain proposed.
Keep changes scoped to one dependency-aware package and do not describe the
implemented subset as production parity.
