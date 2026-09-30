# Shard Replication And Recovery Protocol

> **Status: Partially implemented.**
>
> **Updated:** September 29, 2026.
>
> **Historical design baseline:** `e805f70ff5dba0be9077b9bc32fcd488e837e6d1`
> (PR #141).
>
> D1 sequencing, fencing, checkpoint, and bounded peer-recovery foundations are
> implemented. Later RP and D10 sections remain target design. This document
> does not establish production readiness or OpenSearch/Elasticsearch
> compatibility. The [acceptance matrix](recovery-acceptance-matrix.md) defines
> the evidence required before making broader claims.

## 1. Scope And Roadmap Alignment

The goal is comparable **recovery safety guarantees** to established search
engines: preserve acknowledged operations under the supported failures, reject
obsolete writers, recover a replacement copy without exposing partial data, and
bound the resources needed to recover. Matching endpoint names, election
timeouts, or a successful node-kill demonstration is insufficient.

Keep the existing architecture:

- Raft owns membership, routing, primary terms, and authoritative copy membership.
- Shard primaries own document sequencing; document payloads do not enter Raft.
- Tantivy, vector state, and the WAL form one recoverable shard state.
- `remote_store` uses manifest/split repair, not this document-replay protocol.
  Remote publisher fencing remains a separate roadmap workstream.

| Alignment | Work |
|---|---|
| Gate 0 | Write/failure contract (FS-001); schema/storage format decisions (FS-005, with the relevant FS-003/004 snapshot/lifecycle prerequisites); deterministic failure evidence (FS-007). |
| Gate 1 | Real operation metadata and OCC (FS-009/010), retry identity (FS-011), fencing (FS-012), acknowledgement policy (FS-013), snapshot/recovery work (FS-022/023/024), replica states (FS-025/026), complete vector recovery (FS-027). |
| Invariants | Primary-owned sequences, Raft-only metadata mutation, UUID paths, explicit durability, fail-loud decoding, bounded recovery, no stale-writer success. |
| Prerequisites | Approved contract; versioned storage/wire formats; term-aware operation receipts; atomic checkpoint/snapshot persistence; deterministic boundary controls. |
| Completion | Acceptance cases pass on the implemented protocol; a design review alone completes none of these implementation tasks. |

This is one protocol split into dependency-ordered work packages, not permission
to implement every package in one PR. See the
[roadmap](architecture-roadmap.md#9-roadmap-gates) and
[backlog](next-50-tasks.md).

## 2. Implemented Baseline And Remaining Gaps

The table reflects the implemented branch as of September 29, 2026. Later
sections still describe the broader target protocol; they are not all shipped.

| Surface | Current behavior | Required change |
|---|---|---|
| [Node lifecycle](../src/node/mod.rs) | Promotes only Raft-authoritative in-sync copies, activates restarted/promoted primaries proactively, replays local WAL, and fills local sequence gaps with durable NoOps. | General rollback-to-global-checkpoint and full D10 resync remain unimplemented. |
| [Replica allocation](../src/node/mod.rs) | Accounts for primary and replica loss independently and conditionally removes exact failed allocations. | Failed-allocation exclusion and configurable allocation retry limits remain deferred. |
| [Replication](../src/replication/mod.rs) | Synchronously sends exact operation identities only to Raft-authoritative in-sync replicas; any required replica failure fails the request after the primary may already have mutated. | Client retry identity and explicit ambiguous outcomes remain unimplemented. |
| [Transport](../proto/transport.proto) | Carries term/sequence identity, exact allocation IDs, optional processed/persisted checkpoints, recovery sessions, and physical WAL cursors. | Client-facing idempotency and optimistic concurrency remain separate work. |
| [Checkpoint tracking](../src/engine/sequence.rs) | Persists contiguous processed/persisted prefixes plus above-gap intervals and maximum sequence identity. | Cross-copy rollback/trimming above the global checkpoint remains out of scope. |
| [Automatic recovery](../src/node/mod.rs) | Every node reconciles assigned out-of-sync replicas through bounded file recovery and conditional admission. | Source sessions and pins are still process-local. |
| [WAL suffix](../src/wal/mod.rs) | Streams retained generations in physical order with generation/byte cursors; sequence filtering never drives pagination. | Recovery compression and resumable file chunks are not implemented. |
| [Recovery apply](../src/node/peer_recovery.rs) | Decoding or apply failure stops the session; operations use the common term/sequence-aware planner and admission requires the exact processed barrier. | General divergence rollback and repair above the global checkpoint remain D10 work. |
| [Vector rebuild](../src/engine/composite.rs) | A post-WAL text failure durably marks vectors stale. Every composite writer-rebuild path reconstructs and fsyncs a replacement vector index before clearing the marker. The rebuild still reads a capped document set. | Snapshot vectors or rebuild all vectors from the same logical snapshot; failures keep the copy unavailable. |

Existing [promotion tests](../tests/consensus_integration.rs) exercise metadata
changes. The [restart regression](../tests/restart_regression.rs) restarts the
cluster and checks data preservation. These do not establish a complete
partition, stale-primary, divergent-history, and interrupted-recovery contract.

> **Implementation note — September 29, 2026:** D1 now stores term-aware WAL v2
> entries and versioned committed boundaries, distinguishes empty checkpoints
> from sequence zero, tracks processed and persisted contiguous prefixes across
> gaps, returns real `_seq_no`/`_primary_term` receipts, and computes the global
> checkpoint from persisted authoritative copies. Peer recovery installs the
> source's exact gap-free committed boundary, streams the WAL by physical
> generation/byte cursor without advancing past a source-unapplied frame, and
> uses the final replaying barrier to resume from that cursor before admission.
> Promotion persists its fence, fills local gaps with NoOps, and replicates
> those NoOps in bounded batches. Failed batches remain best-effort for local
> activation but are retried by later lifecycle or request activation at the
> same UUID, allocation, and term. A term/sequence collision durably marks the
> exact allocation as quarantined before closing it; the copy cannot reopen or
> accept replication until Raft removes it and fresh peer recovery installs a
> new allocation identity. Text-only writer replay durably marks vector state
> stale; write, maintenance, recovery-snapshot, barrier, activation, startup,
> and recovery-finalization paths rebuild and fsync vectors before clearing the
> marker. Rebuilds scan every live Tantivy document segment-by-segment in
> bounded batches rather than through a capped search result. Intermediate
> replay commits may persist a committed maximum below the
> durable term-start fence maximum until later WAL batches are replayed; this is
> valid sequence state, not corruption. A stalled empty catch-up response at an
> unchanged physical cursor returns to the exclusive finalize path, including
> after finalization asks for another catch-up. Bulk replication resolves
> authoritative targets before payload serialization; zero-replica bulks avoid
> that work, and non-empty fan-out shares one serialized operation slice across
> replica tasks. Replica receivers decode and source-validate each index
> payload once, then reuse the parsed value while retaining whole-batch
> validation before fence or engine mutation. Dynamic mapping never persists
> the built-in `body` catch-all;
> a plain explicit text mapping reuses its existing Tantivy field. Reserved
> authoritative mapping names or incompatible `body` metadata fail shard open
> as unsupported index formats with recreate-index guidance rather than
> reaching schema construction.
> Earlier build formats are unsupported and require index/cluster recreation
> and reindexing. This does not implement D10 rollback/resync, client retry
> tokens, or full OCC.

> **Implementation note — September 24, 2026:** the first in-sync tracking
> package now stores replica eligibility in Raft routing metadata, targets live
> writes only to that set, refuses promotion when no in-sync copy survives, and
> removes the follower's unsafe partial WAL-suffix replay. Later-added replicas
> remain out of sync until file recovery and conditional admission are
> implemented. See the
> [PR in-sync tracking evidence record](recovery-acceptance-matrix.md#pr-in-sync-tracking-evidence-record-september-24-2026).

> **Implementation note — September 25, 2026:** a bounded file-based peer
> recovery subset now gives later-added and rejoining replicas a committed
> Tantivy file snapshot, a source-side WAL pin at exclusive boundary `B`,
> bounded suffix transfer, an exclusive final write barrier, and
> `(primary, primary_term)` conditional in-sync admission. Restarted/promoted
> primaries activate through a conditional term bump before their first write.
> Target installation uses `PEER_RECOVERY_IN_PROGRESS`, strict schema open, and
> SHA-256 validation; hard links are required on the source. Tantivy
> `SegmentMeta::list_files()` includes optional absent components, so the
> snapshot manifest contains the existing committed components plus
> `meta.json` and `.managed.json`.
>
> This is not full RP-3/RP-5: allocation IDs, durable term fencing, contiguous
> checkpoints, exact snapshot boundaries, and physical WAL cursors are now
> implemented, but there is no general operation-only path selection,
> resumable chunks, compression, or D10 rollback/resync. Source sessions
> and pins are process-local; only pre-finalize idle setups/sessions expire
> after ten minutes, while admitting/settling sessions are resolved by
> settlement rather than idle reaping. The
> [September 25 evidence record](recovery-acceptance-matrix.md#bounded-file-recovery-evidence-record-september-25-2026)
> names the exact executable subset.
>
> **Availability correction — September 25, 2026:** primary engine replacement
> now aborts safe pre-finalize source sessions before Tantivy reopen. Targets
> that sent completion but cannot yet order the membership result persist a
> finalized-awaiting-membership state, remain open, and accept live replication.
> They are closed and marked for a new recovery only after definitive rejection;
> admission or promotion clears the pending marker without replacing the copy.
>
> **Review corrections — September 26, 2026:** StartPeerRecovery now returns a
> pollable asynchronous preparation state, so snapshot/hash duration is not
> bounded by the transport timeout and cancelled RPC futures do not leak pins
> or placeholders. Primary writes revalidate authority inside the shared
> barrier and use the same routing snapshot for fan-out. Abandoned finalize
> sessions commit a newer primary term (with barrier ordering determined by
> whether admission was submitted), making pending targets definitively
> recoverable. Recovery WAL reads use live generation state rather than a
> lagging manifest.
>
> **Round-2 corrections — September 26, 2026:** recovery reads clone and
> validate the live generation list under the translog lock, then scan outside
> it while skipping pre-cursor frames by length. Setup failures are returned on
> the next poll, stale pre-finalize targets can be replaced, and the setup
> engine Arc is released before hashing. Reopen continues after caller
> cancellation, setup lifetime waits use Notify's enable-before-check pattern,
> and blocking cleanup remains on Tokio's blocking pool. Dynamic-mapping writes
> revalidate authority again after their Raft mapping round trip.
>
> **Round-3 corrections — September 26, 2026:** dynamic-mapping reopen is now
> replacement-only. It revalidates the registered index UUID and existing
> shard before source cleanup and again under the per-shard open lock; a
> delete/recreate race returns a retryable error instead of recreating the old
> UUID directory. Setup panic completion releases engine waiters, idle setup
> reaping does not wait through hashing, and bounded WAL scans validate frame
> length plus the complete frame at the captured head.
>
> **Round-4 corrections — September 26, 2026:** index deletion now acquires all
> lifecycle and open locks already registered for the deleted UUID/index, so a
> shard temporarily absent from the engine map cannot escape coordination.
> Reopen requires an existing Tantivy `meta.json` and cannot create a fresh
> index. Primary write permits bind both UUID and term. New WAL writes and
> transferred recovery operations share a 32 MiB total-frame limit enforced
> before mutation; a partial first frame at or beyond the captured head ends
> the scan cleanly, while a below-head overrun or torn-then-appended frame fails
> closed. Retryable forwarded `ABORTED` writes map to HTTP 503.
>
> **Round-5 corrections — September 26, 2026:** the 32 MiB total-frame ceiling
> applies uniformly to writes, restart scan, replay, and recovery. The
> concurrent-append exception applies only to the final captured generation at
> or beyond its captured file size. On restart, an incomplete
> active-generation tail is truncated to the last fully decoded frame and both
> file and directory are fsynced before append; complete or middle corruption
> still fails closed.
>
> **Round-7 corrections — September 27, 2026:** WAL replay now validates the
> internal `_doc_id` and operation payload, deletes that ID for every
> operation, and adds a document back only for an index operation. Startup,
> failed-writer reconstruction, and peer-recovery catch-up therefore preserve
> deletes and fail closed on malformed operation envelopes. Blocking
> maintenance and snapshot preparation acquire
> the writer through the same rebuild-and-replay path as document writes, so
> an idle repaired primary can recover and serve peer recovery without an
> unrelated client mutation. Replay holds the shard translog lock for the
> entire retained suffix, blocking writes to that shard; this can be a long
> critical section when refresh is disabled.
>
> Both the startup delete-resurrection defect and the transient-commit
> acknowledged-write loss existed at the `8f17172` main baseline.
>
> **Allocation identity and replica fencing — September 26, 2026:** Raft now
> assigns every shard copy an allocation ID derived from the assigning log
> position. Recovery start, source sessions, install state, pending membership,
> admission, activation, replication, and copy-failure reports retain that
> identity. Local copies atomically persist index UUID, allocation ID, and a
> monotonic replica term fence; replica RPCs validate those fields and the
> recovery gate before WAL mutation. CreateIndex replicas start out of sync,
> first activation marks the shard initialized, and only the initial primary
> allocation may be created empty before activation. An unopenable initialized
> replica reports `FailShardCopy` and its exact allocation is removed. Primary
> reports are promote-only: an in-sync survivor is promoted when available,
> while a primary without a survivor retains its allocation and stays
> unavailable rather than being converted into a metadata-red empty-copy path.
>
> Allocation IDs now resolve remove-and-re-add ABA for recovery start,
> admission, pending-target observation, and copy-failure reports. Assigned
> read/write/replication opens validate UUID and allocation identity even when
> an engine is already present.
>
> **Allocation/fencing review corrections — September 27, 2026:** an unchanged
> red shard now preserves its absent primary allocation through `UpdateIndex`,
> so healthy sibling shards can fail over, allocate replicas, and receive
> settings changes. Pending recovery observation checks admission first, then
> rejects an exact target allocation when the observed primary changed or its
> term advanced; `MarkReplicaInSync` additionally requires an initialized shard
> with a present primary allocation. Target restart restores an exact matching
> awaiting-membership marker and reopens the finalized copy before scheduling
> recovery. Target begin/prepare and source-session polling refuse to restart a
> finalized or settling transfer. Controlled retryable recovery failures clean
> their partial install and retain the same assignment. Definitive pending
> rejection deliberately restores an install marker; lifecycle then fails the
> replica allocation and schedules a fresh allocation rather than reusing a
> recovery result produced under obsolete primary authority. Duplicate failure
> reports are rate-limited per allocation.
>
> The pre-activation empty-primary rule depends on a node's applied routing view
> not moving backward. Production startup constructs OpenRaft over the
> persistent `raft.db`, and OpenRaft replays committed state before that view is
> used for normal shard service. Losing `raft.db` and rejoining under the same
> node name does not satisfy this assumption; retained shard storage must not be
> treated as a fresh pre-activation allocation in that scenario.
>
> **Storage-failure and idle-activation corrections — September 27, 2026:**
> identity, marker, WAL manifest/frame, and Tantivy metadata/segment
> decode-validation failures are definitive immediately. Other filesystem and
> engine I/O uses operation-specific per-copy retry state with exponential
> backoff (1 second up to 5 seconds) and escalates only after at least three
> failed attempts spanning the configured window, 60 seconds by default.
> Replica escalation removes the exact allocation. Primary
> escalation submits a promote-only failure only when an in-sync candidate
> exists; otherwise routing is retained and the shard remains unavailable.
> Request and lifecycle opens share the same retry state, so request traffic
> cannot bypass backoff. Node lifecycle also invokes the same idempotent
> primary-activation path used by writes after startup or promotion, allowing
> an idle pending target to observe admission or a newer term. If publication
> of the durable pending marker succeeds but its directory fsync reports an
> error, the in-memory target state is still advanced (and retry cleanup can
> reconstruct it from the marker) without a process restart. Recovery aborts
> verify the currently registered index UUID before touching disk, so a delayed
> abort cannot recreate a deleted index incarnation.
>
> **D1 collision/gap correction — September 29, 2026:** definitive
> sequence/version collisions are returned as `DATA_LOSS`, quarantined without
> waiting for a lagging replica routing view, and reported by the primary with
> the exact allocation and captured term. Gap probes are concurrent and
> timeout-bounded; an assigned copy that is still opening is transient
> `UNAVAILABLE`, while only proven identity/allocation mismatch or corruption
> is definitive. Replicas may auto-flush only through their own committed
> persisted prefix and never across a gap.
>
> **Availability-status and Apply corrections — September 27, 2026:** a
> write-only failure no longer bypasses the local activation cache or causes
> periodic primary-term bumps. `primary_unavailable` remains set for the exact
> allocation and term until the first successful local primary write commits a
> conditional `MarkPrimaryAvailable`. Definitive and open-level failures are
> quarantined only after report throttling; repaired storage then requires a
> fresh `ActivatePrimary`, which clears the flag while advancing the term.
> Apply-level escalation keeps the existing engine open for reads and does not
> itself quarantine, reopen, or replay a WAL-appended but engine-failed
> operation. Such an operation has an unknown outcome. In production this
> failure means the Tantivy writer was killed, so the next commit fails, the
> rebuilt writer replays the operation on this copy, and restart replay applies
> it too. When the failure is on the primary, replicas never receive the
> operation, so copies diverge. A force-merge
> writer-replacement failure leaves a typed unavailable-writer state. The next
> write or blocking maintenance/snapshot operation rebuilds the writer with the
> normal heap budget and automatic merge policy, then replays the retained WAL
> suffix before continuing. Transient failure can heal there. Persistent
> rebuild or replay I/O consumes the Apply retry budget only when a write
> triggers the rebuild; maintenance-triggered rebuilds log and retry on each
> maintenance tick without escalating.
> Status-only `MarkPrimaryAvailable` reporting runs in the background so leader
> discovery or forwarding cannot delay an already-successful write response.
> This status introduces an intentional health-semantics difference from
> OpenSearch. FerrisSearch can report red for a write-only fault while the exact
> primary allocation remains assigned, open, and readable; mutations are
> unavailable until repair is proven. OpenSearch red denotes an unassigned
> primary, so the affected shard serves neither reads nor writes.
>
> Candidate selection requires a live, in-sync cluster member. A leader that
> also hosts the primary prefers the highest replica checkpoint it has
> observed; a metadata-only or other non-primary leader has no such local
> observations and uses an unranked live in-sync member. Every in-sync member
> has acknowledged every write required by the current synchronous write set.
>
> A failed Tantivy commit is logged and also invalidates the writer. It does not
> advance `translog.committed` or authorize WAL truncation. The next write,
> blocking maintenance operation, or peer-recovery snapshot rebuilds the writer
> and replays the retained suffix with the same idempotent path used at
> startup. Only a write-triggered rebuild enters the Apply retry budget; an
> idle copy with a persistent fault logs and retries on each maintenance tick
> until a write arrives. Replay holds the shard translog lock for the whole
> suffix, so blocked writes also occupy write-pool threads and can delay other
> shards on the node.
> Allocation can also return a replacement to the same faulty node; a
> MaxRetryAllocationDecider-style exclusion policy and
> `index.allocation.max_retries` setting (OpenSearch defaults to five retries)
> are deferred.

The current maximum document operation size is defined by the encoded WAL
frame, not the raw HTTP body: one operation must fit within 32 MiB including
the four-byte frame header, sequence and operation fields, JSON serialization,
and the internal `_doc_id` / `_source` wrapper. The maximum usable `_source`
therefore varies slightly with document ID and content. The same limit applies
to restart, replay, and peer-recovery transfer.

**Known write-failure limit:** synchronous WAL/fsync/engine failures now fail
the request and enter bounded escalation, but a WAL entry whose later engine
apply fails has an unknown outcome. In production this failure means the
Tantivy writer was killed. The next commit then fails, and the rebuilt writer
replays the entry on this copy, as does a restart. If the failure happened on
the primary, replicas never receive the entry, because replication starts only
after local success. In-sync copies can therefore diverge on up to one refresh
interval of client-failed writes, or longer with refresh disabled, and peer
recovery from this copy can ship the retained entry to a new copy. If a partial
append is followed by later writes, the torn frame can become middle
corruption; restart then fails closed rather than skipping acknowledged
history. Startup tail truncation repairs only a trailing incomplete frame with
no later data.

## 3. Reference Protocols And Intentional Differences

| Reference | Relevant property | FerrisSearch target |
|---|---|---|
| Elasticsearch replication [R1] | Primary-backup replication with master-managed in-sync copies; failed-copy exclusion participates in safe acknowledgement. | Authoritative in-sync allocation IDs, term/configuration checks, and acknowledgements from the required set. |
| OpenSearch peer recovery [R2][R3] | Decides between operation replay and file transfer using history, retained operations, and a suitable commit; finalizes recovery separately. | Verified snapshot plus WAL suffix, or replay from a proven common boundary; explicit admission barrier. |
| Elasticsearch history retention [R4] | Retained operation history supports incremental recovery; expired history can require file copying. | Bounded source-side history leases and snapshot fallback, not indefinite WAL retention. |
| CockroachDB replication [R5] | Raft logs and snapshots recover range replicas. Its quorum-based data plane is a different architecture. | Reuse the snapshot/log separation and failure discipline; do not introduce per-shard Raft or call all-in-sync replication a quorum protocol. |
| FoundationDB testing [R6] | Deterministic simulation exercises failure schedules. | Deterministic fault points plus real-process persistence/network tests and recorded seeds. |

Differences must remain explicit:

- This design does not copy Lucene's file formats or Elasticsearch's internal RPCs.
- The first implementation keeps an affected request failed/indeterminate after
  a replication failure. Later exclusion does not retroactively turn that response
  into success. A safe, identity-preserving retry is a separate operation attempt.
  This differs from Elasticsearch's replication action, which can acknowledge
  the original write after the master confirms the failed copy's exclusion.
- Universal exactly-once client requests, multi-shard transactions, linearizable
  search, and cross-engine point-in-time reads are not provided by recovery.
- Comparable recovery performance requires separate equivalent-workload
  measurements. No throughput or recovery-time parity is claimed here.
- Promotion first reconciles non-conflicting retained tails from reachable
  eligible copies without destroying them. If genuinely conflicting histories
  still require snapshot replacement, version 1 may temporarily activate a
  smaller in-sync set while repair runs. Unlike an implementation that can
  safely trim only the divergent suffix, it does not claim equivalent
  redundancy/availability during that interval. With a minimum of one, losing
  the sole current copy can make the shard unavailable or lose writes accepted
  in that interval; configure a minimum of two to block those writes.

## 4. Fault Model And Acknowledgement Contract

### Supported fault model

Cover process crashes and restarts, delayed/duplicated/reordered messages,
network partitions, metadata-leader changes, primary replacement, interrupted
transfers, and detected disk corruption/exhaustion. Storage must honor successful
fsync and atomic rename guarantees. Byzantine peers and simultaneous destruction
of every durable copy are outside the guarantee.

Raft quorum is required to change shard authority. A surviving metadata quorum
does not imply a surviving copy of document data. If no valid in-sync data copy
survives, the shard remains unavailable; never automatically promote a stale copy.

The zero-acknowledged-loss target applies to request durability: the required
copies persist the operation before success. Existing asynchronous fsync mode
has a weaker crash contract and must not be included in that claim, even when
multiple machines are configured.

### Proposed acknowledgement policy

For an active term/configuration, let `I` be its authoritative in-sync allocation
set, including the primary. A successful write requires:

1. A valid local primary permit for that shard, history, term, and configuration.
2. Every member of `I` returns an acknowledgement bound to the operation,
   history, authority term, and configuration, with
   `durable_applied_prefix > operation.seq_no`.
3. The acknowledged operation itself is below each returned contiguous durable
   boundary. A gap-free prefix below the operation is not sufficient.
4. The configured minimum durable-copy requirement.
5. No concurrent authority transition invalidating that operation's permit.

Proposed setting: `minimum_durable_copies`, default `1` for the existing
single-node profile. A value of `2` blocks writes when fewer than two durable
copies are eligible. The setting is a minimum, not permission to skip another
member of `I`; all in-sync copies must respond. Reject impossible combinations
at configuration time and validate changes through Raft.

This default is a design recommendation, not an approved implementation change.
With one surviving eligible copy and a minimum of one, writes can continue after
safe reconfiguration, but losing that last copy can lose data. With a minimum of
two, availability is deliberately sacrificed until redundancy is restored.

Desired replica count, currently assigned copies, active copies, and `I` are
different concepts. Adding an empty assignment must not block writes by
pretending it is already in sync; nor may it qualify for promotion.

A timeout or failed replica apply may follow a primary mutation. Return the
underlying failure and an indeterminate outcome where appropriate, not a claim
that nothing happened. A rejected-before-mutation request is distinguishable
from an indeterminate one. A completed response records real sequence/term
metadata; bulk results preserve each item's identity and outcome.

## 5. Identities And Durable Boundaries

### Authority and operation identity

| Identity | Owner and rule |
|---|---|
| `(index_uuid, shard_id)` | Permanent shard address; index names are not storage identity. |
| `history_uuid` | Identifies one authoritative shard history. Ordinary recovery/promotion preserves it. Explicit data-loss recovery starts a new history. |
| `allocation_id` | Identifies one assigned data copy. Replacing lost storage creates a new allocation, even on the same named node. |
| `process_incarnation` | Distinguishes process instances and invalidates old sessions; a restart does not prove the copy is healthy. |
| `primary_term` | Strictly increases through a committed Raft promotion; never inferred from a data-node clock. |
| `configuration_generation` | Monotonic version for copy membership and authority transitions; compare-and-set precondition for metadata mutations. |
| `(history_uuid, seq_no)` | One resolved position in the authoritative operation history. The record also carries its origin term and operation digest. |
| `operation_id` | Stable identity for an internal write/retry; never regenerated by forwarding. Includes a request digest to reject identity reuse with different content. |
| `recovery_id` | Identifies a source/target recovery attempt, additionally bound to term, history, allocation, incarnation, and snapshot. |

Raft stores small authority/configuration records, not document records,
per-operation checkpoints, or transfer chunk inventories. Fine-grained applied
positions, retention pins, and recovery progress belong in durable shard state.
Commands updating shard authority compare the expected index UUID, term, and
configuration generation; stale whole-index replacement must not overwrite
concurrent mappings/settings/routing changes.

Every primary write, replica apply, recovery chunk, completion acknowledgement,
and promotion response validates its relevant identities before mutation.
Receiving a higher number in an untrusted RPC does not itself grant authority:
the receiver must validate the committed assignment.

### Exclusive prefixes, not maximum observations

Use exclusive prefixes so `0` means no resolved operations and sequence `0`
remains a real operation. Prefix `p` covers **every** position in `[0, p)`.

- `processed_prefix`: all covered operations have been applied or resolved as
  durable no-ops.
- `persisted_prefix`: all covered journal records are durable.
- `durable_applied_prefix = min(processed_prefix, persisted_prefix)`.
- `global_durable_prefix`: the primary's conservative minimum
  `durable_applied_prefix` across the committed in-sync set.
- `snapshot_prefix`: exact exclusive boundary represented by a snapshot.
- `retained_from`: first position the source can still replay.

Receiving sequence 9 while 8 is absent must not move a prefix from 8 to 10.
Keep bounded gap bookkeeping, or backpressure senders until missing positions
arrive. Pending out-of-order work must not monopolize the worker needed to fill
the gap. Validate overflow before reservation or WAL mutation.

Persist enough metadata to reconstruct prefixes from a verified snapshot and
checksummed WAL on restart. Never reset an existing copy to a manufactured zero
and infer that it is a fresh replica. Global progress may be conservatively
behind after restart; it must never be invented ahead of verified local history.
In particular, a candidate must not discard its own durable history merely
because its last received global-prefix update is behind.

Per-document last-operation metadata and delete tombstones protect OCC and
replay ordering. Retransmission of an already verified operation is idempotent;
the same position/operation ID with different content is a hard history conflict.
Do not apply an old update over a newer value merely because the RPC was retried.

Validation precedes sequence reservation when possible. A reserved gap needs a
durable no-op before advancing its prefix, and the original attempt must be
cancelled/drained under the shard permit before that position is resolved.
A delayed attempt cannot subsequently replace the no-op. Once a real WAL record is durable,
an engine failure cannot be hidden by replacing that record with a no-op: stop
the copy, reconstruct its state, and preserve the ambiguous client outcome.
In the implemented D1 subset, if this happens on the primary before fan-out,
no replica receives that real record. After local replay and a later write
expose the gap, the fixed-target timer normally removes every affected replica
after about 60 seconds and peer recovery rebuilds it from the primary. Promotion
NoOps do not repair this case; targeted live gap repair remains D10 work.

### Storage errors and recoverable crash tails

A write, fsync, or metadata-publication/rename error fail-stops the affected
copy. It issues no further durability acknowledgements, does not reuse the
uncertain sequence/offset, and does not retry fsync and infer that the previous
failed write is now proven. Release request resources, report the underlying
error, and make the allocation unavailable pending verified repair. Removal
from authoritative eligibility still requires a conditional Raft transition.

The durable format must distinguish a committed durability frontier from an
unsealed trailing attempt. For the initial design, acknowledgement requires:

1. Complete, checksummed WAL records through the acknowledged prefix are fsynced.
2. A checksummed durability descriptor identifying generation, physical end,
   prefix, and history digest is atomically published and durably synced,
   including its directory entry.
3. The engine has applied that same contiguous prefix.

Descriptor writes may be group-committed, but their durability cannot trail a
successful response. An implementation can propose a different proven framing
scheme in FS-005; it cannot omit the frontier distinction to obtain throughput.

On restart, discard a torn final unsealed attempt only when a valid durability
descriptor proves it lies wholly beyond the last acknowledged durability
frontier. A checksum/length mismatch within that frontier, mid-log corruption,
or an ambiguous/corrupt descriptor makes the copy ineligible and requires
verified peer/snapshot repair. Never scan past an invalid interior record or
silently truncate from it while retaining later successes. Complete records
beyond the frontier are unacknowledged evidence for canonical reconciliation,
not permission to resume the old writer.

Do not rely solely on persisting a local `FAILED` flag to the same failing disk.
Every restart must revalidate authority and storage; a former primary requires
a new term before issuing permits. If eligibility cannot be proved, fail closed.
A live fail-stopped process reports shard failure to the control plane so
exclusion/promotion can proceed; it must not continue serving while that report
is pending. Local failure state does not itself edit Raft membership. After a
restart, a copy still in the authoritative in-sync set can be considered for
promotion only if its complete protected frontier/history validates and a new
authority handshake succeeds. If exclusion already committed, good local files
alone cannot restore eligibility: the copy needs normal recovery/readmission.

## 6. Write Authority, Reconfiguration, And Promotion

### Normal write

1. The coordinator routes using cluster metadata and preserves operation identity.
2. The primary obtains a shard-local authority permit and checks admission bounds.
3. It sequences and journals the mutation, applies it, and establishes its own
   durable frontier before replicating the same identity to the required
   copies. Version 1 does not overlap replica dispatch with the primary's own
   uncertain fsync. Replica acknowledgements carry durable-applied prefixes,
   not receipt into a queue.
4. Replicas enforce committed source authority, term, and history. A replica
   never allocates a replacement sequence.
5. The primary gathers the required proof before success. On failure it reports
   the error/indeterminate outcome and schedules repair or exclusion.

Term permits coordinate the entire operation with configuration changes, without
holding a Tantivy writer lock across network I/O. Promotion on a candidate drains
or cancels its old-term work before installing a durable fence. All waiting work
has explicit deadlines and releases resources on cancellation.

### Failed-copy exclusion and admission

Exclusion is a conditional Raft change authorized for the active primary term,
not a primary-local edit. It cannot remove the only copy known to contain an
acknowledged operation and then promote an unproven copy. Excluded copies become
stale/recovering and lose promotion eligibility.

Changing the set requires a per-shard admission barrier and a unique transition
ID. Drain in-flight operations and keep new admission closed until the
transition is definitively settled. At any point admitting writes, the
primary's required-ack set must contain the committed in-sync set.

A linearizable read still showing the old generation does **not** cancel an
in-flight proposal: that proposal may commit after the read. Settle a timed-out
transition only by observing its committed result, or by committing a
superseding conditional transition/abort against the same expected generation
that advances the generation and therefore makes the original proposal fail.
The state machine identifies the winning transition and validates both
competing commands with the same compare-and-set rule.

The first implementation keeps write admission closed until one of those
outcomes is established; it does not add the optimization of continuing with
the union of old/proposed ack sets. A deadline bounds request resources, not the
right to resume with guessed membership. Persist/reconstruct the unresolved
transition state; if authority remains unavailable, leave the shard unavailable
for new writes without holding a worker or an unbounded queue.

An affected write that already failed remains failed/indeterminate. Subsequent
writes use the committed set and minimum-copy policy. A retry must identify the
original operation, not allocate a new position just because a response was lost.

### A deduplication hit is not an acknowledgement

The result recorded with a WAL operation describes the local mutation, not proof
that replication succeeded. A deduplication lookup must not bypass authority,
durability, or required-copy checks.

To return success for a retained operation at position `s`, the current ACTIVE
primary must acquire a current permit, verify that the same operation/digest is
in canonical history, and satisfy all five acknowledgement conditions in
Section 4 under its current committed configuration. In particular, every
required copy must attest to a durable-applied prefix above `s`. It may reuse
validated current-term progress evidence, or repair/wait for missing progress;
the stored local result alone is never sufficient.

Otherwise return pending/indeterminate or wait on the original attempt within
the request budget. An obsolete primary cannot manufacture success from its
dedup table after it loses authority. After safe adoption, a new primary can
confirm the original mutation/receipt without allocating another sequence,
while establishing fresh acknowledgement proof under the new authority.

### Promotion protocol

1. **Reserve authority:** Raft conditionally commits a higher term and a
   `PROMOTING` candidate chosen from the committed in-sync allocations. A
   heartbeat timeout is only a trigger to investigate; it is not data proof.
2. **Fence and inventory:** validate the candidate's storage/history, drain
   old-term apply permits, and durably record the new term. Obtain bounded
   history reports from reachable authoritative copies after they validate the
   committed `PROMOTING` record, durably persist its fence, and drain old-term applies.
   Their reported tails are then stable against additional old-term mutations.
   Reports bind the peer's process incarnation; after a peer restart, establish
   a new report before counting it. A persisted fence never decreases because
   the recovering process temporarily has an older local Raft view.
   An unavailable report is not invented as an empty history.
3. **Establish canonical history:** recover the candidate's durable records,
   preserve at least its own verified durable-applied prefix, and resolve its
   old-term tail and gaps. Never trim to a lagging global-prefix observation.
   Previously acknowledged operations must exist on this candidate by the
   in-sync invariant. Unacknowledged operations may survive; failure responses
   did not promise rollback. Version 1 also adopts non-conflicting verified
   same-history records from the frozen peer reports. Do not no-op a reservation
   for which a reachable eligible peer has a matching valid record. If the
   candidate has no valid record at a position and frozen peers disagree on
   its contents, that position was not acknowledged under the required-prefix
   rule. The fixed version-1 rule is a new-term no-op, with the disagreeing
   copies excluded for post-activation repair. This explicit conflict case
   is the exception to adopting the peers' otherwise matching record. Reconstruct
   canonical state in sequence order and set `next_seq_no` beyond every
   retained resolved position.
   Different records at one position are not resolved by numeric term alone:
   preserve the candidate's protected history, validate each record's authority
   provenance, and leave conflicting copies out for post-activation repair.
   Such a divergence is an explicit conservative recovery path, not the ordinary
   case of one same-term replica having an extra in-flight operation.
4. **Classify other copies without destroying eligible history:** verify their
   common boundary and canonical tail. While `PROMOTING`, reconciliation of a
   copy still in the authoritative in-sync set can add matching missing
   records, but cannot roll it back, replace its active generation, or truncate
   its durable history. A conflicting copy is left intact and omitted from the
   proposed new in-sync set; a large checkpoint alone never admits it.
5. **Activate:** after canonical state and the new eligible set are durable,
   Raft conditionally commits `ACTIVE`. The candidate persists/observes that
   configuration before issuing new write permits. Only after this commit has
   removed conflicting copies from promotion eligibility may their histories
   be repaired by snapshot replacement or verified rollback/replay.

The inventory/resync channel in steps 2-4 is a bounded, append-only
**promotion resync**, authorized by the committed `PROMOTING` term and expected
allocations. It is not ordinary snapshot recovery and does not admit new copies
or authorize client writes. Receivers validate this specific authority before
fencing/reporting/appending; a candidate's request alone is insufficient.

Before issuing new-term sequence positions, the candidate adopts and seals its
complete valid records beyond the old durability frontier. It may discard proven
torn unsealed bytes, but must recover a matching valid record from a reachable
eligible peer rather than no-op or reuse that logical position. The explicit
peer-conflict rule above applies when the candidate has no valid record.
Discarding an unsealed attempt must be durable before any otherwise valid reuse
of an unacknowledged reservation. Never remove protected acknowledged history
or leave two live journal records claiming one resolved canonical position.

If only the candidate is usable, it may become `ACTIVE` to source recovery.
New writes stay blocked when `minimum_durable_copies > 1`; with a minimum of
one, they can proceed with the explicitly weaker remaining-copy protection.
Activation and write availability are distinct.

If the candidate crashes during promotion, the next attempt uses a higher term
and verified durable state; it does not reuse an old recovery session or simply
clear the `PROMOTING` flag. The previous authoritative copies retain their data
until an activation/exclusion has safely removed eligibility. Restarting the
same former primary also requires a higher committed term; process identity
alone must not revive old-term permits. Unsupported or corrupt formats prevent activation.

### Why a cached term is not enough

An isolated old primary may not have received the newest cluster state. Merely
checking its cached term cannot fence it.

Safety also depends on **acknowledgement-set intersection**: a valid promotion
candidate was in the previous authoritative in-sync set. After it installs its
new-term fence, the old primary cannot obtain that copy's acknowledgement for
a new old-term mutation. It also cannot commit an old-term membership exclusion
past Raft's term/configuration checks. If the previous set contains only the
old primary, there is no different eligible candidate to promote.

Admission barriers must preserve this argument during every set change.
Do not substitute failure-detector timers or clock leases for it. A delayed
response for an operation already durable in canonical history can still arrive
after promotion; this is different from accepting a new stale mutation.

### Index deletion is a separate authority boundary

The intersection argument alone does not stop an isolated sole primary from
accepting old-index writes after a coordinator removes metadata. The proposed
deletion contract must therefore fence outstanding primary authority before
reporting deletion complete: commit a `DELETING` tombstone, close/drain permits
on the holders, obtain durable fencing proof, and only then finalize deletion
and schedule physical cleanup. New-index UUID checks remain mandatory.
Committing `DELETING` conditionally advances the affected shard configuration
generations. While that tombstone exists, normal promotion/`ACTIVE`, admission,
and exclusion commands for the index are rejected; only transitions belonging
to that deletion can proceed. A delayed pre-deletion command cannot create a
new authority holder after fencing proof was collected.

An unreachable holder without fencing proof keeps deletion pending/unavailable;
a metadata timeout cannot be interpreted as proof that the process stopped.
After restart, an old allocation must observe the tombstone and cannot activate.
A `DELETING` tombstone also reserves the index name. Neither explicit creation
nor auto-creation may bind that name to a new UUID until deletion has finalized;
return pending/conflict instead. A forwarded retry remains bound to its original
UUID and must not silently retarget a replacement index.
This is an explicit proposed behavior change to approve before implementation,
not a claim about the current delete-index endpoint.
Version 1 provides no force-delete escape hatch. A permanently lost holder
without sufficient fencing proof can therefore leave deletion and name reuse
blocked indefinitely. An audited operator override would need a separate
explicit contract; it cannot be implemented as an automatic timeout fallback.

## 7. Snapshot And Incremental Recovery

### Snapshot contract

A versioned manifest binds immutable files to:

- shard/history identity, snapshot ID, exact snapshot prefix, and schema generation;
- durable term/configuration provenance and sequence-allocation state;
- the exact captured Tantivy commit metadata and its immutable file set, with
  checksums, lengths, and format versions; the source's live `meta.json` cannot
  be reread later as a substitute because subsequent commits mutate it;
- document-version/tombstone state needed for OCC and idempotent replay;
- complete vector state at the same logical boundary, or an explicit complete
  rebuild from that snapshot's authoritative document values;
- retained retry-result metadata where the supported retry window requires it.

A snapshot at prefix `B` represents exactly the resolved history below `B`.
An engine commit containing later operations cannot be relabelled with an
earlier global checkpoint. This matters for updates and deletes, not just counts.

The initial implementation should capture a safe snapshot under a short,
bounded shard barrier: drain mutations and their gap resolutions to the primary's
head `B`, and require every in-sync copy's durable-applied prefix to equal `B`.
Commit the matching engine/version/vector state, fsync its
manifest and publication metadata, pin it, then release the barrier. Reuse an
existing verified safe snapshot when possible. Copying its files must happen
after releasing that barrier. If a common boundary cannot be established within
the budget, retain the previous snapshot and fail/retry preparation explicitly.

Snapshot pinning must prevent merge cleanup, WAL pruning, and shard deletion
from invalidating files being transferred. Deletion cancels recovery, waits for
pin release, and then removes only the identified shard/history.

### Choose the recovery path

The version-1 source is the `ACTIVE` primary, not an arbitrary replica or a
`PROMOTING` candidate. It validates the committed target assignment before negotiating:

- **Operation-only:** the target has a verified matching history boundary and
  compatible schema/format; the source retains every required operation.
- **Snapshot plus suffix:** a new/replaced copy, missing retained history,
  incompatible base, or divergent local tail requires a verified snapshot.
- **Unavailable:** there is no eligible source or no valid snapshot/history
  that can establish the required state. Do not return empty recovery success.

Comparing maximum sequence numbers or trusting a target's claimed prefix is
insufficient. Verify the history identity and a canonical history digest at
the common boundary. Existing divergent target state must be removed before
replay; silently skipping its suffix would preserve wrong updates/deletes.

### Session and transfer

1. Create a session bound to source/target allocation and process incarnation,
   source term/configuration, history, and selected snapshot.
2. Atomically pin the snapshot and required WAL floor against pruning before
   promising that incremental completion is possible.
3. Transfer a bounded manifest and checksummed file chunks into a target staging
   generation. Validate file names, lengths, offsets, duplicates, and hashes;
   network paths never choose arbitrary local filesystem destinations.
4. Verify completeness and format compatibility, fsync files and directories,
   and atomically publish the target's active-generation pointer. Preserve the
   previous valid generation until the switch is durable. A restarted target
   sees either complete old state or complete new state, never a mixture.
5. Replay the bounded ordered WAL suffix from `snapshot_prefix` inclusive.
   Duplicate chunks must be idempotent; mismatches and gaps cannot advance
   the durable prefix.
6. Establish the final admission barrier described below.

The recovery receiver is distinct from normal live-replica apply until the
target's base is installed. Concurrent live writes may be streamed or buffered
within explicit limits, but cannot mutate a half-installed engine. Buffer
exhaustion backpressures or aborts the session; it cannot silently drop writes.

On source/primary change, stop the old session and release its reservations.
The new source may reuse verified immutable chunks only after validating them
against its own manifest. Old completion messages cannot activate the target.
A process restart requires a fresh authority/session handshake; durable local
progress is a reuse hint, not a license to continue an obsolete session.

### Final catch-up and in-sync admission

The primary pauses new admission for this shard, drains preceding operations,
and establishes barrier prefix `C`. The target must durably apply the exact
canonical prefix through `C`, including deletes/no-ops, and verify its digest.

Then Raft conditionally adds the target allocation to the in-sync set for that
same term, history, and configuration. The primary observes the committed
generation and changes its required acknowledgement set **before** releasing
new writes. This closes the catch-up/live-write race.

If the membership result is ambiguous, admission stays blocked until the
transition commits or a generation-bumping superseding CAS makes it impossible
to commit. Reading the unchanged old generation is not settlement. If the primary changes, the old completion is
invalid. A target is neither promotion-eligible nor an ordinary search copy
before successful admission. Physical files being present is not readiness.

### Retention and retry

History leases/pins have time and byte limits. Persist a source's retention
floor before granting it, and serialize pruning with lease/snapshot acquisition.
Retention protects replay while the snapshot is copied; a lease is not a
primary-authority lease.

When a lease expires or a storage budget requires cancellation, report the
session failure and release its pins before pruning. A later attempt rechecks
actual availability and falls back to a snapshot when needed. A lease on a
failed source does not promise that a replacement source retained the same WAL.

Internal retry-result retention is also bounded. A retry carries its original
operation ID, content digest, and logical `retry_epoch`. Retiring an epoch is a
coarse Raft-committed change with a monotonic rejection floor, not an inference
from different nodes' wall clocks. Local timers may request retirement but
cannot independently expire deduplication authority. An envelope below the
committed floor is rejected as expired/unknown, never treated as a new mutation.

Within the retained epochs, the operation's original result/digest is durable
with its WAL record on every required copy. Promotion and snapshot/suffix
recovery must preserve this deduplication coverage before admitting the copy.
Local flush and WAL truncation must preserve the retained result records in
durable snapshot/side state before pruning their journal representation.
An identity-preserving retry reuses the mutation identity/result, but returns
success only after the current-authority acknowledgement checks in Section 6.
A new external HTTP request with no recoverable
original identity does not receive this guarantee; a public arbitrary client-key
API is a separate decision.

Tombstone/version pruning likewise requires a durable rejection floor and a
safe snapshot representing those deletes. No retained retry, recovery session,
or accepted replay range may reintroduce an operation below that floor.
Copies needing discarded history must use a snapshot at or beyond the floor.
Pinned older snapshots retain their own complete view until released. A
wall-clock TTL alone cannot authorize tombstone deletion or old-value resurrection.

## 8. Replica State Machine

Membership and recovery state are related but not interchangeable.

| State | Permitted activity | Exit condition |
|---|---|---|
| `UNASSIGNED` | Allocation planning only. | Raft commits a specific target allocation. |
| `INITIALIZING` | Validate disk identity, authority, format, and budgets. | Select a verified recovery path. |
| `SNAPSHOTTING` | Receive/verify staged files; no ordinary reads or writes. | Complete atomic installation. |
| `CATCHING_UP` | Apply validated canonical suffix; not part of required acknowledgements. | Reach the current primary's barrier. |
| `ADMITTING` | Hold final barrier and reconcile conditional Raft membership. | Committed in-sync membership observed, or explicit abort. |
| `IN_SYNC` | Serve assigned operations; participate in required acknowledgements. | Failure, exclusion, relocation, or term change requires reconciliation. |
| `STALE` | Preserve evidence, but do not serve as a current copy or promote. | New validated recovery session. |
| `FAILED` | Surface cause and retryability; release permits/pins. | Explicit retry under current authority, or operator repair. |

`PROMOTING`/`ACTIVE` describe primary authority, not a shortcut around copy
recovery. A restart of an in-sync allocation validates/reconstructs its durable
state and re-establishes its role before serving. Corruption marks it ineligible.
Recovering a replica hosted by the metadata leader follows the same state
machine as recovery on any other node.

The allocator computes each shard's missing copies from desired redundancy and
eligible assignments. Count the loss of a node's primary and replica roles
independently across all shards. Neither aggregate index state nor an HTTP
health color can substitute for the per-copy state machine.

## 9. Resource, Error, And Observability Contract

Recovery must reserve explicit limits for active sessions, in-flight bytes,
chunk size, reorder buffers, staging disk, pinned history, and replay work.
Use node-wide budgets plus per-session reservations; multiplying a per-stream
limit by unlimited streams is not a memory bound.

No full-shard file or WAL suffix is materialized in memory. An operation larger
than the supported bound is rejected before primary mutation or transferred
through an explicitly bounded framed-operation path. Socket flow control alone
does not bound application queues.

Blocking file/engine work stays off Tokio control-plane workers. Long recovery
waits do not occupy the fixed foreground write/search pools. Raft heartbeats
continue on Tokio. Snapshot barriers and in-sync admission have explicit
deadlines, not an unbounded stop-the-world interval.

Errors retain source/target/shard/session context and the underlying cause.
Distinguish stale authority, unknown history, missing retained history,
corruption, incompatible format, exhausted budget, cancellation, and unavailable
source. None can become an empty successful recovery.

Expose progress/state through a bounded per-recovery status API: selected path,
source/target allocation, term/history, snapshot prefix, received/replayed bytes
and operations, outstanding gaps, durable prefix, lease state, and last error.
Prometheus metrics use bounded labels such as phase and error class; IDs belong
in logs/status, not metric labels. Measure queue time and phase time separately.

Admission decisions require evidence; metrics and log messages are not that
evidence. Report excluded/stale copies and unmet minimum durability explicitly
even when other copies continue serving.

## 10. Persistence And Pre-1.0 Format Policy

This protocol changes WAL records, snapshot metadata, and transport messages.
Version them together. Unknown formats and corruption fail closed; only a proven
unsealed trailing attempt beyond the durability frontier can be discarded under
the restart rule in Section 5.
New fields are not silently defaulted into a valid epoch, allocation, or history.

There is no conversion/export path in the current pre-1.0 implementation.
Earlier on-disk schemas, WAL entries/manifests, committed boundaries, copy
identities, vector sidecars, Raft logs/snapshots, and cluster-state wire
snapshots are unsupported. Shard and index data fails with recreate-the-index
guidance. An unsupported or unreadable Raft log or snapshot tells the operator
to "wipe the node data directories and recreate the cluster". Incompatible wire
formats fail the join. No role-specific migration, term-zero default, rolling
mixed-protocol support, or compatibility shim is provided.

Ordinary process restart and same-version recovery remain required. File
deletion and old-generation cleanup must wait until atomic-install and retention
conditions hold. Recovery snapshots are not a complete independent backup or
disaster-recovery product.

## 11. Implementation Packages And Stop Conditions

| Package | Scope and prerequisites | Required acceptance evidence |
|---|---|---|
| RP-0 | Decide FS-001/005 contract and formats; build FS-007 boundary controls/oracle. | Matrix harness requirements; explicit durability modes and failure model. |
| RP-1 | Fix reproduced per-shard replica-loss accounting without changing write semantics. | A01, A03. This can ship independently, but A02's safe rejoin requires RP-5. |
| RP-2 | Complete operation/term identity, contiguous durable prefixes, version/tombstone state, and supported retry records (FS-009/010/011). | I01-I08, W01-W08, D01-D06. Cross-promotion parts rerun after RP-3/5; no safe-promotion claim yet. |
| RP-3 | Conditional shard configuration, fencing, safe promotion and acknowledgement membership (FS-012/013). Depends on RP-2 and deterministic partition controls. | F01-F11, M01-M08. Snapshot-dependent repair portions require RP-4/5. |
| RP-4 | Exact-boundary snapshots, complete vector state, retained history and atomic install (FS-022/023/027). Depends on format/identity/safe-boundary work. | S01-S10, H03. Full retained-history/path-selection cases require RP-5. |
| RP-5 | Bounded suffix streaming and recovery/admission state machine (FS-024/025/026). Depends on RP-3/4. | A02, H01-H05, R01-R09, O01-O06, and preceding cross-package cases. |
| RP-6 | Run the declared end-to-end fault matrix and equivalent-workload recovery benchmarks. | E01-E06 and every preceding mandatory case on the final implementation. |

Keep existing API/transport/restart suites running throughout. Intermediate
packages remain explicitly partial: do not enable a new acknowledgement mode
using incomplete fencing or declare a recovering copy promotable before the
final barrier exists. Each package updates the instructions that own changed
contracts; this design does not silently replace current implementation rules.
Each package also adds the required UTs and ITs at its actual boundaries, as
specified in the matrix's [test ownership](recovery-acceptance-matrix.md#unit-and-integration-test-ownership).
End-to-end certification must not become a reason to defer those regressions.

Before implementation, the maintainer must accept the proposed minimum-copy
default, failure/indeterminate response contract, deletion-fencing availability
trade-off, and pre-1.0 conversion policy.
The algorithmic invariants and their tests are not optional tuning choices.

## 12. References

These are primary references, accessed September 24, 2026. Upstream behaviors
are reference targets, not evidence that FerrisSearch already implements them.

- [R1: Elasticsearch reading and writing documents](https://www.elastic.co/docs/deploy-manage/distributed-architecture/reading-and-writing-documents).
- [R2: OpenSearch 3.8.0 recovery target implementation](https://github.com/opensearch-project/OpenSearch/blob/e5a3c5691be87af6c12dbe3e158c59c04ee72973/server/src/main/java/org/opensearch/indices/recovery/RecoveryTarget.java).
- [R3: OpenSearch 3.8.0 recovery source implementation](https://github.com/opensearch-project/OpenSearch/blob/e5a3c5691be87af6c12dbe3e158c59c04ee72973/server/src/main/java/org/opensearch/indices/recovery/RecoverySourceHandler.java).
- [R4: Elasticsearch history retention](https://www.elastic.co/docs/reference/elasticsearch/index-settings/history-retention).
- [R5: CockroachDB replication layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer).
- [R6: FoundationDB testing](https://apple.github.io/foundationdb/testing.html).
