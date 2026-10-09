# ADR 0001: Write Consistency And Retry Contract

- **Status:** Accepted. D1 is implemented; D2-D14 remain implementation work.
- **Date:** 2026-09-27
- **Accepted:** 2026-10-08
- **Design revision:** 2026-10-05; acknowledgement/failure targets selected for
  bounded modeling, not implemented in production.
- **Backlog:** [FS-001](../next-50-tasks.md#fs-001--decide-the-write-consistency-and-retry-contract)
- **Roadmap:** Gate 0 deliverable "accepted write acknowledgement, retry, OCC,
  and version semantics"; invariants 6.1.1-6.1.6 in
  [`architecture-roadmap.md`](../architecture-roadmap.md#61-write-and-version-invariants)
- **Scope:** `local_shards` document writes (index, create, update, delete, and
  bulk), and the reads writes depend on (GET by ID and refresh). Remote-store
  publication is FS-002.

The complete decision is accepted. D1 is implemented; D2-D14 define required
future behavior until their implementing tasks land. Acceptance does not claim
that those behaviors exist in current source. Section 1.1 describes the code at
6dbf9dc, before D1.

## 1. Context

### 1.1 Current behavior

A single write flows from the REST handler (`src/api/index/mod.rs`) through
`TransportClient::forward_*_to_shard` to the primary's transport handler
(`src/transport/server/mod.rs`). The primary checks activation and its exact
allocation and term (`validated_primary_write_state`). It then appends the
operation to the WAL (with fsync under `request` durability) and applies it to
Tantivy. After leaving the WAL critical section, it sends the operation to every
Raft in-sync replica (`src/replication/mod.rs`). Any replica error fails the
request. Nothing retries internally.

| Gap | Current behavior | Source |
|---|---|---|
| Replica apply order | Concurrent requests can reach a replica out of sequence order, because the primary replicates after leaving its WAL critical section. The replica applies in arrival order, its WAL accepts any order, and replay assumes sequence order. In a review probe (a 3,000-document bulk plus 50 concurrent single writes), the replica WAL was out of order in 4 of 10 runs. In one run, 26 of 50 acknowledged documents differed between primary and replica. Promoting such a replica rolls back acknowledged writes. The TLA+ model allows one client write at a time, so it cannot show this. | `apply_replica_operation`, `append_with_seq`, `write_bulk_with_start_seq` |
| Ambiguous failures | A failure after the primary applied the write returns HTTP 500: a replica error (`success: false`), a 30 s transport timeout, or a lost connection. Only pre-execution failures return 503: two shard-availability cases and `master_not_discovered_exception` from index auto-creation. A stale term or an unactivated primary also returns 500, so the status never separates executed from not executed. | `replicate_write`, `TransportClient` |
| Write availability | The storage escalation budget removes a replica only when its own storage fails. An unreachable replica that is still a cluster member is never removed (`unreachable_in_sync_replica_still_fails_live_write`), and neither is one failing with an unclassified error. Writes fail until the node leaves the cluster or an operator intervenes. The failure detector removes only nodes that stop pinging the Raft leader, so a live replica that fails writes, or one unreachable only from the primary, stays in sync. Each attempt can wait out the 30 s timeout. | `report_local_copy_failure` |
| Fabricated metadata | Bulk successes always report `_version: 1`, `_primary_term: 1`, `result: "created"`, and `_shards: {total: 1, successful: 1}`. Single writes return HTTP 201 and `result: "created"` even when they replace a document. Deletes always report success. | `bulk_success_item`, `index_document_with_id`, `delete_document` |
| Stale reads by update | GET reads only the last refreshed reader. `_update` merges at the coordinator from GET, so an update within the refresh interval can overwrite an acknowledged write. A single client doing the steps in order reproduces this. | `HotEngine::get_document`, `update_document` |
| Ignored parameters | `if_seq_no`, `if_primary_term`, `version`, `version_type`, `op_type`, `routing`, `pipeline`, `wait_for_active_shards`, `timeout`, and `retry_on_conflict` are accepted and ignored. `refresh=wait_for` does nothing. `_update` and DELETE ignore `refresh` entirely. On index and bulk, `refresh=true` refreshes only copies on the coordinating node. | `src/api/index/mod.rs`, `RefreshParam` |
| Bulk parsing | `parse_bulk_ndjson` treats every action as `index` and reads lines in fixed pairs. See the list below. | `parse_bulk_ndjson` |
| Auto-generated IDs | The coordinator generates a UUID per request, so a client retry of `POST /{index}/_doc` creates a duplicate. | `index_document` |
| WAL and engine failures | An operation that reached the WAL and then failed engine apply has an unknown outcome; rebuild or restart replay can apply it on that copy alone. WAL append and fsync errors count toward Apply escalation but are not distinguished in the response, and `append` does not truncate a partial frame. | `HotTranslog::append`, `docs/recovery-protocol.md` |
| Checkpoints and history | A global checkpoint exists only in memory: the minimum of the highest applied `seq_no` on the primary and its replicas, used to bound WAL truncation. Local checkpoints use `fetch_max`, so they ignore gaps, and they are neither persisted nor sent to replicas. WAL entries carry `seq_no`, `op`, and `payload` but no primary term, and there is no resync on promotion. The persisted replay boundary is the highest committed `seq_no` + 1, so after out-of-order apply a restart skips lower operations that arrived after a commit, or fails with an untyped "did not reach WAL head" error that leaves the replica unopenable and never escalates. | `advance_global_checkpoint`, `TranslogEntry`, `for_each_from` |
| Durability scope | `translog_durability` is a node setting, so copies of one shard can run different modes. | `src/config/mod.rs` |

The bulk parser misreads requests in these ways:

- **Delete:** it consumes the next action line as its document and overwrites
  the target with it.
- **Update:** it stores the whole `{"doc": ...}` wrapper as the document.
- **Unparsable source line:** it drops the item with no response entry, so
  later response positions shift.
- **Unparsable action line:** it is not detected. The pair is indexed under the
  body's `_id` or a new UUID, or rejected for a missing `_index` on `/_bulk`.
- **Trailing action without a source line:** it is dropped.
- **`/{index}/_bulk`:** it ignores `_index` on action lines.

### 1.2 Precedent

OpenSearch and Elasticsearch use the same primary-backup model with an in-sync
allocation set and primary terms. The notes below are from OpenSearch 3.8.0
source and the Elasticsearch 7.10 reference, the fork point.

- **Replica failure:** the primary asks the cluster manager to remove the failed
  copy from the in-sync set, conditioned on the primary term. It acknowledges
  the write only after the removal is applied, and reports the failure in
  `_shards.failed` with a 2xx status
  ([data replication model](https://github.com/elastic/elasticsearch/blob/v7.10.2/docs/reference/docs/data-replication.asciidoc#L68-L76),
  [ReplicationOperation](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/action/support/replication/ReplicationOperation.java#L264-L310)).
  Transient replica errors are retried with backoff for up to 60 s
  ([ES #55633](https://github.com/elastic/elasticsearch/pull/55633), 7.9).
- **Replica apply:** only the primary checks conflicts. Replicas apply by
  `seq_no`, skip stale operations, and treat redelivery as idempotent
  ([IndexingStrategyPlanner](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/engine/IndexingStrategyPlanner.java#L161-L203)).
- **Stale primaries:** a replica rejects operations whose term is too old. A
  stale primary's shard-failed request is refused, so it fails itself, and the
  coordinator retries on the new primary
  ([ShardStateAction](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/cluster/action/shard/ShardStateAction.java#L469-L500)).
- **Durability:** under `request`, the translog is fsynced on the primary and
  every replica before the acknowledgement; `async` can lose acknowledged
  writes. Durability is a dynamic per-index setting
  ([translog](https://github.com/elastic/elasticsearch/blob/v7.10.2/docs/reference/index-modules/translog.asciidoc#L25-L63)).
  The write action syncs its operation's physical translog location, not a
  contiguous logical sequence prefix
  ([TransportWriteAction](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/action/support/replication/TransportWriteAction.java#L442-L513)).
- **Sequence numbers:** each operation is identified by (term, seq_no). The
  global checkpoint is the minimum persisted local checkpoint over in-sync
  copies. On promotion, the new primary fills gaps with no-ops and resyncs
  operations above the global checkpoint. Replicas trim older-term operations
  above it, or reset their engine to it
  ([sequence IDs blog](https://www.elastic.co/blog/elasticsearch-sequence-ids-6-0),
  [IndexShard promotion](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/shard/IndexShard.java#L866-L971)).
- **Realtime GET and update:** a per-shard LiveVersionMap records version,
  seq_no, term, and delete state for documents changed since the last refresh.
  - A delete tombstone is pruned only when it is older than `index.gc_deletes`
    (60 s) and at or below the processed checkpoint, so an out-of-order older
    write cannot resurrect the document
    ([InternalEngine](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/engine/InternalEngine.java#L1620-L1646)).
  - Auto-ID append-only writes skip the map while it is marked unsafe.
  - Translog locations are recorded only after a realtime GET turns on location
    tracking. Until then, a realtime GET refreshes an internal reader that
    searches do not see.

  Update runs on the primary and conditions its write on the seq_no and term it
  read
  ([InternalEngine.get](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/engine/InternalEngine.java#L622-L705),
  [UpdateHelper](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/action/update/UpdateHelper.java#L216-L260)).
- **Concurrency control:** `if_seq_no` and `if_primary_term` are checked on the
  primary. A mismatch returns 409 `version_conflict_engine_exception`, per item
  in bulk. Concurrency control by internal version was removed in
  Elasticsearch 7.0
  ([DocWriteRequest](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/action/DocWriteRequest.java#L302-L335)).
- **Retries:** client retries are at-least-once, and a resent auto-ID request
  creates a duplicate. A coordinator retry can report a conflict for a write
  that was applied; this is still open as
  [ES #9967](https://github.com/elastic/elasticsearch/issues/9967). Common
  clients resend automatically on 502, 503, and 504: the OpenSearch Java REST
  client
  ([`RestClient.isRetryStatus`](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/client/rest/src/main/java/org/opensearch/client/RestClient.java#L771-L779))
  and opensearch-py (`retry_on_status` defaults to `(502, 503, 504)`).
- **Engine failures:** a document failure on a replica is tragic and fails that
  copy. A tragic failure on the primary fails the shard and triggers promotion
  ([InternalEngine](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/engine/InternalEngine.java#L1021-L1079)).
- **Bulk:** a malformed action or metadata line rejects the whole request
  ([BulkRequestParser](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/action/bulk/BulkRequestParser.java#L160-L193)).
- **`wait_for_active_shards`:** a pre-flight count of active copies, default 1.
  It is not a durability guarantee
  ([index API](https://github.com/elastic/elasticsearch/blob/v7.10.2/docs/reference/docs/index_.asciidoc#L356-L408)).

## 2. Decision

### D1. Operation identity and replica apply

**Status:** Accepted and implemented for `local_shards` (FS-012). The
implemented behavior and its limits are in
[`recovery-protocol.md`](../recovery-protocol.md). Evidence: the
`d1_*_regression` integration suites in `tests/`, and bounded model checking in
[`specs/tla/README.md`](../../specs/tla/README.md#d1-sequence-aware-replica-apply).

Known limitations of the implementation:

- History convergence after failover (D10) is not implemented. A replica that
  misses an operation the primary wrote to its WAL is removed at the gap
  deadline and rebuilt by peer recovery.
- Pending promotion NoOp retries live only in process memory. A restart loses
  them, and the replica falls back to the gap deadline.
- A collision marker that could not be persisted is lost on restart.
- Every primary activation rebuilds the vector index.
- D2, D5, and D14 remain proposed.
- The model-checking evidence is bounded, not a proof.

- **Identity:** each write is identified by
  `(index UUID, shard, primary term, seq_no)`. The primary assigns `seq_no` at
  WAL append, under the term it validated at admission; that term is
  authoritative for the whole operation. Replicas never assign sequence
  numbers. WAL entries persist the primary term.
- **Replica apply:** replicas and replay apply operations by per-document
  `seq_no`, not by arrival order.
  - An operation not newer than the document's current `seq_no` is recorded
    but not applied.
  - Delete tombstones are retained until they are older than the retention
    window and at or below the processed checkpoint, so a late index cannot
    resurrect a deleted document.
  - A redelivered `seq_no` is acknowledged without being applied again.
    Detecting redelivery above a gap uses the set of processed sequence
    numbers above the checkpoint, not the checkpoint alone.
  - Local checkpoints are gap-aware. The replay start point, WAL truncation,
    and redelivery detection all use the processed checkpoint and the set of
    processed sequence numbers above it, not the highest `seq_no` seen.

  With this, a copy's final state is the same for any delivery order.

### D2. What an acknowledgement means

**Status (2026-10-05):** Selected design target; not implemented. Rust still
fails a request when any required synchronous replica fails, including after
the primary has mutated. This revision reconciles D2 with D1's implemented
operation-based acknowledgement; it does not change that runtime policy.

A 2xx write response means all of the following hold:

1. The primary validated its activation, exact allocation, and term before the
   WAL append.
2. The operation is durable on the primary under the index's durability mode.
3. Every copy in the in-sync set captured for the operation has either applied
   it durably or been removed from the in-sync set by a committed Raft command
   before the response. The in-sync set is captured under the shared write
   guard that peer-recovery admission takes exclusively. The command is
   conditional on the copy's exact allocation ID and the operation's primary
   term.
4. The remaining eligible copies satisfy `minimum_durable_copies`. Its future
   default is **1**, preserving the single-node profile; **2** is an explicit
   redundancy requirement. The primary counts as one copy. This is a lower
   bound, not permission to ignore another in-sync allocation. Reject values
   greater than the configured primary-plus-replica capacity.
5. The primary's permit has not been locally revoked, and no unresolved
   exclusion in that authority epoch remains acknowledgement-blocking.

**Durability is operation-based, not prefix-gated.** Each remaining required
copy must prove that this exact term/sequence operation is applied and durable.
An exclusive durable prefix above the sequence can supply that proof only when
bound to the same history and allocation. A lower prefix cannot disprove
durability of an individually verified operation above a gap. Do not wait for
an unrelated earlier position merely to acknowledge this operation.
Contiguous local/global prefixes still govern WAL retention, safe truncation,
recovery boundaries, and prefix reconstruction. They must never jump a gap.
This preserves the implemented D1 rule and the existing above-gap witnesses.

With the default of one, losing the last remaining durable copy can lose data;
metadata quorum does not replace document copies. With two, writes remain
unavailable until enough eligible copies exist. `request` durability is the
scope of these guarantees; asynchronous fsync retains D3's weaker contract.

A replica failure need not fail the original request when observed committed
exclusion leaves enough eligible durable copies and every condition above holds.

- **Retries before removal:** the primary may retry transient replica errors
  within the request deadline using the same D1 operation identity. An internal
  retry never allocates another sequence; this is not durable external-client
  deduplication, which remains FS-011.
- **Reporting:** `_shards.total`, `_shards.successful`, and `_shards.failed`
  report what happened.
- **Removal that cannot commit:** if the removal cannot commit before the
  deadline, for example because there is no Raft leader, the response is
  indeterminate (D4).
- **Pending removals stick:** a pending removal stays pending after an
  indeterminate response. It is bound to the index UUID, primary allocation,
  primary term, failed allocation, and transition identity. Close new write
  admission in that authority epoch and block acknowledgement of already
  admitted later requests until the exclusion is definitively settled.
  A proposal, transport timeout, or stale view is not a committed removal.
  Observe the accepted committed result or the applied removal. A definitive
  stale-authority rejection revokes the old local permit instead of authorizing
  continued writes. Reconstruct unresolved state before activation after a
  process restart; that cross-restart work remains a runtime prerequisite.
- **A deadline is not rollback:** if the primary WAL may have changed, report
  D4's indeterminate outcome. A failed request may appear later through replay
  or recovery. An observed accepted exclusion can allow the original request
  to succeed only before its response budget expires and only if every other
  condition above holds. An already returned indeterminate result is not
  retroactively converted to success.

Implementing D2 would replace the global rule that synchronous replication
failures are request failures and amend items 11 and 19 of the "Required Rust
contract" in `specs/tla/README.md`; item 19's bounded transport timeout stays.
The [separate D2 model](../../specs/tla/README.md#proposed-write-acknowledgement-and-failure-contract)
checks the selected target. The current shared model and Rust contract remain
unchanged until FS-013 implements it.

### D3. Durability modes

- **`request` (default):** every acknowledged write is fsynced in the WAL on the
  primary and on every copy that remains in sync.
- **`async`:** an acknowledged write can be lost if every copy crashes within the
  sync interval. Documentation must say that `async` does not meet invariant
  6.1.1.
- **Scope:** durability becomes an index setting that applies to every copy of a
  shard.

### D4. Outcome classes

**Status (2026-10-06):** Implemented for `local_shards` primary mutation,
forwarding, and single/bulk failure responses. Typed transport details distinguish
pre-WAL rejection, proven non-execution, and an indeterminate mutation. Preserve
underlying causes and operation-owned term/sequence metadata when known; never
infer a receipt from a later shared checkpoint. Metadata auto-creation and
update preflight GET failures retain their separate error contracts.

The response classification does not implement D2's exclusion/ACK refinements,
D6 retry identity, or D14 fail-stop behavior. Required synchronous replica
failures still fail the request after the primary may have applied it.

Every write response is in exactly one class:

| Class | Status | Durable effect | Safe to retry |
|---|---|---|---|
| Acknowledged | 200, 201 | Durable as defined by D2 and D3 | Only if idempotent (D6) |
| Acknowledged no-op | 404 `not_found` (delete) | Executed; consumes a `seq_no`; the document is unchanged | Yes |
| Rejected | Every 4xx except delete's `not_found`, plus 501 for read-only engines. Examples: 400, 401, 403, 404 `document_missing_exception` or `index_not_found_exception`, 409, 429 | None | Yes, after fixing the request; for 429, after backing off. A 409 on a conditional retry after an indeterminate attempt means the first attempt possibly applied. |
| Not executed | 503 `shard_not_available_exception` or `master_not_discovered_exception` | None; failed before the WAL append | Yes |
| Indeterminate | 500 `write_outcome_unknown` | Unknown; it can appear later through replay, recovery, or promotion | Only if idempotent (D6) |

- **What is indeterminate:** a failure at or after the primary's WAL append is
  indeterminate unless the protocol resolves it to an acknowledgement through
  D2. A replica failure followed by committed removal is therefore
  acknowledged; a primary WAL or engine failure, or a replica removal that
  cannot commit, is indeterminate. WAL append and fsync errors can leave a
  partial frame and are always indeterminate.
- **Response contents:** an indeterminate response includes the primary term
  and `seq_no` when known. WAL append/fsync failures without a returned receipt
  omit the sequence, even if another operation advanced the shared checkpoint.
  Failed homogeneous batches preserve the returned range and assign item
  receipts by request-order offset, including duplicate IDs. Validate the range
  against the actual submitted count, not a count inferred from that range.
- **Transport ambiguity:** a failed connection before dispatch proves
  non-execution. Once dispatched, an unmarked timeout, disconnect, `UNAVAILABLE`,
  or `ABORTED` does not. Only marked, validated pre-mutation details can prove
  non-execution. Missing, malformed, or contradictory failure details produce a
  diagnostic indeterminate error without a fabricated receipt.
- **Retries:** the coordinator does not retry an indeterminate mutation.
  Existing bounded update retries apply only to rejected CAS conflicts.
  Known sequence/term metadata is not a deduplication token.
- **Why 500:** clients that resend automatically on 503 must never do so for
  an indeterminate write.

Supported single-document and bulk outcomes map to those classes as follows:

| API outcome | HTTP/result | Outcome class |
|---|---|---|
| Index creates a document | 201 `created` | Acknowledged |
| Index replaces a document | 200 `updated` | Acknowledged |
| Create succeeds | 201 `created` | Acknowledged |
| Create finds an existing document | 409 `version_conflict_engine_exception` | Rejected |
| Delete removes a document | 200 `deleted` | Acknowledged |
| Delete finds no document | 404 `not_found` | Acknowledged no-op |
| Update mutates a document | 200 `updated` | Acknowledged |
| Update finds no document | 404 `document_missing_exception` | Rejected |
| Conditional write loses a race | 409 `version_conflict_engine_exception` | Rejected |
| Primary admission fails before WAL append | 503 shard/master error | Not executed |
| Execution reaches an unresolved post-admission failure | 500 `write_outcome_unknown` | Indeterminate |
| Well-formed bulk request | 200 outer response; each item uses the corresponding row above | Per-item class |
| Malformed bulk action/metadata line | 400 `illegal_argument_exception` | Rejected before any item executes |

### D5. Stale primaries and missing leaders

**Status (2026-10-05):** Existing replica fences are implemented; local
self-fencing after a rejected old-primary exclusion remains a selected target.

- **Existing fences stay:** primary validation at admission; replica checks of
  UUID, allocation, and term against the durable fence; and conditional
  promotion.
- **Removal is term-conditioned:** the D2 removal command is conditional on the
  operation's term, so a stale primary's removal is rejected. The stale primary
  then answers indeterminate and stops accepting writes for that allocation.
- **No Raft leader:** a primary that sees no leader may still acknowledge a
  write, but only when every required in-sync allocation applied it durably,
  the minimum-copy floor holds, and no unresolved exclusion or revoked permit
  exists. Data copies are not a metadata quorum. D2 exclusion needs a
  committed Raft decision; otherwise the affected post-mutation response is
  indeterminate.
- **Late responses:** delivery of a success already established for a retained
  canonical operation can occur after promotion. It is not permission to
  admit another old-term mutation or to acknowledge from an obsolete dedup
  table without fresh authority/copy proof.

### D6. Retry semantics

Until FS-011 adds operation IDs, client retries are at-least-once:

| Operation | Retry behavior |
|---|---|
| Index with an explicit `_id` | Converges to the same content with a new `seq_no`. |
| Index with an auto-generated ID | Not retry-safe: a client resend creates a duplicate. Clients that retry must supply `_id`. |
| Create (`op_type=create`, once implemented) | After an indeterminate attempt, a retry can return 409. Treat that 409 as possibly applied. |
| Delete | Returns 404 `not_found` if the first attempt applied. |
| Update | Retry-safe only with `if_seq_no` and `if_primary_term`. |

The auto-ID case is a declared deviation from roadmap invariant 6.1.2 until
FS-011 lands. Coordinator retries are allowed only for failures classified
"not executed"; the coordinator never resends an indeterminate operation.
Clients also resend on connection errors, so these rules apply to those
resends too.

### D7. Response metadata

- **Success fields:** `_seq_no` and `_primary_term` are real on every success,
  single or bulk.
- **`_version`:** omitted until per-document versions exist (FS-009). It is never
  fabricated.
- **`result`:** index returns `created` with 201 or `updated` with 200, according
  to whether the document existed. Delete returns `deleted`, or `not_found`
  with 404.
- **`_shards`:** reported per response and per bulk item.

### D8. Visibility

**Status (2026-10-02):** Implemented for `local_shards`: search remains
near-real-time, GET by ID is realtime by default, and `realtime=false` uses
the search-visible reader. Explicit `refresh=true` (also an empty value or
bare `?refresh`) covers the primary and every Raft-authoritative in-sync copy,
including writes coordinated by shardless nodes or replicas and writes whose
primary is a follower. Bulk performs one refresh round per touched shard,
not per item or update attempt. Bounded refresh failures are reported without
changing a returned acknowledged write status or receipt.
Remaining: sequence-covering `refresh=wait_for`, which is still rejected with
400 rather than emulated with a forced refresh.

Single-document index/create/update/delete requests carry refresh intent to
the primary. After synchronous replication acknowledges, the primary refreshes
itself and fans out `RefreshShardCopy` concurrently to the same captured
acknowledgement set, retaining the shared recovery write guard.
REST bulk runs every mutation unit with refresh off, including ordered runs
and update CAS attempts. After all units for a shard finish, one fenced
`RefreshShardWrites` call asks the primary to activate, take the shared guard,
and capture its **current** authoritative in-sync set. Activation precedes the
shared guard because it may need the exclusive barrier. One report is attached
to every acknowledged item on that shard, including a no-op alongside actual
mutations; failed items retain their errors. Direct homogeneous and ordered
bulk RPCs also refresh at most once. Empty, all-error, and no-op-only bulks,
and detected single-update no-ops, add no post-write refresh.

Targets validate the exact UUID, allocation, primary, term, durable fence,
recovery gate, and served engine before and after blocking reader publication.
Missing or replaced copies fail refresh; this path never creates or reopens
shard storage. Coordinators do not choose replica targets from their own view.

Copy refresh waits are capped at five seconds and the enclosing gRPC request's
remaining deadline, reserving 250 ms for reply (or one quarter of a smaller
remaining duration). The deadline is captured before metadata waits and data
work; copy RPCs carry their budget explicitly. Expiry produces a per-copy
refresh failure and releases the primary's shared guard. Blocking maintenance
already running can finish later; timeout does not cancel disk work. The bulk
phase also bounds activation and guard acquisition below its caller deadline.
If the phase cannot return its authoritative set, the coordinator reports a
known-primary phase failure instead of inventing replica results. Data-phase
timeouts and transport disconnects can still leave the write outcome
indeterminate (D4); this is not a general request-deadline or retry protocol.

The primary-owned, once-per-shard bulk round follows
[OpenSearch's shard bulk action](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/bulk/TransportShardBulkAction.java).
FerrisSearch uses separate replica maintenance RPCs rather than embedding
refresh in replica-apply acknowledgements. Two deliberate failure-policy
deviations from
[OpenSearch's replication operation](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/support/replication/ReplicationOperation.java)
are part of D8: OpenSearch fails the whole request when primary post-write
refresh fails (`finishAsFailed`), and treats replica refresh failure as replica
failure (`failShardIfNeeded`). FerrisSearch instead preserves the already
acknowledged mutation and reports visibility failure, without removing a copy
solely for refresh failure. Reader publication is separate from the successful
data acknowledgement; turning its failure into a failed write invites retries
of an operation already applied, including create conflicts. Actual mutation
or synchronous replication failures remain write failures.

For a completed explicit refresh phase, `_shards.total` counts the primary
plus its captured in-sync replicas; `successful` and `failed` describe refresh,
not replication outcomes. Each failed copy has a `failures` entry with index,
shard, node, allocation ID, primary role, and the underlying reason. An
invariant failure with no allocation ID reports the cause and omits that ID,
never fabricating one. `forced_refresh` is emitted only as `true` when the
primary publishes successfully, even if a replica refresh fails; it is omitted
otherwise, as in
[OpenSearch's document response](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/DocWriteResponse.java).
Bulk refresh failures retain item status,
result, sequence, and term and do not set `errors`; that field still depends
only on item error objects. No refresh parameter or `refresh=false` adds no
refresh RPCs or engine refreshes.

HotEngine uses manual reader publication, not Tantivy's background commit
watcher. A commit alone does not make documents searchable. FerrisSearch keeps
its existing explicit publication during flush, force merge, and recovery
replay; in particular, flush publishes a covering reader before pruning WAL
history. This differs from OpenSearch's separation of flush and refresh.
An ordinary peer-recovery snapshot commit does not refresh search visibility.
When snapshot creation must repair stale vectors, the composite engine rebuilds
from a covering commit through a private manual reader without publishing it.
Protocol-trace snapshot capture also uses an unpublished reader.

- **Search:** an acknowledgement does not imply search visibility. Search sees
  a write after the next refresh.
- **`refresh=true`:** attempts bounded refresh of the primary and every in-sync
  copy before the response; inspect `_shards.failed` for incomplete visibility.
- **`refresh=wait_for`:** proposed: return once a refresh covers the operation's
  `seq_no`. Currently rejected with 400.
- **Refresh failure:** a refresh that fails after an acknowledged write keeps
  the acknowledged status. It is reported as a shard failure entry in the
  response, not as a failed write.
- **GET by ID:** realtime by default (D9). `realtime=false` reads the
  search-visible reader.

### D9. Realtime reads and update

**Status (2026-10-01):** Implemented for `local_shards`. Primary, replica,
replay, and recovery index applies retain physical WAL cursors in the existing
live version map. Realtime GET checks map completeness and the document entry
under one short version-map read lock, separate from `ApplyState`. A
complete-map miss releases that lock and reads from the reader without taking
the apply-state or translog mutex, acquiring the searcher after the map lookup.
A complete-map tombstone returns not found without either mutex. Index hits still hold the
translog mutex while resolving the source. Flush publishes its reader before
truncating under that mutex; a missing cursor can therefore fall back to a
reader covering the live version. A behind or missing fallback fails
explicitly, rather than returning stale data. Replay carries cursors from the
streaming decoder without rescanning the WAL per document.

Every explicit reader reload uses one shard-local mutex that covers segment
opening and searcher publication. The helper does not acquire another engine
lock, and callers do not hold the version-map lock while reloading. Reader
publication is monotonic: a complete-map miss cannot obtain a reader older
than the reload that authorized clearing old versions or pruning a tombstone.
Search and GET do not take the reload mutex. Previously borrowed searchers
keep their pinned snapshot.

D1 planning, term state, checkpoints, and planning-snapshot restore still hold
the apply-state mutex for the whole batch. Writers take the version-map lock
briefly for lookup or mutation, with apply state before the map whenever both
are needed. They never hold the map lock through WAL I/O, fsync, Tantivy apply,
or reader lookup. A realtime GET can observe a map entry for an operation
already in the WAL but still in flight; its index hit waits for the translog
mutex before resolving source. A mid-batch delete publishes its tombstone
before the delete is acknowledged. The fast path can return that tombstone
immediately, without waiting for the batch or acknowledgement. This is the same
class of read as an in-flight index hit, not an acknowledgement-only snapshot.
The separate lock removes the bulk apply-state
wait from map misses and tombstones, not all possible map-lock contention.

Replay clears completeness with the map reset and restores it only after the
successful final reader reload, or after a successful empty replay. A realtime
GET that observes an incomplete map waits for the translog mutex. If the map
remains incomplete, GET returns `503 shard_not_available_exception` with the
apply or replay failure cause. Post-WAL apply failure also marks the map
incomplete. Single and bulk update propagate that failure instead
of merging stale source or treating the document as missing. Search and
`realtime=false` keep their reader-only semantics. Conditional writes, create,
and upsert still complete replay before checking the document version.

**Index-incarnation status (2026-09-30):** GET also returns `_index_uuid`,
including when the document is missing. Coordinator update pins the serving
primary's UUID across its GET/CAS cycle and every conflict retry. The primary
rejects a missing or replaced incarnation before sequence assignment or WAL
append, including requests delayed by the recovery barrier or write pool.

**Forwarding metadata status (2026-10-01):** Coordinators carry their applied
cluster-state version as a routing hint to shard targets. Targets validate local
metadata first. They wait up to five seconds only when the hint is ahead and the
index, UUID, shard routing, allocation ID, or primary term fails validation.
Explicit metadata-acknowledgement floors are per index and fence subsequent
operations from the acknowledging coordinator; unrelated index changes do not
stall valid reads. A settings or mapping change acknowledged through another
coordinator is not fenced, so a lagging target can still serve requests with the
older settings or mappings.
A metadata-wait deadline
returns `503 shard_not_available_exception` with required and observed versions,
before document sequence assignment or WAL append. Bulk preserves that error
per item; update does not continue from a failed realtime GET. Search, count,
and SQL propagate the same deadline rather than returning incomplete results.
Generic transport timeouts and connection loss remain potentially indeterminate
and are not converted to retryable document 503s.

Once creation commits, it remains acknowledged even if primary opening fails.
Primary copies open concurrently under existing UUID/allocation guards, with
at most four concurrent opens per node and a separate 20-second budget.
Remote metadata catch-up can add up to five seconds. Readiness failure returns
`shards_acknowledged: false` and logs the cause. Follower coordinator catch-up
failure also preserves the committed acknowledgement. This is primary-only
readiness, not a replica wait, an all-node state acknowledgement, or a new
activation/fencing protocol. Generic transport failures still have an
indeterminate commit outcome.

Remaining: expose the existing internal version-map memory bound as an
operator setting and add `_mget`. Neither is part of this implementation.

- **Live version map:** each shard keeps a map from `doc_id` to
  `(seq_no, term, deleted, WAL position)` for every operation since the last
  refresh.
  - A refresh removes an entry only when the entry is at or below the refresh's
    commit boundary, and only after the new reader is visible. As in
    OpenSearch, a current map and an old map cover the refresh window.
    When old is empty, rotation swaps the maps without cloning keys. Reader
    publication precedes taking the old map; destruction of its entries runs
    after releasing the version-map write lock. Cached window byte totals keep
    rotation and retirement accounting constant-time.
  - Delete tombstones stay until they are older than the retention window and
    at or below the processed checkpoint, because Tantivy has no soft
    deletes that record a delete's `seq_no`.
  - A map that grows past a configured limit forces a refresh.
- **Realtime GET:** checks the map first. A tombstone returns not found, and a
  changed document is read from its WAL entry, which stores the full `_source`.
  WAL truncation cannot remove that entry before a refresh covering the map
  entry is visible. If the map points to an absent or unreadable WAL entry,
  realtime GET and update fail explicitly instead of falling back to a stale
  refreshed reader.
- **`_update`:** the coordinating node performs a realtime GET from the
  primary, recursively merges `doc`, and sends a primary conditional index
  write using the index UUID, `seq_no`, and term it read. The primary compares and appends
  atomically. A version conflict repeats the GET/merge/write cycle up to
  `retry_on_conflict` times (default 0); other errors are not retried.
  Missing documents use a create-only write from `upsert` or, with
  `doc_as_upsert: true`, from `doc`. Without either, the result is
  `404 document_missing_exception`. `detect_noop` defaults to true; equal
  merged content returns `noop` without allocating a sequence or appending.
  Only `doc`, `upsert`, `doc_as_upsert`, and `detect_noop` are accepted body
  keys. Other keys return `400 illegal_argument_exception` naming the key.
- **Index disappearance:** a pinned UUID that no longer names the index
  returns `404 index_not_found_exception`, not a version conflict against the
  replacement. OpenSearch's
  [IndexNotFoundException](https://github.com/opensearch-project/OpenSearch/blob/3.3.0/server/src/main/java/org/opensearch/index/IndexNotFoundException.java)
  extends
  [ResourceNotFoundException](https://github.com/opensearch-project/OpenSearch/blob/3.3.0/server/src/main/java/org/opensearch/ResourceNotFoundException.java),
  which returns HTTP 404. FerrisSearch uses gRPC `NOT_FOUND` internally and
  preserves that error at the HTTP boundary. A UUID rejection has no write
  receipt and is not a copy-I/O failure. The optional UUID is an internal
  primary-write precondition; no new REST query parameter is introduced.
- **Replicas:** they need the same per-document `seq_no` information, from the
  map or a stored `_seq_no`, to apply D1.

### D10. History convergence after failover

In-sync copies must not keep divergent operations after a failover:

1. **Persisted checkpoints:** replace the in-memory global checkpoint with
   gap-aware local checkpoints that are persisted and reported to the primary.
   The primary computes the global checkpoint from them and sends it to
   replicas, which persist it.
2. **No-op entries:** the WAL gains a no-op entry type, used to fill a gap when
   a `seq_no` was assigned but its operation cannot be completed.
3. **Promotion resync:** on promotion, and on activation after a primary
   restart, the primary fills gaps and resyncs operations above the global
   checkpoint. The restart case matters because a sticky pending removal (D2)
   lives only in the old process.
4. **Replica reconciliation:** compare older-term suffixes with the promoted
   canonical history. A lagging global checkpoint or an older term alone is
   not permission to discard an acknowledged above-gap operation. Preserve
   protected history and remove a conflicting copy from authoritative
   eligibility before repair. Trimming the WAL cannot undo operations Tantivy
   already committed, so repair either replays from a verified safe commit or
   re-recovers the copy through peer recovery. The
   [promotion protocol](../recovery-protocol.md#promotion-protocol) owns this
   unimplemented reconciliation.

### D11. Concurrency control

**Status (2026-09-30):** Implemented for `local_shards`: paired
`if_seq_no`/`if_primary_term` on index, delete, and update; create-only index
with `op_type=create` or `PUT`/`POST /{index}/_create/{id}`; and bulk
preconditions. A conflict consumes no sequence and appends no WAL entry.
The primary checks under the same translog mutex as assignment, using the live
map or the reader's `_seq_no`/`_primary_term` fast fields. Replicas apply only
the resulting sequenced mutation, never the condition.
Coordinator update also carries D9's index-incarnation precondition.

Remaining: external versioning and client operation IDs remain unimplemented.

- **Conditional writes:** `if_seq_no` and `if_primary_term` are checked only on
  the primary, against the live version map or the committed index. A mismatch
  returns 409 `version_conflict_engine_exception`, and in bulk it is a per-item
  error.
- **Versions:** concurrency control by internal `version` is rejected. External
  versions are deferred.
- **Create:** `op_type=create` fails with 409 when the document exists.

### D12. Bulk semantics

**Status (2026-09-30):** Implemented: strict action/metadata parsing, source
line boundaries, all four actions, ordered per-shard execution, and per-item
outcomes. `_index` overrides on index-scoped bulk requests are honored and
authorized. Updates use D9; consecutive other actions use one shard RPC.
Unconditional index-only runs retain the existing engine batch path; mixed
or conditional runs execute sequential single-write handlers. Explicit bulk
refresh is deferred until all shard units finish, with one primary-owned
round per touched shard (D8), rather than refreshing each item.

Remaining: FS-014's bounded streaming parser, backpressure, cancellation,
and resource accounting. Request parsing and shard grouping still materialize
the body. Failure classification and known failure receipts follow implemented
D4; D2's acknowledgement/exclusion refinements remain proposed.

- **Results:** items return in request order. `errors` is true only when an
  item has an `error` object; delete's `404 not_found` does not set it.
- **Malformed action lines:** a malformed or unknown action or metadata line
  rejects the whole request with 400, as in OpenSearch, because item boundaries
  are lost after it.
- **Source lines:** each action consumes the number of source lines its type
  defines. A malformed source line is a per-item error.
- **Actions:** `index`, `create`, `delete`, and `update` follow the
  single-document rules. An action line has exactly one supported action with
  object metadata. Delete consumes no source line; other actions require one.
  A missing source line rejects the whole request with a line-numbered 400.
- **Shard-group failures:** every item in the group receives that group's
  outcome class.
- **Metadata keys in sources:** a top-level source key that names a metadata
  field, such as `_id`, `_source`, `_seq_no`, or `_primary_term`, is rejected
  per item with 400 `mapper_parsing_exception`. This rule is implemented with
  D1.

### D13. Unimplemented parameters fail loudly

**Status (2026-10-01):** Implemented for document writes, both bulk routes,
and index creation. Validation runs before routing, auto-creation, or
mutation. Unsupported bulk action metadata rejects the whole request before
any item executes; its reason names the key, action-line number, and one-based
item position.

A write parameter that changes safety semantics and is not implemented returns
400 `illegal_argument_exception` naming the parameter:

- **Honored:** paired `if_seq_no`/`if_primary_term` on index, update, delete,
  and bulk index, update, and delete actions; `op_type=create` on index and
  the `_create` route; and `retry_on_conflict` on `_update` and bulk `update`
  actions.
- **Rejected:** `routing`, `_routing`, `pipeline`, `version`, `_version`,
  `version_type`, `_version_type`, `require_alias`, `require_data_stream`,
  and `dynamic_templates`. Reject conditions, `op_type`, and
  `retry_on_conflict` on endpoints or actions that do not implement them.
  `_create` accepts only absent or `create` for `op_type`; DELETE rejects
  any `op_type`.
- **Refresh:** document and bulk URL parameters accept `true`, the empty
  value (including bare `?refresh`), and `false`. Reject `wait_for` and
  every other value. Explicit refresh uses primary-owned, deadline-bounded
  all-copy rounds and acknowledgement-preserving visibility failure reporting
  as described in D8. Bulk pays one round per touched shard.
  `wait_for` still needs sequence-covering refresh
  listeners; do not emulate it with forced all-copy refresh.
  Reject per-action bulk `refresh`
  and index-creation `refresh`, which have no implementation.
- **Active copies:** accept `wait_for_active_shards` only when absent or
  exactly `1`; bulk metadata also accepts numeric `1`. Reject `all` even
  with zero replicas, higher counts, zero, empty, and invalid values.
  Index creation attempts bounded primary opening and reports
  `shards_acknowledged: false` if readiness fails after commit.
  Document and bulk writes
  keep their existing activation and synchronous in-sync replication paths
  without a replica-count pre-flight protocol. Implement additional active-copy
  checks before accepting other values.
- **Benign options:** keep accepting `timeout`, `pretty`, `human`,
  `error_trace`, and `filter_path`. GET parameter handling, including
  `_source` filtering options and `realtime`, is unchanged. Acceptance
  does not imply implementation of response filtering or deadlines;
  `timeout` becomes the request deadline in FS-029.

`validate_write_parameter` in `src/api/index/mod.rs` owns the explicit
rejection list. Query structs consume implemented parameters before this
helper checks the remaining keys; the bulk parser applies it to action
metadata while preserving implemented conditions and update retry counts.
Move each parameter out of rejection only when its implementation and
result-level regressions land. `_delete_by_query` and `_update_by_query`
remain unimplemented.

### D14. Engine and WAL failures on the write path

**Status (2026-10-05):** Selected target; not implemented by the current
bounded apply-I/O escalation policy. The model treats local permit invalidation
abstractly and does not prove byte-level WAL/fsync failure detection.

- **Primary engine failure:** a post-WAL engine failure makes the operation
  indeterminate and fails the copy. The copy is promote-only when an in-sync
  candidate exists, or marked unavailable otherwise.
- **Primary WAL failure:** a WAL append or fsync failure fails the copy, as an
  engine failure does. Verifying the tail afterward is not safe: Linux can
  report a writeback error once and mark the pages clean, so a read-back check
  can pass while the disk lacks the frame. OpenSearch closes the translog on
  any write or fsync failure
  ([TranslogWriter](https://github.com/opensearch-project/OpenSearch/blob/3.8.0/server/src/main/java/org/opensearch/index/translog/TranslogWriter.java#L548-L590)).
- **Replica failure:** any replica apply failure removes the copy through D2. The
  primary validates documents before the WAL append, so a replica-side
  validation failure means divergence, not a user error.
- **Local action precedes metadata settlement:** revoke the affected copy's
  write/durability-ack permit immediately. A missing metadata quorum cannot
  authorize another mutation or a retry that treats a later successful fsync
  as proof of the failed attempt. Reporting and removal/promotion still use
  conditional Raft commands, never follower-local metadata mutation.

## 3. Failure scenarios

| Scenario | Required outcome |
|---|---|
| Client timeout after the primary committed | The coordinator returns 500 `write_outcome_unknown`, with term and `seq_no` if known. A retry follows D6. |
| Primary crashes after the WAL append, before the response | Indeterminate. If a replica is promoted, D10 decides whether the operation survives on every copy. |
| Concurrent writes reach a replica out of order | The replica applies by per-document `seq_no` (D1), and its final state matches the primary's. |
| One replica fails to apply | It is committed out through D2; acknowledgement then requires all remaining copies and the configured minimum, with `_shards.failed = 1`. Below the minimum or without settled exclusion, the post-mutation result is indeterminate. It rejoins through peer recovery. |
| Replica removal cannot commit (no leader) | Indeterminate. The removal stays pending, and later acknowledgements wait for it (D2). |
| Stale primary after promotion | Replica applies are rejected by term. The stale primary answers indeterminate and fences itself, and the client retries on the new primary. |
| Duplicate client retry | Follows D6 for each operation type. Exactly-once needs FS-011. |
| Concurrent updates to one document | Each update is conditioned on its read. One wins; the other retries or returns 409. |
| Update immediately after an acknowledged write | The update reads the acknowledged write through the live version map. |
| Delete racing an update | Both run on the primary in `seq_no` order. An update of a deleted document returns 404, or 409 when conditioned. |
| Post-WAL engine or WAL failure | Indeterminate, and the copy fails (D14). |

## 4. Alternatives considered

- **Keep "replica failure fails the request".** Rejected. Writes fail for as long
  as a failed replica stays a cluster member, and those failures are
  indeterminate anyway, because the primary already applied the operation.
- **Order replica apply by holding the primary's WAL lock through replication,
  or through a per-replica in-order gate.** Rejected as the end state. A lock
  serializes all writes to a shard behind replica round trips. An in-order gate
  blocks at every `seq_no` gap, so it still needs D10's no-op entries. Either
  remains an option for an interim fix while D1 is built.
- **Quorum acknowledgement per write** (Raft-style log replication for
  documents). Rejected. It replaces the primary-backup and in-sync set design
  that PRs #143-#144 and the TLA+ model are built on. Document data stays out of
  the Raft log.
- **Exactly-once writes now.** Deferred to FS-011, which needs durable
  deduplication state.
- **Refresh before every `_update` instead of a live version map.** Rejected. It
  causes refresh storms under update-heavy load. Elasticsearch 6.3 moved update
  reads to the translog
  ([ES #29264](https://github.com/elastic/elasticsearch/pull/29264)), and 7.6
  did the same for realtime GET
  ([ES #48843](https://github.com/elastic/elasticsearch/pull/48843)).
- **Accept and ignore unsupported parameters.** Rejected. Clients believe checks
  run that do not.

## 5. Consequences

- The selected D2 target can restore writes after committed failed-copy
  exclusion and peer recovery, but only while the configured minimum holds.
  Unresolved exclusions remain unavailable. Transient faults can cause
  recovery churn; the in-request retry budget bounds an attempt, not repeated
  failed allocations. A separate allocation retry limit remains deferred.
- The target write path waits for Raft only when eligibility must change; an
  all-copy durable response without pending exclusion needs no new metadata
  commit. Current Rust retains its fail-request behavior.
- Clients get distinct classes for "not executed" and "indeterminate", and real
  metadata for concurrency control.
- The live version map adds memory proportional to writes per refresh interval,
  plus tombstones for the retention window.
- Rejecting unimplemented parameters can break clients that send them today.
  Those clients were not getting the behavior they asked for.

## 6. Migration impact

FerrisSearch is pre-1.0. It provides no migration path and no backward
compatibility for on-disk, WAL, Raft-log, or wire formats, and it does not
support mixed-version clusters. D1 changes the WAL entry format, the Tantivy
schema, and the durable per-copy sequence state. D10 may change them again.

- A node that finds index data in an older format fails closed. The typed
  error names the component and tells the operator to recreate the index.
- A node that finds a Raft log or snapshot in an older format fails closed. The
  error tells the operator to wipe the node data directories and recreate the
  cluster.

Response-field changes follow the compatibility matrix (FS-006).

## 7. Affected gates and tasks

| Decision | Implementing tasks |
|---|---|
| D1 | FS-012 (replica apply order, a release blocker) and FS-009 |
| D2, D5, D14 | FS-013 |
| D3 | FS-013, FS-026 |
| D4, D6 | FS-009, FS-011, FS-029 |
| D7 | FS-009 |
| D8 | FS-003 |
| D9, D11 | FS-010 |
| D10 | FS-013, FS-024, FS-025 |
| D12 | FS-014, including its interim mitigation |
| D13 | FS-009 for single-document parameters, FS-014 for bulk metadata |

Gate 0 needs this ADR accepted. Two Gate 1 exit criteria depend on D1, D2, D5,
and D10: "no acknowledged operation is lost in the supported failover matrix"
and "stale primaries and stale publishers are fenced". The stale-publisher half
also depends on FS-002.

## 8. Evidence that would invalidate this decision

- Replica fail-out under realistic transient faults causes enough recovery churn
  to outweigh the availability gain. That would argue for a longer in-request
  retry budget, or for operation-based recovery before D2.
- Waiting for the Raft commit that removes a replica exceeds the write latency
  target. That would argue for batching removal commits.
- A TLA+ model of D1, D2, D5, and D10 finds an acknowledged-loss trace. The D1
  model allows up to three overlapping client writes. D2, D5, and D10 need
  model coverage of concurrent writes before they are implemented.
- Live-version-map memory cannot be bounded under a supported workload without
  refreshes that break the latency target.
