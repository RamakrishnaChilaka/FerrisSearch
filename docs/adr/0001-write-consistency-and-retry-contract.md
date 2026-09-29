# ADR 0001: Write Consistency And Retry Contract

- **Status:** Proposed
- **Date:** 2026-09-27
- **Backlog:** [FS-001](../next-50-tasks.md#fs-001--decide-the-write-consistency-and-retry-contract)
- **Roadmap:** Gate 0 deliverable "accepted write acknowledgement, retry, OCC,
  and version semantics"; invariants 6.1.1-6.1.6 in
  [`architecture-roadmap.md`](../architecture-roadmap.md#61-write-and-version-invariants)
- **Scope:** `local_shards` document writes (index, create, update, delete, and
  bulk), and the reads writes depend on (GET by ID and refresh). Remote-store
  publication is FS-002.

This record is proposed. It defines the intended contract. Section 1.1
describes the code at 6dbf9dc; everything else is future behavior until the
implementing tasks land.

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

A failed replica is therefore removed, as in OpenSearch, instead of failing the
client request.

- **Retries before removal:** the primary may retry transient replica errors
  within the request deadline, but only after D1 makes replica apply idempotent
  and order-independent.
- **Reporting:** `_shards.total`, `_shards.successful`, and `_shards.failed`
  report what happened.
- **Removal that cannot commit:** if the removal cannot commit before the
  deadline, for example because there is no Raft leader, the response is
  indeterminate (D4).
- **Pending removals stick:** a pending removal stays pending after an
  indeterminate response. No later acknowledgement on that shard may complete
  until the removal commits, so a copy can never stay in sync with a hole.

This replaces the global rule that synchronous replication failures are request
failures. It also amends items 11 and 19 of the "Required Rust contract"
section in `specs/tla/README.md`; item 19's bounded transport timeout stays.
The model's acknowledgement rule must change before FS-013 implements D2.

### D3. Durability modes

- **`request` (default):** every acknowledged write is fsynced in the WAL on the
  primary and on every copy that remains in sync.
- **`async`:** an acknowledged write can be lost if every copy crashes within the
  sync interval. Documentation must say that `async` does not meet invariant
  6.1.1.
- **Scope:** durability becomes an index setting that applies to every copy of a
  shard.

### D4. Outcome classes

Every write response is in exactly one class:

| Class | Status | Durable effect | Safe to retry |
|---|---|---|---|
| Acknowledged | 200, 201 | Durable as defined by D2 and D3 | Only if idempotent (D6) |
| Acknowledged no-op | 404 `not_found` (delete) | Executed; consumes a `seq_no`; the document is unchanged | Yes |
| Rejected | Every 4xx except delete's `not_found`, plus 501 for read-only engines. Examples: 400, 401, 403, 404 `document_missing_exception` or `index_not_found_exception`, 409, 429 | None | Yes, after fixing the request; for 429, after backing off. A 409 on a conditional retry after an indeterminate attempt means the first attempt possibly applied. |
| Not executed | 503 `shard_not_available_exception` or `master_not_discovered_exception` | None; failed before the WAL append | Yes |
| Indeterminate | 500 `write_outcome_unknown` | Unknown; it can appear later through replay, recovery, or promotion | Only if idempotent (D6) |

- **What is indeterminate:** every failure at or after the primary's WAL
  append, including WAL append and fsync errors, which can leave a partial
  frame.
- **Response contents:** an indeterminate response includes the primary term
  and `seq_no` when known.
- **Why 500:** clients that resend automatically on 503 must never do so for
  an indeterminate write.

### D5. Stale primaries and missing leaders

- **Existing fences stay:** primary validation at admission; replica checks of
  UUID, allocation, and term against the durable fence; and conditional
  promotion.
- **Removal is term-conditioned:** the D2 removal command is conditional on the
  operation's term, so a stale primary's removal is rejected. The stale primary
  then answers indeterminate and stops accepting writes for that allocation.
- **No Raft leader:** a primary that sees no leader may still acknowledge a
  write, but only when every in-sync replica applied it, because D2 removal
  needs a leader. Otherwise the response is indeterminate.

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

- **Search:** an acknowledgement does not imply search visibility. Search sees
  a write after the next refresh.
- **`refresh=true`:** refreshes the primary and every in-sync copy before the
  response.
- **`refresh=wait_for`:** returns once a refresh covers the operation's `seq_no`.
- **Refresh failure:** a refresh that fails after an acknowledged write keeps
  the acknowledged status. It is reported as a shard failure entry in the
  response, not as a failed write.
- **GET by ID:** realtime by default (D9). `realtime=false` reads the
  search-visible reader.

### D9. Realtime reads and update

- **Live version map:** each shard keeps a map from `doc_id` to
  `(seq_no, term, deleted, WAL position)` for every operation since the last
  refresh.
  - A refresh removes an entry only when the entry is at or below the refresh's
    commit boundary, and only after the new reader is visible. As in
    OpenSearch, a current map and an old map cover the refresh window.
  - Delete tombstones stay until they are older than the retention window and
    at or below the processed checkpoint, because Tantivy has no soft
    deletes that record a delete's `seq_no`.
  - A map that grows past a configured limit forces a refresh.
- **Realtime GET:** checks the map first. A tombstone returns not found, and a
  changed document is read from its WAL entry, which stores the full `_source`.
  If that WAL position has been truncated, or the entry is absent, GET reads the
  refreshed reader.
- **`_update`:** runs on the primary. It reads the latest state through the map,
  merges the partial document, and writes conditioned on the `seq_no` and term
  it read. A conflict repeats the cycle up to `retry_on_conflict` times
  (default 0).
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
4. **Replica rollback:** a replica rolls back operations above the global
   checkpoint that belong to an older term. Trimming the WAL cannot undo
   operations Tantivy already committed, so rollback either replays from a safe
   commit at or below the global checkpoint, or re-recovers the copy through
   peer recovery.

### D11. Concurrency control

- **Conditional writes:** `if_seq_no` and `if_primary_term` are checked only on
  the primary, against the live version map or the committed index. A mismatch
  returns 409 `version_conflict_engine_exception`, and in bulk it is a per-item
  error.
- **Versions:** concurrency control by internal `version` is rejected. External
  versions are deferred.
- **Create:** `op_type=create` fails with 409 when the document exists.

### D12. Bulk semantics

- **Results:** items return in request order, and `errors` is true when any item
  failed.
- **Malformed action lines:** a malformed or unknown action or metadata line
  rejects the whole request with 400, as in OpenSearch, because item boundaries
  are lost after it.
- **Source lines:** each action consumes the number of source lines its type
  defines. A malformed source line is a per-item error.
- **Unimplemented actions:** `create`, `delete`, and `update` either follow the
  single-document rules or are rejected per item with 400 until implemented.
- **Shard-group failures:** every item in the group receives that group's
  outcome class.

### D13. Unimplemented parameters fail loudly

A write parameter that changes safety semantics and is not implemented returns
400 `illegal_argument_exception` naming the parameter. This covers:

- the query parameters `if_seq_no`, `if_primary_term`, `version`,
  `version_type`, `op_type=create`, `refresh=wait_for`, `routing`, `pipeline`,
  and `retry_on_conflict`;
- `wait_for_active_shards` values above 1;
- the same keys in bulk action metadata.

Each parameter moves to "honored" when its task lands. Two can be honored early:

- `wait_for_active_shards` as a pre-flight check;
- `wait_for` as a forced refresh, once `refresh=true` covers every copy.

`timeout` becomes the request deadline (FS-029).

### D14. Engine and WAL failures on the write path

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

## 3. Failure scenarios

| Scenario | Required outcome |
|---|---|
| Client timeout after the primary committed | The coordinator returns 500 `write_outcome_unknown`, with term and `seq_no` if known. A retry follows D6. |
| Primary crashes after the WAL append, before the response | Indeterminate. If a replica is promoted, D10 decides whether the operation survives on every copy. |
| Concurrent writes reach a replica out of order | The replica applies by per-document `seq_no` (D1), and its final state matches the primary's. |
| One replica fails to apply | It is removed through D2, then the write is acknowledged with `_shards.failed = 1`. It rejoins through peer recovery. |
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

- A replica fault no longer blocks writes, at the cost of a peer recovery of
  that copy. Transient faults can cause recovery churn. The in-request retry
  budget bounds it. An allocation retry limit, which is deferred work, would
  bound repeated failures.
- The write path waits for a Raft commit only when it removes a replica.
- Clients get distinct classes for "not executed" and "indeterminate", and real
  metadata for concurrency control.
- The live version map adds memory proportional to writes per refresh interval,
  plus tombstones for the retention window.
- Rejecting unimplemented parameters can break clients that send them today.
  Those clients were not getting the behavior they asked for.

## 6. Migration impact

FerrisSearch is pre-1.0 and supports no mixed-version clusters, so wire changes
need no rolling-upgrade path. D1 and D10 change the WAL entry format and need a
WAL format version bump. Nodes fail closed on logs they cannot read, as FS-005
requires. `_flush` keeps entries above the global checkpoint and under
recovery pins, so operators should stop writes, let replicas catch up, and
then flush before upgrading. The new version must read an empty old-format
WAL. Response-field changes follow the compatibility matrix (FS-006).

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
- A TLA+ model of D1, D2, D5, and D10 finds an acknowledged-loss trace. Before
  implementation, `specs/tla` must be extended to allow at least two concurrent
  client writes.
- Live-version-map memory cannot be bounded under a supported workload without
  refreshes that break the latency target.
