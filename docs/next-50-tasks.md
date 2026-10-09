# FerrisSearch: The Next 50 Engineering Tasks

> **Status:** Ranked execution backlog for the 12-24 month architecture roadmap.
>
> **Strategic authority:** [`architecture-roadmap.md`](architecture-roadmap.md).
>
> **Current-behavior authority:** source and tests.
>
> **Last source audit:** 2026-10-09 for FS-002; 2026-10-08 for FS-001 and
> FS-007; 2026-10-05 for FS-013; 2026-10-04 for FS-012; 2026-09-27 for FS-014,
> FS-019, and FS-022 through FS-026, after PRs #142-#144. Other tasks were last
> audited on 2026-07-10.

This is a dependency-aware sequence, not a feature wish list. Rank expresses
current strategic importance; a task still waits for every listed dependency.
No task is considered complete merely because a partial code path exists.

## How To Use This Backlog

- Keep IDs stable when creating issues, plans, or handoffs.
- Verify the source evidence before starting; implementation may have changed.
- State the task ID, roadmap gate, invariants, and acceptance evidence in every
  substantial implementation plan.
- Split a task into reviewable issues when necessary, but do not declare the
  parent complete until all "Done when" criteria are met.
- Re-rank only when new correctness, operational, user, or research evidence
  changes urgency or dependency order.

**Classes**

- **Release blocker:** required for trustworthy production-facing behavior.
- **Scale blocker:** required before the architecture can grow safely.
- **Research opportunity:** validates or differentiates the core systems thesis.

## Re-Ranking And Status Changes (2026-09-27)

PRs #142-#144 added Raft-owned in-sync replica sets, primary terms, allocation
IDs, durable replica fences, file-based peer recovery, bounded storage-failure
escalation, and a bounded TLA+ model of replication and recovery
(`specs/tla/`). The status notes on FS-012 and FS-022 through FS-026 record
what those PRs completed, measured against each task's done criteria.

**Raised: replica apply order (under FS-012).** *Fixed by ADR 0001 D1
(2026-09-30); see the FS-012 status.* Replicas applied replicated
writes in arrival order. The primary replicates after leaving its WAL critical
section, so concurrent writes can arrive out of sequence order. A review probe
with a 3,000-document bulk plus 50 concurrent single writes left the replica
WAL out of order in 4 of 10 runs; in one run, 26 of 50 acknowledged documents
differed between primary and replica. Promoting that replica rolls back
acknowledged writes. This is a release blocker, and its interim ordering fix
lands before the rest of Wave 1, as the FS-014 mitigation does. The TLA+ model
allows one client write at a time, so it could not find this.

**Raised: FS-007** now ranks immediately after FS-001, ahead of FS-002 through
FS-006; its section has moved to match. Ad hoc fault injection in PR #144 found
two acknowledged-data bugs that were present at 8f17172: a transient Tantivy
commit failure lost acknowledged writes, and WAL replay after restart
resurrected acknowledged deletes. The operation-ignoring replay dates to the
first CRUD commit (e95b3ba, #1). The TLA+ model could not see either bug,
because both sat below its durable-operation abstraction. A named failpoint
framework that also emits protocol traces for checking against the model is
the direct defense, and FS-012 and FS-023 depend on it to close their done
criteria.

**Raised: the FS-014 interim mitigation** should land before the rest of Wave
1. The bulk parser turns `delete`, `create`, and `update` actions into
full-document index operations, and a misread can shift every later item. The
full streaming parser keeps its rank.

The rest of the order is unchanged.

## Wave 1 — Freeze Contracts And Write Identity

### FS-001 — Decide The Write Consistency And Retry Contract

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Status (2026-10-08):** Complete. ADR 0001 is accepted. D1 is implemented;
D2-D14 are the contract for FS-009 through FS-014, FS-024 through FS-026, and
FS-029. Acceptance does not claim those implementation tasks are complete.

**Historical status (2026-10-05):** Partial. ADR 0001 selected operation-based durable
acknowledgement, committed term/allocation-conditioned exclusion, sticky
authority-scoped exclusion debt, indeterminate post-mutation outcomes, and local
permit revocation as design targets. The future minimum is one by default with
an explicit floor of two. The stricter-prefix recovery prose and I02, W02, W08,
and F09 acceptance cases are reconciled with D1 and the selected target.
`MC_D2_WriteAck.tla` provides bounded
proposed/current/unsafe controls, not runtime implementation or completion of
the entire ADR's retry, promotion, storage-format, and response criteria.

**Historical status (2026-09-27):** Proposed decision record drafted in
[`adr/0001-write-consistency-and-retry-contract.md`](adr/0001-write-consistency-and-retry-contract.md);
not yet accepted.

**Historical evidence (2026-09-27):** `src/transport/server/mod.rs`,
`src/replication/mod.rs`, and `src/api/index/` can return failure after a primary
mutation; client-visible version/sequence metadata is incomplete.

**Outcome:** An accepted ADR defines operation identity, sequence number,
primary epoch, acknowledged durability, retry behavior, refresh visibility,
partial replication, and conflict semantics for single and bulk writes.

**Done when:** The decision covers timeout-after-commit, primary failover,
replica failure, duplicate retry, update/delete races, and explicitly maps each
supported API response to durable internal state.

### FS-007 — Build Deterministic Distributed Failure Injection

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Status (2026-10-09):** Partial. The `protocol-trace` feature records ordered
schema-v4 Rust executions and checks seeded real-gRPC runs against the bounded
D1 model. `tests/stale_primary_failover.rs` adds the named, client-scoped
`primary_before_replication` pause and a real three-voter metadata partition.
It holds index/delete/bulk after WAL/apply, promotes through committed Raft
commands, establishes newer-term fences, and releases the still-live old
primary's requests. Its result-level assertions cover failed responses,
unchanged canonical WAL/documents/checkpoints, replica engine reopen, and
fresh-allocation peer recovery. The reusable registry also provides the
process-global `primary_after_local_apply_before_replication` pause and a
bounded `replica_after_fence_before_local_apply` failure action. Their
three-node regressions prove stale-term rejection and non-definitive replica
apply failure with an indeterminate primary receipt and no failed-copy
mutation. The named boundary ledger is separate from schema-v4 witness traces.
A general cross-process failpoint framework and publication, hydration,
compaction, and restart crash boundaries remain open.

**Historical status (2026-09-27):** Not started as a framework. PRs #141 and #143 added
in-process pause hooks: force-merge and refresh barriers, snapshot and setup
release channels, and WAL-append and scan barriers. PR #144 added test-only
failure hooks for WAL writes, engine apply, writer replacement, and
assigned-open I/O, plus a bounded TLA+ model in `specs/tla/`. The hooks are ad
hoc and in-process only. None of them is a named failpoint, can crash a
process at a boundary, or records a protocol trace.

**Evidence:** current suites cover many integration paths but cannot
systematically stop at every write, publication, recovery, hydration, and
compaction boundary.

**Outcome:** A test-only failpoint framework can pause, fail, crash, or delay
named boundaries with deterministic orchestration across in-process and
process-backed tests. Failpoints also record protocol events that can be
checked against the `specs/tla` model.

**Status (2026-10-08):** Partial. A feature-gated named pause registry now
provides the first reusable registry boundary,
`primary_after_local_apply_before_replication`, across single, bulk, and delete
writes. A three-node protocol-trace regression pauses a live old primary,
promotes and activates an in-sync replica, releases the write, and proves both
authoritative copies reject the old term without WAL or engine mutation. The
framework still lacks named fail, crash, and delay actions, process-backed
control, and coverage for the remaining FS-007 boundaries.

**Done when:** Tests can reproduce timeout-after-commit, replica failure,
manifest crash points, partial hydration, stale leader/writer, and restart
without sleeps as the correctness mechanism.

### FS-002 — Decide The Fenced Manifest Publication Protocol

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Status (2026-10-09):** Complete. Accepted
[ADR 0002](adr/0002-fenced-manifest-publication-protocol.md) selects parallel
immutable split producers behind one Raft-authorized, storage-CAS-fenced
sequencer per index. It defines writer handoff, operation identity, immutable
audit inventory, idempotent retry, typed ambiguous outcomes, filesystem and
S3-compatible capability contracts, and the required crash/race evidence.
Runtime implementation remains Gate 1 work.

**Evidence:** `StorageManager::append_split_and_publish()` protects
read-modify-write only with a process-local mutex and overwrites the mutable
manifest pointer without compare-and-set.

**Outcome:** An accepted ADR defines the single sequencer, writer lease/epoch,
parent generation, operation ID, conditional pointer update, backend
capabilities, retry, audit history, and failover.

**Done when:** The protocol has an explicit state machine and crash table for
bundle upload, generation write, pointer commit, lease loss, stale writers, and
duplicate operations on filesystem and S3-compatible backends.

### FS-003 — Decide Read Snapshots, Retention, And Visibility

**Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001, FS-002

**Evidence:** local shards, remote manifest generations, refresh, in-flight
root/leaf queries, and future hot deltas do not share one snapshot contract.

**Outcome:** An accepted ADR defines snapshot identity, pinning, read-after-write
boundaries, query consistency, old-generation retention, rollback, and the
watermark at which sealed data replaces mutable data.

**Done when:** The model prevents gaps and double-counting across refresh,
publication, retry, compaction, GC, and a manifest change during an in-flight
query.

### FS-004 — Decide The Unified Lifecycle And Migration Model

**Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001, FS-003

**Evidence:** `local_shards` and `remote_store` currently expose different data
and consistency lifecycles; `src/indexing/mod.rs` is only a placeholder.

**Outcome:** An accepted ADR defines logical index modes, mutable delta
ownership, sealing, immutable publication, read routing, compatibility mode,
and migration/rollback for existing local-shard indices.

**Done when:** The design demonstrates one document/version model across both
tiers and rejects isolated direct CRUD into immutable splits.

### FS-005 — Decide Schema Generations And Durable Format Compatibility

**Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-003, FS-004

**Evidence:** manifests use a fail-closed schema hash, but durable WAL,
manifest, bundle, partial-state, and mixed-generation split compatibility lack
one declared upgrade policy.

**Outcome:** An accepted ADR versions logical schemas independently from
physical formats and defines additive fields, rejected type changes,
reader/writer compatibility windows, rewrite migration, and rollback.

**Done when:** Mixed old/new roots, leaves, manifests, and splits have explicit
read rules and unsupported versions fail before partial execution.

### FS-006 — Publish A Current OpenSearch Compatibility Matrix

**Class:** Release blocker | **Gate:** 0 | **Depends on:** FS-001

**Evidence:** FerrisSearch exposes OpenSearch-shaped endpoints without a
machine-verifiable inventory of semantic differences.

**Outcome:** A versioned matrix lists every advertised endpoint and important
request/response field as supported, partial, intentionally different, or
unsupported, with a test reference.

**Done when:** Refresh, version/conflict, bulk-item, partial-failure, search,
SQL, cluster, security, and maintenance semantics are covered and README claims
link to the matrix rather than using broad compatibility language.

### FS-008 — Establish Reproducible Correctness And Performance Baselines

**Class:** Research opportunity | **Gate:** 0 | **Depends on:** none

**Evidence:** benchmark scripts and historical results do not yet form one
repeatable matrix with hardware, dataset, correctness, cache, durability, and
resource-cost controls.

**Outcome:** A checked-in harness records commit, build, topology, dataset,
queries, warmup, repetitions, cache state, correctness oracle, latency,
throughput, CPU, memory, disk, and object-store bytes/requests.

**Done when:** A new machine can reproduce local-shard and remote-store
baselines and emit raw machine-readable results plus a human summary without
manual result editing.

### FS-009 — Plumb Real Operation Metadata End To End

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001

**Evidence:** write responses contain incomplete or synthetic `_version`,
sequence, and primary-term information.

**Outcome:** WAL allocation, engine mutation, replication RPCs, coordinator
forwarding, bulk items, and REST responses carry authoritative operation ID,
sequence number, document version, and primary epoch.

**Done when:** Single/bulk index, update, and delete return metadata derived
from committed internal state, and failover/retry tests prove continuity.

### FS-010 — Implement Optimistic Concurrency Control

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-009

**Status (2026-09-30):** The `local_shards` CRUD subset is implemented:
realtime GET, primary-side sequence/term conditions, create-only writes, and
coordinator update with CAS retries, upsert, and no-op detection. Conflicts
allocate no sequence and append no WAL. Evidence:
`tests/write_correctness_regression.rs`, the `writes_regression_*` REST tests,
and replicated REST/bulk coverage in `tests/replication_integration.rs`.
Restart/flush preserves the stored concurrency identity. This does not close
the task's failover criteria or implement client retry tokens/external versions.

**Integration note (2026-09-30):** Update pins the primary GET's index UUID
across CAS and retries. A deleted/recreated incarnation returns
`404 index_not_found_exception` before mutation, even if sequence/term tokens
match. REST recreation and queued transport regressions cover both boundaries.

**Historical evidence:** `_update` is read-merge-write and concurrent writers can silently
overwrite each other.

**Outcome:** Index, update, and delete accept and enforce expected sequence/
primary epoch or supported version preconditions at the primary before durable
mutation.

**Done when:** Exactly one concurrent conditional write wins, stale primary
epochs conflict, bulk items preserve per-item conflicts, and restart/failover
does not reset concurrency metadata.

## Wave 2 — Make Writes And Publication Trustworthy

### FS-011 — Add Idempotent Client Operation IDs

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-009

**Evidence:** a client timeout after primary mutation cannot be retried with a
durable guarantee against duplicate application.

**Outcome:** Supported writes carry an operation ID with bounded durable
deduplication across primary retry, forwarding retry, and replica apply.

**Done when:** Duplicate retries return the original result without allocating
new sequence/version state, including timeout-after-commit and failover cases.

### FS-012 — Fence Stale Primaries And Replica Applies

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-009, FS-010

**Status (2026-10-04):** Partial. The live stale-primary criterion now has
deterministic real-Raft/gRPC coverage in `tests/stale_primary_failover.rs`.
An old primary remains reachable with its stale applied view while the other
two voters elect a leader and commit promotion. Delayed index/delete/bulk
requests fail after the targets persist higher-term fences, without changing
their canonical WAL, documents, or checkpoints. A surviving replica's
file-backed engine reopens, and the old primary returns only through
fresh-allocation peer recovery. This closes that bounded evidence gap, not
the entire FS-007 framework, D5 self-fencing policy, D10 history convergence,
or every restart/failover concurrency criterion.

**Status (2026-09-30):** Partial. ADR 0001 D1 is implemented: replicas and
replay apply by per-document `seq_no` and primary term, with gap-aware
checkpoints, ignored redelivery, term-collision quarantine, and promotion NoOp
gap fill. Evidence: the `d1_*_regression` suites, the D1 TLA+ model, and TLA+
validation of traces captured from seeded three-node Rust fault runs. At this
date, the live stale-primary criterion was still unmet; the October 4 status
records the newer coverage.

**Status (2026-09-27):** Partial.
- **Fencing is implemented** (PRs #143-#144), ahead of the listed dependencies:
  - Primary writes require an activated primary at the exact allocation and
    term.
  - Replica RPCs require the index UUID, the target allocation, and a term at
    least as high as both the applied view and the durable replica fence. The
    fence is persisted before WAL mutation.
  - Recovery fetch rejects stale terms, and promotion is a Raft conditional
    command.
  - Evidence: `stale_primary_replication_is_rejected_by_promoted_target`,
    `replica_fence_is_persisted_before_ack_and_restored_on_restart`,
    `conditional_membership_rejects_stale_promotion_and_old_primary_term`,
    `stale_primary_term_rejects_recovery_fetch`, and the TLA+ C2 and fence
    configurations.
- **The stale-primary criterion is unmet:** no test pauses a live old primary,
  promotes a replica, and releases the delayed writes. That test needs FS-007.
- **New release-blocking evidence:** replicas apply operations in arrival
  order, not sequence order. Concurrent writes can therefore leave an in-sync
  replica with older values for acknowledged documents. Its WAL can also end up
  in an order that replay rejects or misapplies. The fix is to apply by
  per-document `seq_no`, with stale-operation skipping, delete tombstones,
  ignored redelivery, and gap-aware checkpoints
  ([ADR 0001](adr/0001-write-consistency-and-retry-contract.md), D1). The fix
  does not depend on FS-007, FS-009, or FS-010. An interim fix that orders
  replica apply can land first.

**Evidence:** routing and sequence preservation exist, but the data plane lacks
a complete primary-epoch check at every mutation and replication boundary.

**Outcome:** Primary epoch is validated by primary writes, replica RPCs,
recovery, and promotion; stale epochs cannot mutate data after leadership or
routing changes.

**Done when:** deterministic tests pause an old primary, promote a replica, and
prove every delayed old-primary write/apply is rejected. Under the review probe
(a large bulk plus concurrent single writes, repeated), primary and replica end
with identical documents, and the replica WAL replays correctly across a
restart.

### FS-013 — Make Write Acknowledgement Policy Explicit

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-011, FS-012

**Status (2026-10-06):** Acknowledgement-policy design/model slice only. The selected D2/D5/D14
targets and copy-floor choice have bounded safety, rejection, and progress
checks in `specs/tla/MC_D2_WriteAck.tla`. Production still fails any required
replica error. [D4 mutation outcome classes and known failure receipts](adr/0001-write-consistency-and-retry-contract.md#d4-outcome-classes)
are implemented, but do not close FS-013. No minimum-copy setting,
success-after-exclusion, restart exclusion-debt reconstruction, or D14
immediate fail-stop policy has shipped. Those require an approved runtime
slice and result-level transport/restart tests before this task can close.

**Status (2026-09-27):** Evidence changed.
- Writes now wait only for the Raft-owned in-sync replica set.
- Any replica failure fails the request, even though the primary already
  mutated.
- The storage escalation budget removes a copy only when its own storage fails.
  An unreachable replica that is still a cluster member is never removed
  (`unreachable_in_sync_replica_still_fails_live_write`), and neither is one
  failing with an unclassified error.
- Until such a node leaves the cluster or an operator intervenes, every write
  to its shards fails, and each attempt can wait out the 30 s transport
  timeout. The failure detector removes only nodes that stop pinging the Raft
  leader.

**Evidence:** writes wait for the Raft-authoritative in-sync replicas, global checkpoint
progress is tied to the slowest replica, and failure after local mutation is
ambiguous.

**Outcome:** A documented policy defines required acknowledgements, degraded
availability, replica exclusion/rejoin, global checkpoint advancement, and
the response to partial replication.

**Done when:** policy is configurable only within supported safe modes, exposed
in response/metrics, and fault tests cover slow, failed, recovering, and stale
replicas without false success.

### FS-014 — Replace Bulk Materialization With A Strict Streaming Pipeline

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-001, FS-009

**Status (2026-09-30):** The interim correctness work is implemented. Parsing
rejects malformed action/metadata lines and missing sources before execution;
invalid JSON sources retain their item positions as 400 errors. Index, create,
delete, and update execute in request order per shard, with real per-item
results and conditions. Index-scoped `_index` overrides are honored and
authorized. Evidence: `writes_regression_*` REST tests and replicated mixed
bulk coverage. Streaming, bounded memory, admission/backpressure, and
cancellation remain open; this is not completion of FS-014.

**Integration note (2026-09-30):** Shard action runs now move their document
sources and serialize borrowed envelopes. Bulk update retains the same
incarnation-pinned CAS logic as single update. Parsing/grouping remain
materialized; these ownership changes do not implement bounded streaming.

**Historical evidence (2026-09-27):** `parse_bulk_ndjson`
(`src/api/index/bulk.rs`) treats every action as `index` and consumes lines in
fixed pairs:
- A `delete` consumes the next action line and overwrites its target with it.
- An `update` stores its whole `{"doc": ...}` wrapper as the document.
- An unparsable source line drops the item with no response entry, so later
  positions shift.
- An unparsable action line goes undetected.
- A trailing action without a source line is dropped.
- `/{index}/_bulk` ignores `_index` on action lines.

An interim mitigation does not depend on FS-001 or FS-009:
- Parse action types strictly.
- Reject the whole request on a malformed or unknown action line, as OpenSearch
  does.
- Reject well-formed but unsupported actions per item.

**Evidence:** bulk parsing/routing materializes request text, action pairs, and
per-shard collections before execution.

**Outcome:** A bounded NDJSON parser validates action/document pairs
incrementally, applies auth/index checks, routes bounded shard batches, and
retains original item order and errors.

**Done when:** multi-gigabyte logical inputs stay within a measured memory
bound; malformed/truncated lines fail precisely; backpressure, cancellation,
refresh, and per-item metadata match the write contract.

### FS-015 — Add Conditional Object-Store Pointer Updates

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-002

**Evidence:** `StorageManager::put()` has no logical compare-and-set capability
for `manifest.current.json`.

**Outcome:** The storage abstraction exposes create-if-absent and
match-version/ETag conditional writes with a clear unsupported-capability
error.

**Done when:** local filesystem and the supported S3-compatible path pass the
same race tests, and publication fails closed on a backend that cannot satisfy
the selected protocol.

### FS-016 — Replicate Manifest Writer Lease And Epoch In Raft

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-002

**Evidence:** Raft knows index metadata but no authoritative manifest writer or
monotonic writer epoch.

**Outcome:** A minimal Raft-managed lease/epoch record elects one sequencer per
index without putting split inventory or query scheduling state in Raft.

**Done when:** acquire, renew, transfer, expiry, leader change, and stale-writer
rejection have state-machine, transport, multi-node, and restart coverage.

### FS-017 — Version Manifests With Parent, Writer Epoch, And Operation ID

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-005, FS-016

**Evidence:** `RemoteStoreManifest` records generation and schema hash but not
the lineage/fencing data required for idempotent concurrent production.

**Outcome:** A backward-declared manifest format records parent generation,
writer epoch, publication operation ID, logical schema generation, and
physical format version.

**Done when:** readers reject broken lineage/unsupported versions, retry can
recognize an already committed operation, and old supported manifests remain
readable within the compatibility window.

### FS-018 — Implement The Fenced Single-Sequencer Commit Path

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-015, FS-016, FS-017

**Evidence:** current publication derives `generation + 1` and overwrites the
pointer under only an in-process mutex.

**Outcome:** Parallel producers stage immutable outputs, while the leased
sequencer validates parent/epoch/op ID and conditionally commits one next
generation.

**Done when:** no accepted concurrent execution loses a split, stale writers
cannot advance the pointer, retries are idempotent, and every commit has an
inspectable audit record and metrics.

### FS-019 — Prove Multi-Writer Publication Under Crash And Failover

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-018

**Evidence:** in-process publication tests cannot prove cross-process fencing.
The bounded TLA+ model in `specs/tla/` found an allocation ABA and a stale-primary
gap in the pre-#144 design, before the Rust fixes. Model the FS-002
sequencer protocol the same way before implementing FS-018.

**Outcome:** Process-backed filesystem and S3-compatible tests run independent
producers and sequencer failover at every publication boundary.

**Done when:** the suite proves no lost committed split, no stale epoch commit,
monotonic lineage, idempotent retry, readable current pointer, and explicit
orphan classification after every injected crash.

### FS-020 — Build A Split Reachability Inventory

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-017

**Evidence:** uploaded bundles and immutable manifests can become unreachable,
but there is no authoritative inventory separating staging, committed,
retained, pinned, and orphaned objects.

**Outcome:** An offline/maintenance scanner enumerates object-store state,
walks retained manifest lineage, classifies every object, and emits bounded
machine-readable inventory without mutating data.

**Done when:** classification handles partial uploads, unknown objects,
retained old generations, active pins, and malformed metadata conservatively.

## Wave 3 — Bound Recovery And Distributed Work

### FS-021 — Add A Dry-Run-First Object-Store Janitor

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-003, FS-020

**Evidence:** node-local cache reaping exists, but object-store artifacts never
enter a retention-aware reclaim protocol.

**Outcome:** A rate-limited janitor consumes reachability inventory, defaults to
dry run, explains every candidate, and deletes only objects beyond the
retention/pin safety window.

**Done when:** false-positive prevention, delayed readers, old snapshots,
partial listings, transient delete failures, restart, and audit output are
covered before destructive mode can be enabled.

### FS-022 — Define And Persist A Shard Snapshot Format

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-005

**Status (2026-09-27):** Partial. PR #143 recovery snapshots are hard-linked
Tantivy file sets taken at a WAL boundary under the translog lock, with a
SHA-256 file manifest. They are scoped to a recovery session, not a persisted
and versioned snapshot format. Vector state is rebuilt rather than captured, and
no format compatibility is declared.

**Evidence:** replica bootstrap is WAL-oriented and cannot efficiently recover
a new or far-behind copy.

**Outcome:** A versioned snapshot captures Tantivy state, checkpoint,
document/version metadata, vector state, schema identity, and integrity
information with an atomic completion marker.

**Done when:** a flushed shard snapshot survives process restart, corruption is
rejected, format compatibility is declared, and creating it does not block
control-plane progress.

### FS-023 — Implement Snapshot Transfer And Atomic Install

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-007, FS-022

**Status (2026-09-27):** Partial.
- **Implemented** (PRs #143-#144):
  - The source serves files in chunks of at most 1 MiB.
  - The target verifies each file's SHA-256
    (`corrupted_recovery_file_checksum_is_rejected`).
  - Pending markers are restored on restart.
  - A source term change settles or rejects the pending target.
  - Persistent local I/O escalates through the storage retry budget.
- **Not a staged install:** preparation deletes the shard directory and writes
  files directly into `index/` under a `PEER_RECOVERY_IN_PROGRESS` marker.
- **Unmet done criteria:** "either the previous valid shard or the new valid
  snapshot" is unmet by design, and disk exhaustion during install is not
  covered.

**Evidence:** no complete snapshot bootstrap exists for replica recovery.

**Outcome:** Primary/source streams a bounded snapshot to a staging directory;
the replica verifies and atomically installs it without exposing partial data.

**Done when:** interruption, retry, disk exhaustion, checksum failure, source
failover, and restart leave either the previous valid shard or the new valid
snapshot, never a mixed state.

### FS-024 — Stream The WAL Suffix During Recovery

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-022, FS-023

**Status (2026-09-27):** Partial.
- **Implemented:** catch-up pulls bounded batches through `FetchRecoveryOps`,
  each at most `MAX_RECOVERY_OPS` (1,024) operations under a byte limit. It
  starts at the snapshot boundary and preserves primary sequence numbers.
- **Conflicts with the done criteria:**
  - Validation rejects reordering but allows sequence gaps
    (`recovery_operation_validation_allows_gaps_but_rejects_reordering`).
  - The source reads the WAL in file order, not sequence order.
  - Duplicate operations are rejected rather than treated as idempotent.
- **Missing:** measured-memory evidence for large suffixes, and a cancellation
  test covering both ends.

**Evidence:** WAL scanning can stream internally, but recovery responses
materialize operation collections.

**Outcome:** Recovery sends bounded ordered chunks after the snapshot
checkpoint, preserving primary sequence values and applying backpressure.

**Done when:** large suffixes remain within measured memory, duplicate chunks
are idempotent, gaps/out-of-order chunks fail, cancellation stops both ends,
and the final checkpoint is contiguous.

### FS-025 — Introduce An Explicit Replica Recovery And ISR State Machine

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-012, FS-013, FS-024

**Status (2026-09-27):** Partial. In-sync membership, allocation IDs, primary
terms, and conditional admission and promotion are Raft-owned (PRs #143-#144).
Pending-recovery markers are durable and restored on restart, and the TLA+
model checks these transitions within bounds. The per-copy lifecycle is still
spread across node, shard, and transport code rather than one explicit state
machine with named states and metrics.

**Evidence:** ISR tracking is checkpoint/lag based and lifecycle recovery is
spread across node, shard, transport, and replication code.

**Outcome:** Replica copies transition through unassigned, initializing,
snapshotting, catching-up, in-sync, stale, and failed states under one
authoritative policy.

**Done when:** allocation, promotion eligibility, acknowledgement membership,
retry/backoff, cancellation, metrics, and restart behavior are driven by tested
state transitions rather than incidental open-shard status.

### FS-026 — Make Replica Policy Settings Reactive And Operable

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-013, FS-025

**Status (2026-09-27):** Not started. PRs #143-#144 added node-level settings,
not reactive cluster settings: `max_concurrent_peer_recoveries`,
`shard_io_failure_escalation_attempts`, and
`shard_io_failure_escalation_window_ms`.

**Evidence:** ISR max lag and slowest-replica checkpoint behavior are fixed
implementation choices. Raft commits settings metadata on every node, but
current request handlers notify already-open engines only on participating
request/leader nodes.

**Outcome:** Safe cluster/index settings define lag thresholds, recovery
timeouts, retry backoff, and supported acknowledgement policy with Raft
replication, per-node committed-state reaction, and operator visibility.

**Done when:** multi-node tests prove every already-open local engine applies a
committed setting without requiring request ingress or reopen, invalid
combinations fail, lagging replicas leave/rejoin predictably, and no policy can
acknowledge a write below the documented durability contract.

### FS-027 — Make Vector Recovery Complete And Explicit

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-022

**Evidence:** vector rebuild scans a fixed maximum and recovery errors can be
hidden or deferred.

**Outcome:** Vector state is restored from the declared snapshot/split format
or rebuilt through an unbounded streaming scan with explicit progress and
failure.

**Done when:** datasets above the old cap recover every vector, update/delete
semantics match document versions, errors keep the shard unavailable, and
restart/failover tests compare vector results to an oracle.

### FS-028 — Add Request-Scoped Cancellation

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** none

**Evidence:** HTTP/gRPC futures may time out or disconnect while collectors,
rayon work, hydration, and residual execution continue consuming resources.

**Outcome:** A request cancellation context is created at ingress and observed
by local search, distributed fan-out, SQL streaming, split hydration, merge,
and long-running maintenance where applicable.

**Done when:** tests cancel each stage and show child work, temporary files,
pins, load counters, and reservations terminate or release within a bounded
time.

### FS-029 — Propagate Absolute Deadlines Across gRPC

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-028

**Evidence:** connection/request timeouts exist, but child RPCs do not share one
end-to-end budget.

**Outcome:** Coordinators derive an absolute deadline, forward remaining
budget, reject expired work before expensive execution, and classify deadline
errors consistently.

**Done when:** nested fan-out cannot outlive the client budget, clock/skew
assumptions are documented, queue time is charged, and timeout metrics identify
the stage that exhausted the budget.

### FS-030 — Add Bounded Admission Queues

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-028, FS-029

**Evidence:** dedicated search/write threads isolate CPU pools but do not bound
all queued distributed, hydration, recovery, or maintenance work.

**Outcome:** Work classes have bounded concurrency/queues and stable overload
responses before spawning tasks or allocating large buffers.

**Done when:** overload tests prove bounded queue depth and tail memory, fair
foreground progress, deadline-aware dequeue, cancellation removal, and
separate queue/execution metrics.

## Wave 4 — Govern Resources And Converge Data

### FS-031 — Reserve Memory, Disk, And In-Flight Bytes

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-030

**Evidence:** column arrays, result buffers, bundle downloads, extracted files,
open readers, and background jobs have separate or incomplete accounting.

**Outcome:** A node resource governor reserves estimated bytes before expensive
work, reconciles actual use, and distinguishes memory, temporary disk,
persistent cache, and object-store inflight bytes.

**Done when:** one oversized query/split is rejected before allocation, every
success/error/cancel path releases reservations, and concurrent stress stays
within configured tolerances.

### FS-032 — Classify And Prioritize Background Work

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-030, FS-031

**Evidence:** refresh, flush, recovery, hydration, force merge, future
compaction, and GC can compete with foreground work despite separate rayon
pools.

**Outcome:** Foreground search/write and background recovery/hydration/
maintenance receive explicit concurrency, CPU, I/O, and byte budgets with
priority and starvation rules.

**Done when:** mixed-load tests show bounded foreground latency, eventual
background progress, no Tokio starvation, and per-class saturation metrics.

### FS-033 — Add End-To-End Distributed Tracing And Stable Error Classes

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-029

**Evidence:** metrics exist, but request/operation identity and stage-level
causality are incomplete across HTTP, gRPC, worker pools, and object storage.

**Outcome:** Request, operation, snapshot, assignment, and task IDs propagate
through structured spans with bounded attributes and stable retryable/
terminal/corruption/overload error classes.

**Done when:** one trace explains queue, search, hydration, retry, partial
merge, and storage time; secrets and high-cardinality IDs never become metric
labels; bulk items retain diagnosable underlying causes.

### FS-034 — Implement The Searchable Mutable Delta

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-004, FS-009, FS-013

**Evidence:** mutable `local_shards` and immutable `remote_store` are separate
index engines rather than stages of one index lifecycle.

**Outcome:** A logical unified index owns a durable replicated hot delta with
explicit refresh/search visibility and the same document/version identity used
by future splits.

**Done when:** writes are searchable before sealing, restart/failover preserve
visibility, standard CRUD metadata is correct, and the delta can be enumerated
for a consistent seal boundary.

### FS-035 — Add Automatic Bounded Sealing Into Immutable Splits

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-018, FS-031, FS-034

**Evidence:** remote splits are manually published from request documents; no
automatic hot-delta lifecycle exists.

**Outcome:** Policy-driven sealing selects a stable delta range, builds a split
under resource limits, and submits it to the fenced sequencer with retryable
operation identity.

**Done when:** size/time/manual triggers, backpressure, crash/retry, concurrent
seals, and empty/oversized batches are tested; one document is never
accidentally published into unbounded tiny splits.

### FS-036 — Commit A Publication Visibility Watermark

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-035

**Evidence:** no durable boundary says which hot-delta versions are fully
represented by an accepted manifest generation.

**Outcome:** Publication commit records a contiguous logical watermark and
keeps the corresponding delta visible until the manifest is safely queryable.

**Done when:** every crash before/after pointer commit yields either hot-only or
hot-plus-immutable visibility without gaps, and retirement is idempotent after
restart.

### FS-037 — Query One Snapshot Across Hot And Immutable Layers

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-036

**Evidence:** current coordinator paths query one engine mode at a time.

**Outcome:** Search DSL and SQL pin one snapshot, query eligible hot and split
sources, suppress overlapping versions, and merge hits/partials with type and
ordering stability.

**Done when:** oracle tests cover publication races, paging, count,
aggregations, grouped SQL, exact sort, refresh, and failover with no duplicate
or missing logical document.

### FS-038 — Add Versioned Tombstones And Supersession

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-010, FS-037

**Evidence:** immutable remote splits have no update/delete model.

**Outcome:** Updates create newer logical versions and deletes create
tombstones; snapshot filtering selects the newest visible version across delta
and splits without relying solely on wall-clock time.

**Done when:** update/delete races, restart, sealing, old snapshots, duplicate
IDs across splits, and delayed publication return oracle-correct results.

### FS-039 — Model Split Lineage And Replacement

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-017, FS-038

**Evidence:** manifests append splits but cannot express that new outputs
replace compacted inputs while preserving old snapshots.

**Outcome:** Manifest metadata records immutable split inputs, replacement
outputs, version/tombstone coverage, and lineage under one fenced commit.

**Done when:** lineage validation rejects cycles, missing inputs, overlapping
replacement commits, and unsafe coverage; retained generations remain fully
readable.

### FS-040 — Build A Fenced, Idempotent Split Compactor

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-031, FS-039

**Evidence:** there is no object-store compaction or merge lifecycle.

**Outcome:** A resource-governed compactor selects by size, age, overlap,
delete density, and query cost; builds outputs; and atomically commits lineage
through the manifest sequencer.

**Done when:** concurrent compactors cannot replace the same inputs, crash/
retry is idempotent, tombstones remain correct, and old snapshots survive
before retention expiry.

## Wave 5 — Complete Lifecycle, Scale, And Evidence

### FS-041 — Implement Snapshot-Safe Mark-And-Sweep GC

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-021, FS-039, FS-040

**Evidence:** a janitor can classify simple orphans, but compaction creates
previously published objects that require snapshot-aware retention.

**Outcome:** GC marks from every retained/pinned manifest and sweeps only
expired unreachable bundles, manifests, hotcache artifacts, and staging data
under bounded request/delete rates.

**Done when:** readers pinned to old generations, delayed leaf RPCs, rollback,
partial listings, compaction crash, and repeated GC cannot delete live data.

### FS-042 — Deliver Backup, Restore, Verify, And Repair Workflows

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-003, FS-022, FS-041

**Evidence:** checksum verification exists for remote bundles, but there is no
complete production recovery workflow for control-plane metadata plus data.

**Outcome:** Documented tools/runbooks snapshot required Raft metadata and data
references, restore to a clean cluster, verify checksums/lineage, and repair or
quarantine reconstructible failures.

**Done when:** a process-backed drill restores a multi-index cluster to a
declared snapshot, validates queries against an oracle, and records RPO/RTO and
irreparable corruption behavior.

### FS-043 — Migrate Existing Indices Without Reinterpreting Semantics

**Class:** Release blocker | **Gate:** 2 | **Depends on:** FS-004, FS-037, FS-042

**Evidence:** current index metadata chooses one engine at creation and has no
online lifecycle migration.

**Outcome:** An explicit migration tool/mode copies or seals eligible
local-shard data into the unified lifecycle, verifies equivalence, switches at
a pinned boundary, and retains rollback state.

**Done when:** mixed-version rolling nodes, interruption/retry, rollback,
unsupported mappings/vectors, and pre/post query equivalence are tested without
silently changing durability or visibility.

### FS-044 — Stream Bundle Hydration And Verification

**Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-028, FS-031

**Evidence:** `fetch_split_into_cache()` obtains full bundle bytes before local
write/extraction.

**Outcome:** Hydration streams object bytes to a temporary artifact, hashes
incrementally, enforces compressed/uncompressed limits, and atomically marks
completion before extraction/read admission.

**Done when:** peak memory is independent of bundle size, cancellation and
short writes remove partial files, decompression bombs are rejected, checksum
failure never warms cache, and concurrent fetch remains single-flight.

### FS-045 — Unify Cache Inventory, Admission, And Eviction

**Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-031, FS-044

**Evidence:** column arrays, split artifacts, reader pins, and query/hydration
memory use separate budgets and inventories.

**Outcome:** One node cache governor accounts for logical/physical bytes,
pinned readers, column entries, artifacts, temporary data, admission cost, and
eviction reason without scanning the filesystem per query.

**Done when:** budgets are enforced under concurrent workloads, pinned data is
safe, oversized entries are rejected, restarts rebuild inventory, and metrics
attribute hits/misses/evictions by tier.

### FS-046 — Maintain A Bounded Leaf Capability And Load Inventory

**Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-033, FS-045

**Evidence:** roots query every candidate leaf for cache/load status on the
request path.

**Outcome:** Roots maintain short-lived, bounded, failure-aware leaf
capability/load/cache summaries with freshness and conservative fallback.

**Done when:** query scheduling avoids all-leaf status fan-out, stale inventory
cannot affect correctness, joins/leaves converge, cardinality stays bounded,
and update traffic is measured separately from query traffic.

### FS-047 — Scale Split Assignment, Retry, And Partial-Failure Semantics

**Class:** Scale blocker | **Gate:** 3 | **Depends on:** FS-028, FS-029, FS-046

**Evidence:** rendezvous ranking and assignment cost grows with split/leaf
counts; root/leaf retries lack a complete request-ID and error contract.

**Outcome:** Snapshot-pinned batch assignments use bounded candidate selection,
assignment IDs, deduplicated retry, retryable/terminal errors, and explicit
partial-result policy.

**Done when:** large split/leaf simulations meet target complexity, node churn
and duplicate responses remain correct, cancellation stops retries, and EXPLAIN
ANALYZE reports assignment, cache, retry, and byte costs.

### FS-048 — Execute Mixed Schema Generations Safely

**Class:** Release blocker | **Gate:** 3 | **Depends on:** FS-005, FS-039

**Evidence:** a single schema hash currently assumes all readable splits share
one mapping fingerprint.

**Outcome:** Splits declare logical schema generation and physical format;
readers materialize missing additive fields as typed nulls, reject incompatible
types, and compaction can rewrite old formats.

**Done when:** local/remote SQL and DSL queries are type-stable across mixed
generations, quoted/unquoted identifiers remain correct, rolling compatibility
is tested, and rollback retains a readable generation.

### FS-049 — Enforce Tenant-Aware Security And Resource Isolation

**Class:** Release blocker | **Gate:** 3 | **Depends on:** FS-030, FS-031, FS-033

**Evidence:** HTTP authn/authz and transport TLS exist, but admission, object
storage, task execution, audit, and caches do not yet form a tenant isolation
contract.

**Outcome:** Principal/index context reaches admission and audit decisions;
quotas bound concurrent work/bytes; internal nodes have authenticated identity;
object-store credentials remain external to Raft/manifests/logs.

**Done when:** cross-tenant cache/task/data access is denied, body-routed APIs
charge the correct principal, quota overload is stable, audit records are
complete/redacted, and adversarial multi-tenant tests show bounded isolation.

### FS-050 — Produce The Production V1 And Research Evidence Package

**Class:** Release blocker + research opportunity | **Gate:** 4 | **Depends on:** FS-006, FS-008,
FS-019, FS-037, FS-041, FS-042, FS-047, FS-048, FS-049

**Evidence:** FerrisSearch's defensible thesis—search-aware pruning, cache-aware
scheduling, search-native partials, and unified hot/immutable planning—needs
correctness and cost evidence, while production claims need conformance and
operational proof.

**Outcome:** A reproducible release artifact includes distributed oracle and
fault matrices, long mixed-workload soak results, OpenSearch conformance
results, backup/restore and rolling-operation drills, benchmark raw data,
ablation studies, quality/cost tradeoffs, and disclosed limitations.

**Done when:** an independent operator can reproduce the evidence; every public
claim points to a result; approximate behavior has quality bounds and exact
fallback; production V1 criteria in the roadmap are individually signed off or
explicitly deferred.

## What Is Intentionally Below This Cut

The top 50 do not prioritize broad endpoint expansion, arbitrary scripting,
cross-index joins, plugin compatibility, remote vector search, or new SQL
syntax. Those can follow once their prerequisites are complete. If new evidence
makes one urgent, re-rank the backlog explicitly rather than inserting it as an
untracked shortcut.
