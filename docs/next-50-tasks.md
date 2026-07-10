# FerrisSearch: The Next 50 Engineering Tasks

> **Status:** Ranked execution backlog for the 12-24 month architecture roadmap.
>
> **Strategic authority:** [`architecture-roadmap.md`](architecture-roadmap.md).
>
> **Current-behavior authority:** source and tests.
>
> **Last source audit:** 2026-07-10.

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

## Wave 1 — Freeze Contracts And Write Identity

### FS-001 — Decide The Write Consistency And Retry Contract

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Evidence:** `src/transport/server/mod.rs`, `src/replication/mod.rs`, and
`src/api/index/` can return failure after a primary mutation; client-visible
version/sequence metadata is incomplete.

**Outcome:** An accepted ADR defines operation identity, sequence number,
primary epoch, acknowledged durability, retry behavior, refresh visibility,
partial replication, and conflict semantics for single and bulk writes.

**Done when:** The decision covers timeout-after-commit, primary failover,
replica failure, duplicate retry, update/delete races, and explicitly maps each
supported API response to durable internal state.

### FS-002 — Decide The Fenced Manifest Publication Protocol

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

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

### FS-007 — Build Deterministic Distributed Failure Injection

**Class:** Release blocker | **Gate:** 0 | **Depends on:** none

**Evidence:** current suites cover many integration paths but cannot
systematically stop at every write, publication, recovery, hydration, and
compaction boundary.

**Outcome:** A test-only failpoint framework can pause, fail, crash, or delay
named boundaries with deterministic orchestration across in-process and
process-backed tests.

**Done when:** Tests can reproduce timeout-after-commit, replica failure,
manifest crash points, partial hydration, stale leader/writer, and restart
without sleeps as the correctness mechanism.

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

**Evidence:** `_update` is read-merge-write and concurrent writers can silently
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

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-009, FS-010

**Evidence:** routing and sequence preservation exist, but the data plane lacks
a complete primary-epoch check at every mutation and replication boundary.

**Outcome:** Primary epoch is validated by primary writes, replica RPCs,
recovery, and promotion; stale epochs cannot mutate data after leadership or
routing changes.

**Done when:** deterministic tests pause an old primary, promote a replica, and
prove every delayed old-primary write/apply is rejected.

### FS-013 — Make Write Acknowledgement Policy Explicit

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-001, FS-011, FS-012

**Evidence:** writes wait for all configured replicas, global checkpoint
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

**Evidence:** no complete snapshot bootstrap exists for replica recovery.

**Outcome:** Primary/source streams a bounded snapshot to a staging directory;
the replica verifies and atomically installs it without exposing partial data.

**Done when:** interruption, retry, disk exhaustion, checksum failure, source
failover, and restart leave either the previous valid shard or the new valid
snapshot, never a mixed state.

### FS-024 — Stream The WAL Suffix During Recovery

**Class:** Scale blocker | **Gate:** 1 | **Depends on:** FS-022, FS-023

**Evidence:** WAL scanning can stream internally, but recovery responses
materialize operation collections.

**Outcome:** Recovery sends bounded ordered chunks after the snapshot
checkpoint, preserving primary sequence values and applying backpressure.

**Done when:** large suffixes remain within measured memory, duplicate chunks
are idempotent, gaps/out-of-order chunks fail, cancellation stops both ends,
and the final checkpoint is contiguous.

### FS-025 — Introduce An Explicit Replica Recovery And ISR State Machine

**Class:** Release blocker | **Gate:** 1 | **Depends on:** FS-012, FS-013, FS-024

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
