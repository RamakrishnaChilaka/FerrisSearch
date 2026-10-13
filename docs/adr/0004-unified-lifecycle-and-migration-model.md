# ADR 0004: Unified Lifecycle And Migration Model

- **Status:** Accepted. The lifecycle is not implemented.
- **Date:** 2026-10-13
- **Accepted:** 2026-10-13
- **Backlog:** [FS-004](../next-50-tasks.md#fs-004--decide-the-unified-lifecycle-and-migration-model)
- **Roadmap:** Gate 0 deliverable "accepted unified lifecycle and migration
  model"; one-product lifecycle and delta ingest in
  [`architecture-roadmap.md`](../architecture-roadmap.md#71-one-product-lifecycle-not-two-engines)
- **Depends on:** [ADR 0001](0001-write-consistency-and-retry-contract.md),
  [ADR 0002](0002-fenced-manifest-publication-protocol.md), and
  [ADR 0003](0003-read-snapshots-retention-and-visibility.md)
- **Scope:** logical document/version representation, lifecycle modes, mutable
  ownership, sealing, immutable publication, tiered reads, direct CRUD policy,
  migration of existing `local_shards` indices, and rollback. Schema generation
  compatibility is FS-005.

This ADR selects one logical data lifecycle with multiple deployment policies.
Acceptance does not claim that current `local_shards` data seals into remote
splits, that current remote bundles carry operation versions or tombstones, or
that an existing index can change lifecycle mode today.

## 1. Context

### 1.1 Current behavior

FerrisSearch currently exposes two separate create-time engines:

- `local_shards` has logical shards, Raft-owned routing and allocation
  identity, primary-assigned `(primary_term, seq_no)` operations, WAL-backed
  mutation, synchronous replica application, refresh, realtime GET, and
  ordinary document CRUD.
- `remote_store` creates zero shards and no shard routing. A dedicated publish
  endpoint builds a complete Tantivy split from submitted documents, gives the
  split a random ID, uploads it, and appends its descriptor to a manifest.
  Ordinary index, bulk, update, and delete requests return 501.

The engine selector is immutable after index creation. Search routes to either
local shards or the remote manifest path; it does not combine them.
`src/indexing/mod.rs` is only a placeholder and does not provide a shared
ingestion or sealing subsystem.

The two durable representations also differ:

- local operations carry document ID, mutation kind, sequence number, and
  primary term; local reads can return that metadata;
- remote manifests and split descriptors carry split identity, checksums,
  counts, sizes, and pruning summaries, but no source shard/history, document
  operation version, tombstone coverage, seal watermark, or compaction lineage.

The current remote publish endpoint accepts isolated document batches rather
than materializing an authoritative mutable history. Publishing the same
logical document again creates another physical copy with no cross-split
version winner. There is no supported migration or rollback between engines.

ADR 0002 defines how one fenced sequencer commits immutable manifests. ADR 0003
defines the snapshot cut between a manifest sealed through `W` and a mutable
reader visible through `V`. This ADR defines the common data and lifecycle that
produce those components.

### 1.2 Required properties

The lifecycle must provide:

1. one operation identity and document-version rule across mutable and
   immutable storage;
2. primary-owned mutable history and refresh semantics;
3. immutable splits built only from a fenced contiguous history range;
4. asynchronous sealing without losing acknowledged or search-visible data;
5. tiered reads with no gap or double count;
6. updates and deletes that remain correct after sealing and compaction;
7. explicit mutable-only, immutable-only, and tiered policies without separate
   logical data models;
8. no isolated direct CRUD into immutable splits;
9. conditional, observable migration and rollback; and
10. a compatibility path for small local deployments and query-only data.

The lifecycle must not put document or split inventory into the Raft log.

## 2. Alternatives considered

### 2.1 Keep two unrelated engines

Rejected. It preserves different write, version, update/delete, read, migration,
and recovery semantics. Features and correctness work would continue to be
implemented twice.

### 2.2 Turn each remote CRUD request into a new split

Rejected. One split per request provides poor write amplification and cannot
define idempotent update/delete, refresh, duplicate suppression, or compaction
without first recreating a mutable operation history.

### 2.3 Dual-write every operation to local shards and object storage

Rejected. A foreground dual write creates a distributed transaction between
WAL/replicas and object storage. Partial success would make acknowledgement and
recovery ambiguous, and immutable publication latency would become write
latency.

### 2.4 Migrate by building a separate index and swapping an alias

Deferred as an optional operator workflow. FerrisSearch does not yet have an
alias contract, and a second index requires a separate write-capture mechanism
to avoid a cutover gap. It does not remove the need for one lifecycle inside a
tiered index.

### 2.5 Shard-owned mutable history that seals asynchronously into splits

Selected. Normal writes have one primary/WAL/replica path. A seal captures a
contiguous prefix without blocking later writes, publishes that prefix through
ADR 0002, and changes read ownership through ADR 0003. Immutable-only and
mutable-only are policies over the same logical record contract.

## 3. Decision

### 3.1 One logical record model

Every document mutation is a `LogicalDocumentOperation`:

```text
LogicalDocumentOperation {
  format_version
  index_uuid
  logical_shard_id
  shard_history_uuid
  doc_id
  operation_version {
    seq_no
    primary_term
  }
  operation_id
  schema_identity
  kind: Index(source) | Delete | NoOp(reason)
}
```

The exact serialized format is implementation work. These semantics are fixed:

- `index_uuid`, logical shard, history UUID, primary term, and sequence number
  identify the authoritative shard history.
- The primary assigns the operation version. A split builder, manifest
  sequencer, compactor, root, or leaf never invents or renumbers it.
- Within one shard history, document versions are ordered by sequence number,
  then primary term for the same sequence identity. A higher term fences stale
  authority but does not make a lower sequence overwrite a later document
  operation.
- Routing is stable for an index incarnation, so every document ID has one
  logical shard history.
- `operation_id` provides external retry identity once FS-011 implements it. It
  is distinct from the primary-assigned version.
- Index is full-source replacement. Update resolves on the primary into one
  conditioned Index or Delete operation. Delete is a versioned tombstone.
- NoOp advances history/coverage but has no document value.

For a read snapshot, the newest visible operation for one document wins. A
Delete hides all older Index operations. Physical duplicates of the same
operation identity are redeliveries and contribute once. Two different
payloads for one identity are corruption.

This extends ADR 0001's operation identity into the immutable lifecycle. It
does not add a separate split-local document version.

### 3.2 Physical representations

The same logical operations may appear in:

- a mutable shard WAL and live engine;
- a sealed local staging snapshot;
- one immutable split;
- a replacement/compacted split; and
- retained old splits needed by snapshots or rollback.

Every immutable split records:

- index UUID, logical shard, and shard history UUID;
- schema identity;
- inclusive logical history coverage;
- every stored document's operation version and delete state;
- source seal ID and ADR 0002 publication operation ID;
- input lineage for compacted replacements;
- checksums, sizes, and pruning summaries; and
- whether coverage is a full base or an incremental range.

A split containing only current sources without operation versions and
tombstones is not eligible for tiered lifecycle reads. Current remote bundles
therefore require an incompatible format revision before they can participate.

The manifest records the complete live split set, per-shard
`sealed_through` watermarks, lineage, and coverage. Split membership remains in
object storage. Raft holds only small lifecycle policy, migration authority,
shard routing, and publication-writer authority.

### 3.3 Lifecycle policies

`IndexLifecycleMode` is explicit Raft metadata, separate from physical reader
implementation:

| Mode | Mutable component | Immutable component | Ordinary CRUD |
|---|---|---|---|
| `MutableOnly` | Required routed shards | None | Allowed |
| `ImmutableOnly` | None | Required manifest | Rejected |
| `Tiered` | Required routed hot deltas | Required manifest, which may initially be empty | Allowed through mutable primaries |

These modes share the record, schema, snapshot, and query contracts. They are
not three engines or three document models.

- `MutableOnly` preserves the small-cluster/local-SSD deployment profile.
- `ImmutableOnly` supports query-only/imported datasets and the current
  read-only remote profile.
- `Tiered` is the target object-store lifecycle.

An index name maps to one mode at a time. Every API that exposes settings,
catalog rows, health, or execution metadata reports the mode explicitly.
Object-store configuration does not implicitly enable tiering.

The current `engine` setting remains immutable compatibility metadata while the
new lifecycle is introduced:

- current `local_shards` maps to `MutableOnly`;
- current `remote_store` maps to a legacy `ImmutableOnly` format; and
- `Tiered` requires the new lifecycle formats and cannot be enabled by changing
  `index.engine`.

Long term, physical engines become implementation details behind lifecycle
components. No third independent data model is introduced.

### 3.4 Mutable ownership

Each logical shard retains Raft-owned primary, allocation, primary term, and
in-sync replica authority in `MutableOnly` and `Tiered`.

The primary:

- owns sequence allocation and OCC checks;
- appends and replicates the logical operation;
- publishes near-real-time readers;
- chooses a seal barrier only from gap-free processed history;
- pins the range needed by an in-flight seal; and
- reports prepared immutable descriptors to the manifest sequencer.

Replicas preserve exact operation identities and seal-retention requirements.
An assigned but out-of-sync copy is not seal authority.

The manifest sequencer serializes immutable publication but does not own
document ordering. A producer may build artifacts on another node only from a
verified, immutable seal input issued by the authoritative shard. Arbitrary
documents cannot be handed directly to the sequencer in `Tiered`.

### 3.5 Seal identity and state machine

One seal is identified by:

```text
SealIdentity {
  index_uuid
  logical_shard_id
  shard_history_uuid
  seal_id
  after_watermark
  through_watermark
  schema_identity
}
```

The range is `(after_watermark, through_watermark]`. The first base seal starts
from the history origin. `through_watermark` is contiguous and cannot cross a
gap, unresolved term transition, or operation whose durable identity is
unknown.

The logical seal states are:

```text
Open
  -> BarrierRecorded
  -> InputPinned
  -> ArtifactBuilt
  -> Prepared
  -> ManifestCommitted
  -> SnapshotVisible
  -> RetentionEligible
  -> Reclaimed
```

Failure before `ManifestCommitted` leaves the mutable range authoritative and
queryable. Failure after manifest commit is resolved by ADR 0002 operation
identity. `SnapshotVisible` means a new ADR 0003 snapshot can select the
manifest plus the exact mutable suffix.

State needed to resume a seal is durable. The immutable object inventory and
attempt records live in object storage. Shard-local pin and barrier evidence
lives with the shard history and is reconstructed before reclaim. Small
migration/policy authority transitions go through Raft.

### 3.6 Seal triggers and backpressure

Seal policy may trigger on:

- mutable bytes;
- document/operation count;
- elapsed open-delta time;
- WAL or local-disk pressure;
- operator request; or
- migration/bootstrap work.

Time is a trigger, never the ordering or coverage authority.

Sealing is asynchronous to ordinary acknowledgements. Acknowledged writes
remain durable in the mutable path and queryable according to refresh semantics
whether object storage is healthy or not.

The mutable tier has bounded local storage. When unpublished/pinned ranges
exceed configured limits:

1. slow or pause new seal production;
2. reject or throttle new writes with an explicit overload reason before
   unbounded growth;
3. expose open-delta bytes, oldest unsealed operation, seal lag, publish lag,
   pinned bytes, and rejection counters; and
4. never discard acknowledged operations or claim a failed seal as visible.

Object-store outage can reduce write availability under sustained pressure,
but it cannot silently lose or hide acknowledged history.

### 3.7 Publication and visibility

For each prepared seal:

1. build deterministic immutable artifacts from the pinned range;
2. verify operation coverage, schema identity, checksum, and sizes;
3. submit the split descriptors under one ADR 0002 operation ID;
4. have the fenced sequencer create a manifest descendant whose
   `sealed_through` advances only through the verified contiguous range;
5. compare-and-set the pointer;
6. resolve ambiguous outcomes by operation ID; and
7. expose the new ownership boundary only through an ADR 0003 snapshot.

A manifest cannot advance a shard's watermark without complete coverage. It
may add multiple shard seals atomically in one generation, but it is not
required to advance every shard together.

Publication retries reuse seal and operation identities. A duplicate prepared
seal either resolves to its existing committed result or conflicts on different
content. It never appends the same logical range twice.

### 3.8 Tiered read routing

One ADR 0003 snapshot determines execution:

- `MutableOnly`: query the pinned mutable reader components.
- `ImmutableOnly`: query the pinned manifest split set.
- `Tiered`: query immutable versions at or below each shard's
  `sealed_through = W` and mutable versions in `(W,V]`, where `V` is the pinned
  mutable visibility boundary.
- migration validation: return the authoritative mode's result and optionally
  execute the candidate mode as a shadow oracle.

All paths merge by logical document version and tombstone. The query planner
may push range ownership into shard/split scans to avoid materializing
duplicates, but optimization cannot change the winner.

A tiered root may run on a query-only node. It sends immutable work to leaves
and mutable-suffix work to the routed shard copies. Query-only compute does not
become write authority.

If the mutable reader cannot serve every operation after `W`, or the manifest
does not prove coverage through `W`, resolution fails. The coordinator never
uses "whatever is available" and reports a complete success.

### 3.9 CRUD and import policy

Normal `_doc`, `_bulk`, `_update`, and `_delete` behavior is:

- allowed in `MutableOnly`;
- allowed in `Tiered`, always routed through the mutable primary; and
- rejected in `ImmutableOnly`.

No REST or transport path mutates an immutable split in place. Immutable
changes are new logical operations in the mutable history followed by seal and
compaction.

The current `POST /{index}/_remote_store/publish` document-batch endpoint is not
the tiered ingest API. During transition it remains an explicitly experimental
`ImmutableOnly` import path. Before the new versioned split format is enabled,
it cannot create tiered data.

A production immutable import must provide or deterministically create:

- stable document IDs;
- logical operation versions in an isolated imported history;
- schema identity;
- complete base coverage;
- publication operation identity; and
- validation that no mutable writer concurrently owns that history.

Import never bypasses ADR 0002 fencing or ADR 0003 snapshot publication.

### 3.10 Migration authority states

Lifecycle transitions are conditional Raft mutations bound to index UUID and a
unique `migration_id`:

```text
MutableOnly
  -> TieringBootstrap
  -> TieringValidate
  -> Tiered

TieringBootstrap | TieringValidate
  -> MutableOnly

Tiered
  -> MutableRollbackPreparing
  -> MutableOnly
```

Raft stores only transition authority and compact progress summaries:

- source and target mode;
- migration ID and policy revision;
- expected index UUID and schema identity;
- base manifest identity;
- per-shard bootstrap watermark;
- validation state;
- rollback deadline/state; and
- terminal outcome.

Detailed split and operation inventory remains in the manifest/audit plane.
Every transition compares the complete expected prior state; a stale
coordinator cannot cut over or roll back a newer migration.

### 3.11 Existing `local_shards` migration

A supported `local_shards` index migrates in place under the same index UUID
and logical shard routing:

1. **Preflight:** verify healthy authoritative shard histories, supported
   durable formats, object-store publication capability, schema identity, free
   space, and configured retention/backpressure limits.
2. **Begin:** commit `TieringBootstrap` through Raft. Ordinary writes continue
   through existing primaries.
3. **Capture:** each shard records a contiguous bootstrap watermark `W`, pins
   the required history/reader, and builds a versioned base split representing
   logical state through `W`.
4. **Publish:** ADR 0002 commits the base manifest and its per-shard coverage.
   Writes after `W` remain in the mutable delta.
5. **Validate:** enter `TieringValidate`. Authoritative reads still use
   `MutableOnly`; selected requests and offline checks compare candidate tiered
   results against the pinned single-mode oracle for the same snapshot.
6. **Cut over:** conditionally commit `Tiered` only if the exact migration,
   manifest, schema, shard histories, and coverage are still current.
7. **Retain:** keep the complete local representation for the configured fast
   rollback window. After it expires, reclaim only under ADR 0003.

Requests that began before cutover finish under `MutableOnly`. Requests resolved
after cutover use `Tiered`. The Raft transition changes read policy; it does not
move, renumber, or re-acknowledge writes.

Migration has no untracked "dual write." One mutable operation stream remains
authoritative throughout.

### 3.12 Validation requirements

Cutover requires result-level evidence, not only counts:

- exact document ID, source, operation version, and tombstone winners;
- query totals, hits, sort values, and aggregations;
- SQL results for supported deterministic queries;
- per-shard history coverage with no missing or duplicate operation identity;
- refresh/read-after-write checks around the bootstrap watermark;
- restart, primary promotion, and sequencer failover during bootstrap; and
- resource/lag measurements proving the configured bounds are respected.

Online shadow comparison is sampled and diagnostic; it cannot prove all
queries. Deterministic migration suites and full inventory/coverage validation
remain mandatory.

### 3.13 Rollback

Rollback is conditional and forward-moving.

#### Before tiered cutover

Cancel the migration through Raft and return to `MutableOnly`. Prepared
artifacts remain inventoried for the janitor. The mutable history was always
authoritative, so no data copy-back occurs.

#### During the fast rollback window

Switch from `Tiered` to `MutableOnly` only after proving the retained local
representation covers the snapshot's complete current logical state. The
conditional Raft transition binds the current migration, manifest, shard
histories, and visibility boundaries.

The immutable pointer is not moved backward. Existing tiered requests finish on
their pinned snapshot, and immutable objects remain retained for diagnosis or
another migration.

#### After local base retirement

An immediate metadata flip is forbidden. Enter
`MutableRollbackPreparing`, reconstruct a complete mutable base from a pinned
manifest plus the current hot suffix, apply version/tombstone winners, replay
later operations, verify coverage and query equivalence, then conditionally
switch to `MutableOnly`.

If reconstruction cannot prove completeness, the index remains `Tiered` and
the rollback fails explicitly.

### 3.14 `ImmutableOnly` compatibility and migration

Current `remote_store` indices remain read-only compatibility indices. Their
version-2 manifests and source-only bundles lack the operation/version and
coverage information required by this ADR.

They do not migrate in place by merely adding local shards. Operators must:

1. recreate or rewrite the dataset into the new versioned immutable base
   format;
2. verify stable IDs, schema, and complete base coverage;
3. create mutable shard histories if the target is `Tiered`; and
4. cut client traffic through an explicit operator-controlled replacement
   procedure.

A future versioned `ImmutableOnly` index may transition to `Tiered` by creating
empty mutable histories above its complete base watermark, but that transition
is not claimed for current data.

### 3.15 Failure and transition table

| Boundary | Authoritative data | Required outcome |
|---|---|---|
| Seal barrier fails | Mutable history | No publication; retry with a new or resumed seal identity |
| Artifact build/upload fails | Mutable history | Keep range pinned as policy permits; inventory partial artifacts |
| Manifest commit is ambiguous | Mutable history until ADR 0002 resolves commit | Do not advance read ownership speculatively |
| Manifest commits while writes continue | Manifest through `W`, mutable history after `W` | New snapshots use exact `(W,V]` partition |
| Primary changes during seal | Fenced shard history | Old authority cannot issue a new valid seal; resume only after continuity proof |
| Migration process crashes | Current Raft lifecycle mode | Reconstruct from migration ID, shard evidence, and publication audit |
| Validation finds mismatch | `MutableOnly` remains authoritative | Block cutover and retain evidence |
| Cutover response is ambiguous | Applied Raft state | Read the committed lifecycle state; never repeat an unconditional transition |
| Object store unavailable after cutover | Mutable writes remain authoritative but sealed backlog grows | Apply configured backpressure before exhausting local bounds |
| Rollback requested after local reclaim | `Tiered` | Rebuild first; no metadata-only flip |
| Compaction rewrites sealed ranges | Current manifest and retained snapshots | Preserve operation versions, tombstones, coverage, and lineage |

## 4. Consequences

### 4.1 Benefits

- Document identity, OCC, retry, update/delete, search, and compaction use one
  logical operation model.
- Ordinary write latency is independent of object-store publication latency
  until configured local capacity applies backpressure.
- Mutable-only small deployments remain supported.
- Query-only and immutable-only deployments do not become write authorities.
- Sealing and migration have explicit resumable identities and safe cutover.
- Rollback cannot erase or reinterpret committed immutable history.
- The design directly supplies ADR 0003's `W`/`V` snapshot partition.

### 4.2 Costs and limitations

- Tiered indices still need routed mutable shard primaries and replicas.
- Splits grow version/tombstone/lineage metadata and require a new format.
- Queries over overlapping incremental splits need efficient version
  resolution until compaction removes superseded data.
- Migration temporarily duplicates local and object-store representations.
- Sustained object-store failure can eventually throttle or stop writes.
- Existing source-only remote indices require rewrite or recreation.
- This ADR does not define mixed schema-generation compatibility; FS-005 does.
- This ADR does not implement external exactly-once retry; FS-011 remains
  required.

## 5. Migration impact

FerrisSearch is pre-1.0. The unified lifecycle introduces incompatible cluster,
WAL/operation, split, manifest, transport, snapshot, and audit formats. Unknown
or legacy data fails closed; there are no compatibility decoders that guess
missing operation versions, tombstones, history UUIDs, or coverage.

Implementation spans:

- Raft lifecycle/migration metadata and conditional commands;
- shard-owned seal barriers, pins, backpressure, and restart recovery;
- versioned immutable split building and validation;
- ADR 0002 publication integration;
- ADR 0003 snapshot/read routing across mutable and immutable work;
- shared version/tombstone query resolution for DSL, count, SQL, and hybrid
  search;
- migration, shadow validation, cutover, and rollback tasks;
- compaction lineage and retention-safe reclamation; and
- settings, health, metrics, task status, and operator diagnostics.

The implementation sequence is:

1. land FS-005 schema and durable-format decisions;
2. add lifecycle metadata without changing current behavior;
3. encode logical versions/tombstones in new split formats;
4. implement resumable seal barriers and publication;
5. implement tiered snapshot execution and version resolution;
6. add migration bootstrap and shadow validation;
7. enable conditional cutover with fast rollback retention;
8. add rebuild rollback and compaction; and
9. enable reclamation only after crash/concurrency evidence.

## 6. Affected roadmap gates and required evidence

- **Gate 0:** this accepted ADR satisfies FS-004's lifecycle decision
  deliverable and unblocks FS-005.
- **Gate 1:** requires lifecycle metadata, fenced publication, explicit
  snapshots, and migration/crash harness foundations.
- **Gate 2:** requires bounded hot-delta ingest, sealing, tiered reads,
  updates/deletes, duplicate suppression, lag/backpressure metrics, and
  local-to-tiered migration.
- **Gate 3:** requires version-aware compaction, retention-safe GC, rollback
  after local retirement, and operational recovery evidence.

Required implementation evidence includes:

1. one operation applied before and after primary promotion retains the correct
   version winner through seal and restart;
2. updates and deletes spanning mutable and immutable ranges match a
   single-mode oracle;
3. writes concurrent with bootstrap appear exactly once after cutover;
4. failed and retried seal publication cannot duplicate a logical range;
5. old and new read modes return equivalent results during validation;
6. an object-store outage preserves acknowledged local history and enforces
   bounded backpressure;
7. cutover and rollback are conditional on exact migration/index identity;
8. process, primary, Raft leader, and manifest sequencer crashes resume without
   a gap;
9. compaction preserves snapshot-visible versions and tombstones;
10. fast rollback works throughout its advertised window;
11. post-retirement rollback rebuilds and verifies before switching; and
12. ordinary CRUD into `ImmutableOnly` or directly into splits remains
    rejected.

## 7. Evidence that would invalidate this decision

Revisit or supersede this ADR if evidence shows any of the following:

- the local shard history cannot serve as a bounded mutable delta without
  unacceptable write amplification or recovery cost;
- version/tombstone resolution across incremental splits cannot meet query
  latency targets even with compaction and pushdown;
- migration requires an atomic cross-index transaction that the in-place model
  cannot express;
- object-store outage backpressure makes the target availability contract
  unacceptable at realistic local-storage budgets;
- retaining local state for fast rollback is prohibitively expensive; or
- a different durable mutable service replaces shard-owned WAL/replication and
  provides a simpler verified operation-ordering boundary.

Any replacement must preserve one logical document/version model,
primary-owned operation identity, asynchronous immutable publication,
gap/duplicate-free snapshot handoff, explicit read-only imports, conditional
migration, and verified rollback.
