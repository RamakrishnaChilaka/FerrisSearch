# ADR 0005: Schema Generations And Durable Format Compatibility

- **Status:** Accepted. The compatibility contract is not implemented.
- **Date:** 2026-10-13
- **Accepted:** 2026-10-13
- **Backlog:** [FS-005](../next-50-tasks.md#fs-005--decide-schema-generations-and-durable-format-compatibility)
- **Roadmap:** Gate 0 deliverable "logical schema generation and durable-format
  compatibility contract"; schema evolution and format migration in
  [`architecture-roadmap.md`](../architecture-roadmap.md#1012-schema-evolution-and-format-migration)
- **Depends on:** [ADR 0003](0003-read-snapshots-retention-and-visibility.md)
  and [ADR 0004](0004-unified-lifecycle-and-migration-model.md)
- **Scope:** logical schema identity and evolution, durable and transient
  component format versions, mixed-generation reads, rolling compatibility,
  rewrite migration, rollback, and fail-closed preflight. This ADR does not
  implement those mechanisms or promise compatibility for current pre-1.0
  formats.

This ADR separates logical schema evolution from physical encodings. A logical
schema generation describes the meaning of indexed fields. Component format
versions describe how manifests, bundles, WAL entries, snapshots, wire
messages, and partial states are encoded. Neither identity substitutes for the
other.

Acceptance does not claim that current manifests can contain mixed schema
generations, that current nodes negotiate format windows, or that existing
indices can be upgraded without recreation.

## 1. Context

### 1.1 Current behavior

FerrisSearch currently has several independent version checks:

| Component | Current identity and behavior |
|---|---|
| Raft cluster-state snapshot | JSON `ClusterState` with `format_version = 1`; unknown or unreadable state fails with wipe-and-recreate-cluster guidance |
| Join-cluster state | Protobuf `ClusterState.format_version = 1`; the receiver requires the exact version |
| Raft log and metadata | JSON decoding has typed fail-closed errors, but commands do not carry a separately negotiated log format |
| WAL | Length-prefixed bincode entry version 2 plus manifest version 2; only the current entry shape is readable |
| Committed sequence boundary | JSON version 2; only the current shape is readable |
| Sequence probe | Protobuf `sequence_format_version = 1`; callers reject a different version |
| Remote pointer and manifest | JSON pointer version 2 and manifest version 2; only exact versions are accepted |
| Split bundle wrapper | `FSBND` plus version byte 1; a different magic is rejected |
| Embedded split data | Tantivy files plus vector payload; no FerrisSearch split-data format descriptor declares library or logical layout compatibility |
| Partial aggregation state | Unversioned bincode, optionally prefixed by compression marker `FSZ1` |
| Distributed SQL batches | Arrow IPC without a FerrisSearch envelope or negotiated schema/format version |

The current checks live in `CLUSTER_STATE_FORMAT_VERSION` and `ClusterState`
in `src/cluster/state.rs`, cluster-state conversion in
`src/transport/server/conversions.rs`, `HotTranslog` in `src/wal/mod.rs`,
`CommittedBoundaryRecord` in `src/engine/sequence.rs`, manifest and bundle I/O
in `src/storage/mod.rs`, partial aggregation encoding in `src/search/mod.rs`,
and Arrow IPC helpers in `src/hybrid/arrow_bridge.rs`.

Remote manifests carry one `schema_hash` over the current mapping names, field
types, and vector dimensions. A reader compares that hash with the live
mapping and rejects any mismatch. This is a useful integrity check for a
frozen schema, but it cannot express that an old split without a newly added
field is still readable. `compute_schema_hash` and `load_manifest` in
`src/storage/mod.rs` implement that behavior.

Dynamic mapping updates are additive in the current Raft state machine:
`AddMappings` inserts absent fields and leaves an existing field definition
unchanged in `ClusterStateMachine::apply_command` in
`src/consensus/state_machine.rs`. The metadata does not assign a schema
generation, retain historical schema descriptors, or identify which
generation built a WAL operation, Tantivy segment, or immutable split.

Consequently:

- manifest generation, WAL generation, cluster-state version, and schema
  evolution can be confused even though they represent different events;
- roots cannot prove that every selected leaf can decode every selected split
  before work starts;
- an unversioned partial state can fail after other shards have already run;
- rolling upgrades have no declared reader/writer compatibility window;
- physical rewrites and logical mapping changes lack one migration protocol;
  and
- rollback safety depends on undeclared library behavior and retained files.

### 1.2 Required properties

The contract must provide:

1. a monotonic logical schema generation owned by Raft for each index;
2. an immutable canonical descriptor and fingerprint for every generation;
3. explicit rules for additive fields and missing values in older data;
4. rejection of in-place changes that reinterpret existing bytes;
5. independent versions for every durable or distributed encoding;
6. declared reader and writer compatibility windows;
7. request preflight before any shard or split produces a partial result;
8. safe mixed-generation reads within one schema lineage;
9. immutable rewrite migration for old physical formats;
10. rollback that retains a readable representation rather than guessing; and
11. exact, component-specific operator remedies for unsupported data.

The contract must preserve ADR 0003 snapshot stability and ADR 0004's single
logical document/version model.

## 2. Alternatives considered

### 2.1 Continue using one schema hash as an equality gate

Rejected. Equality cannot distinguish a safe additive field from an
incompatible type change. Weakening equality ad hoc would turn an integrity
check into an implicit compatibility decoder.

### 2.2 Use manifest generation as schema generation

Rejected. A manifest can advance for publication, compaction, retention, or
rollback without changing schema. A schema can also advance before a new
immutable manifest exists. Coupling them creates false transitions and makes
snapshot identity ambiguous.

### 2.3 Use one global format version for every component

Rejected. WAL, Raft, manifest, bundle, engine files, Arrow IPC, and partial
states have different writers, lifetimes, and migration constraints. A global
number either forces unnecessary rewrites or hides which decoder is required.

### 2.4 Let readers try decoding and fall back

Rejected. Trial decoding can reinterpret bytes, hide corruption, and fail only
after partial distributed execution. Compatibility is declared by metadata
and capabilities, not inferred from successful deserialization.

### 2.5 Require every schema change to create a new index

Rejected for additive fields. It prevents normal dynamic mapping and makes
immutable historical data unnecessarily expensive to rewrite. It remains the
required path for incompatible semantic changes.

### 2.6 Monotonic schema generations plus component format capabilities

Selected. Raft serializes logical schema lineage. Each physical or wire
component has its own explicit version and compatibility window. Roots,
writers, and migration tasks preflight those declarations before execution.

## 3. Decision

### 3.1 Separate identities

The following counters and identities are independent:

| Identity | Meaning |
|---|---|
| `schema_generation` | Monotonic logical field semantics for one index UUID |
| `schema_fingerprint` | Integrity hash of one canonical schema descriptor |
| `manifest_generation` | One immutable publication/compaction lineage step |
| `cluster_state_version` | One applied cluster metadata mutation |
| `wal_generation` | One local translog file-retention generation |
| component `format_version` | Physical encoding understood by a component decoder |

No implementation may compare or derive one of these identities from another.

### 3.2 Raft-owned schema lineage

Each index has:

```text
SchemaState {
  current_generation
  generations {
    generation -> {
      parent_generation
      fingerprint
      semantics_revision
      fields
      dynamic_policy
    }
  }
}
```

Generation 1 is created with the index. Every accepted logical schema mutation
is one Raft command that:

1. compares the expected current generation;
2. validates the proposed descriptor against its parent;
3. assigns `current_generation + 1`;
4. stores the complete immutable descriptor and fingerprint; and
5. advances cluster-state version in the same applied mutation.

A generation is scoped to one index UUID. Delete and recreate starts a new
lineage even if the index name and mappings are identical.

The canonical descriptor uses exact UTF-8 field names and stable explicit
tokens for:

- field type;
- vector dimension;
- index/search analyzer or normalization semantics when supported;
- indexing, doc-value, stored-field, null, and multi-value semantics when
  supported;
- dynamic mapping policy; and
- a `semantics_revision` covering cross-cutting interpretation such as SQL
  identifier binding and date/numeric coercion.

Canonicalization must not use Rust `Debug` output, map iteration order, or a
third-party library's internal schema serialization. The fingerprint includes
an algorithm prefix and verifies the descriptor; it does not decide
compatibility by itself.

Historical descriptors remain reachable while any retained WAL, local reader,
snapshot, manifest, split, migration, or rollback target references them.

### 3.3 Allowed and rejected logical changes

| Change | In-place generation advance |
|---|---|
| Add a mapped field with a new exact name | Allowed |
| Add future field options that affect only newly populated values and have explicit old-value semantics | Allowed only when this contract is extended for that option |
| Change dynamic policy | Allowed as a new generation; it affects future admission only and cannot reinterpret stored fields |
| Change field type | Rejected |
| Change vector dimension | Rejected |
| Change analyzer, normalizer, date interpretation, numeric width, indexing/doc-value behavior, or multi-value meaning | Rejected |
| Rename, remove, or reuse a field name | Rejected |
| Change quoted or unquoted SQL identifier resolution | Rejected within a lineage |
| Add an alias that can bind an existing query to a different physical field | Rejected until a separate alias contract exists |

An incompatible logical change requires a new index UUID or a verified rewrite
into a new schema lineage followed by an explicit operator cutover. A physical
rewrite cannot legalize a semantic reinterpretation inside the old lineage.

Concurrent dynamic discovery of the same field is serialized by Raft. The
winning definition becomes authoritative. A conflicting inferred definition
is a mapping error; it is never silently coerced to the winner.

### 3.4 Schema identity on operations and stored data

Every durable logical operation introduced by the unified lifecycle records
the schema generation under which its source was validated. WAL, replication,
recovery, sealing, snapshots, and audit records preserve that value.

Every immutable split records:

- index UUID and schema generation;
- schema fingerprint;
- split-data format version;
- bundle format version;
- engine format identity required to open embedded files; and
- the ADR 0004 logical history and version metadata.

A manifest records its own format version and the schema generation selected
for the read snapshot. Each split descriptor retains its own schema and
physical format identities; the manifest does not assume that every retained
split was built by the same schema generation or format.

The current whole-manifest `schema_hash` is replaced by per-generation
fingerprints and explicit ancestry checks. Implementations must not bypass a
hash mismatch merely because generation numbers appear compatible.

### 3.5 Mixed schema-generation reads

A request binds once to a target schema generation `G` as part of its ADR 0003
snapshot. Every local reader, WAL overlay, and immutable split selected for the
request must declare generation `S` such that:

1. `S == G`, or `S` is an ancestor of `G`;
2. every edge from `S` through `G` is an allowed additive transition; and
3. the executor supports the target generation's `semantics_revision`.

A component from a newer generation than `G`, a different lineage, an unknown
generation, or a lineage containing an incompatible edge is rejected before
execution.

For a field added after `S`, an old local segment or split supplies a typed
null/missing column:

- `exists` is false;
- predicates that require a value do not match;
- aggregations skip the missing value unless their documented missing-value
  option supplies one;
- sorting uses the request's explicit or stable default missing policy;
- `_source` remains the source stored with the document and does not gain a
  synthesized key; and
- an old component contributes no vector candidates for a field added later;
  the target generation must contain the requested vector field.

The query planner binds field names and result types against `G`, not against
whichever split or Arrow batch is encountered first. Quoted identifiers retain
their exact name. Unquoted identifier canonicalization is defined by the
generation's `semantics_revision` and must be stable across all participating
nodes. A leaf may not rename, case-fold, infer, or widen a field independently.

### 3.6 Component format declarations

Every FerrisSearch-owned durable or distributed envelope has:

```text
FormatCapability {
  component
  min_read_version
  max_read_version
  write_version
}
```

The first implementation may deliberately set all three values to one exact
version. Supporting a range requires explicit decoders and cross-version tests;
adding optional serde or protobuf fields alone does not declare compatibility.

The registry covers at least:

- Raft log command, vote/metadata, and cluster-state snapshot;
- join-cluster state;
- WAL entry, WAL manifest, and committed boundary;
- shard identity/fence and future persisted shard snapshot;
- remote pointer, manifest, split descriptor, bundle wrapper, and split data;
- publication operation/audit records;
- root-to-shard and root-to-leaf request/response envelopes;
- partial aggregation state; and
- FerrisSearch's Arrow IPC envelope and expected Arrow schema semantics.

Checksums protect bytes after a format has been selected. They are not format
or schema versions.

Third-party formats are wrapped by a FerrisSearch version that records the
library format identity and the FerrisSearch logical layout contract. A
Tantivy index or Arrow stream being readable by the linked library is
insufficient evidence that FerrisSearch can interpret it correctly.

### 3.7 Read and write compatibility windows

A node publishes its component capabilities and schema-semantics range in
cluster membership. Capabilities are tied to the exact running build and are
removed when that node leaves or restarts with a different build.

A durable writer may emit version `W` only when:

1. the committed per-index format policy authorizes `W`;
2. every node currently eligible to own, recover, sequence, or read that data
   advertises support for `W`;
3. every retained rollback target can still be served under the declared
   rollback policy; and
4. the write records the chosen version rather than relying on a process
   default.

Readers accept only versions in their declared range and in the committed
policy's readable set. Unknown versions, known-but-disabled versions, missing
required envelopes, and contradictory nested versions fail closed.

Protobuf unknown-field tolerance is not FerrisSearch compatibility. A message
whose required semantic context is absent is rejected even if protobuf can
decode its known fields.

### 3.8 Distributed preflight and no partial execution

The coordinating root validates the complete request snapshot before
dispatching data work:

1. resolve exact index UUID, target schema generation, manifest identity, local
   reader identities, and selected split descriptors;
2. validate schema ancestry and every component format declared by that
   snapshot;
3. choose only leaves whose committed capability advertisement covers those
   schema and format requirements;
4. include the exact required identities and versions in each request; and
5. require the leaf to revalidate them before opening a reader or running a
   collector.

If any required component lacks an eligible executor, the whole request fails
before a shard or split executes. FerrisSearch must not return successful hits,
counts, SQL rows, or aggregations from the compatible subset.

A capability change after preflight is handled as an explicit retry or
reassignment under the same ADR 0003 snapshot. The replacement leaf must pass
the same checks. It must not resolve current schema or manifest state.

Partial aggregation and Arrow payloads carry versioned FerrisSearch envelopes
including schema generation, schema fingerprint, payload format, and result
schema. The coordinator validates the envelope before decoding or merging.
Trailing bytes, unknown variants, mismatched result schemas, and compression
markers unsupported by the declared payload version are errors.

### 3.9 Upgrade protocol: expand, rewrite, contract

A compatible rolling change uses these phases:

1. **Expand readers:** deploy nodes that read the old and proposed new
   versions but continue writing the old versions.
2. **Prove coverage:** verify committed capabilities for every eligible role,
   retained snapshot, recovery source, and rollback target.
3. **Authorize writers:** commit the new write versions and schema semantics
   policy through Raft.
4. **Advance schema if needed:** commit additive schema generations separately
   from the physical format policy.
5. **Rewrite:** create replacement immutable splits or snapshots in the new
   format while preserving logical operation versions, tombstones, lineage,
   and checksums.
6. **Validate:** compare old and rewritten representations under identical
   snapshots and result-level oracles.
7. **Publish forward:** commit a new manifest descendant that selects the
   rewritten representation.
8. **Retain rollback:** keep an independently readable old representation for
   the configured rollback and snapshot-retention window.
9. **Contract:** remove old read versions only after no retained data,
   snapshot, recovery path, or rollback target needs them.

The system reports phase, blockers, bytes/items remaining, oldest required
format, and the exact node or retained object preventing contraction.

Mixed old/new roots and leaves are supported only during a declared window
that satisfies these phases. A new writer is never enabled merely because a
majority or the Raft leader understands its format.

### 3.10 Rewrite migration

Physical formats are migrated by copy-and-verify, never by mutating a
published object or live WAL generation in place.

An immutable rewrite:

1. pins one ADR 0003 source snapshot;
2. reads only declared supported source formats;
3. emits deterministic target artifacts with the same logical document
   winners, tombstones, operation versions, and schema meaning;
4. records source lineage, target versions, operation identity, and checksums;
5. verifies query and history equivalence;
6. publishes through the ADR 0002 fenced sequencer as a new manifest
   descendant; and
7. retains source artifacts until snapshot, migration, and rollback leases
   permit reclamation.

WAL, Raft, or local shard formats that lack an implemented online migration
remain restart or upgrade blockers. The operator must use the component's
documented recreation procedure; the system must not partially rewrite files
while a shard or Raft store is live.

### 3.11 Rollback

There are two distinct rollbacks:

- **Data rollback** publishes a new manifest generation that selects retained
  old-format artifacts or the ADR 0004 retained local representation. It never
  rewinds the pointer or mutates an old manifest.
- **Binary rollback** is allowed only when the old binary's advertised reader
  ranges cover the active Raft, WAL, manifest, bundle, split, snapshot, wire,
  and partial-state formats and its schema-semantics range covers every active
  generation.

If new-only durable state has been committed and no compatible old
representation remains, binary rollback is blocked. Operators must first
rewrite forward into a mutually readable format or restore/recreate according
to the component runbook. A startup flag cannot bypass the check.

### 3.12 Pre-1.0 transition

This ADR defines the target policy; it does not retroactively make current
formats compatible.

The first implementation may introduce incompatible Raft, WAL, manifest,
bundle, split, snapshot, wire, partial-state, and schema metadata. Current
pre-1.0 data that lacks the required identities fails closed rather than being
assigned guessed generation 1.

Operator remedies remain component-specific:

- unsupported index, shard, WAL, manifest, bundle, or split data: **recreate
  the index**; and
- unsupported Raft log, metadata, or cluster-state snapshot: **wipe the node
  data directories and recreate the cluster**.

Errors identify the component, found version or missing identity, supported
range, index UUID when applicable, and blocked operation. They retain the
underlying decode or I/O cause. Unsupported format is not reported as an empty
index, skipped split, partial success, or retryable compatibility fallback.

## 4. Consequences

### 4.1 Benefits

- Additive mappings no longer require every historical split to have the same
  mapping hash.
- Schema meaning remains stable while physical encodings evolve independently.
- Rolling upgrades have an auditable safety predicate instead of decoder
  optimism.
- Roots reject unsupported work before producing partial distributed results.
- Rewrite, retention, and rollback share one lineage and capability model.
- Format errors become diagnosable and carry the correct operator remedy.

### 4.2 Costs and limitations

- Raft metadata retains historical schema descriptors and node capabilities.
- Every durable and distributed envelope needs explicit version ownership.
- Mixed-generation readers must materialize typed missing columns and bind
  plans against the target schema.
- Upgrade orchestration must account for every eligible node and retained
  object, not only a quorum.
- Rewrite temporarily duplicates storage and consumes query/indexing capacity.
- Some pre-1.0 upgrades will still require index or cluster recreation.
- This ADR does not define field aliases, in-place type promotion, cross-index
  schema transactions, or a stable public plugin ABI.

## 5. Migration impact

Implementation spans:

- Raft-owned schema descriptors, generation compare-and-set, and format policy;
- node capability publication and role-aware eligibility checks;
- schema generation on WAL operations, replication, recovery, and sealing;
- per-split schema and physical-format metadata;
- manifest lineage that permits compatible mixed generations;
- versioned root/leaf, partial aggregation, and Arrow envelopes;
- planner/executor typed-null behavior and stable identifier binding;
- rewrite, validation, progress, rollback, and contraction tasks;
- startup and request preflight with typed unsupported-format errors; and
- operator diagnostics and runbooks.

The implementation sequence is:

1. introduce canonical schema descriptors and immutable generation metadata;
2. add the component capability registry without changing current exact
   version behavior;
3. carry schema and format identity through operations, snapshots, manifests,
   splits, and transport;
4. add root and leaf preflight;
5. implement additive mixed-generation reads and result-level tests;
6. implement immutable rewrite and validation;
7. add forward data rollback and binary rollback guards; and
8. enable format contraction only after retained-reference inventory exists.

## 6. Affected roadmap gates and required evidence

- **Gate 0:** this accepted ADR satisfies FS-005's schema and durable-format
  compatibility decision deliverable.
- **Gate 1:** manifests, shard snapshots, publication, and recovery must carry
  explicit schema and physical format identities and reject unsupported
  versions before mutation or partial execution.
- **Gate 2:** tiered reads and sealing must preserve operation and schema
  identity across mutable and immutable representations.
- **Gate 3:** mixed-generation execution, rewrite compaction, retention-safe
  contraction, and rollback must be operationally verified.
- **FS-017:** implements manifest, split, schema, and physical format fields.
- **FS-022:** defines the persisted shard snapshot under this policy.
- **FS-048:** implements mixed schema-generation query execution.

Required implementation evidence includes:

1. a field added at generation `G+1` returns typed null/missing semantics from
   generation `G` local segments and splits across DSL, count, SQL, sorting,
   and aggregations;
2. type, vector-dimension, analyzer, rename, removal, and identifier-semantics
   changes are rejected before metadata mutation;
3. concurrent dynamic mapping conflicts choose one Raft-authoritative type and
   reject the loser before document mutation;
4. old/new roots and leaves execute only snapshots covered by every selected
   node's declared window;
5. one unsupported split, partial-state, Arrow envelope, WAL, or snapshot
   version fails the whole operation before partial success;
6. a leaf restart or reassignment revalidates the original snapshot and
   capabilities;
7. rewrite preserves exact logical document winners, tombstones, operation
   versions, schema meaning, and query results;
8. crashes at every rewrite and publication boundary leave either the old or
   new representation reachable, never a mixed implicit state;
9. forward data rollback preserves in-flight old snapshots;
10. binary rollback is accepted while all active formats are readable and
    blocked after a new-only contraction;
11. retained-reference inventory prevents deletion of the last rollback-
    readable generation; and
12. unsupported index and Raft formats emit their distinct exact operator
    remedies.

## 7. Evidence that would invalidate this decision

Revisit or supersede this ADR if evidence shows any of the following:

- typed-null materialization across realistic mixed generations cannot meet
  correctness or bounded-memory requirements;
- retaining complete schema descriptors in Raft creates unacceptable control-
  plane growth and an external immutable registry can preserve the same
  authority and availability;
- Tantivy or Arrow cannot expose enough stable format identity to build a
  verified FerrisSearch envelope;
- all-node writer gating makes supported rolling operation unavailable and a
  narrower, formally defined role/ownership predicate is proven safe;
- rewrite cannot preserve ADR 0004 operation versions and tombstones without a
  different immutable representation; or
- a future stable release requires a compatibility horizon different from the
  expand/rewrite/contract model.

Any replacement must keep logical schema meaning independent from physical
encoding, reject semantic reinterpretation, fail before partial distributed
execution, and preserve a verified rollback-readable representation.
