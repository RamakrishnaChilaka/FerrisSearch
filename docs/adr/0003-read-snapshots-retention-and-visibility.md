# ADR 0003: Read Snapshots, Retention, And Visibility

- **Status:** Accepted. The protocol is not implemented.
- **Date:** 2026-10-13
- **Accepted:** 2026-10-13
- **Backlog:** [FS-003](../next-50-tasks.md#fs-003--decide-read-snapshots-retention-and-visibility)
- **Roadmap:** Gate 0 deliverable "accepted read snapshot and retention
  semantics"; snapshot-oriented reads in
  [`architecture-roadmap.md`](../architecture-roadmap.md#73-snapshot-oriented-reads)
- **Depends on:** [ADR 0001](0001-write-consistency-and-retry-contract.md) and
  [ADR 0002](0002-fenced-manifest-publication-protocol.md)
- **Scope:** request-stable visibility across local shard readers, immutable
  manifest generations, future mutable hot deltas, compaction replacements,
  retries, and retention. The unified lifecycle representation is FS-004.
  Durable schema compatibility is FS-005.

This ADR selects the logical read and retention contract. Acceptance defines
required future behavior; it does not claim that current search requests pin a
cross-shard snapshot, that old manifest generations are retained safely, or
that local and remote data already share one lifecycle.

## 1. Context

### 1.1 Current behavior

The two implemented engines expose different visibility boundaries:

- `local_shards` search obtains a Tantivy searcher independently on each shard
  when that shard's worker executes. A background or explicit refresh can
  publish a newer reader between shard searches. The coordinator captures
  cluster metadata once, but it does not first resolve or transmit reader
  identities for one request-wide cut.
- Realtime GET is a separate path. By default it consults the live version map
  and WAL when the refreshed reader is behind. `realtime=false` uses the
  search-visible reader. ADR 0001 D8 defines local write refresh behavior;
  `refresh=wait_for` is not implemented.
- A `remote_store` root loads `manifest.current.json` once near the start of
  a search and builds an in-memory split plan from that generation. Retries and
  leaf reassignment reuse those split descriptors, which avoids rereading the
  pointer during that function call.
- The remote root-to-leaf request carries split ID, bundle path, checksum, and
  size, but not the manifest identity, writer epoch, pointer revision, schema
  identity, or a request snapshot ID. The `live_split_ids` field is a cache
  hint, not a retention lease.
- Remote settings contain optional manifest path, generation, checksum, and
  refresh interval fields, but the current read path loads the mutable pointer
  directly; those fields are preserved metadata rather than a snapshot
  protocol.
- Split-reader and artifact pins protect node-local cache files while readers
  are open. There is no object-store manifest retention, compaction lineage,
  snapshot lease, tombstone lifecycle, or object garbage collector.
- A new manifest can become current while a query using the preceding
  generation is still running. This is currently safe only because object
  generations and bundles are not deleted at all.
- `search_after` stabilizes a sort cursor, not the underlying data. Separate
  page requests can observe different refreshes or manifests.

These behaviors provide useful local pinning, but not one declared logical
snapshot across the distributed request. They also do not define how a future
hot delta is replaced by immutable data without a gap or duplicate.

### 1.2 Required properties

The read protocol must provide:

1. one explicit logical snapshot identity per request;
2. stable snapshot use across shard dispatch, leaf retry, and reassignment;
3. exact local-reader and manifest-generation identity;
4. defined read-after-write and publication visibility boundaries;
5. a gap-free, duplicate-free handoff from mutable history to immutable
   splits;
6. version and tombstone rules shared by search and SQL;
7. forward-only rollback and compaction lineage;
8. bounded request and snapshot lifetimes;
9. retention that protects every reachable or in-flight object; and
10. fail-closed behavior when the requested snapshot can no longer be served.

A request snapshot is not a multi-index transaction or a globally simultaneous
wall-clock view. It is a stable vector of per-index and per-shard components
resolved before data execution.

## 2. Alternatives considered

### 2.1 Read whatever is current at each worker

Rejected. This is close to current local-shard behavior. It lets refresh,
publication, or compaction occur between subqueries, so merged results need not
correspond to any declared index state.

### 2.2 Use only the latest manifest generation

Rejected. A manifest identifies immutable splits but says nothing about local
reader commits, future hot-delta visibility, deletes, schema identity, or
in-flight retention.

### 2.3 Use one cluster-wide scalar timestamp

Rejected. FerrisSearch has no transactional timestamp oracle for document
writes. Wall-clock timestamps neither prove complete operation coverage nor
prevent a gap during failover or sealing.

### 2.4 Put every active query and data object in Raft

Rejected. Query and split inventory would make control-plane state grow with
traffic and data volume. Raft owns small authority and lifecycle metadata, not
per-request data-plane inventory.

### 2.5 Stable vector snapshots with bounded leases and logical coverage cuts

Selected. The coordinator resolves immutable manifest identity and per-shard
reader/history identity before execution. Hot and immutable tiers meet at an
explicit contiguous lifecycle watermark. Request-scoped snapshots are protected
by bounded retention grace and local pins; longer-lived named snapshots require
durable leases.

## 3. Decision

### 3.1 Snapshot identity

The canonical internal descriptor is `ReadSnapshot`. Field names may change
during implementation, but the identities and checks do not:

```text
ReadSnapshot {
  format_version
  snapshot_id
  index_uuid
  schema_identity
  cluster_state_version
  resolved_at
  expires_at

  manifest: {
    manifest_path
    manifest_checksum
    generation
    writer_epoch
    pointer_revision
    sealed_through_by_shard
  } | none

  local_shards: {
    shard_id -> {
      history_uuid
      reader_commit_id
      visible_history
      allocation_authority
    }
  }

  max_visible_by_shard
}
```

`schema_identity` is an opaque, fail-closed identity in this ADR. FS-005 defines
its versioned representation. A schema hash alone is sufficient only while
mappings are immutable and its format remains supported.

`reader_commit_id` identifies immutable reader contents, not merely a
Tantivy-local generation counter. `visible_history` describes the exact
operation coverage in that reader. It contains a contiguous processed boundary
and any explicitly represented operations above a gap; a maximum sequence
number alone is not proof of visibility.

The lifecycle protocol uses only a **fenced contiguous history watermark** for
sealing and replacement. It never seals through an unresolved gap. The
watermark is ordered within one shard history and remains unambiguous across
primary-term changes. FS-004 defines its durable encoding with the unified
document lifecycle; implementations must not substitute a later observed
shared checkpoint or wall-clock time.

`snapshot_id` identifies the complete descriptor. The descriptor is
canonicalized and integrity protected when it crosses a trust boundary.
Changing any component creates another snapshot ID.

### 3.2 Snapshot resolution

Every DSL search, query-body count, and SQL execution uses the same resolver:

1. Read one applied cluster-state view and resolve index UUID, schema identity,
   routing, and engine/lifecycle mode.
2. Read and verify the current manifest pointer once, if the lifecycle has an
   immutable component. Pin the exact manifest path and checksum, not just its
   numeric generation.
3. Resolve one serveable reader/history component from each required mutable
   shard.
4. Verify that every component agrees on index UUID, schema identity, and the
   hot/immutable coverage contract.
5. Register bounded local pins, assign a snapshot ID and expiration, and only
   then dispatch query work.

Resolution may run concurrently, but data execution begins only after the
descriptor is internally consistent. If one required component cannot be
pinned, that shard follows the existing partial-search policy. A complete
response never silently substitutes another snapshot. When partial results are
allowed, the response identifies failed components; successful components
still use the resolved descriptor.

The default is **latest at resolution**, not linearizable latest at response.
Writes or publications committed after resolution are outside the request even
if execution has not yet reached a leaf.

One index snapshot is a vector cut. Per-shard components may originate at
different wall-clock instants. FerrisSearch does not claim cross-shard
transactional consistency until a separate transaction protocol exists.

### 3.3 Pinning and transport

Root-to-shard and root-to-leaf requests carry:

- snapshot ID and expiration;
- index UUID and schema identity;
- exact local reader/history identity or manifest identity;
- the assigned split descriptors and logical coverage;
- request/assignment ID; and
- the remaining request deadline.

A retry or reassignment reuses the complete descriptor. It never reloads
`manifest.current.json` or silently chooses a newer reader. A leaf validates
the descriptor before opening data. A cache hit with the wrong checksum,
schema, lineage, or reader identity is a failure, not a fallback.

In-process Tantivy searchers and remote split readers remain pinned for the
duration of their work. Local artifact eviction respects open-reader pins.
Object-store safety does not rely only on process-local pins; section 3.9
defines retention after pointer advancement.

An expired snapshot cannot start new leaf work. Work already admitted before
expiration has only the bounded transport completion allowance included in the
retention calculation. A retry after expiration resolves a new snapshot and is
reported as a new attempt.

### 3.4 Query consistency

All result-producing paths use the same snapshot:

- hits, total count, sort values, and aggregations;
- text and vector legs of hybrid search;
- pushed Tantivy predicates and residual DataFusion execution;
- query-body `_count`;
- SQL semijoin phases and grouped partials; and
- pruning summaries and the split data they select.

A plan may be cached, but execution binds the plan to the current request's
schema and snapshot. A semijoin or multi-phase SQL query cannot resolve a newer
reader between phases.

Distributed output must match a single-node oracle constructed from the same
descriptor. Approximate algorithms may differ only under their existing
explicit opt-in and disclosure rules; they do not relax snapshot identity.

`search_after` by itself remains a cursor over one response ordering and does
not pin later requests. Stable multi-request pagination requires a named
snapshot/PIT handle. Until that handle is implemented, documentation must state
that refresh or publication between pages can change or skip results.

### 3.5 Read-after-write and publication visibility

Visibility is operation-specific:

| Operation and read | Required visibility |
|---|---|
| Local acknowledged write followed by default search | Near-real-time; the write appears only after a reader covering its operation identity is published |
| Local write with successful primary `refresh=true` | A later snapshot resolved against that primary must include the operation or a newer version; reported replica refresh failures retain their ADR 0001 meaning |
| Local write with `refresh=wait_for` | Once implemented, wait for a reader whose exact history coverage includes the operation; never emulate this with an unrelated timed sleep |
| Default realtime GET on the authoritative primary | Uses ADR 0001 D8's live version/WAL path and may be newer than the search snapshot |
| GET with `realtime=false` | Uses a resolved search-visible reader component |
| Remote publication success under ADR 0002 | A later fresh pointer resolution sees that manifest or a descendant; an in-flight older snapshot remains unchanged |
| Ambiguous publication response | Resolve by ADR 0002 operation identity before claiming visibility |

A write acknowledgement alone is not a search-visibility acknowledgement.
`refresh=true` and future `refresh=wait_for` attach visibility to the actual
operation identity. Failover cannot satisfy a visibility wait with a reader
that lacks that operation.

The first production snapshot resolver reads the pointer through a backend
certified for immediate read-after-write on the pointer key. A stale
time-based pointer cache is not part of the safety boundary. If pointer caching
is later introduced, its maximum staleness is explicit and included in the
retention horizon.

### 3.6 Mutable-to-immutable handoff

Each committed manifest declares, per source shard/history, a
`sealed_through` contiguous watermark and the split lineage that covers it.
Each mutable reader snapshot declares exact `visible_history` and a
`max_visible` boundary.

For a manifest sealed through `W` and a mutable snapshot visible through `V`:

- immutable execution supplies logical versions at or below `W`;
- mutable execution supplies logical versions strictly after `W` and no later
  than `V`; and
- `V` must cover `W`, or snapshot resolution fails and retries with a coherent
  component set.

The mutable tier retains sealed operations until the manifest pointer commit is
observably successful. Pointer commit changes which tier owns the range; it
does not by itself delete local history.

During physical overlap, range filtering by logical operation identity prevents
double counting. During a retry, manifest operation identity and split lineage
prevent the same split from being admitted twice. Neither split ID deduplication
nor document ID deduplication alone is sufficient.

All candidates carry document ID, logical operation version, and delete state.
For one snapshot, the newest visible version wins and a visible tombstone
suppresses every older version. This rule is shared by local readers, remote
splits, compaction, search, and SQL. FS-004 defines how current engine modes
produce that common representation.

### 3.7 Publication, compaction, and retry transitions

| Transition | Snapshot rule | Gap/duplicate prevention |
|---|---|---|
| Refresh publishes a newer local reader | Existing requests keep their reader commit; new requests may select the newer commit | Immutable commit identity pins the old contents |
| Manifest pointer advances | Existing requests keep the old path/checksum; new requests resolve the new pointer | Old generation remains retained |
| Leaf fails and work is reassigned | Reuse snapshot and assignment identity | No pointer or reader re-resolution |
| Publication retry returns the prior commit | Reuse the committed manifest operation identity | One logical publication appears once |
| Hot range is sealed into a split | Old snapshots use their prior hot reader; new snapshots use manifest `W` plus hot suffix `(W,V]` | Exact exclusive boundary partitions history |
| Compaction replaces input splits | Publish a new manifest generation with input lineage and equivalent logical coverage | A snapshot uses either old inputs or replacement, never both |
| Delete/tombstone is compacted | Retain it while any retained snapshot can expose an older version | Version winner remains unchanged |
| Reader or manifest cannot be reopened | Fail that snapshot component | Never substitute current data and call it the same snapshot |

Compaction may change physical layout, not logical results for a retained
snapshot. Replacement manifests carry input lineage, output coverage, and a
proof that every live version/tombstone required by retained snapshots remains
represented.

### 3.8 Forward-only rollback

Rollback never moves `manifest.current.json` backward and never mutates an old
generation. It publishes a new fenced generation whose logical contents are
derived from a retained earlier generation, with:

- the current generation as parent;
- a new ADR 0002 operation ID;
- explicit rollback source and reason;
- preserved or advanced schema rules;
- complete split lineage; and
- a new pointer revision and writer epoch validation.

Requests pinned to either generation continue to use their original descriptor.
Rollback cannot resurrect a version hidden by a newer retained tombstone unless
the rollback is an explicit logical write under the document version contract.

### 3.9 Retention and garbage collection

Retention has three layers:

1. **Reader pins:** in-process references protect local reader commits and
   hydrated artifacts while work executes.
2. **Request retention horizon:** every superseded manifest, referenced bundle,
   local sealed range, and required tombstone remains available for longer than
   the maximum allowed request lifetime, leaf retry allowance, cancellation
   propagation bound, pointer-cache staleness, and configured clock-skew
   margin.
3. **Named snapshot leases:** a future externally reusable snapshot/PIT has a
   durable renewable lease. Its descriptor and expiration are discoverable by
   the janitor and relevant shard retention logic. Named snapshots are not
   claimed until this lease is implemented.

Default requests are bounded; an unbounded request cannot hold storage
forever. Implementations define and enforce one maximum snapshot lifetime.
Raising that limit also raises the minimum retention horizon and must be an
explicit operational change.

Object deletion is mark, wait, prove, then sweep:

1. Inventory immutable objects and all retained manifest/operation records.
2. Mark an object unreachable from the current generation, every generation in
   the retention horizon, every unexpired named snapshot, and every unresolved
   ADR 0002 publication.
3. Record the mark durably and wait at least one additional sweep grace.
4. Recompute reachability immediately before deletion.
5. Delete at a bounded rate and record the outcome.

Age alone is never proof of unreachability. A missing audit record, malformed
lineage, unresolved pointer outcome, or unavailable lease inventory blocks
deletion. Node-local cache reaping is not object garbage collection.

Local history, delete tombstones, and version metadata use the same logical
rule: they are reclaimable only after no retained snapshot can require them and
recovery policy no longer pins them.

### 3.10 Named snapshots and API exposure

The internal request snapshot is mandatory. Public named snapshots/PIT are a
later API surface, not implied by this ADR's acceptance.

When implemented, a named snapshot:

- returns an opaque integrity-protected handle;
- has a bounded renewable TTL;
- reuses the same descriptor across requests;
- fails explicitly when expired;
- cannot be extended after its underlying retention lease was lost; and
- is required for guaranteed stable `search_after` pagination.

FerrisSearch must not advertise OpenSearch PIT compatibility until request,
renewal, close, failure, and pagination behavior have a tested compatibility
matrix.

### 3.11 Failure outcomes

| Failure | Required outcome |
|---|---|
| Metadata changes during resolution | Retry resolution from one newer cluster-state view within the request budget |
| Pointer changes after resolution | Continue with the pinned manifest |
| Reader refreshes after resolution | Continue with the pinned reader commit |
| Component expires before dispatch | Resolve a new snapshot; do not reuse the old ID |
| Leaf lacks a referenced artifact but object exists | Hydrate the exact checksum and continue |
| Referenced object is missing or corrupt | Fail the component and surface the identity/cause |
| Old generation was deleted before snapshot expiry | Correctness violation; fail loudly and block further GC |
| Schema identity differs at a leaf | Reject the work; do not coerce or replan silently |
| Hot reader cannot cover the manifest handoff watermark | Reject the component and reacquire a coherent cut |
| Compaction lineage is incomplete | Keep inputs and reject replacement for snapshot resolution |
| Named snapshot lease inventory is unavailable | Refuse renewal and pause deletion |

Partial search behavior remains explicit. A shard failure can produce the
existing partial response where allowed, but a server error cannot be converted
into a zero-hit successful component.

## 4. Consequences

### 4.1 Benefits

- Every successful component has one explainable data view.
- Refresh, publication, retry, and leaf reassignment cannot silently change an
  in-flight request.
- Mutable and immutable data meet at an exact exclusive boundary.
- Compaction and rollback preserve logical results through forward-only
  generations and lineage.
- Retention has a calculable lower bound and fail-closed deletion rules.
- DSL search, count, SQL, and future hybrid lifecycle reads share one contract.

### 4.2 Costs and limitations

- Snapshot acquisition adds a coordination phase before query execution.
- Local engines must retain and reopen reader/history identities, not only the
  latest searcher.
- Root/leaf and shard-search transports gain snapshot and assignment fields.
- Maximum query lifetime becomes an operational storage-retention input.
- Stable cross-request pagination needs durable named-snapshot leases.
- A vector snapshot is not a cross-shard transaction or globally simultaneous
  timestamp.
- This ADR defines the handoff contract but FS-004 still owns the unified
  mutable/immutable representation and migration modes.

## 5. Migration impact

FerrisSearch is pre-1.0. Snapshot, manifest, transport, and lifecycle format
changes do not require backward-compatibility shims.

Implementation must land coherently across:

- snapshot descriptor and integrity encoding;
- local reader commit/history pinning;
- remote manifest and split lineage fields;
- root-to-shard and root-to-leaf transports;
- shared DSL, count, SQL, and hybrid execution context;
- request deadlines and snapshot expiration;
- compaction and forward-rollback publication;
- named snapshot leases before public PIT claims; and
- retention inventory and dry-run garbage collection.

Current remote manifests that lack required coverage and lineage fields cannot
participate in unified hot/immutable reads. They fail closed with the
repository-standard recreate-the-index guidance when the new lifecycle format
is enabled.

Implementation should be staged:

1. pin exact manifest identity in remote requests and add request/assignment
   IDs;
2. add local reader commit identities and request-scoped pins;
3. route search, count, and SQL through one snapshot context;
4. add the FS-004 hot/immutable coverage representation;
5. add compaction lineage and forward rollback;
6. add retention inventory and dry-run reachability;
7. enforce bounded sweep after crash/concurrency evidence; and
8. add durable named snapshots before stable multi-request pagination claims.

## 6. Affected roadmap gates and required evidence

- **Gate 0:** this accepted ADR satisfies FS-003's decision deliverable.
- **Gate 1:** snapshot identity must be explicit in root/leaf requests, and
  concurrent publication plus request tests must keep the chosen generation.
- **Gate 2:** the mutable delta and immutable splits must demonstrate the
  `(W,V]` handoff without gaps or duplicates.
- **Gate 3:** compaction, retention, and GC must preserve retained and in-flight
  snapshots under crash and delayed retry.
- **FS-004:** defines the common logical record and lifecycle modes.
- **FS-005:** defines schema and durable-format compatibility across snapshot
  components.

Required implementation evidence includes:

1. a refresh during multi-shard execution does not mix reader commits;
2. a manifest advance during an in-flight query does not change its split set;
3. leaf retry/reassignment uses the original snapshot;
4. search and SQL match a single-node oracle for the same descriptor;
5. seal/publish overlap returns every logical document version exactly once;
6. publication retry cannot admit one split twice;
7. compaction produces identical results for old and new retained snapshots;
8. a visible tombstone suppresses older versions across tiers;
9. GC preserves a delayed leaf request and every retained generation;
10. forward rollback creates a new generation without breaking old snapshots;
11. expiration fails explicitly rather than substituting current data; and
12. filesystem and certified S3-compatible backends pass the same retention
    and crash-point scenarios.

## 7. Evidence that would invalidate this decision

Revisit or supersede this ADR if evidence shows any of the following:

- request-scoped snapshot acquisition costs exceed the latency target even
  after batching and parallel resolution;
- local reader commits cannot be retained or reconstructed by logical history
  identity without unacceptable storage amplification;
- the hot/immutable exclusive watermark cannot represent failover history
  without a different global operation-ordering primitive;
- bounded retention plus request deadlines cannot protect delayed object-store
  reads on supported backends;
- compaction needs an atomic transaction across multiple independent index
  pointers; or
- a transactional catalog is adopted and provides a simpler, verified snapshot
  and lease boundary.

Any replacement must still provide stable request identity, exact component
validation, operation-based visibility, gap-free tier handoff, version-aware
deduplication, forward-only rollback, and deletion safety for retained and
in-flight reads.
