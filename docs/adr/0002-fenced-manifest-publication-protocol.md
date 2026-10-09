# ADR 0002: Fenced Manifest Publication Protocol

- **Status:** Accepted. The protocol is not implemented.
- **Date:** 2026-10-09
- **Accepted:** 2026-10-09
- **Backlog:** [FS-002](../next-50-tasks.md#fs-002--decide-the-fenced-manifest-publication-protocol)
- **Roadmap:** Gate 0 deliverable "accepted manifest writer-epoch and
  pointer-CAS protocol"; implementation is a Gate 1 prerequisite in
  [`architecture-roadmap.md`](../architecture-roadmap.md#72-single-sequencer-parallel-producers)
- **Scope:** publication of immutable `remote_store` split bundles and manifest
  generations. Read snapshot pinning and retention are FS-003. Compaction and
  deletion use this publication primitive but require their own lifecycle
  decisions.

This ADR selects the first production publication protocol. Acceptance defines
the required behavior; it does not claim that current source provides fencing,
conditional pointer updates, retry deduplication, or safe multi-process
publication.

## 1. Context

### 1.1 Current behavior

`StorageManager::append_split_and_publish` serializes publishers only with a
process-local Tokio mutex. It loads the current manifest, chooses
`generation + 1`, appends a split, writes
`<index_uuid>/manifests/<generation>.json`, and unconditionally overwrites
`<index_uuid>/manifest.current.json`. The pointer contains a generation,
manifest path, and optional ETag, but publication does not use an expected
ETag/version.

`remote_store::publish_docs` generates a random split ID, builds a node-local
Tantivy split, uploads the bundle, and calls that read-modify-write path. A
failure after upload leaves the bundle unreferenced. The dedicated REST
endpoint executes on whichever HTTP node receives it; it is not forwarded to a
unique sequencer.

Consequently, two processes can read the same generation, publish competing
successors, and overwrite each other's pointer. A timeout after an overwrite
has no durable operation identity with which to distinguish a committed
publication from a safe retry. The current filesystem and S3-compatible tests
cover sequential round trips, not concurrent publishers or crash points.

The implementation uses `object_store` 0.13.2. Its conditional write API
distinguishes create-if-absent and update-if-version through `PutMode`, but the
local filesystem implementation does not implement conditional update. Backend
support therefore cannot be inferred from the common trait:

- [`PutMode`](https://docs.rs/object_store/0.13.2/object_store/enum.PutMode.html)
- [`PutOptions`](https://docs.rs/object_store/0.13.2/object_store/struct.PutOptions.html)
- [`LocalFileSystem` conditional update limitation](https://docs.rs/object_store/0.13.2/src/object_store/local.rs.html)
- [Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

### 1.2 Required properties

The first production protocol must provide:

1. one fenced manifest sequencer per index;
2. parallel, independently retryable split production;
3. immutable bundle, operation, and manifest objects;
4. a monotonic writer epoch issued through Raft;
5. manifest parent identity and publication operation identity;
6. a conditional pointer update as the publication linearization point;
7. idempotent recovery after request, process, node, and leader failure;
8. explicit rejection of storage backends without the required semantics; and
9. enough durable inventory to diagnose publication and reclaim abandoned
   artifacts conservatively.

The protocol must not claim general multi-writer manifest merging. It must also
preserve the repository rule that malformed durable data fails loudly.

## 2. Alternatives considered

### 2.1 Keep the process-local publication mutex

Rejected. It prevents races only among tasks sharing one `StorageManager`.
Every HTTP node can currently publish, and a restarted or partitioned process
does not share the lock.

### 2.2 Allow every producer to merge and CAS the manifest

Deferred. Optimistic multi-writer merging needs conflict rebasing, operation
deduplication across rewritten manifests, compaction interaction, and starvation
bounds. It is not needed for the first production protocol and would expand
Gate 1 substantially.

### 2.3 Store manifests or split inventories in Raft

Rejected. Raft owns small control-plane authority records, not growing document
inventories or object payloads. Replicating full manifests would couple metadata
quorum health and log growth to data-plane publication volume.

### 2.4 Use an external transactional catalog

Rejected for the first production protocol. A database such as DynamoDB could
provide conditional metadata writes, but it would add a required service and a
second control plane. FerrisSearch already needs a storage-capability contract
for its supported filesystem and S3-compatible backends.

### 2.5 Single sequencer with Raft epochs and storage pointer CAS

Selected. Producers may build immutable artifacts in parallel. Raft chooses one
sequencer and monotonically increasing epoch per index. The sequencer must fence
the mutable pointer before becoming active, then serializes manifest commits.
Storage CAS, not process identity or lease time, prevents stale writers from
advancing the pointer.

## 3. Decision

### 3.1 Roles and identities

Each publication is identified by:

- `index_uuid`;
- client-supplied `operation_id`, unique within the index;
- `request_fingerprint`, a hash of the canonical logical input and schema
  generation;
- immutable split descriptor including checksum and exact byte sizes;
- manifest `generation` and `parent_manifest`;
- Raft-issued `writer_epoch`; and
- pointer `revision` plus the backend's opaque compare token.

An operation ID is mandatory for production publication. Reusing it with the
same fingerprint resumes or returns the original result. Reusing it with a
different fingerprint is a conflict. Server-generated IDs may be offered only
when the client receives the ID before submitting content; generating one
inside an ambiguously completed request is not retry-safe.

Automatic document IDs inside a publication are derived deterministically from
the operation ID and document ordinal, or are persisted in the operation intent
before bundle construction. Retrying one operation must not silently change
document identities or bundle contents.

The roles are:

- **Producer:** validates input, builds and uploads one or more immutable split
  artifacts, verifies their metadata, and writes a prepared operation record.
  Any node may produce.
- **Sequencer:** validates a prepared operation and creates one successor
  manifest. Exactly one fenced sequencer is active for an index.
- **Coordinator:** accepts the client request and forwards sequencing to the
  active holder. A coordinator does not expose topology errors to the client.

### 3.2 Durable object layout

The implementation will use a new, incompatible manifest format:

```text
<index_uuid>/
  manifest.current.json
  manifests/<generation>/<operation_id>.json
  publications/<operation_id>/intent.json
  publications/<operation_id>/prepared.json
  publications/<operation_id>/committed.json
  publication-attempts/<operation_id>/<attempt_id>.json
  staged/<operation_id>/bundle
```

All objects except `manifest.current.json` are immutable and created with
create-if-absent semantics. A duplicate create succeeds only after reading and
verifying byte-equivalent identity; different content is corruption or an
operation-ID conflict, never an overwrite.

Manifest paths include the operation ID because competing or crashed epochs may
legitimately create different candidates for the same numeric generation.
Generation remains monotonic along the committed parent chain, but the numeric
generation alone is not an object key or a unique commit identity.

The operation records provide durable inventory:

- `intent.json`: operation ID, request fingerprint, schema identity, planned
  artifact paths, and creation metadata;
- `prepared.json`: verified checksum, sizes, document count, split metadata,
  and producer identity;
- `committed.json`: committed manifest identity, generation, writer epoch,
  pointer revision, and observed compare token;
- attempt records: sequencer, epoch, parent, candidate manifest, and terminal
  outcome when one can be recorded.

A crash can leave a later record absent. Recovery derives truth from the
pointer and immutable manifest chain, then repairs missing derived audit
records. Audit records never override pointer reachability.

### 3.3 Manifest and pointer contract

Every manifest contains at least:

```text
format_version
index_uuid
schema_generation and schema_hash
generation
parent_manifest_path
parent_generation
operation_id
request_fingerprint
writer_epoch
published_at
splits
```

Every pointer contains at least:

```text
format_version
index_uuid
pointer_revision
writer_epoch
current_generation
manifest_path
manifest_checksum
last_operation_id
```

Generation zero is an explicit empty pointer with no manifest path. This lets a
new epoch fence an index before its first publication.

Readers validate that the pointer and referenced manifest agree on index UUID,
generation, path checksum, and supported format. Readers do not require the
current Raft writer lease: a committed pointer remains readable while no writer
is active.

### 3.4 Raft writer authority and failover

Raft stores one small authority record per remote-store index:

```text
Unassigned
Fencing { holder_node, writer_epoch, lease_id }
Active {
  holder_node,
  writer_epoch,
  lease_id,
  fenced_pointer_revision,
  fenced_pointer_token
}
```

The exact command and field names are implementation details, but every
transition is conditional on index UUID and the complete preceding authority
identity.

1. **Acquire or replace:** the Raft leader increments the per-index epoch and
   records `Fencing`. An epoch is never reused, including for the same node.
2. **Fence the pointer:** the candidate reads the current pointer and
   conditionally rewrites it with the new epoch and `pointer_revision + 1`
   while retaining the current manifest. If a publisher advances the pointer
   first, the candidate reloads and retries.
3. **Activate:** after the fence CAS is observed, the candidate conditionally
   commits `Active` with the exact pointer revision and compare token.
4. **Renew:** renewal preserves the epoch. It extends liveness authority but
   does not rewrite the pointer.
5. **Replace after expiry, revocation, or holder loss:** start again with a
   strictly greater epoch and complete another pointer fence.

The Raft lease is a liveness mechanism, not the stale-writer safety boundary.
Its expiration may be evaluated by the current leader and may conservatively
pause publication after leader change. Safety comes from the pointer fence and
conditional update.

Revocation becomes effective for object publication when the new epoch's
pointer fence linearizes. An old sequencer commit that wins the pointer CAS
before that fence is a valid old-epoch commit; the candidate must fence the new
head. After the fence, every old writer's expected compare token or epoch is
stale and its update fails. The new holder is not active until Raft records the
completed fence.

A node must stop admitting new sequence attempts when it cannot confirm its
active lease. An already in-flight old-epoch attempt may race the fence, but it
cannot commit after the fence. No wall-clock assumption is part of the safety
proof.

### 3.5 Publication state machine

The logical operation states are:

```text
Received
  -> IntentCreated
  -> ArtifactUploaded
  -> Prepared
  -> ManifestCreated
  -> PointerCommitted
  -> CommitRecorded
  -> Reported
```

Additional terminal classifications are:

- `Conflict`: the operation ID exists with a different fingerprint;
- `Superseded`: a candidate manifest lost pointer CAS and is unreachable;
- `Abandoned`: intent or prepared artifacts have no reachable commit and no
  live attempt;
- `Reclaimable`: retention and reader-snapshot rules permit deletion.

`Reclaimable` is deliberately separate from `Abandoned`. FS-003 defines
snapshot pinning and retention; until it lands, a janitor may inventory and
report abandoned objects but must not delete a possibly reachable manifest or
bundle.

The sequencer commit algorithm is:

1. Confirm the exact `Active` authority and serialize commits locally.
2. Load the operation records.
3. If the operation is already reachable from the current manifest chain,
   return its original committed result.
4. Reject a fingerprint mismatch.
5. Validate the prepared artifact's checksum, sizes, schema identity, and
   immutable object metadata.
6. Read and validate the current pointer and parent manifest.
7. Create a candidate immutable manifest at
   `manifests/<parent generation + 1>/<operation_id>.json`.
8. Conditionally update the pointer using the exact backend compare token,
   expected revision, expected parent, and active writer epoch.
9. Treat successful pointer CAS as the sole commit linearization point.
10. Write the immutable committed and attempt records, then report visibility.

If step 8 loses a same-epoch race, that is an implementation bug because the
active sequencer serializes attempts; fail the attempt loudly. If it loses to a
new epoch, stop and forward or retry through the new holder. Never merge or
overwrite from the stale candidate.

### 3.6 Conditional storage capability

Publication depends on a storage interface stronger than unconditional
`ObjectStore::put`:

```text
create_immutable(path, bytes) -> Created | AlreadyExists(existing identity)
read_with_token(path) -> bytes + opaque compare token
compare_and_swap(path, expected token, replacement) -> Swapped(new token) | Conflict
```

Required semantics are:

- create-if-absent is atomic;
- compare-and-swap is atomic for one pointer key;
- a successful write is immediately readable at that key;
- compare tokens change on every successful pointer replacement;
- returned success means the complete object is durable to the backend's
  documented contract;
- ambiguous transport outcomes remain distinguishable from definite conflicts;
  and
- malformed or unverifiable tokens fail closed.

Backend support is explicit and capability-probed. A node may continue serving
read-only remote data when publication capability is unavailable, but the
publication endpoint fails before producing artifacts and health/diagnostics
name the missing capability. There is no unsafe unconditional fallback.

#### S3-compatible backends

Use conditional `PutObject`: `If-None-Match: *` for immutable creation and
`If-Match` with the observed ETag/version for pointer replacement. Bucket
version IDs may supplement but do not replace the expected-current comparison.

"S3-compatible" configuration alone is insufficient. The backend must pass an
isolated startup or administrative probe that exercises create conflict,
conditional replacement success, stale-token conflict, and immediate readback.
AWS S3 is a target backend. Other services, including RustFS or MinIO, become
certified only when the same contract and crash/concurrency suite passes.

#### Filesystem backend

`object_store` 0.13.2 does not provide conditional update for
`LocalFileSystem`, so FerrisSearch will implement pointer CAS directly:

1. take an OS-released exclusive lock on an index-local lock file;
2. reread and verify the expected pointer revision and content hash;
3. write the replacement to a same-directory temporary file;
4. fsync the file;
5. atomically rename it over the pointer; and
6. fsync the parent directory before reporting success.

Immutable objects use atomic create-new semantics and durable rename from a
same-directory temporary file. This backend is certified only for local
filesystems whose lock, rename, and fsync behavior passes the crash suite.
Shared or network filesystems are unsupported for publication until separately
certified; they do not inherit local-filesystem status by path syntax.

### 3.7 Retry and client outcomes

All retries use the same operation ID and fingerprint.

| Observed result | Client outcome | Retry rule |
|---|---|---|
| Operation is reachable from the committed pointer chain | Success with original generation, manifest identity, operation ID, and `replayed` flag | Stop |
| ID exists with another fingerprint | 409 operation conflict | Do not retry with that ID |
| Failure is proven before pointer CAS and authority remains active | Typed `not_committed` failure | Safe to retry the same ID |
| Pointer CAS reports a definite conflict | Resolve current pointer and authority; internally reroute/rebase only through the active sequencer | Client retries only if the deadline expires |
| Pointer CAS or response has an ambiguous outcome | Typed `indeterminate` failure containing the operation ID | Retry the same ID; never allocate a new ID |
| Active authority cannot be established before artifact creation | Typed unavailable/not-committed failure | Safe to retry the same ID |

A timeout is not evidence of rollback. Before returning `indeterminate`, the
sequencer attempts resolution by reading the current pointer and walking the
bounded retained parent chain or consulting the operation's committed record.
If a later generation includes the operation, the retry returns the original
success.

The success response is emitted only after pointer commit is observed. Missing
post-commit audit records do not change visibility and are repaired
asynchronously.

### 3.8 Crash and race table

| Boundary or race | Durable state | Recovery and client meaning |
|---|---|---|
| Before intent creation | No operation state | Definitely not committed; same-ID retry starts normally |
| After intent, before complete bundle upload | Intent and possibly backend-private partial upload | Resume or replace the same deterministic artifact; janitor may abort stale multipart/temp data after proving no live producer |
| After bundle upload, before prepared record | Intent and immutable bundle | Verify the bundle and write `prepared`, or mark abandoned; do not upload under another identity |
| After prepared record, before manifest creation | Prepared immutable artifact | Any active sequencer may resume |
| During immutable manifest creation | Complete candidate or no visible candidate | Read and verify create result; partial objects must never become visible |
| After manifest creation, before pointer CAS | Unreachable candidate manifest and prepared bundle | Resume CAS if epoch and parent remain current; otherwise record `Superseded` and retain for janitor |
| Pointer CAS definite conflict | Candidate did not become visible through that CAS | Reload pointer; stale epoch stops, active same epoch fails loudly |
| Pointer CAS transport error | Commit status unknown | Read pointer/chain by operation ID; return success if reachable, otherwise `indeterminate` unless non-commit is proven |
| After pointer CAS, before committed record | Publication is visible; audit marker missing | Retry discovers operation in the manifest chain, returns success, and repairs marker |
| After committed record, before response | Publication is visible and fully auditable | Duplicate request returns original success |
| Lease renewal failure before CAS | No new commit may be attempted | Pause and reacquire/forward; prepared data remains resumable |
| New epoch fences while old attempt is in flight | Old attempt may win only before the fence | New holder fences the resulting head; old CAS after fence conflicts |
| Old sequencer retries after fence | Old epoch/token is stale | Definite conflict; it cannot overwrite the head |
| Duplicate same ID and fingerprint | Existing durable state determines progress | Resume or return original result |
| Duplicate same ID, different fingerprint | Conflict evidence | Return 409; never reuse artifacts |
| Filesystem process crash while holding lock | OS releases lock; pointer is old or atomically replaced | Reread revision/hash and resolve exactly as pointer CAS |
| S3-compatible timeout after conditional PUT | Pointer outcome unknown | Conditional readback and operation-chain resolution; no unconditional retry |

### 3.9 Audit, inventory, and reclamation

Raft history records epoch allocation, fencing activation, renewal, and
revocation. Object storage records operation intent, preparation, candidate
manifest, committed result, and terminal attempts. Logs and metrics include
operation ID, writer epoch, pointer revision, parent and candidate manifest
identities, backend result class, and resolution outcome.

The janitor enumerates operation records, not arbitrary key age alone. It never
deletes:

- an artifact referenced by any retained manifest;
- a candidate belonging to an active operation or current fencing epoch;
- data whose pointer-CAS outcome is unresolved; or
- data that may be visible to a pinned reader.

FS-003 supplies the retention and reader-pinning proof needed to move
`Abandoned` to `Reclaimable`. A future compactor must publish replacements
through this same sequencer and operation protocol.

## 4. Consequences

### 4.1 Benefits

- Stale writers cannot advance the pointer after a new epoch fence.
- Parallel producers do not imply parallel manifest writers.
- The pointer CAS is a precise visibility and commit boundary.
- Ambiguous failures are recoverable through durable operation identity.
- Candidate manifests cannot overwrite each other.
- Backend limitations are surfaced before unsafe publication.
- Durable inventory supports conservative cleanup and operational diagnosis.

### 4.2 Costs and limitations

- Publication pauses during sequencer fencing and uncertain authority.
- Raft gains per-index writer-authority state and forwarding commands.
- Publication creates several small audit objects per operation.
- The filesystem backend needs repository-owned locking and durable atomic
  replacement rather than the common `object_store` update path.
- Parent-chain resolution must be bounded by retained audit/index data as
  histories grow.
- This ADR does not provide true multi-writer manifest merging, read snapshot
  semantics, compaction policy, or safe deletion.
- A valid old-epoch commit may linearize before a replacement epoch's pointer
  fence. The new holder must preserve it and fence the resulting head.

## 5. Migration impact

FerrisSearch is pre-1.0 and does not add compatibility shims for this durable
format change. Implementation will bump manifest, pointer, and operation
formats. Existing remote-store indices using the current unfenced layout must
fail closed with clear recreate-the-index guidance; they are not silently
upgraded or published through the new protocol.

The implementation must land coherently across:

- Raft cluster state and commands;
- coordinator forwarding and sequencer lifecycle;
- storage capability detection and backend-specific CAS;
- manifest/pointer/operation formats;
- publication request and typed response contracts;
- metrics, diagnostics, and janitor inventory; and
- filesystem, S3-compatible, transport, failover, concurrency, and crash tests.

Until that implementation lands, documentation must continue to describe
remote publication as experimental and not multi-writer safe.

## 6. Affected roadmap gates and implementation evidence

- **Gate 0:** this accepted ADR satisfies FS-002's decision deliverable.
- **Gate 1:** requires the protocol implementation, concurrent-publisher tests,
  sequencer failover tests, crash-point tests for every row above, backend
  capability tests, and proof that failed staged artifacts remain inventoried.
- **FS-003:** defines read pinning and retention needed for safe reclamation.
- **FS-004:** uses this publication boundary in the unified mutable-to-immutable
  lifecycle.
- **FS-005:** owns longer-term schema and durable format compatibility rules.

Implementation is not complete until tests prove, on the certified filesystem
and S3-compatible backends:

1. two processes cannot lose each other's committed splits;
2. an old epoch cannot commit after the new pointer fence;
3. one operation ID commits at most one logical publication;
4. every ambiguous crash point resolves to committed, not committed, or
   explicitly indeterminate without success-shaped fallback;
5. unsupported conditional semantics disable publication; and
6. inventory and retention rules prevent premature deletion.

## 7. Evidence that would invalidate this decision

Revisit or supersede this ADR if evidence shows any of the following:

- the selected storage backends cannot provide a certifiable conditional
  pointer update or durable atomic filesystem replacement;
- the Raft-fencing handshake admits a stale post-fence pointer commit;
- single-sequencer throughput is below measured Gate 1 workload requirements
  after artifact production is parallelized;
- operation lookup or audit-object growth cannot be bounded without making
  retry resolution unreliable;
- compaction requires atomic publication of multiple independent index heads;
  or
- an external catalog is adopted as a required system component and provides a
  demonstrably simpler authority and transaction boundary.

Any replacement must preserve immutable artifacts, explicit operation identity,
stale-writer fencing, conditional visibility, diagnosable ambiguous outcomes,
and fail-closed backend capability checks.
