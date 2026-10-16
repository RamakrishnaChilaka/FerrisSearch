---
description: "Use for object-store backends, remote manifests, split bundles, hydration, local split caching, root/leaf scheduling, and remote-store lifecycle work."
applyTo: "src/storage/**,src/engine/remote_store.rs,tests/remote_store_s3_integration.rs,docs/remote-store-*.md,scripts/remote_store_*.sh"
---

# Storage And Remote-Store Instructions

## Current Architecture

`StorageManager` in `src/storage/mod.rs` owns two different resources:

- an `object_store::ObjectStore` backend rooted at a bare path, `file://`, or
  `s3://bucket[/prefix]`; and
- a node-local work directory used for split staging, downloaded bundle
  artifacts, extraction, and cache bookkeeping.

The `remote_store` index engine is a shardless read path:

1. A dedicated publish API builds a deterministic Tantivy split bundle.
2. The bundle is uploaded under `<index_uuid>/splits/<split_id>/bundle`.
3. An immutable `manifests/<generation>.json` is written.
4. The mutable `manifest.current.json` pointer is overwritten.
5. A query root loads the current manifest, prunes eligible splits by summary,
   asks data-node leaves for cache/load status, and assigns split batches.
6. Leaves download and verify bundles, reuse `RemoteSplitReaderCache` entries,
   execute search, and return hits and partial aggregations.

`src/storage/mod.rs` owns object layout, checksums, manifest models, bundle
upload/download, and local cache mechanics. `src/engine/remote_store.rs` owns
query pruning, leaf selection, batch execution, reader reuse, and result merge.
Do not collapse these responsibilities into API handlers.

## Current Limits That Must Stay Explicit

- `StorageManager` provides conditional object-store primitives
  (`create_immutable`, `read_with_token`, `compare_and_swap`,
  `compare_and_swap_optional`) and capability probing (FS-015); Raft replicates
  per-index `ManifestWriterAuthority` leases and monotonic epochs (FS-016);
  manifests and pointers record parent lineage, writer epoch, operation ID,
  request fingerprint, schema generation, and format capability windows
  (FS-017); `remote_store` publication stages immutable bundles plus
  `intent`/`prepared`/`committed`/attempt records and sequences commits through
  the active fenced sequencer (FS-018/FS-019); and `StorageManager` provides a
  non-mutating reachability inventory (`build_reachability_inventory`, FS-020)
  and a conservative dry-run-first janitor (`run_janitor`, FS-021). However,
  general optimistic multi-writer manifest merging, split compaction, and
  versioned document deletion are not implemented yet.
- Standard `_doc`, `_bulk`, `_update`, and `_delete` APIs do not mutate a
  remote-store index. The dedicated publish endpoint is not near-real-time
  ingest.
- There is no object-store split compactor or tombstone lifecycle yet. The
  FS-021 janitor defaults to `dry_run = true` and in destructive mode reclaims
  only unpinned `Abandoned` and `Superseded` objects outside the active-lease
  and retention horizons when listing and lineage are healthy. Node-local cache
  reaping remains separate from object-store reclamation.
- Split hydration currently downloads the full bundle into memory before
  writing/extracting it; do not describe this as streaming hydration.
- Root scheduling performs per-query leaf status discovery. There is no
  continuously maintained leaf inventory or admission controller.
- No hotcache sidecar is consumed yet, and remote split readers do not support
  the vector path.
- Manifest generations do not yet define a cross-engine read snapshot with
  mutable local shards.

These are roadmap blockers, not implementation details to hide with retries or
best-effort fallbacks.

## Manifest And Publication Invariants

- Index UUID, manifest generation, split ID, checksum, schema hash, and bundle
  path are identities. Validate them and fail closed on disagreement.
- Pointer and manifest format versions must match the current constants.
  Unknown versions and placeholder checksums are unsupported pre-1.0 formats;
  return the shared recreate-the-index error instead of adapting or reporting
  an "unsupported" success entry.
- Publish immutable data before the mutable pointer. A failed pointer update
  may leave unreachable immutable objects; it must not expose a partial split.
- Immutable split bundles exclude all mutable WAL state, including
  `translog.committed`, the manifest, and generation files. Cleanup failure
  aborts publication; never upload a dangling checkpoint or initialize a
  reader's WAL from it to hide missing protected history.
- Never use `unwrap_or_default()` when decoding a pointer, manifest, summary, or
  checksum-bearing payload.
- Never overwrite a manifest generation with different content.
- Duplicate split IDs in one manifest are invalid.
- Mapping fingerprints use `compute_schema_hash()` and must be deterministic.
  A live mapping mismatch rejects the manifest rather than coercing a reader.
- Field summaries are pruning aids, not truth. Missing, capped, unknown, or
  unsupported summaries must keep a split; they must never cause false-negative
  pruning.
- Mapped keyword `field_terms` summaries must use the same recursive
  flattening, string/number/boolean coercion, null skipping, and per-field
  deduplication as `HotEngine` indexing. If every distinct value cannot fit
  within the summary cap, omit that field's summary entirely.
- Publish validates every source before staging. Top-level `_id` is request
  metadata and is stripped before indexing; every other shared reserved
  document key is a `400 mapper_parsing_exception` and must not create a bundle
  or manifest.
- Any future multi-writer protocol needs conditional pointer publication,
  monotonic writer fencing, idempotent operation identity, and crash evidence.
  A process mutex or "last generation + 1" is not sufficient.
- Conditional storage (`src/storage/conditional.rs`) must preserve atomic
  create-if-absent (`create_immutable`), verified readback with integrity-checked
  `CompareToken`s (`read_with_token`), and atomic pointer replacement
  (`compare_and_swap`). Malformed, tampered, or cross-backend compare tokens
  must fail closed with `ConditionalStorageError::InvalidToken`, and ambiguous
  transport errors must remain `ConditionalStorageError::Indeterminate` rather
  than masquerading as definite conflicts.
- Local filesystem conditional writes must keep companion `.ferris.lock#0`,
  `.ferris.state#0`, and `.ferris.*.tmp#0` files in the target's parent
  directory with `#0` suffixes so `LocalFileSystem::list` ignores them, and
  must never unlink or rename over the `flock` lock file itself.
- Uncertified backends (including shared/network filesystems and S3 stores
  without working `If-Match`/`If-None-Match` and `ETag` support) must fail
  closed before staging or uploading publication artifacts while leaving
  read-only search/verify and `/_cluster/health` diagnostics operational.
- Any future GC must prove an object is unreachable from every retained
  snapshot and active reader before deletion.

## Cache And Hydration Invariants

- Cache paths are keyed by index UUID and split ID, not index names.
- A `.done` marker is written only after checksum verification and successful
  extraction. Partial directories are not warm artifacts.
- Concurrent fetches use per-split single-flight locking.
- Open reader-cache entries pin their split artifacts. Reaping must not remove
  pinned data.
- Cache accounting includes artifact size and last access; a zero budget means
  "no configured cap", not "delete everything", unless source explicitly says
  otherwise.
- Keep object-store bytes, extraction work, open-reader memory, and Tantivy
  reader memory distinct in metrics and future reservations.
- Route object GET/PUT operations through `StorageManager`'s metered helpers.
  Record one bounded request outcome per attempt and successful logical
  transfer bytes only after payload completion; do not bypass the counters
  with direct backend calls.
- Do not hold global cache locks across network or filesystem awaits.

## Root And Leaf Execution

- Only nodes with `NodeRole::Data` are eligible leaves; master-only nodes may be
  roots.
- Rendezvous ranking must be deterministic for the same index, manifest,
  split, and node set.
- Current preference order is reader-warm, artifact-warm, lower inflight
  bytes, then lower queue depth among highly ranked candidates.
- Batch RPCs return per-split success or error. Do not silently drop failed
  splits or decoded hits.
- Pruning counters distinguish published, candidate, pruned, and assigned
  splits. Preserve those meanings across REST, SQL, and EXPLAIN ANALYZE.
- A cache/load status RPC is advisory. Correctness cannot depend on a leaf
  still having the reported artifact when work arrives.

## Required Tests

For manifest or lifecycle changes, add:

- local-object-store unit tests for exact object layout and failure order;
- real S3-compatible tests when conditional/object-store behavior matters;
- cross-process or independently constructed writer tests for publication;
- crash/failure injection between bundle, manifest, and pointer writes;
- corruption and checksum mismatch tests;
- schema mismatch and unknown-version rejection;
- cache single-flight, pinning, and partial-download cleanup;
- multi-node root/leaf tests with shared object storage and distinct node-local
  caches;
- result-level pruning tests proving no candidate split was incorrectly
  removed; and
- metrics/assertions for bytes fetched, cache hits, retries, and per-split
  failures when those surfaces change.

Do not substitute an in-process mutex test for a multi-writer storage protocol.

## Roadmap Alignment

Remote-store changes should advance one of the canonical lifecycle stages in
`docs/architecture-roadmap.md`: fenced publication, hot-delta sealing,
snapshot-aware reads, versioned deletes, compaction lineage, retention-safe GC,
or bounded hydration/scheduling. State the stage and its prerequisite in the
change description.
