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

- Publication is serialized only by a process-local `publish_lock`. There is no
  object-store compare-and-set, writer epoch, or cross-process fencing.
- `append_split_and_publish()` is a read-modify-write sequence. Concurrent
  publishers in different processes can lose manifest entries.
- Standard `_doc`, `_bulk`, `_update`, and `_delete` APIs do not mutate a
  remote-store index. The dedicated publish endpoint is not near-real-time
  ingest.
- There is no object-store split compactor, tombstone lifecycle, retention
  watermark, or garbage collector. Node-local cache reaping is not object GC.
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
- Publish immutable data before the mutable pointer. A failed pointer update
  may leave unreachable immutable objects; it must not expose a partial split.
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
- Any future multi-writer protocol needs conditional pointer publication,
  monotonic writer fencing, idempotent operation identity, and crash evidence.
  A process mutex or "last generation + 1" is not sufficient.
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
