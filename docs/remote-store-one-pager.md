# Remote Store 1-Pager

## Current Status (April 2026)

- ✅ `IndexEngine::{LocalShards, RemoteStore}` wired end-to-end; both engines are creatable.
- ✅ `remote_store` indices are shardless (zero shard routing) at create time.
- ✅ Writes to `remote_store` indices are rejected with `501 Not Implemented` + `illegal_argument_exception` across every ingest handler (single `_doc`, `_bulk`, `_update`, `_delete`). `IndexEngine::supports_writes()` + `api::reject_write_if_engine_read_only()` gate all write paths.
- ✅ Object-store-backed `StorageManager` (filesystem backend via `object_store`) wired into `AppState` and `Node::new`, rooted at `<data_dir>/_remote_store/`.
- ✅ Manifest I/O: `load_manifest_pointer`, `load_manifest`, `load_current_manifest`, `publish_manifest`, `append_split_and_publish` (serialized per-storage via an in-process `publish_lock`).
- ✅ Coordinator-local read path: `src/engine/remote_store.rs::search()` loads the current manifest, opens each published split as a `HotEngine`, runs `search_query`, merges results into the same `DistributedDslSearchResult` the `local_shards` path uses. Schema hash is computed from `IndexMetadata.mappings` and validated on manifest load (fails closed on mismatch).
- ✅ Split publish API: `POST /{index}/_remote_store/publish` accepts `{ "docs": [...] }`, builds a Tantivy split under `<storage_root>/<uuid>/.staging/<split_id>/`, atomically renames into `splits/<split_id>/`, then appends a new `Published` entry via `append_split_and_publish` (bumps manifest generation). Runs locally on the receiving node; 100K-doc hard cap per publish.
- ❌ Leaf gRPC fan-out (root/leaf split distribution) — not yet implemented.
- ❌ Tarball/hotcache/LRU split cache — not yet implemented; split dirs must currently be laid out as `HotEngine` data directories (`<bundle_path>/index/meta.json`).
- ❌ Janitor/GC for splits orphaned by failed publishes or stale manifest generations — not yet implemented.
- ❌ k-NN on `remote_store` — currently rejected with 501.

## Decision

`remote_store` is a shardless, read-optimized engine.

- Raft owns logical index registration: `IndexMetadata`, mappings, immutable settings, and index existence.
- Object storage owns the published split inventory and is the source of truth for what is searchable.
- Search branches on `IndexSettings.engine` before any shard routing. `ShardManager` and `shard_routing` are not used on the `remote_store` read path.
- `remote_store` should ship with distributed root/leaf execution from day one, not as a single-node coordinator-local fallback.
- Any node may still accept the client request, but the runtime should distinguish three responsibilities:
    - ingress coordinator: receives the REST request and resolves the target index
    - root: fetches the manifest, prunes splits, assigns work to leaves, and merges results
    - leaf: owns remote split caches, opens split readers, executes search or SQL on assigned splits, and returns partials
- Root and leaf are execution roles, not client-visible leader requirements. The coordinator pattern remains intact: the client still talks to any node.
- Implementation choice: use the Rust `object_store` crate as the storage abstraction boundary. Do not bind the first implementation directly to S3 or Azure SDKs.
- Testing and local development should use a filesystem-backed object store (`file://...`) through the same abstraction. Cloud object stores are optional deployment backends, not a development prerequisite.

## Object Layout

```text
<object_store_uri>/<index_uuid>/
  manifest.current.json
  manifests/000000000042.json
  splits/<split_id>/bundle.tar.zst
  splits/<split_id>/hotcache.bin
```

- `manifest.current.json` is the only mutable object. It points to the current immutable manifest generation.
- `manifests/<generation>.json` is immutable once published.
- Readers only search `Published` splits from the current manifest generation.
- Leaves can hydrate full split bundles locally. The manifest format does not depend on whether later readers switch to range reads.

## Manifest Schema

FerrisSearch should use a two-layer manifest: a tiny mutable pointer plus an immutable generation manifest.

> Note: the Rust types currently shipped in `src/storage/mod.rs` and `src/cluster/state.rs` are a strict subset of the schema below. Fields such as `retention_period_secs` and `split_target_bytes` on `RemoteStoreSettings` are planned additions that do not exist in code yet.

```rust
struct RemoteManifestPointer {
    version: u32,
    current_generation: u64,
    manifest_path: String,
    manifest_etag: Option<String>,
}

struct RemoteStoreManifest {
    version: u32,
    engine: &'static str, // "remote_store"
    index_uuid: String,
    index_name: String,
    generation: u64,
    published_at: String,
    schema_hash: String,
    settings: RemoteStoreSettings,
    splits: Vec<RemoteSplitManifest>,
}

struct RemoteStoreSettings {
    object_store_uri: String,
    manifest_refresh_ms: u64,
    hotcache_bytes: u64,
    split_cache_bytes: u64,
    retention_period_secs: Option<u64>,
    split_target_bytes: Option<u64>,
}

struct RemoteSplitManifest {
    split_id: String,
    state: SplitState, // Published splits are readable
    bundle_path: String,
    bundle_etag: Option<String>,
    hotcache_path: Option<String>,
    checksum: String,
    doc_count: u64,
    size_bytes: u64,
    uncompressed_bytes: u64,
    hotcache_bytes: Option<u64>,
    time_range: Option<SplitTimeRange>,
    tags: std::collections::BTreeMap<String, String>,
}

struct SplitTimeRange {
    field: String,
    min: String,
    max: String,
}
```

- `schema_hash` lets readers fail closed if the manifest does not match `IndexMetadata.mappings`.
- `time_range` and `tags` are the first pruning keys. They are enough for root-side split planning without inventing fake shards.
- `bundle_etag`, `size_bytes`, `hotcache_bytes`, and `checksum` drive cache admission, integrity checks, hydration, and stale-cache eviction.
- `hotcache_path` stays optional so a split can still be published as a single artifact when operationally simpler.
- Raft should not replicate split membership. Raft stores the logical index and remote-store settings; the manifest stores published splits.
- Later schema additions can include field-presence summaries, sort metadata, delete-opstamp ranges, or bloom/filter summaries without changing the basic split model.

## Reader And Cache Lifecycle

FerrisSearch terms for the read path:

1. `src/api/search.rs` branches on `index.settings.engine`. `local_shards` stays on the current shard planner; `remote_store` goes to a remote manifest planner before any shard routing.
2. The ingress coordinator chooses a root-capable node. If the landing node is root-capable, it keeps the request; otherwise it forwards internally. This preserves the client-facing coordinator pattern while allowing dedicated execution roles.
3. The root fetches `manifest.current.json`, then the immutable manifest generation from `src/storage/`, using ETag or generation-aware caching.
4. The root prunes candidate splits by `time_range`, `tags`, and query/index scope. There is no `shard_routing` lookup on this path.
5. For each split, the root computes an ordered candidate list with rendezvous hashing over `(index_uuid, generation, split_id)` and the set of eligible leaf nodes.
6. The root then applies cache-aware scheduling inside the top-ranked candidates:
    - prefer a warm `RemoteSplitReader`
    - otherwise prefer a warm hydrated artifact
    - otherwise prefer the least-loaded candidate by in-flight bytes and queue depth
7. Leaves use `RemoteSplitCache` keyed by `(index_uuid, generation, split_id)`.
8. On a cache miss, a leaf downloads `hotcache.bin` and hydrates `bundle.tar.zst` into `<data_dir>/remote_cache/<index_uuid>/<generation>/<split_id>/`.
9. `src/engine/` adds a read-only `RemoteSplitReader` that opens the hydrated Tantivy directory and implements the read side of `SearchEngine`.
10. The root sends batched split plans to leaves over gRPC. A leaf executes `search_query`, `sql_record_batch`, or grouped-partials collectors on its assigned splits and returns partials, hit lists, or Arrow batches.
11. The root merges remote leaf results exactly the way FerrisSearch already merges shard partials today.
12. Three cache tiers keep the system stable:
     - manifest cache: in-memory JSON + ETag keyed by index and generation
     - artifact cache: on-disk split bytes under `remote_cache/`
     - reader cache: open `RemoteSplitReader` handles keyed by `(index_uuid, generation, split_id)`
13. Eviction is LRU by bytes, never evicting in-use readers. A new manifest generation makes removed splits stale, and a background reaper deletes them only after all readers release their handles.

This makes the steady-state execution model: object store -> leaf-local hydrated split -> read-only Tantivy reader -> root merge.

## Storage Backend Choice

- Use the `object_store` crate as the backend interface. It gives FerrisSearch one storage API across local files, S3, Azure Blob, GCS, and other object-store-compatible targets.
- For tests and local dev, use the local filesystem backend through that same interface. This keeps tests cheap, deterministic, and fast while exercising the same manifest and hydration logic.
- Do not make S3 or Azure the required first backend. Your budget constraint is real, and there is no architectural reason to force paid cloud infrastructure into unit, integration, or local cluster workflows.
- I would not use RustFS as the primary test substrate for the first implementation. It adds another moving piece without buying much unless we specifically need to simulate remote object-store semantics that the filesystem backend cannot cover.
- If later we need S3-specific behavior such as ETag quirks, eventual consistency simulation, or signed-request testing, add a separate compatibility layer with MinIO or LocalStack-style harnesses. That should be additive, not the default path.

## Execution Topology

- Root/leaf split execution is the baseline architecture, not a follow-on optimization.
- Ingress remains coordinator-safe: any node can receive the request.
- Root capability and leaf capability should be explicit node attributes so the scheduler does not assume every node does every job forever.
- Rendezvous hashing is the default split-to-leaf affinity function because it keeps cache locality stable across node joins and leaves without central shard ownership.
- Cache-aware scheduling should not violate affinity blindly. The right policy is: hash first for stable ownership, then pick among the top candidates using cache warmth and load.
- Leaf RPCs must be batched by split set, not one split per RPC, or the control plane will drown in small remote-store requests.
- Root retry logic must preserve determinism: if one leaf fails a split assignment, retry the next rendezvous-ranked candidate and mark the failed leaf unhealthy for subsequent assignments in the same request.
- A physically dedicated root tier can come later, but the protocol and scheduler should already assume root and leaf are separate execution roles.

## First Code Seams

- `src/cluster/state.rs`: add concrete `RemoteStoreSettings` fields to `IndexSettings`, plus explicit root and leaf capability metadata for node selection.
- `src/storage/`: object-store trait, manifest fetcher, local artifact cache, stale-split reaper, and cache metadata needed by the scheduler.
- `Cargo.toml`: add `object_store` when the real storage client lands, starting with the local filesystem backend in tests and dev.
- `src/engine/`: `RemoteSplitReader` implementing the read side of `SearchEngine`.
- `src/api/search.rs`: branch on engine before shard routing, choose a root-capable node, build a remote split plan, and merge leaf responses.
- `proto/transport.proto`, `src/transport/client.rs`, and `src/transport/server.rs`: add batched root-to-leaf RPCs for remote split DSL and SQL execution.
- `src/node/` and `src/cluster/`: advertise root and leaf capability, surface node health/load signals, and feed them into rendezvous-based split scheduling.