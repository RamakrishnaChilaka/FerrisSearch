---
description: "Use for SearchEngine, Tantivy, vector indexes, grouped collectors, column caches, and remote split readers."
applyTo: "src/engine/**"
---

# Engine Module — src/engine/

## Architecture
Each shard has a **CompositeEngine** that wraps two sub-engines:
- **HotEngine** (Tantivy) — full-text search (inverted index, BM25 scoring)
- **VectorIndex** (USearch) — vector search (HNSW graph, cosine/L2/IP)

## SearchEngine Trait (src/engine/mod.rs)
```rust
pub trait SearchEngine: Send + Sync {
    // Document operations
    fn add_document(&self, doc_id: &str, payload: Value) -> Result<String>;
    fn add_document_with_receipt(&self, doc_id: &str, payload: Value) -> Result<IndexWriteReceipt>;
    fn bulk_add_documents(&self, docs: Vec<(String, Value)>) -> Result<Vec<String>>;
    fn bulk_add_documents_with_receipt(&self, docs: Vec<(String, Value)>) -> Result<BulkWriteReceipt>;
    fn delete_document(&self, doc_id: &str) -> Result<u64>;
    fn delete_document_with_receipt(&self, doc_id: &str) -> Result<DeleteWriteReceipt>;
    fn apply_replica_operation(&self, op: SequencedOperation) -> Result<ReplicaApplyReceipt>;
    fn apply_replica_batch(&self, ops: Vec<SequencedOperation>) -> Result<ReplicaBulkApplyReceipt>;
    fn get_document(&self, doc_id: &str) -> Result<Option<Value>>;
    fn get_document_with_metadata(&self, doc_id: &str, realtime: bool) -> Result<Option<DocumentRead>>;

    // Engine lifecycle
    fn refresh(&self) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn flush_with_global_checkpoint(&self) -> Result<()>;  // Retains WAL above global_cp
    fn doc_count(&self) -> u64;

    // Search
    fn search(&self, query_str: &str) -> Result<Vec<Value>>;
    fn search_query(&self, req: &SearchRequest) -> Result<(Vec<Value>, usize, HashMap<String, PartialAggResult>)>;
    fn sql_record_batch(&self, req: &SearchRequest, columns: &[String], needs_id: bool, needs_score: bool) -> Result<Option<SqlBatchResult>>;
    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<Value>>;
    fn search_knn_filtered(&self, field: &str, vector: &[f32], k: usize, filter: Option<&QueryClause>) -> Result<Vec<Value>>;

    // Checkpoint tracking (replication)
    fn local_checkpoint(&self) -> Option<u64>;
    fn update_local_checkpoint(&self, seq_no: u64);
    fn global_checkpoint(&self) -> Option<u64>;
    fn update_global_checkpoint(&self, checkpoint: u64);
}
```

### search_query Collector Selection
- Query-string parsing and shard-failure semantics are owned by
  [`search.instructions.md`](search.instructions.md). Reuse the canonical schema
  builder when validating queries for empty remote-store indices; do not make
  malformed queries appear valid merely because no split is available.
- `size=0` with no aggs: uses `(None::<AggCollector>, Count)` — skip TopDocs entirely
- `size=0` with aggs: uses `(AggCollector, Count)` — aggs without hit materialization
- `size>0` with fast-field sort: uses `TopDocs::order_by_fast_field()` for Tantivy-native sorting
- `size>0` default: uses `(TopDocs::with_limit(bounded_window), AggCollector?, Count)`
- The shard-local TopDocs window is
  `from.saturating_add(size).min(searcher.num_docs() as usize)`, with a minimum
  collector capacity of 1 for an empty shard. Clamp before score, fast-field
  sort, grouped aggregation, and cursor collectors. The coordinator applies
  pagination; exact Count totals remain independent of the bounded hit window.
- Apply the same live-document bound to `sql_record_batch`, shared search
  helpers, and kNN filter collectors. SQL already folds OFFSET into the
  requested size. Never pass a user-controlled huge capacity to `TopDocs`,
  including any fast-field sort variant.
- Clamp native vector search and filtered oversampling to the indexed vector
  count, use saturating multiplication, and size hit buffers from returned
  candidates. Validate filters even when no vector index exists.
- Bound SQL streaming batch allocations by the existing 8192-row ceiling and
  the pinned searcher's live docs (minimum capacity 1). Zero grouped top-K
  sizes produce empty buckets without selecting index `size - 1`.
- Validate the shared 64-field sort-width bound before cursor expansion or
  per-hit sort-value allocation, even on direct engine/transport requests.

### Seq Ownership Rule
- `add_document()` / `bulk_add_documents()` / `delete_document()` are for local primary-originated writes that allocate new WAL seq_nos
- `apply_replica_operation` / `apply_replica_batch` are the only production
  replica/recovery entry points and require explicit sequence and primary term
- Replica/recovery code MUST preserve the primary-assigned seq_no when writing to WAL; do not route replicated operations through the local-allocation methods
- Primary transport handlers must use the receipt-returning methods. The
  convenience ID/count methods delegate to them and intentionally discard only
  the receipt.
- A receipt belongs to its operation, even if another write advances a checkpoint
  before replication. Never reconstruct its sequence from `last_seq_no()` or
  `local_checkpoint()`.
- A non-empty bulk receipt has a contiguous WAL-reserved start; an empty batch
  has no assigned sequence. Replica/recovery apply methods are required
  implementations, not defaults that allocate new primary sequences.
- Every primary, replica, recovery, and delete operation must encode to a WAL
  frame no larger than `MAX_WAL_FRAME_BYTES` (32 MiB including the frame
  header). Reject larger operations as validation errors before WAL or engine
  mutation; validate every item before writing any bulk bytes.
- Conditional primary methods compare live/committed document versions under
  the translog mutex, before sequence assignment or append. Conflicts return
  `VersionConflictError` with no WAL or sequence effect. Create checks presence;
  delete receipts distinguish absent documents but still assign a sequence.
  `IndexWriteReceipt.created` and bulk `created` flags come from this same
  critical section, in request order.
  Primary index-only bulk may reuse its initial live/committed versions in
  the apply planner only within that same translog critical section. Keep
  shadow versions authoritative for duplicate IDs; never reuse the cache
  across refresh, replica apply, or replay.
- Keep `get_document()` searcher-only for existing internal consumers.
  REST/OCC uses `get_document_with_metadata()`: realtime checks map completeness
  and the live version under one short version-map read lock, separate from
  `ApplyState`. A complete-map miss acquires the searcher after releasing that
  lock and reads without the apply-state or translog mutex; a complete-map
  tombstone returns missing. Index hits release the map lock, then hold the translog
  mutex through re-lookup and WAL cursor reads, so flush cannot prune between
  lookup and read. Reader fallback must cover the live version. An incomplete
  map waits for the mutex and fails with the replay cause if it remains
  incomplete; never return stale source or a false 404 after failed replay.
- Keep D1 planning, checkpoints, term tracking, and planning-snapshot restore
  under the whole-batch apply-state mutex. Acquire apply state before the
  version map whenever both are needed, never the reverse. Hold the map's
  write lock only around map mutation, not WAL I/O, fsync, Tantivy apply, or
  reader lookup. Publish map entries per applied operation; an in-flight
  index hit still waits for the translog mutex before reading its WAL source.
  A mid-batch delete's tombstone can return immediately before acknowledgement.

## CompositeEngine (src/engine/composite.rs)
```rust
pub struct CompositeEngine {
    text: HotEngine,
    vector: RwLock<Option<VectorIndex>>,
    data_dir: PathBuf,
    global_cp: Mutex<Option<u64>>, // persisted global checkpoint, primary only
    vector_recovery: Mutex<()>,    // serializes text replay with vector rebuild
}
```
- `HotEngine` owns gap-aware processed/persisted interval tracking.
  `CompositeEngine` delegates sequence stats and keeps the global persisted
  checkpoint monotonic so late acknowledgements cannot regress progress.

### Constructors
- `HotEngine::open_existing_with_mappings()` /
  `CompositeEngine::open_existing_with_mappings()` require an existing Tantivy
  `index/meta.json` and never create a fresh index. Dynamic-mapping reopen uses
  this path after dropping the old engine.
- `new(data_dir, refresh_interval)` — default refresh loop (static interval)
- `new_with_mappings(data_dir, refresh_interval, mappings, durability, column_cache)` — with schema + WAL + shared column cache

### Refresh Loop (reactive)
```rust
// start_refresh_loop_reactive(engine, refresh_rx, flush_threshold_rx)
tokio::select! {
    () = tokio::time::sleep(interval) => { engine.refresh(); }
    result = refresh_rx.changed() => {
        // Update interval from settings change
        interval = *refresh_rx.borrow_and_update();
    }
}
```
- Subscribes to `SettingsManager::watch_refresh_interval()` watch channel
- Subscribes to `SettingsManager::watch_flush_threshold()` for WAL auto-flush
- Reacts to dynamic `refresh_interval_ms` and `flush_threshold_bytes` settings changes without restart
- Each refresh/auto-flush tick must run on Tokio's blocking pool (`spawn_blocking`) because `refresh()`, checkpoint-aware truncation, and vector persistence all perform blocking I/O; never run shard maintenance inline on async runtime workers or Raft heartbeats can stall during multi-shard compaction bursts
- Primary auto-flush must use the global checkpoint and skip when it is
  `None`; sequence zero is a valid safe checkpoint.
- Replica apply tracks a separate monotonic local persisted prefix. When no
  primary global checkpoint exists, replica auto-flush may truncate only
  through that prefix. Clear this replica-only bound before primary activation.
- Background auto-flush should use best-effort helpers so maintenance ticks defer instead of blocking active ingestion or vector persistence

### Vector Auto-detection
- On `add_document()`: scans payload for arrays of numbers
- Auto-creates VectorIndex if a `knn_vector` field is encountered
- `rebuild_vectors()` constructs a replacement USearch index from the
  authoritative Tantivy document view. Every rebuild commits the healthy text
  writer and opens a private `ReloadPolicy::Manual` reader on that commit.
  Never rebuild from the last published reader: it can omit applied but
  unrefreshed documents. Never publish the private reader or rotate the realtime
  version map for vector repair. Non-stale writes do not add a rebuild commit.
- Capture the complete in-memory applied set immediately after commit, under
  one apply-state lock: processed checkpoint, maximum sequence, and missing
  intervals through that maximum. Require the same triple after enumeration.
  Replica gaps are valid; include every applied live document above a gap.
  Never require a gap-free prefix to rebuild or open a vector copy. This proof
  does not change the persisted recovery-boundary format or the separate
  gap-free requirement for exporting peer snapshots.
  If coverage changed or enumeration fails, return the error and keep
  `vectors.stale` and the current vector index.
  Persist and fsync the covering replacement, atomically swap it into memory,
  and only then clear `vectors.stale`. Mark even explicit rebuilds stale before
  attempting the commit so failures remain durable.
- Vector rebuild enumerates every live document directly from each Tantivy
  segment and feeds the replacement index in bounded batches. Never use
  `TopDocs` or a search-result limit for rebuild input; deleted documents must
  remain excluded and shards above 100,000 live documents must rebuild fully.
- A text apply failure after WAL persistence durably creates `vectors.stale`.
  Its temporary file also means stale on restart. Primary writes, replica
  apply, refresh, flush, force merge, peer-snapshot preparation, recovery
  barriers, primary activation, startup open, dynamic-mapping reopen, and
  peer-recovery finalization must rebuild vectors before clearing that state or
  publishing a replacement engine.
- `vector_recovery` serializes those rebuild paths with vector mutations so a
  later failed text operation cannot be hidden by an earlier rebuild clearing
  the marker.
- Rebuild lock order is `vector_recovery`, maintenance, translog, writer, then
  `apply_state`. Capture applied-set coverage before releasing the writer.
  After boundary persistence and private-reader creation, release translog
  before enumeration so WAL-backed realtime GET does not wait for the scan.
  Retain maintenance through enumeration and validation to prevent truncation.
  The private searcher pins its segment files. Hold
  `vector_recovery` through vector persistence, swap, and marker removal.
  Private readers take only Tantivy-internal metadata locks, not the reader
  publication mutex. Text recovery remains responsible for replaying a failed
  writer before a later rebuild; vector repair does not initiate publication.
- Prepared vector side effects use operation kind, document ID, sequence, term,
  and the prepared vector only; do not retain a deep copy of document JSON for
  post-rebuild application. Replica planning borrows its input operations and
  document IDs, and already-appended primary/replay operations do not construct
  another WAL envelope.
- Background refresh must call the composite `refresh()` path, not
  `HotEngine::refresh()` directly, or it bypasses vector recovery.

### Reserved Document Metadata

- Every primary, replica, recovery, and direct engine index source uses
  `common::validate_document_source()` before WAL or engine mutation.
- The shared reserved list contains `_id`, `_doc_id`, `_source`, `_seq_no`,
  `_primary_term`, `_version`, `_index`, and `_routing`.
- `FieldRegistry.fields` excludes every reserved name even when it exists in
  the authoritative Tantivy schema, so a validation bypass cannot append a
  second internal sequence or term value.
- Dynamic mapping ignores reserved names, and engine construction rejects
  reserved explicit mappings before Tantivy schema creation or evolution.
- `body` remains the non-reserved built-in catch-all text field. Dynamic and
  strict mapping discovery ignore it. A plain explicit text mapping reuses the
  existing schema field rather than adding a duplicate.
- Engine creation and reopen validate authoritative mappings before Tantivy
  schema construction. Reserved names, a non-text or parameterized `body`
  mapping, and an invalid built-in body schema fail as
  `UnsupportedIndexFormatError` with recreate-index guidance; they must never
  reach a schema-builder panic.

## RemoteStore Engine (src/engine/remote_store.rs)
- `remote_store` is a shardless read path. Root nodes load the published manifest for an index, query per-leaf cache/load status over gRPC, and batch split assignments to data-node leaves.
- Newly published splits persist exact manifest summaries under `field_ranges` (mapped integer/float/date min/max) and `field_terms` (small exact mapped keyword/boolean distinct sets). Keyword summaries must reuse `HotEngine`'s index-time recursive flatten/coerce/null-skip semantics; exceeding the distinct-value cap omits the entire field summary instead of publishing a partial set. Root-side search prunes published splits against those summaries for supported `term` and `range` filters before rendezvous scheduling; missing or unsupported metadata must keep the split. GET/POST search responses and SQL/EXPLAIN ANALYZE paths that execute through remote_store expose `remote_store.pruning` counters for published, candidate, pruned, and assigned split counts.
- Leaf selection uses rendezvous ranking over `(index_uuid, manifest_generation, split_id, node_id)`, then chooses among the top-ranked candidates by `reader_cached` > `artifact_cached` > lower `inflight_bytes` > lower `queue_depth`.
- Leaves use `RemoteSplitReaderCache` to reuse open `HotEngine` readers across requests. Reader entries pin the underlying cached split directory for as long as the reader stays live.
- `StorageManager::cached_split_status()` reports warm-artifact state, `begin_remote_store_batch()` / `remote_store_load_snapshot()` publish live load signals, and `reap_split_cache()` removes stale or over-budget split directories after batches while leaving pinned artifacts intact.
- Master-only coordinators are valid remote_store roots; only nodes with `NodeRole::Data` are eligible leaves.

## HotEngine (src/engine/tantivy.rs)
```rust
// Key internals
field_registry: RwLock<FieldRegistry>  // maps field names → Tantivy Field handles
wal: Option<Arc<dyn WriteAheadLog>>    // per-shard WAL
```
- **Dynamic fields**: creates Tantivy fields on first encounter
- **`body` field**: catch-all for unmapped textual content
- `matching_doc_ids(clause)` — returns doc ID set for k-NN pre-filtering
- Build every `HotEngine` reader with `ReloadPolicy::Manual`, including
  remote-split readers and test fixtures. Route every reload through
  `HotEngine::reload_reader()`. Its shard-local mutex covers both opening
  segments and publishing the searcher, so an older reload cannot replace a
  newer reader. Do not enable Tantivy's commit watcher.
- The reader-reload mutex is a leaf lock. Callers may already hold vector
  recovery, maintenance, translog, or writer locks. The helper acquires no
  other engine locks and releases its mutex before version-map access.
  Never reload while holding the version-map lock. Search and realtime GET
  borrow searchers without taking the reload mutex.
- If an isolated reader-reload mutex is poisoned, recover its unit-valued guard,
  clear poison under the guard, log the failure, and return an error for that
  attempt. A later call that reaches the helper can retry under the mutex.
  This is safe because Tantivy constructs the complete searcher before its
  atomic store; a segment-open panic does not publish partial state.
  Production callers also hold maintenance or translog. A reload panic poisons
  those outer locks, so later refresh, flush, or replay can fail before reaching
  this recovery and remain failed closed until reopen. Do not claim automatic
  production recovery. Explicit copy-failure escalation is a follow-up.
  Do not recover maintenance, translog, apply-state, or version-map locks through
  this policy or hide a repeated panic or underlying storage error.
- A commit alone does not publish search visibility. Refresh publishes before
  retiring old versions or pruning covered tombstones. Preserve the existing
  explicit publication during flush, force merge, and replay: flush needs a
  covering reader before WAL pruning. Ordinary peer-snapshot commits export
  committed index files and leave search publication to the next refresh.
  Both composite peer-snapshot APIs repair stale vectors through the same
  covering private-reader path, without changing search visibility.
  Protocol-trace snapshot capture also uses a private reader on the committed
  files instead of publishing them.
- `replay_translog()` and failed-writer reconstruction share the same WAL-suffix
  replay helper. They stream entries via `for_each_from()` starting at the
  persisted committed checkpoint.
- Replay must stay idempotent across repeated restart or write-path recovery:
  validate `_doc_id` for every operation and `_source` for index operations,
  delete `_id` for every operation, add content back only for index operations,
  commit in bounded batches, and persist `translog.committed` only after each
  successful intermediate commit. Missing fields are typed WAL corruption.
- Replay holds the translog lock for the entire retained suffix so no new WAL
  entry can be appended before reconstruction is complete. This blocks writes
  to that shard and can be a long critical section when refresh is disabled.
- Replay clears map completeness together with the map reset. It restores
  completeness only after successful reader publication and replay completion,
  including an empty suffix. Intermediate commits and failed replay leave the
  map incomplete. Refresh clears old entries only after publishing a covering
  reader; the byte limit forces refresh or rejects writes instead of evicting.
  Steady rotation swaps current and old when old is empty. Reader reload
  precedes taking old and subtracting its cached byte total; callers drop the
  returned retired map only after releasing the version-map write lock.
  Non-empty-window merge/rollback and tombstone pruning can still do linear
  work under that lock.
  Failed map rotation, rollback, or post-WAL apply also invalidates completeness
  and the writer. Map state and completeness share the same version-map lock.
- The durable term-start maximum comes from the copy fence and may be ahead of
  `CommittedBoundaryRecord.max_seq_no` at an intermediate replay commit. This is
  valid because WAL-only operations have not reached that batch yet. Validation
  still requires every recorded current-term processed interval to stay at or
  below both the committed maximum and the term-start maximum.
- `translog_size_bytes()` exposes the current WAL size for the auto-flush loop
- The Tantivy `IndexWriter` heap budget is intentionally capped at 64 MiB per shard. Multi-shard restart/open paths must not reserve the old 512 MiB-per-shard budget or nodes with many local shards can OOM before recovery completes.
- Force merge is serialized only within one `HotEngine`. It temporarily installs
  `NoMergePolicy`, commits, consumes the writer to drain already-scheduled
  Tantivy merges, reopens the writer, performs the manual merge without holding
  a lock needed by merge completion, and restores the prior automatic policy on
  success or failure. Refresh and flush share the same shard-local maintenance
  lock so they cannot invalidate the requested final segment bound.
- If force merge cannot replace the drained writer, the next document write
  attempts one writer rebuild before appending a new WAL operation, using the
  normal writer heap budget and automatic merge policy. A transient replacement
  failure can therefore heal on that write. Rebuild I/O failures retain typed
  causes, and primary/replica handlers account persistent failures under the
  shard Apply retry key.
- Every production Tantivy commit boundary—refresh, flush, checkpoint-aware
  flush, vector rebuild, recovery snapshot, replay batches, and pre-force-merge
  commit—must fail the `WriterState` on error. No later write may reuse that
  writer.
- Before the next write appends a new WAL entry, or before blocking refresh,
  flush, force-merge preparation, or peer-snapshot commit proceeds, a failed
  writer is rebuilt and the retained suffix
  `[translog.committed, WAL next_seq)` is replayed and committed with the
  normal automatic merge policy. Persistent rebuild or replay I/O is an Apply
  failure only on the write path; a rebuild triggered by refresh, flush, or
  snapshot preparation logs and retries on the next maintenance tick without
  escalating. Best-effort `try_flush_with_global_checkpoint()` may return
  `Ok(false)` instead of rebuilding.
- `force_merge(0)` is invalid. Successful force merge must verify the final
  searchable segment count is at most the requested positive bound while
  preserving document values, deletes, and the committed WAL boundary.
- `rebuild_vectors()` is only called when the index has `KnnVector` fields in
  its mappings. The shard manager gates this check because the full
  segment/alive-document scan is proportional to shard size; never call it
  unconditionally.
- Even the direct `HotEngine::start_refresh_loop()` path must offload `refresh()` through Tokio's blocking pool if it is used; never run Tantivy commit/reload inline on an async interval task
- Replica/recovery writes use explicit-sequence append APIs, including
  arbitrary ordered batches, so persisted WAL operation identities match the
  primary even when delivery order differs from sequence order.
- Peer snapshot creation holds maintenance then translog then writer locks,
  captures an exact committed boundary and physical WAL end, durably persists
  `translog.committed`, registers the WAL pin at the first sequence above the
  processed prefix before releasing the translog lock, and hard-links the
  existing committed segment components plus
  `meta.json`/`.managed.json`. Tantivy's `SegmentMeta::list_files()` can name
  optional absent components; transfer only files that actually exist.
- Snapshot installation does not transfer the live processed interval set above
  a gap. Source snapshot creation therefore requires
  `processed_checkpoint == max_seq_no`; the target retries after source
  replay/activation closes the gap instead of accepting a lossy boundary.
- Catch-up scanning stops the physical cursor before the first WAL frame not
  yet processed by the source. The target then enters finalization, whose
  exclusive barrier rebuilds/replays a failed source writer before serving the
  remaining suffix from that same cursor.
- A persisted committed checkpoint and any WAL truncation must be derived from
  a successful Tantivy commit boundary. Never advance or prune past operations
  that the corresponding commit did not make durable.
- Live index versions carry WAL cursors for primary, replica, recovery, and
  replay applies. Append batches obtain positions from frame headers; replay
  carries each cursor from `for_each_from_at()`, including physical gaps
  caused by committed-entry skips. Do not locate replay positions by repeated
  sequence scans. Flush reloads the reader under the translog mutex before
  pruning; refresh clears old map entries only after reader publication.
- Snapshot hashes run after lock release. Unlocked byte-copy fallback is
  forbidden when hard links are unavailable.
- `StartPeerRecovery` snapshot preparation runs in a detached, cancellation-safe
  task. The short RPC returns/polls preparation status; hashing is outside the
  maintenance and translog critical sections, so large shards are not bounded
  by the ordinary 30-second transport request timeout.

### Shared Column Cache Budget
- `resolve_column_cache_budget()` is a blocking startup probe. On Linux it reads
  host `MemTotal`, maps `/proc/self/cgroup` through `/proc/self/mountinfo`, and
  takes the tightest visible finite cgroup v2 `memory.max` or cgroup v1
  `memory.limit_in_bytes` across the current cgroup and its visible ancestors.
- Respect mount roots, cgroup namespaces, and nested paths; never walk above the
  selected cgroup mount. `memory.high` and v1 soft limits are reclaim signals,
  not hard allocation caps, and must not size this cache.
- Missing/unavailable controls may fall back with a startup diagnostic.
  Malformed authoritative procfs, mount mapping, or hard-limit values are
  startup errors. A configured percentage of `0` disables the cache without
  probing host or cgroup files.
- The cache capacity is one component budget, not a total-process memory bound.

### Field Schema Flags
Numeric fields use three Tantivy flags (mirrors OpenSearch default doc_values: true):
- INDEXED - inverted index, enables search queries (term/range/match)
- STORED - preserves original value, retrievable in results
- FAST - columnar storage, critical for range queries, sorting, and aggregations

The `_id` field uses `(STRING | STORED).set_fast(None)` — enables fast-field columnar access so the SQL fast-field path can read `_id` without loading the full stored document.

Integer and Float fields get all three: INDEXED | STORED | FAST.
Keyword and Boolean fields get: STRING | STORED + FAST (set_fast(None) for dictionary-encoded columnar).
Without FAST, range queries scan the inverted index (slow on high-cardinality fields).
With FAST, Tantivy reads a columnar structure - orders of magnitude faster for range queries, sorting, and aggregations.

### Keyword Values
- Declared keyword fields accept scalar strings/numbers/booleans, nulls, and
  nested arrays of those values. Flatten for indexing, coerce scalars to text,
  skip nulls, and deduplicate values within a document. Preserve `_source`.
- Validate keyword objects before any WAL append or writer mutation, including
  the entire shard batch and explicit-sequence paths. Replay must surface invalid
  values rather than silently omit indexed data.
- Numeric keyword arrays are not vector fields. Keep them out of automatic
  vector detection.
- Query DSL terms buckets count matching documents once per keyword value.
  SQL's direct columnar readers remain scalar-first; this does not introduce
  SQL array expressions or `UNNEST` semantics.

### Fast-Field Aggregations (Single-Pass Collector)
Aggregations run in the same Tantivy search pass as hit collection via `AggCollector` -- a custom
`tantivy::collector::Collector` implementation. Combined with TopDocs via tuple collector:
`(TopDocs, Option<AggCollector>, Count)` for hit-returning requests, or `(Option<AggCollector>, Count)`
for agg-only `size=0` requests. When no aggs are requested, `None` adds zero overhead.

### Hybrid SQL And Distributed Partial Execution
- Do not modify Tantivy fast-field storage format for hybrid SQL work. Use Tantivy's Rust APIs directly.
- Direct access patterns already expected in this module:
    - numeric columns: `segment_reader.fast_fields().f64(name)` / `.i64(name)`
    - keyword columns: open `StringFastFieldReader` (`StrColumn` + ordinal `Column<u64>`) and use `first()` / `first_vals()` on the ordinal column, then `ord_to_str()`
- `sql_record_batch(req, columns, needs_id, needs_score)` is the reference pattern for projecting matched docs from fast fields into Arrow without `_source` materialization. It builds `type_hints` from `SqlFieldReader` variants (F64/I64 → `ColumnKind::Float64`, DateMillis → `ColumnKind::TimestampMillis`, Str → `ColumnKind::Utf8`) and passes them to `build_record_batch_with_hints()` so that zero-result queries still produce correctly-typed Arrow columns instead of defaulting to Utf8.
- In the flat fast-field path, `_id` should reuse the same per-segment array/take/reorder flow as other string fast fields. Do not keep a separate top-doc-order decode/clone loop for `_id` unless profiling proves the shared path regressed.
- `sql_streaming_batch_handle(req, columns, needs_id, needs_score, batch_size)` is the primary streaming API for score-free explicit-column SQL. It must return `total_hits` and `collected_rows` up front plus a lazy `next_batch()` closure so the coordinator can register local `StreamingTable` partitions without first building a `Vec<RecordBatch>`.
- `sql_streaming_batches(req, columns, needs_id, needs_score, batch_size)` is now the eager compatibility wrapper that drains the lazy handle into memory for tests, buffered compatibility paths, and transport code that has not yet been converted to the handle.
- The local streaming batch handle must batch doc IDs per emitted Arrow batch and feed those slices into `Column::first_vals()` / `StringFastFieldReader::first_ords_batch()`. Do not regress the streaming `tantivy_fast_fields` path to per-doc `first()` / `first_text()` loops for numeric, date, `_id`, or keyword columns.
- `sql_streaming_batch_handle()` is only valid for `_score`-free queries whose requested columns are fast-field-backed on every segment. If any column resolves to `SourceFallback` or the SQL query needs `_score`, return `None` and let the caller stay on `sql_record_batch()` or the broader fallback path.
- **`can_stream_sql_batches(columns, needs_score)`** is the eligibility guard on `HotEngine`. Returns `false` if `needs_score` is true or any column on any segment resolves to `SqlFieldReader::SourceFallback`. Called by the `SearchEngine::sql_streaming_batch_handle` impl before delegating to the inner method.
- **`BitSetCollector`** is a custom `tantivy::collector::Collector` that collects ALL matched doc IDs as a `Vec<SegmentBitSet>`. Each `SegmentBitSet` is a `Vec<u64>` manual bitset (1 bit per doc, ~500KB for 4M docs). `SegmentBitSetCursor` iterates those words lazily, and `StreamingBatchState` / `StreamingSegmentState` use the cursor plus fast-field readers to produce Arrow `RecordBatch`es of `STREAMING_BATCH_SIZE` (8192) rows each.
- **`ColumnBuilder`** is an enum (`F64`/`I64`/`TimestampMillis`/`Str`/`Null`) that wraps Arrow builders and appends values from `SqlFieldReader`s. The string variant keeps a reusable scratch buffer so streaming string columns do not allocate a fresh `String` per doc. Catch-all arms use `unreachable!()` to fail loud on type mismatches instead of silently skipping rows.
- Streaming `ColumnBuilder` batch paths should also reuse per-column scratch vectors for numeric values and string ordinals across emitted batches instead of allocating fresh `Vec<Option<...>>` buffers on every batch.
- If you touch SQL string fast-field reads (`_id`, keyword projections, selective arrays, or streaming batches), do not reintroduce per-doc `term_ords()` iterators in the hot path; use the shared ordinal reader instead.
- When `needs_id` is false, `_id` fast-field reads are skipped and the Arrow `_id` column is filled with empty strings.
- When `needs_score` is false, score collection is skipped and the Arrow `_score` column is filled with zeros.
- Zero-hit lazy handles must still emit one empty batch with the correct Arrow schema before returning `None`, so streamed `StreamingTable` partitions can initialize without schema drift or bogus rows.
- The planner detects `needs_id`/`needs_score` by checking whether the SQL query references `_id` or synthetic `_score` in any projection, filter, GROUP BY, or ORDER BY.
- Zero-column SQL queries such as `SELECT 1 FROM ...` must still preserve one output row per hit without pretending they need `_score`. Handle that in `sql_record_batch()` directly rather than overloading `needs_score` for row-count preservation.
- For grouped analytics over matched docs, prefer shard-local partial aggregation from fast fields and merge compact partials at the coordinator.
- Fall back to `_source` materialization only for fields or expressions that cannot be read from fast fields or stored fields.
- Tantivy is the preferred execution engine for search-aware work: pushdown, ranking, field reads, and shard-local partial aggregation should stay here.
- DataFusion is a downstream consumer of Arrow batches or merged partial states; do not move text-search behavior, broad scan-style execution, or default matched-doc execution into it.
- If a new SQL feature can be implemented by extending fast-field collectors or compact partial-state merging, prefer that over coordinator-side row materialization.

**Architecture (mirrors OpenSearch's aggregation design):**
- `AggCollector` implements `Collector` -- `for_segment()` opens fast-field columns per segment
- `AggSegmentCollector` implements `SegmentCollector` -- `collect(doc, score)` reads column values and accumulates
- String `terms` aggs count term ords per segment in `collect()`, then resolve ord→string once in `harvest()`
- Use bounded dense ordinal counters for small dictionaries, with a sparse
  fallback for larger dictionaries. Do not allocate an unbounded vector from
  field cardinality or truncate partial buckets before coordinator merging.
- Choose the dense/sparse strategy once per segment and keep them as specialized
  collector variants. Do not add another per-document enum dispatch to the
  sparse `HashMap` hot loop.
- Numeric term keys retain their underlying integer/float representation until
  harvest. Never cast integer keys to `f64` or integral floats to `i64`.
- Invalid ordinals and dictionary read failures propagate as query errors;
  harvesting must not silently drop their buckets.
- `harvest()` returns per-segment data, `merge_fruits()` merges across segments into `HashMap<String, PartialAggResult>`

### Grouped Metrics Collector (Ordinal-Based)
The `GroupedAggCollector` computes grouped analytics (GROUP BY + aggregate functions) in a single Tantivy pass using fast fields.
- **Zero-allocation hot path**: `collect()` uses `GroupKeyReader::keys_batch(...) -> Option<u64>` to get fast-field ordinals. For string columns, this is the dictionary ordinal; for numerics, the bit-reinterpreted value. `None` represents SQL `NULL` out-of-band. No String allocations, no JSON serialization per doc.
- **Collision-free composite keys**: Single-column GROUP BY uses `OrdHashMap<u64>` for non-null values plus a dedicated null bucket. Width-2 GROUP BY uses packed `(u64, u64)` keys for non-null pairs plus dedicated null-mask buckets. Width 3+ falls back to `HashMap<Vec<Option<u64>>>` (exact key match). No hash collisions are introduced by the key encoding.
- **Batch ordinal reads**: Single-column GROUP BY buffers doc IDs and reads ordinals in batches of 1024 via `Column::first_vals()` — faster than per-doc `term_ords()`.
- **Batch numeric reads**: All numeric metric columns (sum/avg/min/max) are batch-read via `NumCol::first_vals_f64()` in the same 1024-doc batch as ordinals. This eliminates per-doc `first_f64()` calls — for 1.8M docs × 2 numeric columns, that's ~3.6M per-doc reads replaced by ~3,500 batch calls. The batch values are stored in pre-allocated `numeric_buffers` on `GroupedAggSegmentEntry` and consumed during accumulator updates.
- **Batch path handles all single-column metrics**: The batch path now processes count-only AND numeric ( `sum`, `avg`, `min`, `max`) queries — not just `count(*)`. Ordinals and numerics are read in batch, accumulators updated from pre-fetched buffers.
- **Deferred string resolution**: `harvest()` calls `GroupKeyReader::resolve(ord) → serde_json::Value` once per unique group to produce the final `GroupedMetricsBucket`s.
- **Identity hasher**: `OrdHasher` treats u64 ordinals as their own hash — zero hash computation in the per-doc path.
- **Packed pair hasher**: The width-2 packed-key path must stay on the dedicated `PairHashMap` / `PairHasher` instead of the default SipHash map. Route-style `(pickup, dropoff)` workloads are structured enough that the mix step still matters, but general-purpose hashing in the per-doc loop is too expensive once `Vec<u64>` allocation has already been removed.
- **Null handling invariant**: Grouped numeric keys must never overload a payload bit pattern as the null sentinel. Signed integer values like `-1` must remain distinct from SQL `NULL` in single-key, packed-pair, and multi-key grouped paths.
- **Pre-sized HashMap**: `num_terms()` from the dictionary provides approximate group count for `HashMap::with_capacity()`.
- **Top-K selection**: When ORDER BY + LIMIT are present, uses `select_nth_unstable_by` (O(N) average) instead of full sort (O(N log N)). Only the top-K subset is fully sorted.
- Per-shard partial results are serialized with `bincode-next` into the `partial_aggs_json` bytes field over gRPC, then merged at coordinator via `merge_aggregations()`
- Agg-only `size=0` requests skip `TopDocs` and hit materialization entirely
- **Direct scan for match_all**: When `query.is_match_all() && size == 0 && has_grouped_metrics`, `grouped_partials_direct_scan()` bypasses Tantivy's scorer/collector entirely — iterates segment fast-field columns directly in batches of 1024. All paths (single-column, multi-column, global) use batched reads.
- **Batched multi-column and global paths**: `flush_batch_multi()` batch-reads ordinals for ALL key readers and all numeric columns, then accumulates. Avoids per-doc `Vec<u64>` allocation for composite keys and per-doc fast-field reads. Used by both the standalone direct scan and the collector's `collect()` path for multi-column GROUP BY and ungrouped aggregates.
- **Flat array accumulation**: For single-column keyword GROUP BY with <2M unique groups on match_all queries, replaces HashMap with pre-allocated `Vec<u64>/Vec<f64>` arrays indexed directly by ordinal — zero hash computation, zero collision handling, cache-friendly sequential access. `FlatMetric::Count` and `FlatMetric::Stats` provide parallel arrays for each metric. `flat_scan_segment()` uses contiguous range-based doc buffers (no per-doc push) and `flat_flush_batch()` implements the accumulate loop.
- **Shard-level top-K pruning**: When `ShardTopK { limit, sort_by, descending }` is set on `GroupedMetricsAggParams`, each shard emits only the top `limit` buckets (default: `(offset + limit) * 3 + 10`) sorted by the named metric. The flat-scan path applies top-K on ordinals BEFORE resolving strings via `select_nth_unstable_by` (O(N) average), avoiding ord→string resolution for 99%+ of groups. The collector and direct-scan paths apply `apply_shard_top_k()` after segment merge. The planner only sets `shard_top_k` when ORDER BY references a metric column (not a group column). This is approximate — the 3× multiplier makes missed global top-K groups extremely unlikely.
- **StringArena**: `flat_scan_segment()` batch-resolves ordinals into a contiguous `Vec<u8>` arena (`StringArena`) instead of N individual `String` heap allocations. Each resolved string is `(offset, len)` into the arena. Only the final `serde_json::Value::String` conversion allocates a per-group String. Reduces allocator pressure from 364K small allocs to one large contiguous buffer per segment.
- **Parallel segment scanning**: The direct scan path uses `std::thread::scope` (not rayon, to avoid nested-pool deadlocks) to scan all segments concurrently. Each segment gets its own OS thread with independent flat arrays and fast-field readers. Results are merged after all threads complete. Achieved ~43% speedup on full-scan GROUP BY (13.9s → 8.0s search time on 1.8M docs).

**Supported aggregation types:**
- **Numeric** (Stats, Min, Max, Avg, Sum, ValueCount): reads `NumCol` (wraps `Column<f64>` or `Column<i64>`)
- **Histogram**: reads numeric column, buckets by `floor(value / interval)`
- **Terms**: reads `StrColumn` (dictionary-encoded keyword fields) or numeric column for numeric fields

**Key types in `src/engine/tantivy.rs`:**
- `NumCol` -- wraps i64/f64 fast-field columns with `first_f64()` (per-doc) and `first_vals_f64()` (batch) coercion
- `SegmentAggEntry` -- per-segment column + accumulator (NumericStats, Histogram, TermsStr, TermsNum, Skip)
- `SegmentAggData` -- harvested per-segment result (Stats, Histogram, Terms)
- `AggKind` / `ResolvedAggSpec` -- resolved from `AggregationRequest` before search

### Type-Safe Term Creation (CRITICAL)
All Tantivy `Term` objects MUST match the schema field type. A mismatch can
silently produce zero hits or panic in a fast-field range collector.

Use the `typed_term()` helper for ALL term creation in queries:
```rust
fn typed_term(&self, field: Field, value: &serde_json::Value) -> Result<Term> {
    // Checks schema via self.index.schema().get_field_entry(field).field_type()
    // Returns a correctly typed term or a classified query parse error.
}
```

**Where `typed_term()` is used:**
- `QueryClause::Term` — exact match queries
- `QueryClause::Terms` — a set of exact matches
- `QueryClause::Range` — range bounds (gte/lte/gt/lt)
- `QueryClause::Fuzzy` — fuzzy term construction
- `search_after` — cursor equality and range bounds

Malformed Integer, Float, or Date values must fail with the existing
`QueryParseError` classification and preserve the field, value, expected type,
and parser cause. Never fall back to a text term for a numeric field.
Floats must be finite. Integer JSON values use exact i64/u64 conversion, never
a float round-trip; integral floating literals must be in the signed range,
and fractional or out-of-range integer values are rejected.
Reuse `typed_term_for_schema` when validating empty/pruned remote-store
requests against their canonical mapping-derived schema.

**Common pitfall:** JSON integer `10` on a float field. `serde_json::Number::as_i64()` succeeds
before `as_f64()`, creating the wrong term type. `typed_term()` checks the schema first to avoid this.

### Type-Safe Document Indexing
`build_tantivy_doc_inner()` takes a `&Schema` parameter and checks the field type before
adding numeric values:
```rust
// For a Number value on a mapped field:
match schema.get_field_entry(field).field_type() {
    FieldType::F64(_) => doc.add_f64(field, ...),  // float fields always get f64
    FieldType::I64(_) => doc.add_i64(field, ...),  // integer/date fields always get i64
    FieldType::U64(_) => doc.add_u64(field, ...),
    _ => {}
}
// For a String value on an i64 field (Date):
// Parses ISO 8601 → epoch millis via common::date::parse_iso8601_to_epoch_millis()
```
This prevents JSON integer `99` being stored as `i64` in an `f64` field (which would make it
unsearchable by float range queries). Date fields accept both ISO 8601 strings and epoch millis integers, and mapped Date fields canonicalize `_source` to UTC ISO 8601 on ingest so GET/search/SQL paths do not leak raw epoch millis or original offsets.

## VectorIndex (src/engine/vector.rs)
- USearch HNSW wrapper (connectivity=16, expansion_add=128, expansion_search=64)
- `add_with_doc_id(doc_id, vector)`, `search(query, k) -> (keys, distances)`
- Binary persistence: `save(path)` / `open(path, dimensions, metric)`
- Doc ID ↔ numeric key mapping via `HashMap` + bincode serialization

## Column Cache (src/engine/column_cache.rs)

### Architecture
Segment-aware, lazy-loaded column cache backed by `moka`. Shared across all shards on a node via `Arc<ColumnCache>`.
- **Key**: `(SegmentId, column_name, format)` — Tantivy segments are immutable once committed, so cached data never goes stale.
- **Value**: either an Arrow `ArrayRef` covering all docs in a segment for one SQL column, or grouped-partials decoded full-segment values keyed by doc ID (`f64`, `i64`, or string ordinals).
- **Eviction**: Size-bounded (weighted by `ArrayRef::get_array_memory_size()`), LRU eviction by moka.
- One shared capacity budget covers both SQL Arrow arrays and grouped-partials decoded columns.

### Construction Chain
`Node::new()` → `ShardManager::new_full(data_dir, durability, column_cache)` → `CompositeEngine::new_with_mappings(..., column_cache)` → `HotEngine::new_with_mappings(..., column_cache)`.
- Cache capacity is derived from `AppConfig::column_cache_size_percent` (default 10, capped at 90% of system RAM).
- Selectivity threshold is derived from `AppConfig::column_cache_populate_threshold` (default 5, percentage 0–100).
- `ColumnCache::new(max_bytes, populate_threshold_percent)` stores both capacity and threshold.
- `compute_cache_bytes(percent)` reads `/proc/meminfo`, falls back to 1 GB.
- Set `column_cache_size_percent: 0` to disable caching entirely.
- Set `column_cache_populate_threshold: 0` to always eagerly populate on miss (old behavior).
- Set `column_cache_populate_threshold: 100` to never eagerly populate (only use cache if already populated by a prior broad query).

### Cache Guard — Oversized Segment Protection
Before building a full-segment array, `should_cache_full_segment_array(reader, max_doc, cache_max)` estimates the Arrow array size:
- **Numeric (F64/I64)**: `max_doc * 8 + null_bitmap`
- **String**: `offsets + null_bitmap + (max_doc * estimated_avg_term_len)` — avg term len is sampled from up to 32 dictionary terms via `estimate_string_array_value_bytes()`, then multiplied by 2× as a safety margin.
- If the estimate exceeds `cache_max / 4`, the segment is too large to cache and `build_selective_array()` reads only matching doc IDs directly into Arrow — no full-segment allocation.
- The `ColumnCache::insert()` method has a secondary guard: arrays larger than 25% of capacity are silently dropped.
- `build_full_segment_array()` for strings uses `StringBuilder::with_capacity(max_doc, 0)` — deferred string buffer allocation to avoid large upfront memory spikes.

### Integration with grouped_partials
- Match-all grouped-partials direct scans are the only grouped path that may populate new full-segment cache entries.
- Grouped cache entries store typed numeric values or string ordinals for direct `doc_id` lookup by grouped readers.
- Filtered grouped collectors may reuse warm grouped cache entries, but they must not populate new full-segment entries from partial scans.

### Integration with sql_record_batch
When the fast path is eligible (`!needs_stored_doc && !columns.is_empty()`):
1. Group matched docs by segment ordinal
2. For each column × segment: check cache hit → on miss, check selectivity threshold → if above threshold, build full array + cache + `take()` → if below threshold, `build_selective_array` (no cache population)
3. Concatenate per-segment arrays, reorder to match original `top_docs` order
4. Build `RecordBatch` with `_id`/`_score` columns + data columns
Falls back to per-doc stored-doc reading when any column requires `SourceFallback`.

## Routing (src/engine/routing.rs)
- `calculate_shard(doc_id, num_shards) -> u32` — Murmur3 hash modulo
- `route_document(doc_id, metadata) -> Option<NodeId>` — returns primary node for doc

## Checkpoint Semantics
- **Processed checkpoint**: highest contiguous processed prefix; `None` means
  nothing processed and differs from `Some(0)`.
- **Persisted checkpoint**: highest contiguous prefix that is both processed
  and WAL-durable.
- **Global checkpoint**: monotonic minimum persisted checkpoint across the
  primary and every authoritative in-sync replica. Sample the primary's
  persisted prefix after replication, before taking the tracker lock, and use
  each replica's highest reported prefix for its current UUID, allocation ID,
  and primary term. A missing current-copy report holds progress back.
- Above-gap interval sets retain exact processed/persisted identities; maximum
  sequence is tracked separately and must never substitute for a checkpoint.
- `flush_with_global_checkpoint()`: retains WAL entries above global_cp for replica recovery
