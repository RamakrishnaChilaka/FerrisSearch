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
    fn add_document_with_seq(&self, doc_id: &str, payload: Value, seq_no: u64) -> Result<String>;
    fn bulk_add_documents(&self, docs: Vec<(String, Value)>) -> Result<Vec<String>>;
    fn bulk_add_documents_with_start_seq(&self, docs: Vec<(String, Value)>, start_seq_no: u64) -> Result<Vec<String>>;
    fn delete_document(&self, doc_id: &str) -> Result<u64>;
    fn delete_document_with_seq(&self, doc_id: &str, seq_no: u64) -> Result<u64>;
    fn get_document(&self, doc_id: &str) -> Result<Option<Value>>;

    // Engine lifecycle
    fn refresh(&self) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn flush_with_global_checkpoint(&self) -> Result<()>;  // Retains WAL above global_cp
    fn doc_count(&self) -> u64;

    // Search
    fn search(&self, query_str: &str) -> Result<Vec<Value>>;
    fn search_query(&self, req: &SearchRequest) -> Result<(Vec<Value>, usize, HashMap<String, PartialAggResult>)>;
```

### search_query Collector Selection
- `size=0` with no aggs: uses `(None::<AggCollector>, Count)` — skip TopDocs entirely
- `size=0` with aggs: uses `(AggCollector, Count)` — aggs without hit materialization
- `size>0` with fast-field sort: uses `TopDocs::order_by_fast_field()` for Tantivy-native sorting
- `size>0` default: uses `(TopDocs::with_limit(from + size), AggCollector?, Count)`
- TopDocs limit is always `from + size` (not `max(from+size, 100)`) — each shard collects exactly the requested count; the coordinator handles cross-shard merging
    fn sql_record_batch(&self, req: &SearchRequest, columns: &[String], needs_id: bool, needs_score: bool) -> Result<Option<SqlBatchResult>>;
    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<Value>>;
    fn search_knn_filtered(&self, field: &str, vector: &[f32], k: usize, filter: Option<&QueryClause>) -> Result<Vec<Value>>;

    // Checkpoint tracking (replication)
    fn local_checkpoint(&self) -> u64;
    fn update_local_checkpoint(&self, seq_no: u64);
    fn global_checkpoint(&self) -> u64;
    fn update_global_checkpoint(&self, checkpoint: u64);
}
```

### Seq Ownership Rule
- `add_document()` / `bulk_add_documents()` / `delete_document()` are for local primary-originated writes that allocate new WAL seq_nos
- `*_with_seq` methods are for replica apply and recovery replay only
- Replica/recovery code MUST preserve the primary-assigned seq_no when writing to WAL; do not route replicated operations through the local-allocation methods

## CompositeEngine (src/engine/composite.rs)
```rust
pub struct CompositeEngine {
    text: HotEngine,
    vector: RwLock<Option<VectorIndex>>,
    data_dir: PathBuf,
    checkpoint: AtomicU64,   // local checkpoint (seq_no)
    global_cp: AtomicU64,    // global checkpoint (primary only)
}
```
- `CompositeEngine` updates its local checkpoint from the explicit seq in replica/recovery paths and from `text.last_seq_no()` for local primary writes

### Constructors
- `new(data_dir, refresh_interval)` — default refresh loop (static interval)
- `new_with_mappings(data_dir, refresh_interval, mappings, durability, column_cache)` — with schema + WAL + shared column cache

### Refresh Loop (reactive)
```rust
// start_refresh_loop_reactive(engine, refresh_rx)
tokio::select! {
    () = tokio::time::sleep(interval) => { engine.refresh(); }
    result = refresh_rx.changed() => {
        // Update interval from settings change
        interval = *refresh_rx.borrow_and_update();
    }
}
```
- Subscribes to `SettingsManager::watch_refresh_interval()` watch channel
- Reacts to dynamic `refresh_interval_ms` settings changes without restart

### Vector Auto-detection
- On `add_document()`: scans payload for arrays of numbers
- Auto-creates VectorIndex if a `knn_vector` field is encountered
- `rebuild_vectors()` — recovers USearch index from Tantivy docs on startup (crash recovery)

## HotEngine (src/engine/tantivy.rs)
```rust
// Key internals
field_registry: RwLock<FieldRegistry>  // maps field names → Tantivy Field handles
wal: Option<Arc<dyn WriteAheadLog>>    // per-shard WAL
```
- **Dynamic fields**: creates Tantivy fields on first encounter
- **`body` field**: catch-all for unmapped textual content
- `matching_doc_ids(clause)` — returns doc ID set for k-NN pre-filtering
- `replay_translog()` — crash recovery from WAL, replaying only entries at or above the persisted committed checkpoint
- Replica/recovery writes use `append_with_seq()` / `write_bulk_with_start_seq()` under the hood so persisted WAL seq_nos match the primary's numbering

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

### Fast-Field Aggregations (Single-Pass Collector)
Aggregations run in the same Tantivy search pass as hit collection via `AggCollector` -- a custom
`tantivy::collector::Collector` implementation. Combined with TopDocs via tuple collector:
`(TopDocs, Option<AggCollector>, Count)` for hit-returning requests, or `(Option<AggCollector>, Count)`
for agg-only `size=0` requests. When no aggs are requested, `None` adds zero overhead.

### Hybrid SQL And Distributed Partial Execution
- Do not modify Tantivy fast-field storage format for hybrid SQL work. Use Tantivy's Rust APIs directly.
- Direct access patterns already expected in this module:
    - numeric columns: `segment_reader.fast_fields().f64(name)` / `.i64(name)`
    - keyword columns: `segment_reader.fast_fields().str(name)` with `term_ords(doc)` and `ord_to_str()`
- `sql_record_batch(req, columns, needs_id, needs_score)` is the reference pattern for projecting matched docs from fast fields into Arrow without `_source` materialization. It builds `type_hints` from `SqlFieldReader` variants (F64/I64 → `ColumnKind::Float64`, Str → `ColumnKind::Utf8`) and passes them to `build_record_batch_with_hints()` so that zero-result queries still produce correctly-typed Arrow columns instead of defaulting to Utf8.
- When `needs_id` is false, `_id` fast-field reads are skipped and the Arrow `_id` column is filled with empty strings.
- When `needs_score` is false, score collection is skipped and the Arrow `score` column is filled with zeros.
- The planner detects `needs_id`/`needs_score` by checking whether the SQL query references `_id` or `score`/`_score` in any projection, filter, GROUP BY, or ORDER BY.
- **Safety rule**: When `required_columns` is empty and neither `_id` nor `score` is referenced (e.g., `SELECT count(*)`), the planner forces `needs_score = true` so the batch has the correct row count. Without this, the Arrow batch would have 0 rows and DataFusion would return `count(*) = 0`.
- For grouped analytics over matched docs, prefer shard-local partial aggregation from fast fields and merge compact partials at the coordinator.
- Fall back to `_source` materialization only for fields or expressions that cannot be read from fast fields or stored fields.
- Tantivy is the preferred execution engine for search-aware work: pushdown, ranking, field reads, and shard-local partial aggregation should stay here.
- DataFusion is a downstream consumer of Arrow batches or merged partial states; do not move text-search behavior, broad scan-style execution, or default matched-doc execution into it.
- If a new SQL feature can be implemented by extending fast-field collectors or compact partial-state merging, prefer that over coordinator-side row materialization.

**Architecture (mirrors OpenSearch's aggregation design):**
- `AggCollector` implements `Collector` -- `for_segment()` opens fast-field columns per segment
- `AggSegmentCollector` implements `SegmentCollector` -- `collect(doc, score)` reads column values and accumulates
- String `terms` aggs count term ords per segment in `collect()`, then resolve ord→string once in `harvest()`
- `harvest()` returns per-segment data, `merge_fruits()` merges across segments into `HashMap<String, PartialAggResult>`

### Grouped Metrics Collector (Ordinal-Based)
The `GroupedAggCollector` computes grouped analytics (GROUP BY + aggregate functions) in a single Tantivy pass using fast fields.
- **Zero-allocation hot path**: `collect()` uses `GroupKeyReader::key(doc) → u64` to get fast-field ordinals. For string columns, this is the dictionary ordinal; for numerics, the bit-reinterpreted value. No String allocations, no JSON serialization per doc.
- **Collision-free composite keys**: Single-column GROUP BY uses `OrdHashMap<u64>` (identity hasher). Multi-column uses `HashMap<Vec<u64>>` (exact ordinal match). No hash collisions possible.
- **Batch ordinal reads**: Single-column GROUP BY buffers doc IDs and reads ordinals in batches of 1024 via `Column::first_vals()` — faster than per-doc `term_ords()`.
- **Batch numeric reads**: All numeric metric columns (sum/avg/min/max) are batch-read via `NumCol::first_vals_f64()` in the same 1024-doc batch as ordinals. This eliminates per-doc `first_f64()` calls — for 1.8M docs × 2 numeric columns, that's ~3.6M per-doc reads replaced by ~3,500 batch calls. The batch values are stored in pre-allocated `numeric_buffers` on `GroupedAggSegmentEntry` and consumed during accumulator updates.
- **Batch path handles all single-column metrics**: The batch path now processes count-only AND numeric ( `sum`, `avg`, `min`, `max`) queries — not just `count(*)`. Ordinals and numerics are read in batch, accumulators updated from pre-fetched buffers.
- **Deferred string resolution**: `harvest()` calls `GroupKeyReader::resolve(ord) → serde_json::Value` once per unique group to produce the final `GroupedMetricsBucket`s.
- **Identity hasher**: `OrdHasher` treats u64 ordinals as their own hash — zero hash computation in the per-doc path.
- **Pre-sized HashMap**: `num_terms()` from the dictionary provides approximate group count for `HashMap::with_capacity()`.
- **Top-K selection**: When ORDER BY + LIMIT are present, uses `select_nth_unstable_by` (O(N) average) instead of full sort (O(N log N)). Only the top-K subset is fully sorted.
- Per-shard partial results are serialized with `bincode-next` into the `partial_aggs_json` bytes field over gRPC, then merged at coordinator via `merge_aggregations()`
- Agg-only `size=0` requests skip `TopDocs` and hit materialization entirely
- **Direct scan for match_all**: When `query.is_match_all() && size == 0 && has_grouped_metrics`, `grouped_partials_direct_scan()` bypasses Tantivy's scorer/collector entirely — iterates segment fast-field columns directly in batches of 1024. All paths (single-column, multi-column, global) use batched reads.
- **Batched multi-column and global paths**: `flush_batch_multi()` batch-reads ordinals for ALL key readers and all numeric columns, then accumulates. Avoids per-doc `Vec<u64>` allocation for composite keys and per-doc fast-field reads. Used by both the standalone direct scan and the collector's `collect()` path for multi-column GROUP BY and ungrouped aggregates.
- **Flat array accumulation**: For single-column keyword GROUP BY with <2M unique groups on match_all queries, replaces HashMap with pre-allocated `Vec<u64>/Vec<f64>` arrays indexed directly by ordinal — zero hash computation, zero collision handling, cache-friendly sequential access. `FlatMetric::Count` and `FlatMetric::Stats` provide parallel arrays for each metric. `flat_scan_segment()` uses contiguous range-based doc buffers (no per-doc push) and `flat_flush_batch()` implements the accumulate loop.
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
All Tantivy `Term` objects MUST match the schema field type. A type mismatch (e.g., `i64` term
on an `f64` field) causes **silent 0-hit results** — Tantivy won't error, just returns nothing.

Use the `typed_term()` helper for ALL term creation in queries:
```rust
fn typed_term(&self, field: Field, value: &serde_json::Value) -> Term {
    // Checks schema via self.index.schema().get_field_entry(field).field_type()
    // Returns the correctly typed Term (from_field_f64, from_field_i64, from_field_text, etc.)
}
```

**Where `typed_term()` is used:**
- `QueryClause::Term` — exact match queries
- `QueryClause::Range` — range bounds (gte/lte/gt/lt)
- `QueryClause::Fuzzy` — fuzzy term construction

**Common pitfall:** JSON integer `10` on a float field. `serde_json::Number::as_i64()` succeeds
before `as_f64()`, creating the wrong term type. `typed_term()` checks the schema first to avoid this.

### Type-Safe Document Indexing
`build_tantivy_doc_inner()` takes a `&Schema` parameter and checks the field type before
adding numeric values:
```rust
// For a Number value on a mapped field:
match schema.get_field_entry(field).field_type() {
    FieldType::F64(_) => doc.add_f64(field, ...),  // float fields always get f64
    FieldType::I64(_) => doc.add_i64(field, ...),  // integer fields always get i64
    FieldType::U64(_) => doc.add_u64(field, ...),
    _ => {}
}
```
This prevents JSON integer `99` being stored as `i64` in an `f64` field (which would make it
unsearchable by float range queries).

## VectorIndex (src/engine/vector.rs)
- USearch HNSW wrapper (connectivity=16, expansion_add=128, expansion_search=64)
- `add_with_doc_id(doc_id, vector)`, `search(query, k) -> (keys, distances)`
- Binary persistence: `save(path)` / `open(path, dimensions, metric)`
- Doc ID ↔ numeric key mapping via `HashMap` + bincode serialization

## Column Cache (src/engine/column_cache.rs)

### Architecture
Segment-aware, lazy-loaded Arrow array cache backed by `moka`. Shared across all shards on a node via `Arc<ColumnCache>`.
- **Key**: `(SegmentId, column_name)` — Tantivy segments are immutable once committed, so cached data never goes stale.
- **Value**: Arrow `ArrayRef` covering all docs in a segment for one column. Queries extract matching rows via `arrow::compute::take()`.
- **Eviction**: Size-bounded (weighted by `ArrayRef::get_array_memory_size()`), LRU eviction by moka.

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

### Integration with sql_record_batch
When the fast path is eligible (`!needs_stored_doc && !columns.is_empty()`):
1. Group matched docs by segment ordinal
2. For each column × segment: check cache hit → on miss, check selectivity threshold → if above threshold, build full array + cache + `take()` → if below threshold, `build_selective_array` (no cache population)
3. Concatenate per-segment arrays, reorder to match original `top_docs` order
4. Build `RecordBatch` with `_id`/`score` columns + data columns
Falls back to per-doc stored-doc reading when any column requires `SourceFallback`.

## Routing (src/engine/routing.rs)
- `calculate_shard(doc_id, num_shards) -> u32` — Murmur3 hash modulo
- `route_document(doc_id, metadata) -> Option<NodeId>` — returns primary node for doc

## Checkpoint Semantics
- **Local checkpoint**: highest contiguous seq_no applied on this replica/primary
- **Global checkpoint**: min of all in-sync replicas' local checkpoints (primary only)
- `flush_with_global_checkpoint()`: retains WAL entries above global_cp for replica recovery
