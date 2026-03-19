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
    fn bulk_add_documents(&self, docs: Vec<(String, Value)>) -> Result<Vec<String>>;
    fn delete_document(&self, doc_id: &str) -> Result<u64>;
    fn get_document(&self, doc_id: &str) -> Result<Option<Value>>;

    // Engine lifecycle
    fn refresh(&self) -> Result<()>;
    fn flush(&self) -> Result<()>;
    fn flush_with_global_checkpoint(&self) -> Result<()>;  // Retains WAL above global_cp
    fn doc_count(&self) -> u64;

    // Search
    fn search(&self, query_str: &str) -> Result<Vec<Value>>;
    fn search_query(&self, req: &SearchRequest) -> Result<(Vec<Value>, usize, HashMap<String, PartialAggResult>)>;
    fn search_knn(&self, field: &str, vector: &[f32], k: usize) -> Result<Vec<Value>>;
    fn search_knn_filtered(&self, field: &str, vector: &[f32], k: usize, filter: Option<&QueryClause>) -> Result<Vec<Value>>;

    // Checkpoint tracking (replication)
    fn local_checkpoint(&self) -> u64;
    fn update_local_checkpoint(&self, seq_no: u64);
    fn global_checkpoint(&self) -> u64;
    fn update_global_checkpoint(&self, checkpoint: u64);
}
```

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

### Constructors
- `new(data_dir, refresh_interval)` — default refresh loop (static interval)
- `new_with_mappings(data_dir, refresh_interval, mappings, durability)` — with schema + WAL

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
- `replay_translog()` — crash recovery from WAL

### Field Schema Flags
Numeric fields use three Tantivy flags (mirrors OpenSearch default doc_values: true):
- INDEXED - inverted index, enables search queries (term/range/match)
- STORED - preserves original value, retrievable in results
- FAST - columnar storage, critical for range queries, sorting, and aggregations

Integer and Float fields get all three: INDEXED | STORED | FAST.
Keyword and Boolean fields get: STRING | STORED + FAST (set_fast(None) for dictionary-encoded columnar).
Without FAST, range queries scan the inverted index (slow on high-cardinality fields).
With FAST, Tantivy reads a columnar structure - orders of magnitude faster for range queries, sorting, and aggregations.

### Fast-Field Aggregations (Single-Pass Collector)
Aggregations run in the same Tantivy search pass as hit collection via `AggCollector` -- a custom
`tantivy::collector::Collector` implementation. Combined with TopDocs via tuple collector:
`(TopDocs, Option<AggCollector>, Count)`. When no aggs are requested, `None` adds zero overhead.

**Architecture (mirrors OpenSearch's aggregation design):**
- `AggCollector` implements `Collector` -- `for_segment()` opens fast-field columns per segment
- `AggSegmentCollector` implements `SegmentCollector` -- `collect(doc, score)` reads column values and accumulates
- `harvest()` returns per-segment data, `merge_fruits()` merges across segments into `HashMap<String, PartialAggResult>`
- Per-shard partial results returned through gRPC, merged at coordinator via `merge_aggregations()`

**Supported aggregation types:**
- **Numeric** (Stats, Min, Max, Avg, Sum, ValueCount): reads `NumCol` (wraps `Column<f64>` or `Column<i64>`)
- **Histogram**: reads numeric column, buckets by `floor(value / interval)`
- **Terms**: reads `StrColumn` (dictionary-encoded keyword fields) or numeric column for numeric fields

**Key types in `src/engine/tantivy.rs`:**
- `NumCol` -- wraps i64/f64 fast-field columns with `first_f64()` coercion
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

## Routing (src/engine/routing.rs)
- `calculate_shard(doc_id, num_shards) -> u32` — Murmur3 hash modulo
- `route_document(doc_id, metadata) -> Option<NodeId>` — returns primary node for doc

## Checkpoint Semantics
- **Local checkpoint**: highest contiguous seq_no applied on this replica/primary
- **Global checkpoint**: min of all in-sync replicas' local checkpoints (primary only)
- `flush_with_global_checkpoint()`: retains WAL entries above global_cp for replica recovery
