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
    fn search_query(&self, req: &SearchRequest) -> Result<(Vec<Value>, usize)>;
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
