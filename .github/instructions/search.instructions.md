# Search Module — src/search/mod.rs

## SearchRequest
```rust
pub struct SearchRequest {
    pub query: QueryClause,                         // default: MatchAll
    pub size: usize,                                // default: 10
    pub from: usize,                                // default: 0
    pub knn: Option<KnnQuery>,                      // optional k-NN search
    pub sort: Vec<SortClause>,                      // default: sort by _score desc
    pub aggs: HashMap<String, AggregationRequest>,  // aggregations
}
```

## QueryClause Variants
| Variant | Description |
|---------|-------------|
| `MatchAll(Value)` | Match all documents |
| `Match(HashMap<String, Value>)` | Full-text match on a field |
| `Term(HashMap<String, Value>)` | Exact term match |
| `Wildcard(HashMap<String, Value>)` | Wildcard pattern (`*` any, `?` single) |
| `Prefix(HashMap<String, Value>)` | Prefix match |
| `Fuzzy(HashMap<String, FuzzyParams>)` | Fuzzy match (edit distance 0-2, default 1) |
| `Range(HashMap<String, RangeCondition>)` | Range: `{ gt, gte, lt, lte }` |
| `Bool(BoolQuery)` | `{ must, should, must_not, filter }` |

## FuzzyParams
```rust
pub struct FuzzyParams {
    pub value: String,
    pub fuzziness: Option<u8>,  // 0-2, default 1
}
```

## BoolQuery
```rust
pub struct BoolQuery {
    pub must: Vec<QueryClause>,
    pub should: Vec<QueryClause>,
    pub must_not: Vec<QueryClause>,
    pub filter: Vec<QueryClause>,  // non-scoring (used for range, term filters)
}
```

## k-NN Search
```rust
pub struct KnnQuery {
    pub fields: HashMap<String, KnnParams>,  // field_name → params
}

pub struct KnnParams {
    pub vector: Vec<f32>,
    pub k: usize,
    pub filter: Option<QueryClause>,  // optional pre-filter
}
```

## Aggregations
| Type | Struct Fields | Description |
|------|---------------|-------------|
| `Terms` | `field, size` | Top-N buckets by value (default size 10) |
| `Stats` | `field` | min, max, sum, count, avg |
| `Min` | `field` | Minimum value |
| `Max` | `field` | Maximum value |
| `Avg` | `field` | Average value |
| `Sum` | `field` | Sum of values |
| `ValueCount` | `field` | Count of values |
| `Histogram` | `field, interval` | Fixed-interval numeric buckets |

### Aggregation Flow
1. Per-shard: `engine.search_query(req)` computes partial aggregations via Tantivy's `AggCollector`
2. Remote shards serialize partials into the `partial_aggs_json` bytes field; local shards return partials directly
3. Coordinator: `merge_aggregations()` combines per-shard partial results
4. Returned in response under `"aggregations"` key

## Sort
- `SortClause::Simple(String)` — `"_score"` or field name
- `SortClause::Field(HashMap<String, SortOrder>)` — `{ "year": "desc" }`
- Default sort (no `sort` clause): `_score` descending
- Nulls sort last

## Search Flow (Scatter-Gather)
1. Coordinator receives `POST /{index}/_search` with SearchRequest
2. Look up all shards for the index from cluster state
3. Local shards → `engine.search_query(req)` directly
4. Remote shards → scatter via gRPC `forward_search_dsl_to_shard()`
5. Gather results: merge hits by score, merge aggregations, apply from/size
6. Return `{ "_shards": {...}, "hits": { "total": {...}, "hits": [...] }, "aggregations": {...} }`

## Hybrid Search (BM25 + k-NN)
When both `query` and `knn` are present:
1. Full-text search produces BM25-scored results
2. k-NN search produces distance-scored results
3. Results merged using Reciprocal Rank Fusion (RRF)
