---
description: "Use for search request types, Query DSL, sorting, cursor pagination, aggregations, and hybrid result semantics."
applyTo: "src/search/**,src/api/search/**"
---

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
    pub search_after: Option<Vec<Value>>,           // optional cursor for deep pagination
}
```

## QueryClause Variants
| Variant | Description |
|---------|-------------|
| `MatchAll(Value)` | Match all documents |
| `Match(HashMap<String, Value>)` | Full-text match on a field |
| `QueryString(QueryStringParams)` | Tantivy-backed query-string subset with `query` and optional `default_field` |
| `Term(HashMap<String, Value>)` | Exact term match |
| `Terms(HashMap<String, Vec<Value>>)` | Match any exact term in a field's set |
| `Wildcard(HashMap<String, Value>)` | Wildcard pattern (`*` any, `?` single) |
| `Prefix(HashMap<String, Value>)` | Prefix match |
| `Fuzzy(HashMap<String, FuzzyParams>)` | Fuzzy match (edit distance 0-2, default 1) |
| `Range(HashMap<String, RangeCondition>)` | Range: `{ gt, gte, lt, lte }` |
| `Bool(BoolQuery)` | `{ must, should, must_not, filter }` |

### QueryClause Helpers
- `is_match_all()` — returns true if this is a `MatchAll` query. Used by `_count` fast path and SQL `count(*)` detection.

## Query-string subset

- URI `GET /{index}/_search?q=...` lowers to `QueryString`, then uses the same
  distributed DSL gatherer as `POST /{index}/_search`. Do not restore the
  separate URI path that counted a capped hit list instead of all matches.
- Omitted `q` defaults to `*:*`. Standalone `*:*` matches every live document,
  including documents with no indexed `body` terms.
- URI `df` and DSL `default_field` are optional literal field selectors.
  Bare `*` without either selector matches all documents, including documents
  with no indexed text. Ordinary unqualified terms still fall back to `body`.
  The DSL accepts only `query` and `default_field`.
- Presence applies only to standalone `field:*` or `*` with an explicit field.
  Fast fields use Tantivy's `ExistsQuery`; indexed text with field norms checks
  document lengths without enumerating terms. Text without field norms uses
  an all-terms fallback whose cost grows with vocabulary and postings.
  Missing fields match no documents. Do not fall back to `body` for an unknown
  named field.
- Keyword presence includes empty strings, and numeric presence includes zero.
  Text presence requires an indexed token; empty or analyzer-empty text does
  not match. This is an explicit difference from OpenSearch field existence.
- These wildcard rewrites apply only to standalone expressions. All other
  strings use the existing Tantivy parser; this is not full Lucene syntax.
- Parse failures retain the complete input query and typed parser cause.
  Transport must preserve that classification and cause rather than flattening
  it into an untyped success-false envelope.
- Keep parser and engine execution on the existing search worker pools.
  Existing `match` and SQL `text_match` success semantics stay unchanged.
- Range, term, terms, pushed SQL predicates, kNN filters, and numeric/date
  cursor values share fallible schema-typed conversion. Invalid values return
  classified query errors, not text terms, zero-hit successes, or assertions.
- Before constructing a Tantivy hit collector, bound the window by the shard's
  live document count and saturate `from + size`. Never pass a user-controlled
  huge capacity directly to TopDocs. Coordinator pagination overflow remains
  HTTP 400, while valid huge windows preserve total counts and result values.

## Shard failure responses

- When no shard succeeds and at least one fails, return
  `search_phase_execution_exception`, `reason: "all shards failed"`, and
  `failed_shards` with index, shard (or remote split), node, and nested reason.
- Return HTTP 400 only when every failure is a client parse or validation
  error. Preserve `query_shard_exception` and its `parse_exception` cause.
  Unavailable targets return 503; other engine failures return 500. A server
  failure must not be downgraded to 400 because another shard had a parse error.
  An unknown tokenizer is a server configuration failure, even though Tantivy
  reports it through `QueryParserError`.
- Partial failures remain HTTP 200 with `_shards.failed` and
  `_shards.failures`. A successful shard with zero hits is still successful.
  `allow_partial_search_results=false` is not implemented.
- Contained search-worker panics are server-side shard failures (500 when all
  shards fail) with the operation and panic message preserved. Keep subsequent
  requests usable. A local or remote kNN error fails that shard's complete work;
  a successful text leg must not hide a failing vector leg or count twice.
- Missing primary nodes and unavailable routed copies count as failures, not
  silently skipped work.
- Query-body `_count` and materialized/distributed SQL inherit these rules.
  Metadata-only `_count` and SQL `count(*)` reject an entirely unavailable shard
  set. SQL planning, fast-field execution, and grouped-result shaping remain
  unchanged; SQL paths using the shared gatherer inherit its error policy.
  Catalog listing semantics remain unchanged.
- Remote-store search identifies split failures and preserves the final cause
  after leaf retries; successful retries must not remain failed. An empty
  manifest or an entirely pruned candidate set remains a successful empty search.
- Empty remote-store candidate sets still validate query strings and typed
  query/cursor/filter values against the canonical mapping-derived schema on
  the search pool. Invalid queries return
  400 with their parser cause; no shard failed because no split was dispatched.
- `_count?q=...` and `_msearch` are not implemented. Do not claim otherwise.

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

Bound `k` and filtered candidate oversampling by the actual vector count before
native search or allocation. `num_candidates` is not implemented. Terms
aggregation sizes truncate actual collected buckets; they do not reserve the
requested size. Composite and top_hits aggregations are not implemented and
their request variants are rejected before collection.

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

### Terms Correctness And Counter Bounds
- Numeric terms preserve typed identity until harvest: signed integers never
  pass through `f64`, and floats normalize signed zero and NaN identity without
  saturating large integral values to `i64`.
- String terms use a bounded dense ordinal counter only when the segment
  dictionary has at most 1024 terms; larger dictionaries use the sparse map.
  Do not allocate a cardinality-sized unbounded vector.
- Invalid ordinals or dictionary decode failures fail the query instead of
  returning partial buckets.
- Declared keyword arrays are flattened/coerced at ingest and deduplicated per
  document, so one document contributes at most once to a given terms bucket.
  This does not imply SQL ARRAY values or `UNNEST`; direct SQL readers remain
  scalar-first.
- Remote-store keyword term summaries use those same canonical indexed values.
  A summary is eligible to prune only when it contains the complete distinct
  set; missing or cap-exceeded summaries must keep the split.

## Sort
- DSL sort lists and explicit SQL ORDER BY lists support at most 64 fields.
  Reject wider lists before engine dispatch/collection; enforce the same guard
  on direct engine/transport paths. This bounds per-hit sort annotation and
  quadratic cursor prefix expansion, not the result window.
- `SortClause::Simple(String)` — `"_score"` or field name
- `SortClause::Field(HashMap<String, SortOrder>)` — `{ "year": "desc" }`
- Default sort (no `sort` clause): `_score` descending
- Nulls sort last

## search_after Cursor Pagination
- OpenSearch/ES-compatible deep-pagination cursor. Pass the previous page's last hit's `sort` array as `search_after` on the next request.
- Validation in `search_documents_dsl` returns 400 `illegal_argument_exception` when:
    - `sort` is empty or its length does not match `search_after.len()`
    - `from != 0` (cursor replaces offset pagination)
    - any sort clause is `_score`
    - `knn` is also present (the cursor filter only applies to the text leg; combining would let page 2 repeat page 1 kNN hits)
- Engine builds a separate hits-only query that ANDs the cursor filter onto the user query: `BooleanQuery { (Must, user_query), (Must, cursor_filter) }`. The cursor filter is a `Should`-OR of `sort.len()` prefix clauses: prefix `i` is `(Eq sort[0]..sort[i-1]) AND (StrictInequality sort[i])`. `Bound::Excluded` is on the cursor side, `Bound::Unbounded` on the other; direction flips for `Desc`.
- **Total/aggregations invariant**: when `search_after` is present, `Count` and aggregation collectors run against the unfiltered `user_query`, not the cursor-filtered hits query. Only `TopDocs` uses the cursor-filtered query. This guarantees `hits.total` and `aggregations` are identical across all pages of a paginated request.
- Engine calls `crate::search::sort_hits(hits, sort_clauses)` after Tantivy collection (Tantivy's fast-field collector only orders by the primary sort key). `sort_hits` both sorts and annotates each hit with `sort: [v0, v1, ...]` in a single pass; annotation is idempotent (hits that already carry `sort` are left untouched). The coordinator preserves the `sort` field through local and remote shard re-enrichment in `execute_distributed_dsl_search`.
- **Response shape**: when `sort` is non-empty, every hit's `_score` is `null` and `hits.max_score` is `null`. When `sort` is empty, `_score` is the BM25 score and `max_score` is the max across returned hits (or `null` when there are no hits).
- **Sort uniqueness requirement (current limitation)**: Tantivy 0.25's `order_by_fast_field` is single-key only. The fast-field collector orders only by `sort[0]`; secondary sort keys are only applied to the shard-local hit set in `sort_hits` and do **not** influence Tantivy's top-K selection. Consequently, when many docs share the same primary sort value, Tantivy breaks ties by doc-address. `search_after` advances past the tuple `(last_primary, last_secondary, ...)`, but docs with the same `last_primary` that lost the doc-address tie-break on page N will be skipped on page N+1. For globally correct deep pagination today: ensure the primary sort field's values are unique across the queryable result set, or accept that ties may drop docs across page boundaries. Multi-key tuple-sort that pushes all sort keys into a custom Tantivy collector is tracked as future work.
- Supported sort fields for global ordering correctness: numeric fast-fields (Integer/Float/Date) with **unique** primary values. String sort (`_id`, keyword) compiles, filters correctly, and pages forward without overlap but does not guarantee global top-K ordering across pages without a custom Tantivy collector.

## Search Flow (Scatter-Gather)
1. Coordinator receives `POST /{index}/_search` with SearchRequest
2. Look up all shards for the index from cluster state
3. Local shards → `engine.search_query(req)` directly
4. Remote shards → scatter via gRPC `forward_search_dsl_to_shard()`
5. Gather results: merge default-score shard hit lists at the coordinator, re-apply explicit/custom sorts at the coordinator when shard-local ordering is not sufficient, merge aggregations, apply from/size
6. Return `{ "_shards": {...}, "hits": { "total": {...}, "hits": [...] }, "aggregations": {...} }`

## Hybrid SQL Planning Guidance
- Search-aware SQL planning should push `text_match`, term filters, and range filters into Tantivy before any Arrow/DataFusion stage.
- Distributed grouped analytics should prefer shard-local partial execution over shipping matched rows to the coordinator.
- Tantivy fast fields are the first-choice column source for grouped analytics, partial aggregates, sort keys, and pushed-down structured filters.
- Only fall back to coordinator-side row materialization when the query contains projections or expressions that cannot be executed from shard-local fast fields and partial states.
- Plan SQL in two stages:
    1. search-aware stage in Tantivy for match/filter/pushdown and shard-local partials
    2. residual SQL stage in DataFusion for remaining tabular semantics
- Treat `materialized_hits_fallback` as a compatibility path. New work should try to shrink that path, not expand it.
- Avoid describing the SQL feature as "SQL over hits" except when explicitly documenting the fallback path.

## Hybrid Search (BM25 + k-NN)
When both `query` and `knn` are present:
1. Full-text search produces BM25-scored results
2. k-NN search produces distance-scored results
3. Results merged using Reciprocal Rank Fusion (RRF)
