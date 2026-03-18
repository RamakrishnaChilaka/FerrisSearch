# Vector Search

FerrisSearch supports approximate nearest neighbor (ANN) vector search using [USearch](https://github.com/unum-cloud/usearch) with HNSW indexing. Vector search can be combined with full-text BM25 search for hybrid retrieval.

## Architecture

Each shard maintains two parallel indexes:

- **Tantivy index** — full-text search (inverted index, BM25 scoring)
- **USearch index** — vector search (HNSW graph, cosine/L2/inner product)

Both are managed by `CompositeEngine`, which transparently routes operations to the appropriate index. Vector fields are auto-detected from document payloads — any JSON array of numbers is indexed as a vector.

```
┌─────────────────────────────────────┐
│          CompositeEngine            │
│                                     │
│  ┌──────────────┐ ┌──────────────┐  │
│  │  HotEngine   │ │ VectorIndex  │  │
│  │  (Tantivy)   │ │  (USearch)   │  │
│  │              │ │              │  │
│  │ text fields  │ │ embeddings   │  │
│  │ _id, _source │ │ HNSW graph   │  │
│  └──────────────┘ └──────────────┘  │
└─────────────────────────────────────┘
```

## Indexing Vectors

Any document field containing a numeric array is automatically indexed as a vector.

```bash
PUT /movies/_doc/1
{
  "title": "The Matrix",
  "year": 1999,
  "embedding": [0.9, 0.1, 0.2]
}
```

The vector is indexed into USearch using a hash of the document ID as the key. The text fields (`title`, `year`) go into Tantivy. The raw JSON is stored in `_source`.

## k-NN Search

Find the k nearest neighbors to a query vector:

```bash
POST /movies/_search
{
  "knn": {
    "embedding": {
      "vector": [0.9, 0.1, 0.2],
      "k": 5
    }
  }
}
```

The response includes `_knn_distance` (raw distance) and `_score` (converted to a relevance score via `1 / (1 + distance)`):

```json
{
  "_id": "1",
  "_score": 0.999,
  "_knn_distance": 0.0001,
  "_knn_field": "embedding",
  "_source": { "title": "The Matrix", ... }
}
```

### Distributed k-NN

On a multi-node cluster, k-NN queries are scattered to all shards (local and remote via gRPC), then gathered at the coordinator node. Each shard independently runs the vector search on its local USearch index and returns its top-k hits.

## Pre-filtered k-NN Search

Restrict vector search to only documents matching a filter query. The filter uses the same Query DSL as regular search (match, term, bool, range, wildcard, prefix, fuzzy).

```bash
POST /movies/_search
{
  "knn": {
    "embedding": {
      "vector": [0.9, 0.1, 0.2],
      "k": 5,
      "filter": {
        "match": { "genre": "action" }
      }
    }
  }
}
```

### How filtering works

USearch (HNSW) doesn't natively support filtered search. FerrisSearch uses a **post-filtering with oversampling** strategy:

1. **Oversample** — Fetch `k × 10` candidates from the HNSW index (capped at index size)
2. **Filter** — Run the filter query against Tantivy to get the set of matching document IDs
3. **Intersect** — Keep only vector candidates whose `_id` is in the filter set
4. **Truncate** — Return the top `k` filtered results, ordered by distance

```
         USearch (HNSW)                    Tantivy
  ┌─────────────────────┐        ┌─────────────────────┐
  │ search(vector, k×10) │        │ matching_doc_ids(   │
  │                     │        │   filter_query)     │
  │ → candidates        │        │ → allowed_ids       │
  └────────┬────────────┘        └────────┬────────────┘
           │                              │
           └──────────┬───────────────────┘
                      │
              ┌───────▼───────┐
              │  intersect +  │
              │  take top k   │
              └───────────────┘
```

This approach trades a small amount of recall for filter support. With the default 10× oversampling factor, it works well when the filter matches more than ~10% of the corpus. For very selective filters (matching <1% of docs), some relevant neighbors may be missed.

### Filter examples

**Match filter:**
```json
{ "filter": { "match": { "genre": "scifi" } } }
```

**Bool filter with range:**
```json
{
  "filter": {
    "bool": {
      "must": [{ "match": { "genre": "action" } }],
      "filter": [{ "range": { "year": { "gte": 2000 } } }]
    }
  }
}
```

**Term filter:**
```json
{ "filter": { "term": { "status": "published" } } }
```

## Hybrid Search

Combine full-text BM25 search with vector similarity in a single query:

```bash
POST /movies/_search
{
  "query": { "match": { "title": "Matrix" } },
  "knn": {
    "embedding": {
      "vector": [0.9, 0.1, 0.2],
      "k": 5
    }
  }
}
```

### Reciprocal Rank Fusion (RRF)

When both `query` and `knn` are present, results are merged using Reciprocal Rank Fusion. Each document gets an RRF score based on its rank in each result set:

```
rrf_score = 1/(k + rank_text) + 1/(k + rank_knn)
```

Where `k = 60` (OpenSearch default). Documents appearing in **both** result sets receive contributions from both ranks and naturally float to the top. Documents in only one list get a single rank contribution.

This eliminates the duplicate-hit problem — each document appears exactly once in the final results, with a unified score that balances text relevance and vector similarity.

### Hybrid with pre-filter

Filters can be combined with hybrid search:

```bash
POST /movies/_search
{
  "query": { "match": { "title": "Matrix" } },
  "knn": {
    "embedding": {
      "vector": [0.9, 0.1, 0.2],
      "k": 5,
      "filter": { "match": { "genre": "scifi" } }
    }
  }
}
```

The filter applies only to the kNN results. The text query runs independently. Both result sets are then merged with RRF.

## Distance Metrics

Configured at vector index creation time (currently auto-detected as cosine):

| Metric | Description | Use case |
|--------|-------------|----------|
| Cosine | Angle between vectors (normalized) | Text embeddings, general purpose |
| L2 (Euclidean) | Straight-line distance | Image features, spatial data |
| Inner Product | Dot product | When vectors are pre-normalized |

## HNSW Parameters

The USearch HNSW index uses these defaults:

| Parameter | Value | Description |
|-----------|-------|-------------|
| `connectivity` (M) | 16 | Edges per node in the HNSW graph |
| `expansion_add` (ef_construction) | 128 | Search width during index building |
| `expansion_search` (ef_search) | 64 | Search width during queries |
| `quantization` | F32 | Vector element precision |
