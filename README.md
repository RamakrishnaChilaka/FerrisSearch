<p align="center">
  <img src="docs/logo.png" alt="FerrisSearch" width="400">
</p>

# FerrisSearch

<p align="center">
  <strong>A distributed search engine written in Rust, powered by <a href="https://github.com/quickwit-oss/tantivy">Tantivy</a></strong>
</p>

<p align="center">
  <a href="#getting-started">Getting Started</a> &middot;
  <a href="#api-reference">API Reference</a> &middot;
  <a href="#benchmarks">Benchmarks</a> &middot;
  <a href="#replication">Replication</a> &middot;
  <a href="#testing">Testing</a>
</p>

---

FerrisSearch is a high-performance, Rust-native distributed search engine with OpenSearch-compatible REST APIs. Built for teams that want the familiar OpenSearch interface with the performance and safety of Rust.

> **⚡ Performance:** 2M documents — ingestion at **9,384 docs/sec**, search at **p50 = 28.9ms**, zero errors — [see benchmarks](#benchmarks)

## Highlights

- **OpenSearch-compatible REST API** — drop-in `PUT /{index}`, `POST /_doc`, `GET /_search` endpoints
- **Raft consensus** — cluster state managed by [openraft](https://github.com/datafuselabs/openraft); quorum-based leader election, linearizable writes, automatic failover, persistent log storage via [redb](https://github.com/cberner/redb)
- **Vector search** — k-NN approximate nearest neighbor search via [USearch](https://github.com/unum-cloud/usearch) (HNSW algorithm); hybrid full-text + vector queries
- **Distributed clustering** — multi-node clusters with shard-based data distribution
- **Synchronous replication** — primary-replica replication over gRPC; writes acknowledged only after all in-sync replicas confirm
- **Scatter-gather search** — queries fan out across shards, results merged and returned
- **Crash recovery** — binary write-ahead log (WAL) with configurable durability (`request` fsync-per-write or `async` timer-based); sequence number checkpointing and translog-based replica recovery
- **Zero external dependencies** — no JVM, no Zookeeper, just a single binary

## Getting Started

### Prerequisites

- Rust (2024 edition)
- Protobuf compiler (`protoc`)

### Single node

```bash
cargo run
```

### Docker

```bash
docker build -t ferrissearch .
docker run -p 9200:9200 -p 9300:9300 ferrissearch
```

```bash
curl http://localhost:9200/
```
```json
{"name": "ferrissearch-node", "version": "0.1.0", "engine": "tantivy"}
```

### Multi-node cluster

```bash
# Terminal 1
./dev_cluster.sh 1    # HTTP 9200 · Transport 9300 · Raft ID 1

# Terminal 2
./dev_cluster.sh 2    # HTTP 9201 · Transport 9301 · Raft ID 2

# Terminal 3
./dev_cluster.sh 3    # HTTP 9202 · Transport 9302 · Raft ID 3
```
### Configuration

Configure via `config/ferrissearch.yml` or `FERRISSEARCH_*` environment variables:

| Option | Default | Description |
|--------|---------|-------------|
| `node_name` | `node-1` | Node identifier |
| `cluster_name` | `ferrissearch` | Cluster name |
| `http_port` | `9200` | REST API port |
| `transport_port` | `9300` | gRPC transport port |
| `data_dir` | `./data` | Data storage directory |
| `seed_hosts` | `["127.0.0.1:9300"]` | Seed nodes for discovery |
| `raft_node_id` | `1` | Unique Raft consensus node ID |
| `translog_durability` | `request` | Translog fsync mode: `request` (per-write) or `async` (timer) |
| `translog_sync_interval_ms` | (unset) | Background fsync interval when durability is `async` (default: 5000) |

## API Reference

### Indices

```bash
# Create an index
curl -X PUT 'http://localhost:9200/my-index' \
  -H 'Content-Type: application/json' \
  -d '{"settings": {"number_of_shards": 1, "number_of_replicas": 1}}'

# Create an index with field mappings
curl -X PUT 'http://localhost:9200/movies' \
  -H 'Content-Type: application/json' \
  -d '{
    "settings": {"number_of_shards": 3, "number_of_replicas": 1},
    "mappings": {
      "properties": {
        "title":     {"type": "text"},
        "genre":     {"type": "keyword"},
        "year":      {"type": "integer"},
        "rating":    {"type": "float"},
        "embedding": {"type": "knn_vector", "dimension": 3}
      }
    }
  }'

# Delete an index
curl -X DELETE 'http://localhost:9200/my-index'

# Get index settings
curl 'http://localhost:9200/my-index/_settings'

# Update dynamic settings (refresh_interval, number_of_replicas)
curl -X PUT 'http://localhost:9200/my-index/_settings' \
  -H 'Content-Type: application/json' \
  -d '{"index": {"refresh_interval": "2s", "number_of_replicas": 2}}'
```

**Supported field types:** `text` (analyzed), `keyword` (exact match), `integer`, `float`, `boolean`, `knn_vector`.
Unmapped fields are indexed into a catch-all "body" field for backward compatibility.

### Documents

```bash
# Index a document (auto-generated ID)
curl -X POST 'http://localhost:9200/my-index/_doc' \
  -H 'Content-Type: application/json' \
  -d '{"title": "Hello World", "tags": "rust search"}'

# Index a document with explicit ID
curl -X PUT 'http://localhost:9200/my-index/_doc/1' \
  -H 'Content-Type: application/json' \
  -d '{"title": "Hello World", "year": 2024}'

# Get a document
curl 'http://localhost:9200/my-index/_doc/{id}'

# Delete a document
curl -X DELETE 'http://localhost:9200/my-index/_doc/{id}'

# Partial update a document (merge fields)
curl -X POST 'http://localhost:9200/my-index/_update/1' \
  -H 'Content-Type: application/json' \
  -d '{"doc": {"rating": 9.5, "genre": "scifi"}}'

# Bulk index
curl -X POST 'http://localhost:9200/my-index/_bulk' \
  -H 'Content-Type: application/json' \
  -d '[
    {"_doc_id": "doc-1", "_source": {"name": "Alice"}},
    {"_doc_id": "doc-2", "_source": {"name": "Bob"}}
  ]'
```

### Search

```bash
# Match all
curl 'http://localhost:9200/my-index/_search'

# Query string with pagination
curl 'http://localhost:9200/my-index/_search?q=rust&from=0&size=10'

# DSL: match query
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"title": "search engine"}}}'

# DSL: bool query (must + must_not)
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "must": [{"match": {"title": "rust"}}],
        "must_not": [{"match": {"title": "web"}}]
      }
    }
  }'

# DSL: bool query (should = OR)
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "should": [
          {"match": {"title": "rust"}},
          {"match": {"title": "python"}}
        ]
      }
    },
    "from": 0,
    "size": 5
  }'
```

```bash
# Fuzzy query (typo-tolerant search)
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"fuzzy": {"title": {"value": "rsut", "fuzziness": 2}}}}'
```

### Vector Search (k-NN)

```bash
# Index documents with embedding vectors
curl -X PUT 'http://localhost:9200/my-index/_doc/1' \
  -H 'Content-Type: application/json' \
  -d '{"title": "Rust search engine", "embedding": [1.0, 0.0, 0.0]}'

# k-NN search: find 3 nearest neighbors
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"knn": {"embedding": {"vector": [1.0, 0.0, 0.0], "k": 3}}}'

# k-NN with pre-filter: only search within matching documents
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "knn": {
      "embedding": {
        "vector": [1.0, 0.0, 0.0],
        "k": 5,
        "filter": {"match": {"genre": "action"}}
      }
    }
  }'

# Hybrid: full-text + vector search (merged with Reciprocal Rank Fusion)
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"match": {"title": "rust"}},
    "knn": {"embedding": {"vector": [1.0, 0.0, 0.0], "k": 5}}
  }'
```

Vector fields are auto-detected when an array of numbers is indexed. Uses [USearch](https://github.com/unum-cloud/usearch) (HNSW algorithm) with cosine similarity by default. See [docs/vector-search.md](docs/vector-search.md) for architecture details.

### Sorting

```bash
# Sort by field ascending
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "sort": [{"year": "asc"}]}'

# Sort by field descending with _score tiebreaker
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "sort": [{"year": "desc"}, "_score"]}'

# Sort with object syntax
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "sort": [{"rating": {"order": "desc"}}]}'
```

Default sort (no `sort` clause) is by `_score` descending. Nulls sort last.

### Aggregations

```bash
# Terms aggregation: top genres
curl -X POST 'http://localhost:9200/movies/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"match_all": {}},
    "size": 0,
    "aggs": {
      "genres": {"terms": {"field": "genre", "size": 10}}
    }
  }'

# Stats aggregation: min/max/avg/sum/count
curl -X POST 'http://localhost:9200/movies/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"match_all": {}},
    "size": 0,
    "aggs": {
      "rating_stats": {"stats": {"field": "rating"}}
    }
  }'

# Histogram: group by decade
curl -X POST 'http://localhost:9200/movies/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"match_all": {}},
    "size": 0,
    "aggs": {
      "by_decade": {"histogram": {"field": "year", "interval": 10}}
    }
  }'

# Multiple aggregations + filtered query
curl -X POST 'http://localhost:9200/movies/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"match": {"genre": "scifi"}},
    "size": 0,
    "aggs": {
      "avg_rating": {"avg": {"field": "rating"}},
      "max_year": {"max": {"field": "year"}}
    }
  }'
```

Supported aggregation types: `terms`, `stats`, `min`, `max`, `avg`, `sum`, `value_count`, `histogram`.
Use `"aggregations"` as an alias for `"aggs"`. Aggregations run on query-filtered hits and merge correctly across shards.

```bash
# DSL: range query (inside bool filter)
curl -X POST 'http://localhost:9200/my-index/_search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {
      "bool": {
        "must": [{"match": {"title": "rust"}}],
        "filter": [{"range": {"year": {"gte": 2020, "lte": 2026}}}]
      }
    }
  }'
```

### Operations

```bash
curl -X POST 'http://localhost:9200/my-index/_refresh'   # Make recent writes searchable
curl -X POST 'http://localhost:9200/my-index/_flush'      # Fsync translog to disk
```

### Monitoring

```bash
curl 'http://localhost:9200/_cluster/health'    # Cluster health
curl 'http://localhost:9200/_cluster/state'     # Cluster state (nodes, indices, master)
curl 'http://localhost:9200/_cat/nodes'         # List nodes
curl 'http://localhost:9200/_cat/master'        # Current master node
curl 'http://localhost:9200/_cat/shards'        # List shards
curl 'http://localhost:9200/_cat/indices'       # List indices
```

> Append `?pretty` to any endpoint for formatted JSON.

## Consensus & Replication

FerrisSearch uses two complementary replication mechanisms:

### Cluster state (Raft consensus)
All cluster metadata — node membership, index definitions, shard assignments, master identity — is managed by Raft:

1. Mutations are proposed to the Raft leader via `client_write(ClusterCommand)`
2. Leader replicates the log entry to a majority of voters
3. Once committed, every node's state machine applies the change identically
4. Leader election happens automatically if the current leader dies (1.5–3s timeout)
5. Dead nodes are detected after 15s of missed heartbeats and removed from the cluster
6. When a dead node held a primary shard, the best in-sync replica is promoted to primary
7. New leader observes a 20s grace period before scanning for dead nodes to avoid false positives

### Document data (gRPC replication)
Document writes use direct primary-to-replica replication with sequence number tracking:

1. Client writes to the primary shard; WAL assigns a monotonic seq_no
2. Primary persists to its engine and WAL, updating its local checkpoint
3. Primary forwards the operation (with seq_no) to all in-sync replicas via gRPC
4. Each replica applies the write, updates its local checkpoint, and returns it
5. Primary computes the global checkpoint (min of all checkpoints) and advances it
6. Write is acknowledged to the client after all replicas confirm
7. On flush, translog entries above the global checkpoint are retained for recovery
8. A recovering replica requests missing ops from the primary's translog and replays them

## Testing

```bash
cargo test                                      # All 476 tests
cargo test --lib                                # Unit tests (416)
cargo test --test consensus_integration          # Raft consensus tests (29)
cargo test --test replication_integration        # Replication tests (31)
```

Integration tests run entirely in-process — they spin up real gRPC servers with isolated temp directories. No external services needed.

## Benchmarks

3-node cluster (single dev box), 2M documents (~1 GB), 3 shards, 0 replicas.

**Environment:** AMD EPYC 7763 (8 cores / 16 threads), 32 GB RAM, Ubuntu 24.04 (WSL2)

### Ingestion

2,000,000 documents (~954 MB) via `opensearch-py` bulk API in batches of 5,000 docs.

| Metric | Value |
|--------|-------|
| Documents | 2,000,000 |
| Errors | 0 |
| Total time | 213.1s |
| Throughput | **9,384 docs/sec** |

**Bulk batch latency (400 batches × 5,000 docs):**

| Min | Avg | p50 | p95 | p99 | Max |
|-----|-----|-----|-----|-----|-----|
| 307.8ms | 429.2ms | 380.3ms | 705.9ms | 946.4ms | 2117.3ms |

### Search

10,000 queries across 20 query types, concurrency = 4.

```
Query Type                 Count  Err      Min      Avg      p50      p95      p99      Max   Hits/q
────────────────────────────────────────────────────────────────────────────────────────────────────
agg_filtered                 500    0     9.8ms    32.4ms    18.1ms   140.5ms   170.2ms   327.2ms  250026
agg_histogram_price          500    0    73.0ms   104.0ms    89.8ms   205.5ms   278.5ms   329.0ms 2000000
agg_stats_price              500    0    34.3ms    59.1ms    49.0ms   148.7ms   193.1ms   286.0ms 2000000
agg_terms_category           500    0   257.1ms   348.8ms   324.7ms   513.1ms   584.2ms   632.7ms 2000000
bool_filter_range            500    0     9.9ms    31.9ms    19.8ms   140.0ms   167.6ms   291.0ms   21752
bool_must                    500    0     8.7ms    29.9ms    16.3ms   137.6ms   170.3ms   253.4ms   82446
bool_should                  500    0     7.8ms    29.2ms    16.0ms   138.0ms   167.4ms   284.7ms  335418
complex_bool                 500    0    36.1ms    62.2ms    46.5ms   172.2ms   266.9ms   296.0ms  534522
fuzzy_title                  500    0     6.1ms    23.7ms    12.7ms    78.2ms   161.5ms   188.9ms  122918
match_all                    500    0    23.1ms    47.5ms    33.9ms   151.2ms   182.6ms   257.5ms 2000000
match_description            500    0    21.4ms    42.9ms    30.9ms   147.4ms   178.6ms   209.3ms 1716614
match_title                  500    0     5.5ms    25.7ms    12.9ms   132.6ms   179.8ms   303.6ms  124661
paginated                    500    0    12.3ms    54.2ms    44.5ms   160.0ms   186.7ms   334.8ms 1246079
prefix_title                 500    0     6.0ms    26.4ms    12.6ms   141.3ms   169.9ms   255.1ms  129316
range_price                  500    0    11.0ms    37.6ms    24.6ms   146.2ms   171.2ms   213.9ms  506005
range_rating                 500    0    20.6ms    48.3ms    35.9ms   156.4ms   183.2ms   241.4ms 1378638
sort_price_asc               500    0    19.8ms    45.5ms    30.6ms   155.1ms   255.2ms   290.9ms 2000000
sort_rating_desc             500    0     7.4ms    27.5ms    14.0ms   136.4ms   167.7ms   292.3ms  132315
term_category                500    0     6.7ms    26.3ms    13.6ms   134.8ms   172.6ms   319.6ms  249987
wildcard_title               500    0     6.2ms    25.0ms    13.0ms   126.3ms   156.2ms   276.4ms  132196
────────────────────────────────────────────────────────────────────────────────────────────────────
TOTAL                      10000    0     5.5ms    56.4ms    28.9ms   266.3ms   404.0ms   632.7ms
```

**10,000 queries | 0 errors | 70.8 queries/sec | p50 = 28.9ms | concurrency = 4**

Reproduce with:
```bash
pip install opensearch-py
python3 scripts/ingest_1gb.py                              # populate 2M docs
python3 scripts/search_1gb.py --queries 500 --concurrency 4 # run search benchmark
```

## Project Structure

```
src/
├── api/           REST API handlers (Axum)
├── cluster/       Cluster state, membership, shard routing
├── config/        Configuration loading
├── consensus/     Raft consensus (openraft): types, store, state machine, network
├── engine/        Tantivy search engine wrapper
├── replication/   Primary → replica replication
├── shard/         Shard lifecycle management
├── transport/     gRPC client & server (tonic) + Raft RPCs
├── wal/           Write-ahead log
└── main.rs

proto/             gRPC service definitions (including Raft RPCs)
tests/             Integration tests (consensus + replication)
config/            Default configuration
```

## Roadmap

### Search & Query
- [x] Pagination support (`from` / `size` parameters)
- [x] Sort by field and `_score`
- [x] Bool queries (`must`, `should`, `must_not`, `filter`)
- [x] Range queries (`gt`, `gte`, `lt`, `lte`)
- [x] Wildcard and prefix queries
- [x] Return `_score` in search results
- [x] Aggregations (terms, histogram, stats)

### Vector Search (k-NN)
- [x] USearch integration for HNSW-based approximate nearest neighbor search
- [x] `knn_vector` field type in index mappings
- [x] Index vectors alongside documents (`PUT /_doc` with embedding field)
- [x] k-NN search API (`POST /_search` with `knn` clause)
- [x] Distance metrics: cosine, L2 (euclidean), inner product
- [x] Hybrid search: combine BM25 full-text scores + vector similarity in one query
- [x] k-NN across shards (scatter-gather for vector queries)
- [ ] Quantization support (f16, i8) for memory efficiency
- [ ] Disk-backed vector indexes (mmap via USearch)
- [x] Pre-filtering: apply bool/range filters before vector search

### Index Management
- [x] Field mappings in `PUT /{index}` (explicit schema definition)
- [ ] Dynamic vs. strict mapping modes
- [x] Update document API (`POST /{index}/_update/{id}`)
- [ ] Index aliases
- [ ] Index templates
- [x] Dynamic settings updates (`number_of_replicas`, `refresh_interval`)
- [ ] Document versioning and optimistic concurrency control

### Cluster Reliability
- [x] Leader election with quorum consensus (Raft)
- [x] Node failure detection and automatic removal
- [x] Leader failover with automatic re-election
- [ ] Automatic shard rebalancing across nodes
- [x] Shard reassignment on node failure
- [x] Replica promotion on primary failure (ISR-aware: picks replica with highest checkpoint)
- [ ] Shard awareness (co-location prevention)
- [ ] Delayed allocation for rolling restarts
- [x] Persistent Raft log (disk-backed storage)

### Replication & Recovery
- [x] Sequence number checkpointing (local + global checkpoints)
- [x] In-sync replica set (ISR) tracking
- [x] Replica recovery (catch-up from primary via translog replay)
- [x] Translog retention above global checkpoint for recovery
- [ ] Segment-level replication (ship segment files instead of individual docs)
- [ ] Slow replica detection and backpressure

### Transport & Resilience
- [x] Connection pooling (reuse gRPC channels)
- [ ] Retry with exponential backoff
- [ ] Circuit breaker for unresponsive nodes
- [ ] TLS encryption for gRPC transport
- [ ] Adaptive request timeouts

### Storage & Durability
- [ ] Snapshot and restore
- [ ] Remote storage backends (S3, GCS, Azure Blob)
- [ ] Tiered storage (hot/warm/cold)
- [x] Translog retention policies (checkpoint-based)
- [x] Configurable translog durability (`request` / `async`)
- [ ] Rolling translog segments

### Observability
- [ ] Prometheus metrics endpoint (`/_metrics`)
- [ ] Per-node and per-shard stats APIs
- [ ] Query latency histograms
- [ ] Index size and document count tracking
- [ ] OpenTelemetry tracing integration

### Security
- [ ] Basic authentication (username/password)
- [ ] Role-based access control (RBAC)
- [ ] TLS for HTTP API
- [ ] Encryption at rest
Apache-2.0
