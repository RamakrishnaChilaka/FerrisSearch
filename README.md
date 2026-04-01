<p align="center">
  <img src="docs/logo.png" alt="FerrisSearch" width="400">
</p>

# FerrisSearch

<p align="center">
  <strong>Distributed search engine in Rust with hybrid SQL analytics, Raft consensus, vector search, and OpenSearch-compatible APIs</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> ·
  <a href="#sql-over-search-results">SQL</a> ·
  <a href="#api-reference">API</a> ·
  <a href="#benchmarks">Benchmarks</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#testing">Testing</a> ·
  <a href="#roadmap">Roadmap</a>
</p>

---

~31,000 lines of Rust. Single binary. No JVM, no external dependencies.

**Search** in 0.6ms. **Search + GROUP BY** on 4M docs in **60ms**. **Filtered aggregations** in **2–4ms**.

## What It Does

FerrisSearch runs SQL analytics directly on search results. One query, one system:

```sql
SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes
FROM "hackernews"
WHERE text_match(title, 'rust')
GROUP BY author
HAVING posts > 5
ORDER BY avg_upvotes DESC
LIMIT 10
```

Tantivy's inverted index narrows 4M docs to ~12K matches. Each shard computes grouped partial aggregates on fast fields. Coordinator merges partials from 3 shards. **60ms**.

Every response tells you how the query ran:

```json
{
  "execution_mode": "tantivy_grouped_partials",
  "planner": {
    "text_match": {"field": "title", "query": "rust"},
    "pushed_down_filters": [],
    "group_by_columns": ["author"],
    "has_residual_predicates": false
  }
}
```

## Key Features

- **OpenSearch-compatible REST API** — `PUT /{index}`, `POST /_doc`, `GET /_search`, `POST /_bulk`
- **Hybrid SQL over search results** — `text_match()` + `GROUP BY` + `HAVING` + `ORDER BY` + `LIMIT` in one query
- **Four execution modes** — `count_star_fast`, `tantivy_fast_fields`, `tantivy_grouped_partials`, `materialized_hits_fallback` — chosen automatically by the planner
- **Raft consensus** — cluster state via [openraft](https://github.com/datafuselabs/openraft); leader election, linearizable writes, automatic failover, persistent log via [redb](https://github.com/cberner/redb)
- **Synchronous replication** — primary-replica with ISR tracking over gRPC; writes acknowledged after all in-sync replicas confirm
- **Vector search** — k-NN via [USearch](https://github.com/unum-cloud/usearch) (HNSW); hybrid full-text + vector queries with pre-filtering
- **Write-ahead log** — binary WAL with configurable durability (`request` fsync-per-write or `async` timer-based)
- **Workload isolation** — dedicated [rayon](https://github.com/rayon-rs/rayon) thread pools for search and write traffic; bulk indexing cannot starve search latency or Raft heartbeats
- **Dynamic field mapping** — fields are auto-detected and created on first document encounter
- **Prometheus metrics** — `/_metrics` endpoint with request latencies, search/index counters, SQL mode tracking, cluster gauges, and process stats (CPU, RSS, threads, FDs)
- **Interactive SQL console** — `ferris-cli` with colored tables, EXPLAIN ANALYZE visualization, query history
- **SQL correctness tests** — [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) `.slt` files (same framework as DataFusion, CockroachDB, DuckDB)

## Tech Stack

| Component | Crate | Purpose |
|-----------|-------|---------|
| Full-text search | [Tantivy](https://github.com/quickwit-oss/tantivy) 0.25 | Inverted index, BM25 scoring, fast-field columnar storage |
| SQL execution | [DataFusion](https://datafusion.apache.org/) 53 + [Arrow](https://arrow.apache.org/) | Columnar SQL over matched docs |
| Consensus | [openraft](https://github.com/datafuselabs/openraft) 0.10 | Raft leader election, log replication |
| Vector search | [USearch](https://github.com/unum-cloud/usearch) | HNSW approximate nearest neighbor |
| gRPC transport | [tonic](https://github.com/hyperium/tonic) 0.13 | Inter-node RPCs, shard forwarding |
| Persistent storage | [redb](https://github.com/cberner/redb) 3 | Raft log on disk |
| Thread pools | [rayon](https://github.com/rayon-rs/rayon) | Search/write workload isolation |
| Allocator | [jemalloc](https://github.com/tikv/jemallocator) | Reduced memory fragmentation |

## Quick Start

### Prerequisites

- Rust 1.94+ (2024 edition)
- Protobuf compiler (`protoc`)

### Single node

```bash
cargo run
curl http://localhost:9200/
```

### 3-node cluster

```bash
./dev_cluster_release.sh --nodes 3
```

Or manually:

```bash
./dev_cluster.sh 1    # HTTP 9200 · Transport 9300 · Raft ID 1
./dev_cluster.sh 2    # HTTP 9201 · Transport 9301 · Raft ID 2
./dev_cluster.sh 3    # HTTP 9202 · Transport 9302 · Raft ID 3
```

### Docker

```bash
docker build -t ferrissearch .
docker run -p 9200:9200 -p 9300:9300 ferrissearch
```

### SQL Console

```bash
cargo build --release --bin ferris-cli
./target/release/ferris-cli
./target/release/ferris-cli -c "SHOW TABLES"
./target/release/ferris-cli -c "SELECT count(*) FROM \"hackernews\""
```

### Load 4M Hacker News stories

```bash
pip install pyarrow requests
python3 scripts/ingest_hackernews.py
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
| `translog_durability` | `request` | WAL fsync mode: `request` (per-write) or `async` (timer) |
| `translog_sync_interval_ms` | (unset) | Background fsync interval when durability is `async` |

## SQL Over Search Results

### How It Works

```
SQL Query → Hybrid Planner
  ├── text_match(), =, >, <, BETWEEN, IN → pushed into Tantivy
  ├── GROUP BY + aggregates → per-shard grouped partial collectors (fast fields)
  ├── HAVING, ORDER BY, LIMIT → applied post-merge at coordinator
  └── residual predicates → DataFusion on Arrow batches
```

The planner splits work between Tantivy and DataFusion. For eligible queries, each shard (local and remote) computes partial aggregates on fast-field columns. The coordinator merges the compact partials — no rows are shipped between nodes.

### Execution Modes

| Mode | When | What happens |
|------|------|-------------|
| `count_star_fast` | `SELECT count(*)` with no WHERE | Reads doc count metadata, no scan |
| `tantivy_grouped_partials` | `GROUP BY` + aggregates | Each shard computes partials on fast fields, coordinator merges |
| `tantivy_fast_fields` | Projected columns without `SELECT *` | Each shard reads fast-field columns, coordinator runs DataFusion |
| `materialized_hits_fallback` | `SELECT *` or unsupported expressions | Full `_source` materialization |

### SQL Features

- `text_match(field, 'query')` — full-text search pushed into Tantivy
- `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN` — structured filter pushdown
- `GROUP BY` with `count(*)`, `sum()`, `avg()`, `min()`, `max()`
- `HAVING` — post-merge filtering on grouped results
- `ORDER BY` with `LIMIT` and `OFFSET`
- `score` as a SQL column
- `EXPLAIN` and `EXPLAIN ANALYZE` with per-stage timings
- `SHOW TABLES`, `DESCRIBE <index>`, `SHOW CREATE TABLE <index>`
- Alias-safe pushdown — `WHERE posts > 10` when `posts` is `count(*) AS posts` correctly stays as a DataFusion residual, never pushed to Tantivy

### Example Queries

```sql
-- Count all docs (answers from metadata, ~2ms)
SELECT count(*) FROM hackernews;

-- Search + filter + sort (~20ms)
SELECT title, comments, author FROM hackernews
WHERE text_match(title, 'startup') AND comments > 100
ORDER BY comments DESC LIMIT 10;

-- Search + grouped analytics (60ms on 4M docs)
SELECT author, count(*) AS posts, avg(upvotes) AS avg_up
FROM hackernews WHERE text_match(title, 'rust')
GROUP BY author HAVING posts > 5
ORDER BY avg_up DESC LIMIT 10;

-- Filtered aggregation (~3ms)
SELECT count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc
FROM hackernews WHERE text_match(title, 'GPT OR LLM');
```

## API Reference

### Indices

```bash
# Create with mappings
curl -X PUT 'http://localhost:9200/movies' -H 'Content-Type: application/json' -d '{
  "settings": {"number_of_shards": 3, "number_of_replicas": 1},
  "mappings": {"properties": {
    "title": {"type": "text"}, "genre": {"type": "keyword"},
    "year": {"type": "integer"}, "rating": {"type": "float"},
    "embedding": {"type": "knn_vector", "dimension": 3}
  }}
}'

# Delete
curl -X DELETE 'http://localhost:9200/movies'

# Settings
curl 'http://localhost:9200/movies/_settings'
curl -X PUT 'http://localhost:9200/movies/_settings' -H 'Content-Type: application/json' \
  -d '{"index": {"number_of_replicas": 2, "refresh_interval_ms": 10000}}'
```

**Field types:** `text`, `keyword`, `integer`, `float`, `boolean`, `knn_vector`. Unmapped fields are auto-detected on first document.

### Documents

```bash
# Index (auto ID)
curl -X POST 'http://localhost:9200/movies/_doc' -H 'Content-Type: application/json' \
  -d '{"title": "Rust: The Movie", "year": 2026}'

# Index (explicit ID)
curl -X PUT 'http://localhost:9200/movies/_doc/1' -H 'Content-Type: application/json' \
  -d '{"title": "Rust: The Movie", "year": 2026}'

# Get / Delete
curl 'http://localhost:9200/movies/_doc/1'
curl -X DELETE 'http://localhost:9200/movies/_doc/1'

# Partial update
curl -X POST 'http://localhost:9200/movies/_update/1' -H 'Content-Type: application/json' \
  -d '{"doc": {"rating": 9.5}}'

# Bulk (OpenSearch NDJSON format)
curl -X POST 'http://localhost:9200/movies/_bulk' -H 'Content-Type: application/json' -d '
{"index":{"_id":"1"}}
{"title":"Movie A","year":2024}
{"index":{"_id":"2"}}
{"title":"Movie B","year":2025}
'

# ?refresh=true makes docs immediately searchable
curl -X POST 'http://localhost:9200/movies/_doc?refresh=true' -H 'Content-Type: application/json' \
  -d '{"title": "Instant", "year": 2026}'
```

### Search

```bash
# Query string
curl 'http://localhost:9200/movies/_search?q=rust&size=10'

# DSL: match
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"title": "search engine"}}}'

# DSL: bool
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"bool": {
    "must": [{"match": {"title": "rust"}}],
    "filter": [{"range": {"year": {"gte": 2020}}}]
  }}}'

# Fuzzy
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"fuzzy": {"title": {"value": "rsut", "fuzziness": 2}}}}'

# Sort
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "sort": [{"year": "desc"}, "_score"]}'

# Count
curl 'http://localhost:9200/movies/_count'
```

**Query types:** `match`, `term`, `bool` (must/should/must_not/filter), `range`, `wildcard`, `prefix`, `fuzzy`, `match_all`.

### Aggregations

```bash
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"match_all": {}}, "size": 0, "aggs": {
    "genres": {"terms": {"field": "genre", "size": 10}},
    "rating_stats": {"stats": {"field": "rating"}},
    "by_decade": {"histogram": {"field": "year", "interval": 10}}
  }}'
```

**Types:** `terms`, `stats`, `min`, `max`, `avg`, `sum`, `value_count`, `histogram`.

### Vector Search (k-NN)

```bash
# k-NN search
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"knn": {"embedding": {"vector": [1.0, 0.0, 0.0], "k": 5}}}'

# With pre-filter
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"knn": {"embedding": {"vector": [1.0, 0.0, 0.0], "k": 5,
    "filter": {"match": {"genre": "action"}}
  }}}'

# Hybrid: full-text + vector (Reciprocal Rank Fusion)
curl -X POST 'http://localhost:9200/movies/_search' -H 'Content-Type: application/json' \
  -d '{"query": {"match": {"title": "rust"}},
    "knn": {"embedding": {"vector": [1.0, 0.0, 0.0], "k": 5}}}'
```

### SQL

```bash
# SQL query
curl -X POST 'http://localhost:9200/movies/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT genre, count(*) AS cnt FROM movies GROUP BY genre ORDER BY cnt DESC"}'

# EXPLAIN ANALYZE (with timings)
curl -X POST 'http://localhost:9200/movies/_sql/explain' -H 'Content-Type: application/json' \
  -d '{"query": "SELECT ...", "analyze": true}'

# Global SQL
curl -X POST 'http://localhost:9200/_sql' -H 'Content-Type: application/json' \
  -d '{"query": "SHOW TABLES"}'
```

### Monitoring

```bash
curl 'http://localhost:9200/_cluster/health'
curl 'http://localhost:9200/_cluster/state'
curl 'http://localhost:9200/_cat/nodes'
curl 'http://localhost:9200/_cat/shards'
curl 'http://localhost:9200/_cat/indices'
curl 'http://localhost:9200/_cat/master'

# Prometheus metrics (text exposition format)
curl 'http://localhost:9200/_metrics'
```

Append `?pretty` to any endpoint for formatted JSON.

### Operations

```bash
curl -X POST 'http://localhost:9200/movies/_refresh'
curl -X POST 'http://localhost:9200/movies/_flush'
```

## Benchmarks

3-node cluster on a single machine, 3 shards, 0 replicas. Intel i5-13600K (14c/20t), 32 GB RAM, NVMe.

### Ingestion

| Dataset | Throughput |
|---------|------------|
| Synthetic 1GB (2M docs) | **26,734 docs/sec** |
| Hacker News (4M stories) | **48,700 docs/sec** |

### Search (2M docs, Home Desktop)

| Query Type | Min | p50 |
|------------|-----|-----|
| match_title | 0.6ms | 1.2ms |
| fuzzy_title | 1.1ms | 1.3ms |
| term_category | 1.5ms | 1.7ms |
| bool_must | 1.7ms | 1.9ms |
| agg_filtered | 2.3ms | 2.6ms |
| match_all (2M) | 8.7ms | 9.0ms |

**4,000 queries · 0 errors · p50 = 6.1ms · min = 0.6ms**

### Hybrid SQL (4M Hacker News stories, i5-13600K)

| Query | Mode | Time |
|-------|------|------|
| `SELECT count(*)` | `count_star_fast` | ~2ms |
| text_match + filter + ORDER BY LIMIT | `tantivy_fast_fields` | ~20ms |
| text_match + GROUP BY 4,715 authors | `tantivy_grouped_partials` | **60ms** |
| Filtered aggregation (per query) | `tantivy_grouped_partials` | **2–4ms** |

### Filtered Aggregations (4M docs, i5-13600K)

Each query does `text_match` + `count` + `avg` across 4M docs:

| Topic | Posts | Avg ↑ | Avg 💬 | Time |
|-------|-------|-------|--------|------|
| Rust | 12,473 | 35 | 14.9 | 2ms |
| Python | 31,044 | 19 | 6.7 | 2ms |
| Machine Learning | 55,430 | 15 | 4.8 | 3ms |
| China | 37,765 | 13 | 6.5 | 3ms |
| India | 17,860 | 11 | 5.5 | 4ms |
| Silicon Valley | 21,261 | 15 | 10.4 | 3ms |

Example query for the Rust row:
```sql
SELECT count(*) AS posts, avg(upvotes) AS avg_up, avg(comments) AS avg_disc
FROM hackernews WHERE text_match(title, 'rust')
```

Reproduce:
```bash
pip install pyarrow requests
python3 scripts/ingest_hackernews.py
bash scripts/hackernews_queries.sh
python3 scripts/search_1gb.py --queries 200 --concurrency 1
```

## Architecture

### Cluster

- **Raft consensus** manages cluster state (nodes, indices, shard assignments, master)
- Every node is a **coordinator** — any request on any node routes to the right place

### Search Path

1. Coordinator receives query → looks up shard assignments
2. Local shards: dispatched to the **search thread pool** (rayon) in parallel
3. Remote shards: scattered via gRPC `SearchShardDsl` RPCs
4. Results gathered, merged, sorted, paginated at coordinator

### Write Path

1. Coordinator routes doc by Murmur3 hash → primary shard node
2. Primary writes to WAL → indexes in Tantivy → replicates to all ISR replicas
3. Replicas apply with the primary's seq_no → return checkpoint
4. Primary advances global checkpoint → acknowledges client

### SQL Path

1. Planner splits: `text_match` + structured filters → Tantivy; `GROUP BY` + aggs → fast-field collectors; residual → DataFusion
2. Each shard (local and remote) produces compact partial states (not rows)
3. Coordinator merges partials, applies HAVING, sorts, applies LIMIT/OFFSET
4. Response includes `execution_mode` and `planner` metadata

### Thread Pool Isolation

Dedicated rayon thread pools for search and write workloads. Bulk indexing cannot starve search latency or Raft heartbeats.

### Replication

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| Cluster state | Raft log replication | Nodes, indices, routing |
| Document data | gRPC primary→replica | Per-shard, per-write |
| Recovery | Translog replay | Missed ops from primary's WAL |

## Testing

```bash
cargo test                                      # All 760 tests
cargo test --lib                                # Unit tests (640)
cargo test --bin ferris-cli                      # CLI tests (33)
cargo test --test consensus_integration          # Raft consensus (30)
cargo test --test replication_integration        # Replication (39)
cargo test --test rest_api_integration           # REST API (17)
cargo test --test sql_correctness                # SQL correctness (1 test, 52 sqllogictest assertions)
```

Integration tests run in-process with isolated temp directories. No external services needed.

SQL correctness uses the [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) `.slt` format — same framework as DataFusion, CockroachDB, DuckDB, and RisingWave. Tests assert **result values**, not just plan metadata.

## Project Structure

```
src/
├── api/           REST API handlers (Axum)
├── cli.rs         Interactive SQL console (ferris-cli)
├── cluster/       Cluster state, membership, settings
├── config/        Configuration loading
├── consensus/     Raft consensus (openraft)
├── engine/        Tantivy + USearch engine integration
├── hybrid/        SQL planner, Arrow bridge, DataFusion execution
├── replication/   Primary → replica replication
├── search/        Query DSL, aggregations, merge logic
├── shard/         Shard lifecycle, ISR tracking
├── transport/     gRPC client & server (tonic)
├── wal/           Write-ahead log
├── worker.rs      Rayon thread pools for search/write isolation
└── main.rs        Server entry point

tests/slt/         SQL correctness test files (.slt)
proto/             gRPC service definitions
config/            Default configuration
scripts/           Ingestion and benchmark scripts
```

## Roadmap

### Done

- [x] OpenSearch-compatible REST API (index CRUD, doc CRUD, search, bulk)
- [x] Raft consensus with persistent log, leader election, automatic failover
- [x] Synchronous primary-replica replication with ISR tracking
- [x] WAL with configurable durability and translog-based replica recovery
- [x] Hybrid SQL: `text_match` + `GROUP BY` + `HAVING` + `ORDER BY` + `LIMIT` + `OFFSET`
- [x] Four execution modes with automatic planner selection
- [x] Predicate pushdown: `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN`
- [x] Alias-safe pushdown (SELECT aliases never pushed to Tantivy)
- [x] `EXPLAIN ANALYZE` with per-stage timings
- [x] Distributed grouped partial aggregation across shards
- [x] Distributed fast-field SQL (Arrow IPC between nodes)
- [x] `count_star_fast` metadata path
- [x] `ORDER BY` + `LIMIT` pushdown into Tantivy's TopDocs
- [x] Dynamic field mapping (auto-detect on first document)
- [x] Vector search (k-NN via USearch, hybrid with BM25, pre-filtering)
- [x] Dedicated search/write thread pools (rayon)
- [x] Interactive SQL console with EXPLAIN visualization
- [x] Global `/_sql` with `SHOW TABLES`, `DESCRIBE`, `SHOW CREATE TABLE`
- [x] sqllogictest correctness suite
- [x] Prometheus metrics (`/_metrics`) — request latencies, search/index throughput, SQL mode counters, cluster gauges, process stats

### Planned

- [ ] `COUNT(DISTINCT field)`
- [ ] `_msearch` API (batch searches)
- [ ] `search_after` cursor-based pagination
- [ ] TLS encryption
- [ ] Snapshot and restore
- [ ] Index aliases and templates
- [ ] Column cache (in-memory flat arrays for analytics-hot columns)

## What's Honestly Missing

- **No TLS** — localhost-only deployments
- **No authentication** — zero access control
- **No `_msearch`** — can't batch search requests (blocks Kibana/Grafana)
- **Not battle-tested at scale** — tested on 4M docs, not 400M
- **Full-table GROUP BY is slow** — ~6.5s on 4M docs without a `text_match` filter (inherent to search-engine storage; filtered path is fast)

## License

Apache-2.0

---

Built with [Tantivy](https://github.com/quickwit-oss/tantivy), [Arrow](https://arrow.apache.org/), [DataFusion](https://datafusion.apache.org/), [openraft](https://github.com/datafuselabs/openraft), [USearch](https://github.com/unum-cloud/usearch), and [tonic](https://github.com/hyperium/tonic).
