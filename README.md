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
  "streaming_used": false,
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
- **Four execution modes** — `count_star_fast`, `tantivy_fast_fields`, `tantivy_grouped_partials`, `materialized_hits_fallback` — chosen automatically by the planner; executed fast-field fallbacks also report `streaming_used` when local bitset streaming or remote streamed shard batches are used
- **Raft consensus** — cluster state via [openraft](https://github.com/datafuselabs/openraft); leader election, linearizable writes, automatic failover, persistent log via [redb](https://github.com/cberner/redb), and authoritative join snapshots so shard reopen/orphan cleanup never run from lossy pre-catch-up state
- **Synchronous replication** — primary-replica with ISR tracking over gRPC; writes acknowledged after all in-sync replicas confirm
- **Vector search** — k-NN via [USearch](https://github.com/unum-cloud/usearch) (HNSW); hybrid full-text + vector queries with pre-filtering
- **Write-ahead log** — binary WAL with configurable durability (`request` fsync-per-write or `async` timer-based)
- **Workload isolation** — dedicated [rayon](https://github.com/rayon-rs/rayon) thread pools for search and write traffic; bulk indexing cannot starve search latency or Raft heartbeats
- **Dynamic field mapping** — fields are auto-detected and created on first document encounter
- **Prometheus metrics** — `/_metrics` endpoint with request latencies, search/index counters, SQL mode tracking, cluster gauges, and process stats (CPU, RSS, threads, FDs)
- **Column cache** — segment-aware Arrow array cache for SQL analytics; configurable memory limit (`column_cache_size_percent`) and selectivity threshold (`column_cache_populate_threshold`); ~1000× speedup on repeated fast-field queries
- **Interactive SQL console** — `ferris-cli` with colored tables, EXPLAIN ANALYZE visualization, query history, `Tab` completion, `\watch`, `Ctrl-R` reverse search, and NDJSON SQL stream consumption via `/_sql/stream`
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

`ferris-cli` now reads the global `POST /_sql/stream` endpoint and reconstructs the normal table view from streamed `meta` and `rows` NDJSON frames. For explicit-column fast-field queries, the coordinator now feeds remote shard batches into DataFusion through streaming partitions instead of first staging them in a coordinator `MemTable`.

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
| `sql_group_by_scan_limit` | `1000000` | Max docs scanned for GROUP BY fallback queries on the `tantivy_fast_fields` path (0 = unlimited). Flat non-grouped fast-field queries keep their own internal 100K default unless `LIMIT` pushdown applies. |
| `transport_tls_enabled` | `false` | Enable inter-node gRPC TLS (requires building with `--features transport-tls`) |
| `transport_tls_cert_file` | (unset) | PEM certificate for the gRPC transport server when TLS is enabled |
| `transport_tls_key_file` | (unset) | PEM private key for the gRPC transport server when TLS is enabled |
| `transport_tls_ca_file` | (unset) | PEM CA certificate used by transport clients to verify peers |

If `transport_tls_enabled: true` is set without compiling `--features transport-tls`, node startup fails instead of silently falling back to plaintext transport.

## SQL Over Search Results

### How It Works

```
SQL Query → Hybrid Planner
  ├── top-level ANDed text_match(), =, >, <, BETWEEN, IN → pushed into Tantivy
  ├── GROUP BY + aggregates → per-shard grouped partial collectors (fast fields)
  ├── HAVING, ORDER BY, LIMIT → applied post-merge at coordinator
  └── residual predicates → DataFusion on Arrow batches
```

The planner splits work between Tantivy and DataFusion. For eligible queries, each shard (local and remote) computes partial aggregates on fast-field columns. The coordinator merges the compact partials — no rows are shipped between nodes.

Scan limits are mode-specific, not global. Flat `tantivy_fast_fields` queries still default to an internal 100K match cap unless `LIMIT` pushdown applies. `GROUP BY` queries that fall back to `tantivy_fast_fields` use `sql_group_by_scan_limit` instead (default 1M, `0 = unlimited`), while `tantivy_grouped_partials` scans all matching docs.

When a fast-field fallback query can stay fully columnar, remote shards now send multiple Arrow IPC batches to the coordinator over gRPC instead of one large unary payload, the coordinator feeds those batches into DataFusion through streaming partitions, and the client receives the final SQL result as NDJSON over one long-lived HTTP response. The remaining major limitation is wildcard/export-style `SELECT *`, which still uses `materialized_hits_fallback`.

### SQL Features

- `text_match(field, 'query')` — full-text search pushed into Tantivy; multiple top-level `AND`ed `text_match(...)` predicates become multiple search `must` clauses
- `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN` — structured filter pushdown
- `GROUP BY` with `count(*)`, `sum()`, `avg()`, `min()`, `max()`
- `HAVING` — post-merge filtering on grouped results
- `ORDER BY` with `LIMIT` and `OFFSET`
- `_score` as the synthetic SQL relevance column; mapped `score` remains a normal field
- `EXPLAIN` and `EXPLAIN ANALYZE` with per-stage timings
- `POST /{index}/_sql/stream` and `POST /_sql/stream` — NDJSON SQL stream endpoints (`meta` frame followed by `rows` frames)
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
4. Response includes `execution_mode`, `planner`, and runtime flags such as `streaming_used`

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
cargo test                                      # All 951 tests
cargo test --lib                                # Unit tests (792)
cargo test --bin ferris-cli                      # CLI tests (61)
cargo test --test consensus_integration          # Raft consensus (33)
cargo test --test replication_integration        # Replication (40)
cargo test --test replication_integration --features transport-tls  # Replication with encrypted gRPC transport
cargo test --test rest_api_integration           # REST API (24)
cargo test --test sql_correctness                # SQL correctness (1 test, 163 sqllogictest assertions)
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
- [x] Streamed shard SQL batches over gRPC for fast-field fallback queries
- [x] `count_star_fast` metadata path
- [x] `ORDER BY` + `LIMIT` pushdown into Tantivy's TopDocs
- [x] Dynamic field mapping (auto-detect on first document)
- [x] Vector search (k-NN via USearch, hybrid with BM25, pre-filtering)
- [x] Dedicated search/write thread pools (rayon)
- [x] Interactive SQL console with EXPLAIN visualization, `Tab` completion, and `\watch`
- [x] Global `/_sql` with `SHOW TABLES`, `DESCRIBE`, `SHOW CREATE TABLE`
- [x] sqllogictest correctness suite
- [x] Prometheus metrics (`/_metrics`) — request latencies, search/index throughput, SQL mode counters, cluster gauges, process stats
- [x] Column cache — segment-aware Arrow array cache with selectivity-based population, configurable memory limit and populate threshold
- [x] Multiple `text_match()` predicates — top-level ANDed full-text searches lowered as separate Bool `must` clauses
- [x] GROUP BY fallback safety — configurable `sql_group_by_scan_limit` (default 1M) for expression GROUP BY / unsupported aggregates, plus guarded local bitset streaming for `_score`-free fully fast-field-backed fallbacks; errors instead of silently wrong results
- [x] Inter-node gRPC TLS (`--features transport-tls`)
- [x] CLI truncation warning for capped fast-field queries

### Planned

- [x] Semantic binder for SQL planning — clause-aware `BindContext` resolves identifiers as `Source`/`Synthetic`/`Aggregate`/`Unresolved` before capability analysis; `derive_columns()` and `derive_group_by()` replace raw AST walkers for `required_columns`, `needs_id`, `needs_score`, and GROUP BY eligibility
- [x] Query shape validation — `validate_query_shape()` rejects CTEs, UNION/EXCEPT/INTERSECT, subqueries (IN/EXISTS/scalar), derived tables, JOINs, and multi-table FROM with clear error messages instead of leaking DataFusion internals
- [ ] Branch-aware boolean lowering for `text_match()` inside `OR` / complex expressions — today `text_match()` must remain a top-level `AND` predicate because residual SQL/DataFusion cannot evaluate it; future support must lower the full boolean subtree into Tantivy instead of leaving residual `text_match()` work behind
- [ ] Streaming fast-field TableProvider — custom DataFusion `TableProvider` reading Tantivy fast fields as streaming Arrow batches (8K rows/batch), eliminating the GROUP BY scan limit entirely
- [ ] Streaming export / wildcard scan path — support large `SELECT *` and other unbounded result-set queries as block-streamed export-style execution instead of materializing full shard batches on remote nodes and the coordinator
- [ ] `COUNT(DISTINCT field)`
- [ ] `_msearch` API (batch searches)
- [ ] `search_after` cursor-based pagination
- [ ] HTTP TLS
- [ ] Snapshot and restore
- [ ] Index aliases and templates
- [ ] Parquet sidecar — write Parquet alongside Tantivy segments for 100M+ scale, cross-index joins, external tool integration

#### Same-Index Semijoin / Subquery MVP

Goal: support same-index uncorrelated semijoin queries such as:

```sql
SELECT title, author, upvotes
FROM hackernews
WHERE author IN (
  SELECT author
  FROM hackernews
  GROUP BY author
  HAVING COUNT(*) > 100
)
ORDER BY upvotes DESC
LIMIT 20;
```

Initial scope:
- same-index only
- uncorrelated subqueries only
- `IN (SELECT key ...)` only
- inner query may use `GROUP BY`, `HAVING`, `text_match`, and pushable structured filters

Non-goals for the MVP:
- correlated subqueries
- joins
- CTEs / `WITH`
- `EXISTS`, `NOT EXISTS`, `ANY`, `ALL`
- multi-index subqueries
- general DataFusion `TableProvider` integration

Execution shape for the MVP:
1. Bind the outer query and detect supported semijoin shape.
2. Plan and execute the inner query first.
3. Materialize the qualifying key set at the coordinator.
4. Lower the outer predicate into the existing hybrid execution path.
5. Keep unsupported shapes as explicit planning errors.

Task tracker (target: about 1 hour per task):

- [x] 1. Add planner-side query-shape validation for subqueries and return clear `unsupported_query_shape` errors instead of leaking DataFusion table-resolution failures.
- [x] 2. Add regression tests for unsupported shapes: correlated subquery, join, CTE, `EXISTS`, and multi-index subquery.
- [x] 3. Introduce a small clause-aware binder layer for identifier resolution in the planner (`Source`, `Synthetic`, `Output`).
- [x] 4. Convert `_id` / `_score` detection and `required_columns` extraction to consume bound identifiers instead of raw string walkers.
- [x] 5. Convert GROUP BY plain-column eligibility checks to use the binder so expression keys and output aliases bail out cleanly.
- [ ] 6. Add a bound representation for supported semijoin subqueries: outer key expression + inner query plan + same-index validation.
- [ ] 7. Implement planner detection for `expr IN (SELECT key FROM same_index ...)` with exactly one projected inner key column.
- [ ] 8. Validate MVP constraints during planning: same index, uncorrelated inner query, no joins, no `SELECT *`, no unsupported set operations.
- [ ] 9. Reuse the existing grouped-partials / fast-field planner for the inner subquery so grouped `HAVING` queries stay search-aware.
- [ ] 10. Add a coordinator-side execution step that runs the inner subquery first and materializes the qualifying key set.
- [ ] 11. Lower the outer semijoin predicate into the existing hybrid path as a concrete key filter, with cardinality guardrails and a clear overflow error.
- [ ] 12. Extend `EXPLAIN` / `EXPLAIN ANALYZE` output to show the two-stage semijoin pipeline: inner key build, outer filtered execution.
- [ ] 13. Add single-node result-correctness coverage for supported semijoin queries, including grouped inner queries with `HAVING` and `text_match`.
- [ ] 14. Add multi-node integration coverage to ensure remote shard partials and coordinator semijoin lowering produce the same result as single-node execution.
- [ ] 15. Benchmark the MVP on `hackernews`, document latency/cardinality limits, and decide whether the next phase should be a real FerrisSearch `TableProvider` or additional binder-driven lowering shapes.

## What's Honestly Missing

- **No HTTP TLS** — client-facing HTTP is plaintext (gRPC inter-node TLS available via `transport-tls` feature flag)
- **No authentication** — zero access control
- **No `_msearch`** — can't batch search requests (blocks Kibana/Grafana)
- **Not battle-tested at scale** — tested on 4M docs, not 400M
- **Full-table GROUP BY is slow** — ~6.5s on 4M docs without a `text_match` filter (inherent to search-engine storage; filtered path is fast)

## License

Apache-2.0

---

Built with [Tantivy](https://github.com/quickwit-oss/tantivy), [Arrow](https://arrow.apache.org/), [DataFusion](https://datafusion.apache.org/), [openraft](https://github.com/datafuselabs/openraft), [USearch](https://github.com/unum-cloud/usearch), and [tonic](https://github.com/hyperium/tonic).
