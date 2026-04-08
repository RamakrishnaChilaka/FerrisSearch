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

FerrisSearch is a distributed search engine in Rust for workloads that need full-text search, structured filters, and analytics in the same request.

It combines Tantivy for text search, DataFusion and Arrow for residual SQL, openraft for cluster coordination, tonic for inter-node RPC, and USearch for vector retrieval. The result is a single binary with no JVM and no external control plane.

- Search-native SQL: `text_match(...)` plus `GROUP BY`, `HAVING`, `ORDER BY`, and `LIMIT`
- Small operational surface: one Rust service, local disk, HTTP + gRPC
- Cluster-ready behavior: leader election, shard routing, failover, and restart validation are built in

## Why FerrisSearch

FerrisSearch is strongest when OpenSearch-style APIs are useful, but the important questions do not stop at top hits.

- Search a corpus, then aggregate over only the matched documents
- Keep deployment simple without giving up failover or leader election
- Mix text, keyword, numeric, and vector queries inside one cluster

## One Query, One System

FerrisSearch runs SQL analytics directly on search results:

```sql
SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes
FROM "hackernews"
WHERE text_match(title, 'rust')
GROUP BY author
HAVING posts > 5
ORDER BY avg_upvotes DESC
LIMIT 10
```

Tantivy narrows 4M documents to about 12K matches. Each shard computes grouped partial aggregates on fast fields. The coordinator merges those compact partials across 3 shards. On the Hacker News dataset, that query runs in about 60 ms.

Every SQL response tells you how the planner executed the query:

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

## Highlights

- **OpenSearch-compatible REST API** — `PUT /{index}`, `POST /_doc`, `GET /_search`, `POST /_bulk`
- **Search-aware SQL planner** — `text_match()` with `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, and same-index semijoin support
- **Four execution modes** — `count_star_fast`, `tantivy_fast_fields`, `tantivy_grouped_partials`, and `materialized_hits_fallback`, with `streaming_used` reported whenever the fast-field path streams locally or over gRPC
- **Raft-backed coordination** — leader election, shard routing, and failover for a multi-node cluster
- **Synchronous shard replication** — primary-replica writes over gRPC with ISR tracking; writes are acknowledged only after all in-sync replicas confirm
- **Vector search** — k-NN via [USearch](https://github.com/unum-cloud/usearch) with hybrid text + vector querying
- **Generation-based WAL** — durable writes, replica catch-up, and background auto-flush
- **Stable restarts** — covered by a real three-node flush + restart regression
- **CLI and observability** — `ferris-cli`, `EXPLAIN ANALYZE`, Prometheus metrics, and planner metadata in SQL responses
- **Test depth** — 1134 automated tests, including a real three-node flush + restart regression

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

### Single Node

```bash
cargo run
curl http://localhost:9200/
```

### Three-Node Cluster

```bash
./dev_cluster_release.sh --nodes 3
```

Or start the dev cluster manually:

```bash
./dev_cluster.sh 1    # HTTP 9200 · Transport 9300 · Raft ID 1
./dev_cluster.sh 2    # HTTP 9201 · Transport 9301 · Raft ID 2
./dev_cluster.sh 3    # HTTP 9202 · Transport 9302 · Raft ID 3
```

### Load Sample Data

```bash
python3 -m pip install pyarrow requests
python3 scripts/ingest_hackernews.py
```

### First SQL Query

```bash
curl -s 'http://localhost:9200/hackernews/_sql' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes FROM \"hackernews\" WHERE text_match(title, '\''rust'\'') GROUP BY author ORDER BY avg_upvotes DESC LIMIT 5"
  }'
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

`ferris-cli` reads the global `POST /_sql/stream` endpoint, reconstructs normal table output from streamed NDJSON frames, and follows the same quoted-index and case-insensitive source-column rules as the REST API.

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
| `sql_group_by_scan_limit` | `1000000` | Max docs scanned for `GROUP BY` fallback queries on the `tantivy_fast_fields` path (`0` = unlimited). Flat non-grouped fast-field queries keep their own internal 100K ceiling unless `LIMIT` pushdown applies. |
| `transport_tls_enabled` | `false` | Enable inter-node gRPC TLS (requires building with `--features transport-tls`) |
| `transport_tls_cert_file` | (unset) | PEM certificate for the gRPC transport server when TLS is enabled |
| `transport_tls_key_file` | (unset) | PEM private key for the gRPC transport server when TLS is enabled |
| `transport_tls_ca_file` | (unset) | PEM CA certificate used by transport clients to verify peers |

If `transport_tls_enabled: true` is set without compiling `--features transport-tls`, node startup fails instead of silently falling back to plaintext transport.

## SQL Over Search Results

FerrisSearch keeps search-native work in Tantivy and only uses DataFusion for the SQL semantics that cannot stay in the search engine.

| Stage | Runs In | Purpose |
|-------|---------|---------|
| `text_match()` + pushable filters | Tantivy | Narrow the matched document set |
| `GROUP BY` + core aggregates | Tantivy fast fields | Build compact shard-local partial states |
| `HAVING`, merge, final sort, `LIMIT/OFFSET` | Coordinator | Combine shard results into the final answer |
| Residual expressions | DataFusion | Finish relational work that cannot be pushed down |

For eligible grouped queries, shards ship partial states instead of rows. For explicit-column fast-field fallback queries, local shards stream Arrow batches lazily and remote shards can stream multiple Arrow IPC batches over gRPC into DataFusion partitions instead of forcing the coordinator to buffer one large unary response.

### Execution Modes

| Mode | When It Is Used |
|------|------------------|
| `count_star_fast` | Metadata-only `count(*)` queries |
| `tantivy_fast_fields` | Flat structured queries answerable from fast fields |
| `tantivy_grouped_partials` | Search-aware grouped analytics over matched docs |
| `materialized_hits_fallback` | Wildcard projection or shapes that cannot stay columnar |

Scan limits are mode-specific. Flat `tantivy_fast_fields` queries keep their internal 100K match ceiling unless `LIMIT` pushdown applies. `GROUP BY` queries that fall back to `tantivy_fast_fields` use `sql_group_by_scan_limit` instead. `tantivy_grouped_partials` scans the full matched set.

### SQL Features

- `text_match(field, 'query')` — full-text search pushed into Tantivy; multiple top-level `AND`ed `text_match(...)` predicates become multiple search `must` clauses
- `=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `IN` — structured filter pushdown
- `GROUP BY` with `count(*)`, `sum()`, `avg()`, `min()`, `max()`
- same-index semijoin subqueries: top-level `expr IN (SELECT key FROM same_index ...)` with uncorrelated inner queries
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

-- Same-index semijoin with grouped inner query
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
    "released_at": {"type": "date"},
    "embedding": {"type": "knn_vector", "dimension": 3}
  }}
}'

# Delete
curl -X DELETE 'http://localhost:9200/movies'

# Settings
curl 'http://localhost:9200/movies/_settings'
curl -X PUT 'http://localhost:9200/movies/_settings' -H 'Content-Type: application/json' \
  -d '{"index": {"number_of_replicas": 2, "refresh_interval_ms": 10000, "flush_threshold_bytes": 536870912}}'
```

`flush_threshold_bytes` is a per-index WAL auto-flush threshold. The default is `536870912` (512 MB). Setting it to `0` disables automatic background flushes.
Background auto-flush is best-effort: it skips the tick while the shard is busy ingesting or persisting vectors instead of blocking live writes, and the maintenance tick itself runs on blocking threads so compaction does not stall Raft heartbeats.

**Field types:** `text`, `keyword`, `integer`, `float`, `boolean`, `date`, `knn_vector`. Unmapped fields are auto-detected on first document.

Mapped `date` fields accept ISO 8601 strings with timezone offsets or raw epoch millis. Returned `_source`, search hits, and SQL rows normalize mapped dates to UTC ISO 8601 strings.

Current date support covers storage, normalization, range filtering, sorting, and SQL projection. The next time-handling step is SQL time functions such as `date_trunc`, `extract`, and timezone-aware bucketing over mapped `date` fields.

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

Refresh and flush are cluster-wide fan-out operations. The coordinator dispatches its own node in the same per-node set as remote nodes, and maintenance will not create fresh shard data when the authoritative UUID directory is missing.

## Benchmarks

Benchmarks below were run on a 3-node cluster on one machine with 3 shards and 0 replicas. Hardware: Intel i5-13600K (14c/20t), 32 GB RAM, NVMe.

### Ingestion

| Dataset | Throughput |
|---------|------------|
| Synthetic 1GB (2M docs) | **26,734 docs/sec** |
| Hacker News (4M stories) | **48,700 docs/sec** |

### Search (2M docs)

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

### Filtered Aggregations (4M docs)

Each query does `text_match` + `count` + `avg` across 4M docs:

| Topic | Posts | Avg Upvotes | Avg Comments | Time |
|-------|-------|-------------|--------------|------|
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

### Coordinator Model

- Any node can accept a request and route it to the correct leader or shard primary
- Raft keeps cluster metadata consistent across the cluster
- Clients do not need to know which node is the leader before sending requests

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

### Operational Behavior

- Restarts are expected to preserve shard data and cluster ownership without manual repair
- Background maintenance is isolated from live query and write traffic so flushes and recovery work do not stall normal requests

### Replication

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| Cluster state | Raft log replication | Nodes, indices, routing |
| Document data | gRPC primary→replica | Per-shard, per-write |
| Recovery | Translog replay | Missed ops from primary's WAL |

## Testing

```bash
cargo test                                      # All 1134 tests
cargo test --lib                                # Unit tests (957)
cargo test --bin ferris-cli                      # CLI tests (64)
cargo test --test consensus_integration          # Raft consensus (33)
cargo test --test replication_integration        # Replication (39)
cargo test --test replication_integration --features transport-tls  # Replication with encrypted gRPC transport
cargo test --test rest_api_integration           # REST API (39)
cargo test --test restart_regression             # Real 3-node flush + restart regression (1)
cargo test --test sql_correctness                # SQL correctness (1 test, 175 sqllogictest assertions)
```

Most integration tests run in-process with isolated temp directories. The `restart_regression` suite goes further: it spawns real `ferrissearch` processes, creates a 3-node Raft cluster, indexes data, flushes, restarts every node, and verifies both document count and UUID-backed shard directories.

SQL correctness uses the [sqllogictest](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) `.slt` format — same framework as DataFusion, CockroachDB, DuckDB, and RisingWave. Tests assert **result values**, not just plan metadata.

For a longer manual restart workflow, run:

```bash
scripts/restart_regression.py --docs 2000000 --batch-size 1000
```

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

### Shipped

- [x] OpenSearch-compatible REST API (index CRUD, doc CRUD, search, bulk)
- [x] Raft consensus with persistent log, leader election, automatic failover
- [x] Synchronous primary-replica replication with ISR tracking
- [x] WAL with configurable durability and translog-based replica recovery
- [x] Hybrid SQL: `text_match` + `GROUP BY` + `HAVING` + `ORDER BY` + `LIMIT` + `OFFSET`
- [x] Four execution modes with automatic planner selection
- [x] Distributed grouped partial aggregation across shards
- [x] Distributed fast-field SQL (Arrow IPC between nodes)
- [x] Same-index semijoin support for `expr IN (SELECT key FROM same_index ...)`
- [x] Dynamic field mapping (auto-detect on first document)
- [x] Vector search (k-NN via USearch, hybrid with BM25, pre-filtering)
- [x] Dedicated search/write thread pools (rayon)
- [x] Interactive SQL console with EXPLAIN visualization, `Tab` completion, and `\watch`
- [x] Global `/_sql` with `SHOW TABLES`, `DESCRIBE`, `SHOW CREATE TABLE`, and streamed SQL output
- [x] sqllogictest correctness suite
- [x] Prometheus metrics and column cache for repeated fast-field queries
- [x] Restart and rejoin safety guardrails for UUID-based shard data
- [x] Inter-node gRPC TLS (`--features transport-tls`)

### Next

- [ ] Time functions for mapped `date` fields — support SQL temporal functions such as `date_trunc`, `extract`, `now()`, and timezone-aware bucketing/filtering without forcing queries onto the generic fallback path
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

### Semijoin Status

FerrisSearch already supports same-index uncorrelated semijoins such as:

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

Current scope is intentionally narrow: same index only, uncorrelated only, and `IN (SELECT key ...)` only. Unsupported subquery shapes fail during planning instead of quietly falling back to something expensive or incorrect.

The coordinator deduplicates inner keys, ignores `NULL`s, and caps the materialized key set at 50,000 distinct values before lowering the outer query.

## Current Trade-Offs

- **No client-facing HTTP TLS yet** — inter-node gRPC TLS exists behind `--features transport-tls`, but the HTTP API is still plaintext
- **No authentication yet** — there is no built-in access control layer
- **No `_msearch` yet** — batched search requests are still missing
- **Large unfiltered analytics are slower than search-aware analytics** — full-table `GROUP BY` is much slower than the filtered grouped-partials path
- **Not yet proven at hundreds of millions of docs** — the project is heavily tested on millions of documents, but it is not claiming Elasticsearch-scale production mileage yet

## License

Apache-2.0

---

Built with [Tantivy](https://github.com/quickwit-oss/tantivy), [Arrow](https://arrow.apache.org/), [DataFusion](https://datafusion.apache.org/), [openraft](https://github.com/datafuselabs/openraft), [USearch](https://github.com/unum-cloud/usearch), and [tonic](https://github.com/hyperium/tonic).
