# I Built a Search Engine in Rust That Runs SQL Analytics Over 4M Documents in 92ms

*FerrisSearch: 28K lines of Rust. Full-text search in 0.6ms. Grouped analytics on 4M docs in 92ms. Topic-level analytics in 3–8ms per query.*

> **TL;DR:** I built a distributed search engine in Rust that lets you run SQL over full-text search results. One query searches 4 million Hacker News stories, finds every Rust post, groups them by 4,715 authors, computes per-author engagement metrics, and sorts in 92ms. Simpler topic-level analytics run in 3–8ms.
>
> The practical payoff: you can answer relevance + analytics questions without maintaining a separate search-to-warehouse pipeline.

---

## The Query That Started It All

```sql
SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes,
       avg(comments) AS avg_discussion
FROM "hackernews"
WHERE text_match(title, 'rust')
GROUP BY author
ORDER BY avg_upvotes DESC
```

**92ms.** That's how long it takes to search 4 million Hacker News stories, find every post about Rust, group them by 4,715 distinct authors, compute per-author average upvotes and comment engagement, and sort by the most viral contributors.

For a product team, that means a feature that often turns into two systems and an ETL job can stay as one query against one service.

You can express the same analysis in Elasticsearch or OpenSearch, but the authoring model is very different:

```json
{
  "query": {"match": {"title": "rust"}},
  "size": 0,
  "aggs": {
    "by_author": {
      "terms": {"field": "author", "size": 1000},
      "aggs": {
        "avg_upvotes": {"avg": {"field": "upvotes"}},
        "avg_comments": {"avg": {"field": "comments"}}
      }
    }
  }
}
```

FerrisSearch lets you write that as SQL. The more interesting difference is execution transparency. Every query response includes:

- **`execution_mode`** — which optimization path the planner chose
- **`EXPLAIN ANALYZE`** — per-stage timings for planning, search, merge, and DataFusion
- **Pushdown visibility** — which predicates ran in Tantivy and which stayed in DataFusion

Elasticsearch has `_profile` for shard-level Lucene collector breakdowns, and OpenSearch added PPL for piped analytics. What I wanted here was one place to see the whole decision chain: what got pushed into the search engine, what stayed in the SQL layer, and how much each stage cost.

That all runs in a single Rust binary.

---

## Why I Built This

I've spent years working on search infrastructure at major cloud providers. I've seen these systems from the inside: the JVM tuning, the shard allocation edge cases, and the tension between search features and analytics requirements.

One thing always stood out: customers wanted analytics over search results, and the existing options were awkward. You either wrote deeply nested aggregation JSON, piped results to a separate analytics system, or pulled data into application code. The search engine and the analytics engine stayed separate even when the data was the same.

I wanted to explore what happens when you design a search engine from scratch with analytics as a first-class capability, not bolted on but woven into the query planner. No legacy runtime constraints, no backward compatibility requirements, just the best ideas from modern systems research combined in one binary.

FerrisSearch is that exploration.

---

## What Is FerrisSearch?

FerrisSearch is a distributed search engine written in Rust. It has OpenSearch-compatible REST APIs, Raft consensus for cluster state, and a hybrid SQL layer that runs analytics directly on search results.

It's 28,000 lines of Rust. No JVM, no extra services, no separate analytics database. Single binary.

**Tech stack:**
- [Tantivy](https://github.com/quickwit-oss/tantivy) for full-text indexing and BM25 scoring
- [Apache Arrow](https://arrow.apache.org/) + [DataFusion](https://datafusion.apache.org/) for columnar SQL execution
- [openraft](https://github.com/datafuselabs/openraft) for Raft consensus
- [USearch](https://github.com/unum-cloud/usearch) for hybrid vector search (HNSW k-NN alongside full-text)
- [tonic](https://github.com/hyperium/tonic) for gRPC inter-node transport

---

## The Numbers

### Ingestion

| Dataset | Docs | Throughput |
|---------|------|-----------|
| Synthetic 1GB (2M docs) | 2,000,000 | **26,734 docs/sec** |
| Hacker News stories | 4,010,957 | **48,700 docs/sec** |

> **Environment:** All numbers on this page were collected on a consumer desktop — Intel i5-13600K, 32 GB RAM, Crucial P3 Plus NVMe, Linux Mint. 3-node cluster on a single machine. Zero errors across all runs.

### Search (2M docs, 20 query types, 4,000 queries)

```
match_title       min=0.6ms   p50=1.2ms
fuzzy_title       min=1.1ms   p50=1.3ms
term_category     min=1.5ms   p50=1.7ms
bool_must         min=1.7ms   p50=1.9ms
agg_filtered      min=2.3ms   p50=2.6ms
match_all (2M)    min=8.7ms   p50=9.0ms

Overall: p50=6.1ms | 133 queries/sec | 0 errors
```

That is **0.6ms** for a full-text match query across 2 million documents.

### Hybrid SQL (4M Hacker News stories, i5-13600K)

| Query | Time |
|-------|------|
| `count(*)` across 4M docs | **20ms** |
| text_match + filter + ORDER BY LIMIT 10 | **19ms** |
| text_match + GROUP BY 4,715 authors | **92ms** |
| Cross-topic analytics (per query) | **3–8ms** |

---

## How It Works

The key insight is simple: **search and analytics don't need to be separate systems.** If the search engine already has columnar storage and the analytics layer already has columnar execution, you can skip the data shipping and run analytics where the data lives.

Here's what happens when you run:

```sql
SELECT author, avg(upvotes), count(*)
FROM "hackernews"
WHERE text_match(title, 'startup') AND comments > 100
ORDER BY count(*) DESC
```

FerrisSearch doesn't run search first and SQL second. It does something more efficient:

1. **Plan** — the SQL planner splits the query: `text_match` and `comments > 100` are pushed into Tantivy, `GROUP BY + avg + count` routes to shard-local partial collectors
2. **Execute per-shard** — each shard does an inverted-index lookup, reads `author`/`upvotes`/`comments` from fast-field columnar storage, and computes partial aggregates in one pass
3. **Merge** — the coordinator receives compact partial states rather than rows, merges them, computes final averages, sorts, and returns

For the showcase Rust query across 4M docs and 3 nodes (single machine):

```
planning: 0.1ms → search: 86ms → merge: 6ms → total: 92ms
```

For a filtered fast-field query (`text_match + WHERE + ORDER BY LIMIT`):

```
planning: 0.1ms → search: 7ms → datafusion: 1ms → total: 8ms
```

**No intermediate row materialization. No document shipping to another system.**

---

## Four Execution Modes

The planner automatically picks the fastest path:

| Mode | When | Example |
|------|------|---------|
| `count_star_fast` | `SELECT count(*)` with no WHERE | Reads doc_count() metadata. **20ms on 4M docs.** |
| `tantivy_fast_fields` | Projected columns + filter + ORDER BY LIMIT | Columnar reads from Tantivy fast fields. **19ms on 4M docs.** |
| `tantivy_grouped_partials` | GROUP BY + aggregates | Shard-local partial agg, coordinator merge. **92ms on 4M docs.** |
| `materialized_hits_fallback` | SELECT * or unsupported expressions | Falls back to full hit materialization. Compatibility mode. |

You can see which mode was used in every response:

```json
{
  "execution_mode": "tantivy_fast_fields",
  "matched_hits": 474,
  "rows": [...]
}
```

---

## Real-World Example: Cross-Topic Content Intelligence

Elasticsearch can compute aggregations. ClickHouse can run SQL. But combining full-text relevance search with SQL-style analytics in one query typically means one of these tradeoffs:

1. Writing nested aggregation DSL in Elasticsearch/OpenSearch
2. Piping search results into an analytics database
3. Stitching search and analytics together in application code

FerrisSearch does it in one SQL query, with introspection into how that query ran.

**"What are the hottest topics on Hacker News, and how does each community engage?"**

Six SQL queries on 4M HN stories (i5-13600K, 3 nodes on a single machine):

```
               Topic |  Posts |  Avg ↑ |  Avg 💬 |  Best ↑ |  Top 💬 |  Time
  ────────────────────┼───────┼───────┼────────┼────────┼────────┼──────
               India | 17,860 |    11 |    5.5 |   1,483 |     795 |    5ms
               China | 37,765 |    13 |    6.5 |   2,493 |   1,087 |    6ms
      Silicon Valley | 21,261 |    15 |   10.4 |   3,172 |   2,780 |    4ms
                Rust | 12,473 |    35 |   14.9 |   1,582 |     991 |    3ms
              Python | 31,044 |    19 |    6.7 |   1,616 |     957 |    5ms
    Machine Learning | 55,430 |    15 |    4.8 |   2,029 |   1,148 |    8ms
```

**3–8ms per query.** Each query does full-text search across 4 million documents and then computes five aggregate metrics. Six queries, six topics, total wall clock under 35ms.

The data tells a story: Rust posts get the highest average engagement (35 upvotes, 14.9 comments per post) — the Rust community is passionate. Machine Learning has the most raw coverage (55K posts) but lower per-post engagement. Silicon Valley posts spark the most debate (10.4 avg comments, top post hit 2,780 comments).

---

## EXPLAIN ANALYZE

Every query can be introspected with `EXPLAIN ANALYZE`:

```bash
curl -XPOST 'http://localhost:9200/hackernews/_sql/explain' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT title, comments, author FROM \"hackernews\" WHERE text_match(title, '\''startup'\'') AND comments > 100 ORDER BY comments DESC LIMIT 10",
    "analyze": true
  }'
```

The response includes per-stage timings and the execution pipeline:

```json
{
  "execution_mode": "tantivy_fast_fields",
  "matched_hits": 474,
  "timings": {
    "planning_ms": 0.1,
    "search_ms": 7.0,
    "datafusion_ms": 1.0,
    "total_ms": 8.0
  },
  "strategy_reason": "All projected columns can be read from Tantivy fast fields",
  "rows": [
    {"title": "Ask HN: Pros and cons of working at a startup in 2018?", "comments": 428, "author": "..."},
    ...
  ]
}
```

---

## What's Under the Hood

Beyond the SQL layer, FerrisSearch is a real distributed system:

- **Raft consensus** ([openraft](https://github.com/datafuselabs/openraft)) manages cluster state — leader election, node membership, index metadata
- **Synchronous replication** — primary-replica with in-sync replica tracking, translog-based recovery
- **Write-ahead log** — binary WAL with configurable durability (`fsync` per write or async timer)
- **Shard routing** — Murmur3 hash-based document routing across shards
- **gRPC transport** — inter-node communication via [tonic](https://github.com/hyperium/tonic) with connection pooling
- **653 tests** — unit, consensus integration, replication integration, and REST API integration tests

---

## Why Rust?

1. **No GC pauses.** Having worked on JVM-based search engines at scale, I've seen stop-the-world GC pauses firsthand, especially under heavy mixed read/write workloads. Modern JVMs have improved significantly, but Rust removes that class of problem at the language level. Latency stays more predictable because memory management is deterministic.
2. **Single binary.** `cargo build --release` produces one ~30MB binary. No JVM tuning, no heap sizing, no classpath debugging. Deploy by copying a file.
3. **The ecosystem is ready.** Tantivy, Arrow, DataFusion, tonic, and openraft are now mature enough to build a real distributed system on top of. Five years ago this would have been much harder.

---

## What's Honestly Missing

FerrisSearch is not production-ready yet. Gaps you should know about:

- **No dynamic field mapping** — you must define schemas upfront (OpenSearch auto-detects types on first document)
- **No TLS** — zero encryption, localhost-only deployments
- **No `search_after` pagination** — deep pagination breaks at high offsets
- **No `_msearch`** — can't batch search requests (Kibana/Grafana integration blocked)
- **High-cardinality GROUP BY is slow** — 400K distinct groups = 5.5s (per-doc HashMap insertion)
- **Not battle-tested at scale** — tested on 4M docs, not 400M

The planner works and the numbers are real, but the missing features above are the difference between an impressive prototype and something you'd trust in production.

---

## What's Next

If you want to contribute, these are good starting points:

- **Dynamic field mapping** — auto-detect JSON value types on first document and create the schema automatically (removes the biggest usability gap vs OpenSearch)
- **`search_after` pagination** — cursor-based deep pagination using sort values from the last result
- **`_msearch` API** — batch multiple search requests in one call (unlocks Kibana/Grafana integration)
- **Prometheus metrics** — add a `/_metrics` endpoint exposing query latency histograms and shard health
- **TLS** — required before any non-localhost deployment

---

## Try It

```bash
git clone https://github.com/RamakrishnaChilaka/ROpenSearch.git
cd ROpenSearch

# Build
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Start 3-node cluster
./dev_cluster_release.sh 1 &
./dev_cluster_release.sh 2 &
./dev_cluster_release.sh 3 &

# Ingest 4M Hacker News stories
python3 -m pip install pyarrow requests
python3 scripts/ingest_hackernews.py

# Run the showcase query
curl -s 'http://localhost:9200/hackernews/_sql/explain' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes FROM \"hackernews\" WHERE text_match(title, '\''rust'\'') GROUP BY author ORDER BY avg_upvotes DESC",
    "analyze": true
  }' | python3 -m json.tool
```

---

*FerrisSearch is open source under Apache-2.0. Built with [Tantivy](https://github.com/quickwit-oss/tantivy), [Arrow](https://arrow.apache.org/), [DataFusion](https://datafusion.apache.org/), [openraft](https://github.com/datafuselabs/openraft), and [USearch](https://github.com/unum-cloud/usearch).*

*If this kind of hybrid search and analytics system interests you, star the repo. It helps me justify spending more time on it.*
