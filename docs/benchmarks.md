# FerrisSearch Benchmarks

Two environments tested: a cloud dev box (WSL2) and a consumer desktop (bare metal).
Both run 3-node local clusters with 3 shards and 0 replicas.

---

## Environments

### Dev Box (WSL2)
- **CPU**: AMD EPYC 7763 (8 cores / 16 threads)
- **RAM**: 32 GB
- **Storage**: Virtualized disk (WSL2)
- **OS**: Ubuntu 24.04 (WSL2 on Windows)

### Home Desktop (bare metal)
- **CPU**: Intel Core i5-13600K (14 cores / 20 threads, boost to 5.1 GHz)
- **RAM**: 32 GB
- **Storage**: Crucial P3 Plus NVMe SSD (1TB, PCIe Gen4)
- **OS**: Linux Mint 22.3 (Zena)

---

## Ingestion

### Synthetic 1GB Dataset (2M docs, ~500 bytes each)

| Metric | Dev Box (WSL2) | Home Desktop |
|--------|---------------|--------------|
| Documents | 2,000,000 | 2,000,000 |
| Errors | 0 | 0 |
| Throughput | 10,402 docs/sec | **26,734 docs/sec** |
| Speedup | — | **2.6x** |

### Hacker News Stories (real-world Parquet data)

| Metric | Dev Box (WSL2) | Home Desktop |
|--------|---------------|--------------|
| Documents | 500,000 | 4,010,957 |
| Errors | 0 | 0 |
| Throughput | 18,839 docs/sec | **48,700 docs/sec** |
| Speedup | — | **2.6x** |

---

## Search — Synthetic 1GB (2M docs, 20 query types)

### Home Desktop (i5-13600K, concurrency=1, 4,000 queries)

```
Query Type                 Count  Err      Min      Avg      p50      p95      p99      Max   Hits/q
────────────────────────────────────────────────────────────────────────────────────────────────────
agg_filtered                 200    0     2.3ms     2.9ms     2.6ms     4.3ms     4.6ms     5.1ms  249975
agg_histogram_price          200    0    21.9ms    22.3ms    22.2ms    22.9ms    23.3ms    23.7ms 2000000
agg_stats_price              200    0     8.0ms     8.7ms     8.6ms     9.1ms     9.2ms    10.5ms 2000000
agg_terms_category           200    0    16.0ms    16.5ms    16.2ms    17.9ms    22.2ms    25.5ms 2000000
bool_filter_range            200    0     3.4ms     5.4ms     5.3ms     7.1ms    10.1ms    10.6ms   24598
bool_must                    200    0     1.7ms     2.3ms     1.9ms     3.2ms     9.9ms    10.6ms   75274
bool_should                  200    0     1.5ms     2.2ms     1.8ms     2.9ms     7.7ms    10.4ms  291003
complex_bool                 200    0    20.0ms    20.6ms    20.4ms    21.6ms    24.0ms    26.2ms  534846
fuzzy_title                  200    0     1.1ms     1.7ms     1.3ms     2.5ms    11.8ms    17.2ms  127621
match_all                    200    0     8.7ms     9.1ms     9.0ms     9.2ms    15.3ms    17.8ms 2000000
match_description            200    0     6.7ms     7.8ms     7.8ms     8.0ms     8.5ms    10.9ms 1711788
match_title                  200    0     0.6ms     1.2ms     1.2ms     1.5ms     5.1ms     9.3ms  111218
paginated                    200    0     3.9ms    12.9ms    13.2ms    20.7ms    21.9ms    24.4ms 1246053
prefix_title                 200    0     1.2ms     2.1ms     1.3ms    12.1ms    12.4ms    18.0ms  191149
range_price                  200    0     4.1ms     7.4ms     7.4ms    10.2ms    11.0ms    16.2ms  503701
range_rating                 200    0    10.4ms    12.4ms    12.5ms    13.2ms    14.9ms    16.0ms 1384024
sort_price_asc               200    0     8.4ms     8.8ms     8.7ms     9.1ms    11.1ms    15.5ms 2000000
sort_rating_desc             200    0     1.0ms     1.8ms     1.8ms     2.6ms     3.1ms    10.4ms  101633
term_category                200    0     1.5ms     1.9ms     1.7ms     2.9ms     3.1ms     3.1ms  249998
wildcard_title               200    0     1.2ms     1.9ms     1.8ms     2.4ms     3.0ms    15.7ms   95277
────────────────────────────────────────────────────────────────────────────────────────────────────
TOTAL                       4000    0     0.6ms     7.5ms     6.1ms    22.0ms    22.7ms    26.2ms

  4,000 queries | 0 errors | 133.4 queries/sec | concurrency=1
```

### Dev Box (EPYC WSL2, concurrency=4, 10,000 queries)

```
Query Type                 Count  Err      Min      Avg      p50      p95      p99      Max   Hits/q
────────────────────────────────────────────────────────────────────────────────────────────────────
agg_filtered                 500    0     4.9ms    14.8ms    11.7ms    33.0ms    50.9ms    77.2ms  249989
agg_histogram_price          500    0    47.4ms    60.4ms    57.6ms    79.4ms    93.7ms   142.0ms 2000000
agg_stats_price              500    0    16.1ms    27.9ms    25.4ms    45.3ms    53.8ms    92.9ms 2000000
agg_terms_category           500    0    39.9ms    54.2ms    52.3ms    75.2ms    86.0ms    92.8ms 2000000
bool_filter_range            500    0     8.9ms    20.8ms    17.3ms    38.7ms    53.4ms    82.5ms   19648
bool_must                    500    0     7.8ms    18.7ms    13.8ms    45.2ms    66.8ms    95.1ms   84833
bool_should                  500    0     7.1ms    18.2ms    13.9ms    41.5ms    55.1ms    91.8ms  345825
complex_bool                 500    0    34.5ms    46.5ms    43.6ms    66.1ms    78.3ms   121.0ms  534730
fuzzy_title                  500    0     5.5ms    16.4ms    11.8ms    38.1ms    57.3ms    69.7ms  114947
match_all                    500    0    23.1ms    36.2ms    33.9ms    55.1ms    62.9ms    91.4ms 2000000
match_description            500    0    20.0ms    32.8ms    29.9ms    52.9ms    66.8ms    81.3ms 1711887
match_title                  500    0     5.2ms    15.1ms    11.1ms    34.2ms    49.2ms    62.7ms  120821
paginated                    500    0    12.3ms    37.0ms    36.6ms    58.0ms    74.4ms   100.2ms 1246055
prefix_title                 500    0     5.8ms    16.4ms    12.2ms    37.7ms    51.1ms    73.8ms  147570
range_price                  500    0     9.6ms    24.5ms    22.0ms    43.4ms    57.2ms    63.8ms  492218
range_rating                 500    0    20.7ms    38.4ms    36.2ms    58.7ms    71.8ms   124.8ms 1409617
sort_price_asc               500    0    19.4ms    32.1ms    29.0ms    55.0ms    65.2ms    83.1ms 2000000
sort_rating_desc             500    0     6.6ms    16.7ms    12.6ms    36.7ms    51.3ms    73.6ms  124650
term_category                500    0     6.2ms    16.0ms    12.4ms    33.0ms    48.7ms    59.6ms  250008
wildcard_title               500    0     5.8ms    16.6ms    12.2ms    40.1ms    48.1ms    61.9ms  106150
────────────────────────────────────────────────────────────────────────────────────────────────────
TOTAL                      10000    0     4.9ms    28.0ms    24.8ms    60.0ms    75.5ms   142.0ms

  10,000 queries | 0 errors | 142.4 queries/sec | p50=24.8ms | concurrency=4
```

### Head-to-Head (p50 latency)

| Query Type | Home i5 (c=1) | Dev EPYC (c=4) | Speedup |
|-----------|---------------|----------------|---------|
| match_title | **1.2ms** | 11.1ms | **9.3x** |
| fuzzy_title | **1.3ms** | 11.8ms | **9.1x** |
| term_category | **1.7ms** | 12.4ms | **7.3x** |
| bool_must | **1.9ms** | 13.8ms | **7.3x** |
| agg_filtered | **2.6ms** | 11.7ms | **4.5x** |
| match_all (2M docs) | **9.0ms** | 33.9ms | **3.8x** |
| sort_price_asc (2M) | **8.7ms** | 29.0ms | **3.3x** |
| **Overall p50** | **6.1ms** | **24.8ms** | **4.1x** |

---

## Hacker News SQL Benchmarks (4M docs, Home Desktop)

### Query Timings

| # | Query | Mode | Time |
|---|-------|------|------|
| Q1 | `_count` match_all | metadata fast | **20ms** |
| Q2 | SQL `count(*)` | `count_star_fast` | **20ms** |
| Q3 | `_count` "rust" | text match | **19ms** |
| Q4 | `_count` "machine learning" | text match | **20ms** |
| Q5 | text_match ML + GROUP BY | `tantivy_grouped_partials` | **398ms** |
| Q6 | text_match rust + GROUP BY | `tantivy_grouped_partials` | **114ms** |
| Q7 | Global avg/max (4M scan) | `tantivy_grouped_partials` | **384ms** |
| Q8 | text_match + filter + LIMIT | `tantivy_fast_fields` | **21ms** |
| Q9 | text_match + ORDER BY LIMIT | `tantivy_fast_fields` | **19ms** |
| Q10 | text_match + ORDER BY LIMIT | `tantivy_fast_fields` | **20ms** |
| Q11 | range + ORDER BY LIMIT | `tantivy_fast_fields` | **19ms** |
| Q12 | BETWEEN + ORDER BY LIMIT | `tantivy_fast_fields` | **19ms** |
| Q13 | DSL sort by upvotes | scatter-gather | **20ms** |
| Q14 | DSL bool query | scatter-gather | **19ms** |
| Q15 | DSL terms agg (400K authors) | agg collector | **5,520ms** |
| Q16 | DSL stats agg | agg collector | **27ms** |

### EXPLAIN ANALYZE Internals

**Q17: text_match "python" + GROUP BY author**
```
planning: 0.1ms → search: 216ms → merge: 16ms → total: 232ms
Searched 4M stories, matched 12,638 python posts, grouped by 6,827 authors
Mode: tantivy_grouped_partials
```

**Q18: text_match "startup" + comments > 100 + ORDER BY LIMIT 10**
```
planning: 0.1ms → search: 7ms → datafusion: 1ms → total: 8ms
Searched 4M stories, matched 474 startup posts with 100+ comments, returned top 10
Mode: tantivy_fast_fields
```

### Showcase Query

```sql
SELECT author, count(*) AS posts, avg(upvotes) AS avg_upvotes,
       avg(comments) AS avg_discussion
FROM "hackernews"
WHERE text_match(title, 'rust')
GROUP BY author ORDER BY avg_upvotes DESC
```

**92ms** — searched 4M stories → matched 12,473 Rust posts → grouped 4,715 authors → computed per-author metrics → sorted. One API call.

Internal timing: planning=0.1ms, search=86ms, merge=6ms

### Cross-Topic Intelligence (4M docs, i5-13600K)

Six `text_match` + SQL analytics queries:

| Topic | Posts | Avg ↑ | Avg 💬 | Best ↑ | Top 💬 | Time |
|-------|-------|-------|--------|--------|--------|------|
| India | 17,860 | 11 | 5.5 | 1,483 | 795 | 5ms |
| China | 37,765 | 13 | 6.5 | 2,493 | 1,087 | 6ms |
| Silicon Valley | 21,261 | 15 | 10.4 | 3,172 | 2,780 | 4ms |
| Rust | 12,473 | 35 | 14.9 | 1,582 | 991 | 3ms |
| Python | 31,044 | 19 | 6.7 | 1,616 | 957 | 5ms |
| Machine Learning | 55,430 | 15 | 4.8 | 2,029 | 1,148 | 8ms |

Total wall clock: **~35ms** for all six queries.

All three queries: **~360ms total**.

---

## Reproduce

```bash
# Build
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Start cluster
./dev_cluster_release.sh 1 &
./dev_cluster_release.sh 2 &
./dev_cluster_release.sh 3 &

# Ingest
pip install opensearch-py pyarrow requests
python3 scripts/ingest_1gb.py                              # 2M synthetic docs
python3 scripts/search_1gb.py --queries 200 --concurrency 1  # search benchmark
python3 scripts/ingest_hackernews.py                       # 4M HN stories
bash scripts/load_nyc_taxis_100m.sh                        # 100M NYC taxi rows end-to-end: build/start cluster, download, ingest, benchmark
bash scripts/hackernews_queries.sh                         # SQL benchmark
```
