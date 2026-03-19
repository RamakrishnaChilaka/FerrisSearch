#!/usr/bin/env python3
"""
Benchmark search queries against FerrisSearch's benchmark-1gb index.

Run ingest_1gb.py first to populate the index, then:

    /tmp/osenv/bin/python3 scripts/search_1gb.py
    /tmp/osenv/bin/python3 scripts/search_1gb.py --queries 500 --warmup 50
    /tmp/osenv/bin/python3 scripts/search_1gb.py --host localhost --port 9200 --concurrency 4

Reports per-query-type latency stats (min, avg, p50, p95, p99, max) and throughput.
"""

import argparse
import random
import statistics
import string
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from opensearchpy import OpenSearch

INDEX_NAME = "benchmark-1gb"

CATEGORIES = ["electronics", "books", "clothing", "sports", "home", "toys", "food", "health"]
WORDS = [
    "search", "engine", "distributed", "cluster", "shard", "replica", "index",
    "document", "query", "filter", "aggregation", "vector", "embedding", "node",
    "raft", "consensus", "tantivy", "rust", "performance", "benchmark", "test",
    "product", "review", "rating", "price", "shipping", "warehouse", "inventory",
    "customer", "order", "payment", "delivery", "tracking", "analytics", "report",
    "dashboard", "metric", "monitor", "alert", "notification", "service", "api",
    "endpoint", "request", "response", "latency", "throughput", "bandwidth",
]


# ── Query generators ──────────────────────────────────────────────


def query_match_all():
    return {"name": "match_all", "body": {"query": {"match_all": {}}, "size": 10}}


def query_match_title():
    term = random.choice(WORDS)
    return {"name": "match_title", "body": {"query": {"match": {"title": term}}, "size": 10}}


def query_match_description():
    terms = f"{random.choice(WORDS)} {random.choice(WORDS)}"
    return {"name": "match_description", "body": {"query": {"match": {"description": terms}}, "size": 10}}


def query_term_category():
    cat = random.choice(CATEGORIES)
    return {"name": "term_category", "body": {"query": {"term": {"category": cat}}, "size": 10}}


def query_bool_must():
    return {
        "name": "bool_must",
        "body": {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": random.choice(WORDS)}},
                        {"match": {"description": random.choice(WORDS)}},
                    ]
                }
            },
            "size": 10,
        },
    }


def query_bool_should():
    return {
        "name": "bool_should",
        "body": {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"title": random.choice(WORDS)}},
                        {"match": {"title": random.choice(WORDS)}},
                        {"match": {"title": random.choice(WORDS)}},
                    ]
                }
            },
            "size": 10,
        },
    }


def query_bool_filter_range():
    low = round(random.uniform(5.0, 200.0), 2)
    high = round(low + random.uniform(50.0, 300.0), 2)
    return {
        "name": "bool_filter_range",
        "body": {
            "query": {
                "bool": {
                    "must": [{"match": {"title": random.choice(WORDS)}}],
                    "filter": [{"range": {"price": {"gte": low, "lte": high}}}],
                }
            },
            "size": 10,
        },
    }


def query_range_price():
    low = round(random.uniform(10.0, 500.0), 2)
    high = round(low + random.uniform(100.0, 400.0), 2)
    return {
        "name": "range_price",
        "body": {"query": {"range": {"price": {"gte": low, "lte": high}}}, "size": 10},
    }


def query_range_rating():
    low = round(random.uniform(1.0, 3.5), 1)
    return {
        "name": "range_rating",
        "body": {"query": {"range": {"rating": {"gte": low}}}, "size": 10},
    }


def query_fuzzy_title():
    word = random.choice(WORDS)
    # Introduce a typo
    if len(word) > 3:
        pos = random.randint(1, len(word) - 2)
        typo = word[:pos] + random.choice(string.ascii_lowercase) + word[pos + 1 :]
    else:
        typo = word
    return {
        "name": "fuzzy_title",
        "body": {"query": {"fuzzy": {"title": {"value": typo, "fuzziness": 2}}}, "size": 10},
    }


def query_prefix_title():
    word = random.choice(WORDS)
    prefix = word[: max(3, len(word) // 2)]
    return {
        "name": "prefix_title",
        "body": {"query": {"prefix": {"title": prefix}}, "size": 10},
    }


def query_wildcard_title():
    word = random.choice(WORDS)
    pattern = word[:3] + "*"
    return {
        "name": "wildcard_title",
        "body": {"query": {"wildcard": {"title": pattern}}, "size": 10},
    }


def query_sort_price_asc():
    return {
        "name": "sort_price_asc",
        "body": {"query": {"match_all": {}}, "sort": [{"price": "asc"}], "size": 10},
    }


def query_sort_rating_desc():
    return {
        "name": "sort_rating_desc",
        "body": {
            "query": {"match": {"title": random.choice(WORDS)}},
            "sort": [{"rating": "desc"}, "_score"],
            "size": 10,
        },
    }


def query_agg_terms_category():
    return {
        "name": "agg_terms_category",
        "body": {
            "query": {"match_all": {}},
            "size": 0,
            "aggs": {"top_categories": {"terms": {"field": "category", "size": 10}}},
        },
    }


def query_agg_stats_price():
    return {
        "name": "agg_stats_price",
        "body": {
            "query": {"match_all": {}},
            "size": 0,
            "aggs": {"price_stats": {"stats": {"field": "price"}}},
        },
    }


def query_agg_histogram_price():
    return {
        "name": "agg_histogram_price",
        "body": {
            "query": {"match_all": {}},
            "size": 0,
            "aggs": {"price_buckets": {"histogram": {"field": "price", "interval": 100}}},
        },
    }


def query_agg_with_filter():
    cat = random.choice(CATEGORIES)
    return {
        "name": "agg_filtered",
        "body": {
            "query": {"term": {"category": cat}},
            "size": 0,
            "aggs": {
                "avg_rating": {"avg": {"field": "rating"}},
                "price_range": {"stats": {"field": "price"}},
            },
        },
    }


def query_complex_bool():
    return {
        "name": "complex_bool",
        "body": {
            "query": {
                "bool": {
                    "must": [{"match": {"description": random.choice(WORDS)}}],
                    "should": [{"match": {"title": random.choice(WORDS)}}],
                    "must_not": [{"term": {"category": random.choice(CATEGORIES)}}],
                    "filter": [{"range": {"price": {"gte": 10.0, "lte": 500.0}}}],
                }
            },
            "sort": [{"rating": "desc"}],
            "size": 20,
        },
    }


def query_paginated():
    page = random.randint(0, 50)
    return {
        "name": "paginated",
        "body": {
            "query": {"match": {"description": random.choice(WORDS)}},
            "from": page * 20,
            "size": 20,
        },
    }


ALL_QUERY_GENERATORS = [
    query_match_all,
    query_match_title,
    query_match_description,
    query_term_category,
    query_bool_must,
    query_bool_should,
    query_bool_filter_range,
    query_range_price,
    query_range_rating,
    query_fuzzy_title,
    query_prefix_title,
    query_wildcard_title,
    query_sort_price_asc,
    query_sort_rating_desc,
    query_agg_terms_category,
    query_agg_stats_price,
    query_agg_histogram_price,
    query_agg_with_filter,
    query_complex_bool,
    query_paginated,
]


# ── Benchmark runner ──────────────────────────────────────────────


def percentile(sorted_data, p):
    """Calculate percentile from sorted data."""
    if not sorted_data:
        return 0
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def run_single_query(client, query_spec):
    """Execute a single search query and return (name, latency_ms, hit_count, success)."""
    name = query_spec["name"]
    body = query_spec["body"]
    start = time.perf_counter()
    try:
        resp = client.search(index=INDEX_NAME, body=body)
        elapsed_ms = (time.perf_counter() - start) * 1000
        hits = resp.get("hits", {}).get("total", {})
        hit_count = hits.get("value", 0) if isinstance(hits, dict) else hits
        return (name, elapsed_ms, hit_count, True)
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return (name, elapsed_ms, 0, False)


def print_stats_table(results_by_type):
    """Print a formatted table of latency statistics per query type."""
    header = f"{'Query Type':<25} {'Count':>6} {'Err':>4} {'Min':>8} {'Avg':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'Max':>8} {'Hits/q':>8}"
    print(header)
    print("─" * len(header))

    all_latencies = []
    total_queries = 0
    total_errors = 0

    for name in sorted(results_by_type.keys()):
        entries = results_by_type[name]
        latencies = sorted([e[1] for e in entries if e[3]])
        errors = sum(1 for e in entries if not e[3])
        avg_hits = statistics.mean([e[2] for e in entries if e[3]]) if latencies else 0

        total_queries += len(entries)
        total_errors += errors
        all_latencies.extend(latencies)

        if latencies:
            print(
                f"{name:<25} {len(entries):>6} {errors:>4} "
                f"{min(latencies):>7.1f}ms {statistics.mean(latencies):>7.1f}ms "
                f"{percentile(latencies, 50):>7.1f}ms {percentile(latencies, 95):>7.1f}ms "
                f"{percentile(latencies, 99):>7.1f}ms {max(latencies):>7.1f}ms "
                f"{avg_hits:>7.0f}"
            )
        else:
            print(f"{name:<25} {len(entries):>6} {errors:>4} {'(all failed)':>50}")

    all_latencies.sort()
    print("─" * len(header))
    if all_latencies:
        print(
            f"{'TOTAL':<25} {total_queries:>6} {total_errors:>4} "
            f"{min(all_latencies):>7.1f}ms {statistics.mean(all_latencies):>7.1f}ms "
            f"{percentile(all_latencies, 50):>7.1f}ms {percentile(all_latencies, 95):>7.1f}ms "
            f"{percentile(all_latencies, 99):>7.1f}ms {max(all_latencies):>7.1f}ms "
            f"{'':>8}"
        )


def main():
    parser = argparse.ArgumentParser(description="Benchmark search on FerrisSearch benchmark-1gb index")
    parser.add_argument("--host", default="localhost", help="FerrisSearch host")
    parser.add_argument("--port", type=int, default=9200, help="FerrisSearch port")
    parser.add_argument("--queries", type=int, default=200, help="Total queries per query type")
    parser.add_argument("--warmup", type=int, default=20, help="Warmup queries per type (excluded from stats)")
    parser.add_argument("--concurrency", type=int, default=1, help="Parallel search threads")
    parser.add_argument(
        "--types",
        nargs="*",
        default=None,
        help="Query types to run (default: all). Use --list-types to see available types.",
    )
    parser.add_argument("--list-types", action="store_true", help="List available query types and exit")
    args = parser.parse_args()

    if args.list_types:
        print("Available query types:")
        for gen in ALL_QUERY_GENERATORS:
            sample = gen()
            print(f"  {sample['name']}")
        return

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )

    # Verify connectivity and index
    info = client.info()
    print(f"Connected to {info['name']} v{info['version']}")

    try:
        resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
        doc_count = resp["hits"]["total"]["value"]
        print(f"Index '{INDEX_NAME}' has {doc_count:,} documents")
    except Exception as e:
        print(f"Error: cannot query index '{INDEX_NAME}': {e}")
        print("Run ingest_1gb.py first to populate the index.")
        return

    # Filter query generators
    generators = ALL_QUERY_GENERATORS
    if args.types:
        type_set = set(args.types)
        generators = [g for g in generators if g()["name"] in type_set]
        if not generators:
            print(f"No matching query types. Use --list-types to see available types.")
            return

    print(f"\nBenchmark config:")
    print(f"  Query types:  {len(generators)}")
    print(f"  Queries/type: {args.queries} (+{args.warmup} warmup)")
    print(f"  Concurrency:  {args.concurrency}")
    print(f"  Total:        {len(generators) * (args.queries + args.warmup):,} queries")

    # ── Warmup ──
    if args.warmup > 0:
        print(f"\nWarming up ({args.warmup} queries per type)...", end="", flush=True)
        for gen in generators:
            for _ in range(args.warmup):
                spec = gen()
                run_single_query(client, spec)
        print(" done")

    # ── Benchmark ──
    print(f"\nRunning benchmark ({args.queries} queries per type)...\n")
    results_by_type = {}
    total_start = time.time()
    completed = 0
    total_to_run = len(generators) * args.queries

    if args.concurrency <= 1:
        # Sequential
        for gen in generators:
            for _ in range(args.queries):
                spec = gen()
                result = run_single_query(client, spec)
                name = result[0]
                results_by_type.setdefault(name, []).append(result)
                completed += 1
                if completed % 50 == 0:
                    elapsed = time.time() - total_start
                    qps = completed / elapsed if elapsed > 0 else 0
                    print(
                        f"\r  {completed:>6} / {total_to_run} queries ({qps:.0f} q/s)",
                        end="",
                        flush=True,
                    )
    else:
        # Concurrent
        work_items = []
        for gen in generators:
            for _ in range(args.queries):
                work_items.append(gen())
        random.shuffle(work_items)

        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = {pool.submit(run_single_query, client, spec): spec for spec in work_items}
            for future in as_completed(futures):
                result = future.result()
                name = result[0]
                results_by_type.setdefault(name, []).append(result)
                completed += 1
                if completed % 50 == 0:
                    elapsed = time.time() - total_start
                    qps = completed / elapsed if elapsed > 0 else 0
                    print(
                        f"\r  {completed:>6} / {total_to_run} queries ({qps:.0f} q/s)",
                        end="",
                        flush=True,
                    )

    total_elapsed = time.time() - total_start
    overall_qps = completed / total_elapsed if total_elapsed > 0 else 0

    print(f"\r  {completed:>6} / {total_to_run} queries ({overall_qps:.0f} q/s)")
    print(f"\n{'=' * 100}")
    print(f"SEARCH BENCHMARK RESULTS — {INDEX_NAME} ({doc_count:,} docs)")
    print(f"{'=' * 100}\n")

    print_stats_table(results_by_type)

    print(f"\n{'=' * 100}")
    print(f"  Total queries:   {completed:,}")
    print(f"  Total time:      {total_elapsed:.1f}s")
    print(f"  Throughput:      {overall_qps:,.1f} queries/sec")
    print(f"  Concurrency:     {args.concurrency}")
    print(f"{'=' * 100}")


if __name__ == "__main__":
    main()
