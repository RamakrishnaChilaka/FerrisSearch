#!/usr/bin/env python3
"""Performance test for stored fields optimization.

Compares SQL query performance between:
- tantivy_fast_fields path (all columns are fast-field-backed)
- materialized_hits_fallback path (SELECT * forces stored doc loading)

Usage:
    python3 scripts/perf_stored_fields.py [--docs 50000]
"""

import argparse
import json
import time
import requests
import statistics

BASE = "http://localhost:9200"


def ingest_docs(n: int):
    """Bulk-ingest n documents."""
    print(f"Ingesting {n} documents...")
    batch_size = 5000
    total_ingested = 0
    start = time.time()

    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        lines = []
        for i in range(batch_start, batch_end):
            lines.append(json.dumps({"index": {"_id": str(i)}}))
            lines.append(json.dumps({
                "title": f"Product {i}",
                "price": round(10.0 + (i % 1000) * 0.99, 2),
                "category": ["electronics", "books", "clothing", "food", "toys"][i % 5],
                "rating": round(1.0 + (i % 50) * 0.08, 2),
                "quantity": i % 200,
                "description": f"This is product number {i} with various features and specifications"
            }))
        body = "\n".join(lines) + "\n"
        resp = requests.post(f"{BASE}/perf_test/_bulk", data=body,
                           headers={"Content-Type": "application/x-ndjson"})
        resp.raise_for_status()
        total_ingested += (batch_end - batch_start)
        if total_ingested % 10000 == 0:
            print(f"  {total_ingested}/{n} ingested...")

    elapsed = time.time() - start
    print(f"Ingested {n} docs in {elapsed:.1f}s ({n/elapsed:.0f} docs/sec)")

    # Refresh
    requests.post(f"{BASE}/perf_test/_refresh")
    print("Refreshed.")


def run_query(sql: str, iterations: int = 20) -> dict:
    """Run a SQL query multiple times and return timing stats."""
    times = []
    result = None
    for i in range(iterations):
        start = time.time()
        resp = requests.post(f"{BASE}/perf_test/_sql",
                           json={"query": sql},
                           headers={"Content-Type": "application/json"})
        elapsed = (time.time() - start) * 1000  # ms
        resp.raise_for_status()
        result = resp.json()
        times.append(elapsed)

    return {
        "execution_mode": result.get("execution_mode", "unknown"),
        "matched_hits": result.get("matched_hits", 0),
        "rows": len(result.get("rows", [])),
        "p50_ms": round(statistics.median(times), 2),
        "p95_ms": round(sorted(times)[int(len(times) * 0.95)], 2),
        "mean_ms": round(statistics.mean(times), 2),
        "min_ms": round(min(times), 2),
        "max_ms": round(max(times), 2),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=50000, help="Number of docs to ingest")
    parser.add_argument("--iterations", type=int, default=30, help="Query iterations")
    parser.add_argument("--skip-ingest", action="store_true")
    args = parser.parse_args()

    if not args.skip_ingest:
        ingest_docs(args.docs)

    print(f"\nRunning each query {args.iterations} times...\n")
    print("=" * 80)

    # Test 1: Fast-field path — specific mapped columns
    sql1 = "SELECT category, price, rating FROM perf_test WHERE text_match(description, 'product')"
    print(f"\n[1] FAST-FIELD PATH (all columns mapped):")
    print(f"    SQL: {sql1}")
    r1 = run_query(sql1, args.iterations)
    print(f"    Mode: {r1['execution_mode']}")
    print(f"    Matched: {r1['matched_hits']}, Rows: {r1['rows']}")
    print(f"    p50={r1['p50_ms']}ms  p95={r1['p95_ms']}ms  mean={r1['mean_ms']}ms  min={r1['min_ms']}ms  max={r1['max_ms']}ms")

    # Test 2: Materialized fallback path — SELECT *
    sql2 = "SELECT * FROM perf_test WHERE text_match(description, 'product')"
    print(f"\n[2] MATERIALIZED FALLBACK (SELECT *):")
    print(f"    SQL: {sql2}")
    r2 = run_query(sql2, args.iterations)
    print(f"    Mode: {r2['execution_mode']}")
    print(f"    Matched: {r2['matched_hits']}, Rows: {r2['rows']}")
    print(f"    p50={r2['p50_ms']}ms  p95={r2['p95_ms']}ms  mean={r2['mean_ms']}ms  min={r2['min_ms']}ms  max={r2['max_ms']}ms")

    # Test 3: Fast-field with aggregation
    sql3 = "SELECT category, avg(price) AS avg_price, count(*) AS cnt FROM perf_test WHERE text_match(description, 'product') GROUP BY category"
    print(f"\n[3] GROUPED PARTIALS:")
    print(f"    SQL: {sql3}")
    r3 = run_query(sql3, args.iterations)
    print(f"    Mode: {r3['execution_mode']}")
    print(f"    Matched: {r3['matched_hits']}, Rows: {r3['rows']}")
    print(f"    p50={r3['p50_ms']}ms  p95={r3['p95_ms']}ms  mean={r3['mean_ms']}ms  min={r3['min_ms']}ms  max={r3['max_ms']}ms")

    # Test 4: Fast-field with LIMIT
    sql4 = "SELECT category, price FROM perf_test WHERE text_match(description, 'product') ORDER BY price DESC LIMIT 10"
    print(f"\n[4] FAST-FIELD + LIMIT PUSHDOWN:")
    print(f"    SQL: {sql4}")
    r4 = run_query(sql4, args.iterations)
    print(f"    Mode: {r4['execution_mode']}")
    print(f"    Matched: {r4['matched_hits']}, Rows: {r4['rows']}")
    print(f"    p50={r4['p50_ms']}ms  p95={r4['p95_ms']}ms  mean={r4['mean_ms']}ms  min={r4['min_ms']}ms  max={r4['max_ms']}ms")

    print("\n" + "=" * 80)
    if r1['p50_ms'] > 0 and r2['p50_ms'] > 0:
        speedup = r2['p50_ms'] / r1['p50_ms']
        print(f"\nFast-field vs Fallback speedup (p50): {speedup:.1f}x")
    print()


if __name__ == "__main__":
    main()
