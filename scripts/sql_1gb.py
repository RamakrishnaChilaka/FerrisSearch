#!/usr/bin/env python3
"""
Benchmark SQL queries against FerrisSearch's benchmark-1gb index.

Run ingest_1gb.py first to populate the index, then:

    /tmp/osenv/bin/python3 scripts/sql_1gb.py
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --queries 200 --warmup 20 --concurrency 4
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --suite throughput
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --suite stress
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --types grouped_category fast_fields_projection
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --explain-sample --seed 42
    /tmp/osenv/bin/python3 scripts/sql_1gb.py --json-out /tmp/sql-benchmark.json

Reports per-query-type latency stats, average matched hits / returned rows,
error counts, and execution mode counts so you can see which path each SQL
shape used. The harness supports warmup, concurrency, seeded randomized
workloads, and JSON result export so runs are easier to compare over time.
"""

import argparse
import json
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from opensearchpy import OpenSearch

INDEX_NAME = "benchmark-1gb"
SQL_INDEX_NAME = '"benchmark-1gb"'

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


def build_rng(seed):
    return random.Random(seed) if seed is not None else random.Random()


def sql_fast_fields_projection(rng):
    term = rng.choice(WORDS)
    low = round(rng.uniform(25.0, 300.0), 2)
    high = round(low + rng.uniform(50.0, 250.0), 2)
    return {
        "name": "fast_fields_projection",
        "suite": "throughput",
        "query": (
            f"SELECT title, category, price, rating, score "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}') "
            f"AND price >= {low} AND price <= {high} "
            f"ORDER BY score DESC LIMIT 25"
        ),
    }


def sql_grouped_category(rng):
    term = rng.choice(WORDS)
    return {
        "name": "grouped_category",
        "suite": "throughput",
        "query": (
            f"SELECT category, count(*) AS total, avg(price) AS avg_price, max(rating) AS max_rating "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}') "
            f"GROUP BY category "
            f"ORDER BY total DESC, category ASC"
        ),
    }


def sql_grouped_filtered(rng):
    term = rng.choice(WORDS)
    category = rng.choice(CATEGORIES)
    threshold = round(rng.uniform(50.0, 400.0), 2)
    return {
        "name": "grouped_filtered",
        "suite": "throughput",
        "query": (
            f"SELECT category, count(*) AS total, min(price) AS min_price, avg(rating) AS avg_rating "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}') "
            f"AND category = '{category}' "
            f"AND price >= {threshold} "
            f"GROUP BY category "
            f"ORDER BY total DESC"
        ),
    }


def sql_aggregate_summary(rng):
    term = rng.choice(WORDS)
    return {
        "name": "aggregate_summary",
        "suite": "throughput",
        "query": (
            f"SELECT avg(price) AS avg_price, count(*) AS total, max(rating) AS max_rating "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}')"
        ),
    }


def sql_order_by_price(rng):
    category = rng.choice(CATEGORIES)
    return {
        "name": "order_by_price",
        "suite": "throughput",
        "query": (
            f"SELECT title, category, price, score "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE category = '{category}' "
            f"ORDER BY price DESC LIMIT 50"
        ),
    }


def sql_topk_wide_projection(rng):
    term = rng.choice(WORDS)
    return {
        "name": "topk_wide_projection",
        "suite": "throughput",
        "query": (
            f"SELECT title, category, price, rating, in_stock, score "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}') "
            f"ORDER BY score DESC LIMIT 100"
        ),
    }


def sql_select_all_fallback(rng):
    term = rng.choice(WORDS)
    return {
        "name": "select_all_fallback",
        "suite": "stress",
        "query": (
            f"SELECT * FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}')"
        ),
    }


def sql_unbounded_projection_stress(rng):
    term = rng.choice(WORDS)
    return {
        "name": "unbounded_projection_stress",
        "suite": "stress",
        "query": (
            f"SELECT title, category, price, rating, score "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE text_match(description, '{term}') "
            f"ORDER BY score DESC"
        ),
    }


def sql_global_sort_stress(rng):
    category = rng.choice(CATEGORIES)
    return {
        "name": "global_sort_stress",
        "suite": "stress",
        "query": (
            f"SELECT title, category, price, score "
            f"FROM {SQL_INDEX_NAME} "
            f"WHERE category = '{category}' "
            f"ORDER BY price DESC"
        ),
    }


ALL_QUERY_GENERATORS = [
    sql_fast_fields_projection,
    sql_grouped_category,
    sql_grouped_filtered,
    sql_aggregate_summary,
    sql_order_by_price,
    sql_topk_wide_projection,
    sql_select_all_fallback,
    sql_unbounded_projection_stress,
    sql_global_sort_stress,
]

SUITES = {"throughput", "stress", "all"}


def parse_status_file(pid):
    status = {}
    with open(f"/proc/{pid}/status", "r", encoding="utf-8") as handle:
        for line in handle:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            status[key.strip()] = value.strip()
    return status


def parse_smaps_rollup(pid):
    metrics = {}
    path = f"/proc/{pid}/smaps_rollup"
    if not os.path.exists(path):
        return metrics
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            metrics[key.strip()] = value.strip()
    return metrics


def parse_kb_field(value):
    if value is None:
        return None
    parts = value.split()
    if not parts:
        return None
    try:
        return int(parts[0])
    except ValueError:
        return None


def find_local_ferrissearch_pid():
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            with open(f"/proc/{pid}/comm", "r", encoding="utf-8") as handle:
                comm = handle.read().strip()
            if comm == "ferrissearch":
                return pid
        except OSError:
            continue
    return None


def sample_process_memory(pid):
    status = parse_status_file(pid)
    smaps = parse_smaps_rollup(pid)
    return {
        "timestamp_s": time.time(),
        "rss_kb": parse_kb_field(status.get("VmRSS")) or parse_kb_field(smaps.get("Rss")),
        "vm_size_kb": parse_kb_field(status.get("VmSize")),
        "vm_data_kb": parse_kb_field(status.get("VmData")),
        "pss_anon_kb": parse_kb_field(smaps.get("Pss_Anon")),
        "pss_file_kb": parse_kb_field(smaps.get("Pss_File")),
        "private_dirty_kb": parse_kb_field(smaps.get("Private_Dirty")),
    }


class MemorySampler:
    def __init__(self, pid, interval_s):
        self.pid = pid
        self.interval_s = interval_s
        self.samples = []
        self._stop_event = threading.Event()
        self._thread = None

    def _capture(self):
        try:
            self.samples.append(sample_process_memory(self.pid))
        except OSError:
            self._stop_event.set()

    def _run(self):
        while not self._stop_event.wait(self.interval_s):
            self._capture()

    def start(self):
        self._capture()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + 1.0)
        self._capture()

    def summary(self):
        if not self.samples:
            return None
        rss_values = [sample["rss_kb"] for sample in self.samples if sample.get("rss_kb") is not None]
        if not rss_values:
            return None
        first = self.samples[0]
        last = self.samples[-1]
        peak = max(self.samples, key=lambda sample: sample.get("rss_kb") or 0)
        return {
            "pid": self.pid,
            "sample_count": len(self.samples),
            "interval_s": self.interval_s,
            "rss_start_kb": first.get("rss_kb"),
            "rss_end_kb": last.get("rss_kb"),
            "rss_peak_kb": peak.get("rss_kb"),
            "rss_delta_kb": (last.get("rss_kb") or 0) - (first.get("rss_kb") or 0),
            "pss_anon_start_kb": first.get("pss_anon_kb"),
            "pss_anon_end_kb": last.get("pss_anon_kb"),
            "pss_file_start_kb": first.get("pss_file_kb"),
            "pss_file_end_kb": last.get("pss_file_kb"),
            "private_dirty_start_kb": first.get("private_dirty_kb"),
            "private_dirty_end_kb": last.get("private_dirty_kb"),
            "samples": self.samples,
        }


def percentile(sorted_data, p):
    if not sorted_data:
        return 0
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def sql_request(client, path, query):
    return client.transport.perform_request(
        method="POST",
        url=path,
        body={"query": query},
    )


def run_single_query(client, query_spec):
    name = query_spec["name"]
    query = query_spec["query"]
    start = time.perf_counter()
    try:
        resp = sql_request(client, f"/{INDEX_NAME}/_sql", query)
        elapsed_ms = (time.perf_counter() - start) * 1000
        rows = resp.get("rows", [])
        planner = resp.get("planner", {})
        return {
            "name": name,
            "latency_ms": elapsed_ms,
            "matched_hits": resp.get("matched_hits", 0),
            "row_count": len(rows),
            "execution_mode": resp.get("execution_mode", "unknown"),
            "has_residual_predicates": planner.get("has_residual_predicates", False),
            "error": None,
            "success": True,
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "latency_ms": elapsed_ms,
            "matched_hits": 0,
            "row_count": 0,
            "execution_mode": "failed",
            "has_residual_predicates": False,
            "error": str(exc),
            "success": False,
        }


def print_stats_table(results_by_type):
    header = (
        f"{'Query Type':<24} {'Count':>6} {'Err':>4} {'Min':>8} {'Avg':>8} "
        f"{'p50':>8} {'p95':>8} {'p99':>8} {'Max':>8} {'Hits/q':>8} {'Rows/q':>8}"
    )
    print(header)
    print("─" * len(header))

    all_latencies = []
    total_queries = 0
    total_errors = 0

    for name in sorted(results_by_type.keys()):
        entries = results_by_type[name]
        successes = [entry for entry in entries if entry["success"]]
        latencies = sorted(entry["latency_ms"] for entry in successes)
        errors = len(entries) - len(successes)
        avg_hits = statistics.mean(entry["matched_hits"] for entry in successes) if successes else 0
        avg_rows = statistics.mean(entry["row_count"] for entry in successes) if successes else 0

        total_queries += len(entries)
        total_errors += errors
        all_latencies.extend(latencies)

        if latencies:
            print(
                f"{name:<24} {len(entries):>6} {errors:>4} "
                f"{min(latencies):>7.1f}ms {statistics.mean(latencies):>7.1f}ms "
                f"{percentile(latencies, 50):>7.1f}ms {percentile(latencies, 95):>7.1f}ms "
                f"{percentile(latencies, 99):>7.1f}ms {max(latencies):>7.1f}ms "
                f"{avg_hits:>7.0f} {avg_rows:>7.1f}"
            )
        else:
            print(f"{name:<24} {len(entries):>6} {errors:>4} {'(all failed)':>59}")

    all_latencies.sort()
    print("─" * len(header))
    if all_latencies:
        print(
            f"{'TOTAL':<24} {total_queries:>6} {total_errors:>4} "
            f"{min(all_latencies):>7.1f}ms {statistics.mean(all_latencies):>7.1f}ms "
            f"{percentile(all_latencies, 50):>7.1f}ms {percentile(all_latencies, 95):>7.1f}ms "
            f"{percentile(all_latencies, 99):>7.1f}ms {max(all_latencies):>7.1f}ms "
            f"{'':>8} {'':>8}"
        )


def print_execution_mode_summary(results_by_type):
    print("\nExecution modes by query type:")
    for name in sorted(results_by_type.keys()):
        counts = {}
        residuals = 0
        for entry in results_by_type[name]:
            if not entry["success"]:
                continue
            counts[entry["execution_mode"]] = counts.get(entry["execution_mode"], 0) + 1
            if entry["has_residual_predicates"]:
                residuals += 1
        formatted = ", ".join(f"{mode}={count}" for mode, count in sorted(counts.items()))
        if not formatted:
            formatted = "no successful queries"
        print(f"  {name:<24} {formatted} | residual_predicates={residuals}")


def build_summary(results_by_type, total_elapsed, doc_count, args, selected_type_names, memory_summary):
    query_types = {}
    total_queries = 0
    total_errors = 0
    all_latencies = []
    error_samples = {}

    for name, entries in sorted(results_by_type.items()):
        successes = [entry for entry in entries if entry["success"]]
        failures = [entry for entry in entries if not entry["success"]]
        latencies = sorted(entry["latency_ms"] for entry in successes)
        mode_counts = {}
        for entry in successes:
            mode_counts[entry["execution_mode"]] = mode_counts.get(entry["execution_mode"], 0) + 1
        if failures:
            error_samples[name] = failures[0].get("error", "unknown error")

        query_types[name] = {
            "count": len(entries),
            "errors": len(failures),
            "avg_matched_hits": statistics.mean(entry["matched_hits"] for entry in successes)
            if successes
            else 0,
            "avg_row_count": statistics.mean(entry["row_count"] for entry in successes)
            if successes
            else 0,
            "execution_modes": mode_counts,
            "latency_ms": {
                "min": min(latencies) if latencies else 0,
                "avg": statistics.mean(latencies) if latencies else 0,
                "p50": percentile(latencies, 50) if latencies else 0,
                "p95": percentile(latencies, 95) if latencies else 0,
                "p99": percentile(latencies, 99) if latencies else 0,
                "max": max(latencies) if latencies else 0,
            },
        }
        total_queries += len(entries)
        total_errors += len(failures)
        all_latencies.extend(latencies)

    return {
        "index": INDEX_NAME,
        "doc_count": doc_count,
        "seed": args.seed,
        "queries_per_type": args.queries,
        "warmup_per_type": args.warmup,
        "concurrency": args.concurrency,
        "suite": args.suite,
        "selected_types": selected_type_names,
        "total_queries": total_queries,
        "total_errors": total_errors,
        "total_time_s": total_elapsed,
        "throughput_qps": (total_queries / total_elapsed) if total_elapsed > 0 else 0,
        "memory": memory_summary,
        "latency_ms": {
            "min": min(all_latencies) if all_latencies else 0,
            "avg": statistics.mean(all_latencies) if all_latencies else 0,
            "p50": percentile(all_latencies, 50) if all_latencies else 0,
            "p95": percentile(all_latencies, 95) if all_latencies else 0,
            "p99": percentile(all_latencies, 99) if all_latencies else 0,
            "max": max(all_latencies) if all_latencies else 0,
        },
        "query_types": query_types,
        "error_samples": error_samples,
    }


def write_summary_json(path, summary):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")


def make_query_specs(generators, queries, seed):
    rng = build_rng(seed)
    work_items = []
    for gen in generators:
        for _ in range(queries):
            work_items.append(gen(rng))
    rng.shuffle(work_items)
    return work_items


def explain_samples(client, generators):
    print("\nSample EXPLAIN plans:")
    rng = build_rng(0)
    for gen in generators:
        spec = gen(rng)
        resp = sql_request(client, f"/{INDEX_NAME}/_sql/explain", spec["query"])
        print(f"\n[{spec['name']}]")
        print(f"  query: {spec['query']}")
        print(f"  strategy: {resp.get('execution_strategy', 'unknown')}")
        columns = resp.get("columns", {})
        print(
            "  columns: "
            f"required={columns.get('required', [])} "
            f"group_by={columns.get('group_by', [])} "
            f"selects_all={columns.get('selects_all', False)}"
        )


def format_mb(kb):
    if kb is None:
        return "n/a"
    return f"{kb / 1024:.1f} MB"


def print_memory_summary(memory_summary):
    if memory_summary is None:
        return
    print("\nServer memory summary:")
    print(f"  PID:           {memory_summary['pid']}")
    print(f"  Samples:       {memory_summary['sample_count']} @ {memory_summary['interval_s']:.1f}s")
    print(f"  RSS start:     {format_mb(memory_summary['rss_start_kb'])}")
    print(f"  RSS peak:      {format_mb(memory_summary['rss_peak_kb'])}")
    print(f"  RSS end:       {format_mb(memory_summary['rss_end_kb'])}")
    print(f"  RSS delta:     {format_mb(memory_summary['rss_delta_kb'])}")
    print(f"  Anon start:    {format_mb(memory_summary['pss_anon_start_kb'])}")
    print(f"  Anon end:      {format_mb(memory_summary['pss_anon_end_kb'])}")
    print(f"  File start:    {format_mb(memory_summary['pss_file_start_kb'])}")
    print(f"  File end:      {format_mb(memory_summary['pss_file_end_kb'])}")
    print(f"  Dirty start:   {format_mb(memory_summary['private_dirty_start_kb'])}")
    print(f"  Dirty end:     {format_mb(memory_summary['private_dirty_end_kb'])}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark SQL on FerrisSearch benchmark-1gb index")
    parser.add_argument("--host", default="localhost", help="FerrisSearch host")
    parser.add_argument("--port", type=int, default=9200, help="FerrisSearch port")
    parser.add_argument("--queries", type=int, default=100, help="Total queries per query type")
    parser.add_argument("--warmup", type=int, default=10, help="Warmup queries per type (excluded from stats)")
    parser.add_argument("--concurrency", type=int, default=1, help="Parallel SQL request threads")
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible randomized workloads")
    parser.add_argument(
        "--server-pid",
        type=int,
        default=None,
        help="Optional ferrissearch PID to sample for lightweight memory tracking",
    )
    parser.add_argument(
        "--memory-sample-interval",
        type=float,
        default=1.0,
        help="Process memory sampling interval in seconds when tracking a local ferrissearch PID",
    )
    parser.add_argument(
        "--suite",
        choices=sorted(SUITES),
        default="throughput",
        help="Benchmark suite to run: throughput for bounded-result comparisons, stress for heavy result-shipping cases, or all",
    )
    parser.add_argument(
        "--types",
        nargs="*",
        default=None,
        help="Query types to run (default: all). Use --list-types to see available types.",
    )
    parser.add_argument("--list-types", action="store_true", help="List available query types and exit")
    parser.add_argument(
        "--explain-sample",
        action="store_true",
        help="Print one EXPLAIN sample per selected query type before benchmarking",
    )
    parser.add_argument(
        "--json-out",
        default=None,
        help="Optional path to write a machine-readable benchmark summary as JSON",
    )
    args = parser.parse_args()

    if args.list_types:
        print("Available query types:")
        rng = build_rng(0)
        for gen in ALL_QUERY_GENERATORS:
            spec = gen(rng)
            print(f"  {spec['name']} [{spec['suite']}]")
        return

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False,
        verify_certs=False,
        timeout=60,
    )

    info = client.info()
    print(f"Connected to {info['name']} v{info['version']}")

    try:
        resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
        doc_count = resp["hits"]["total"]["value"]
        print(f"Index '{INDEX_NAME}' has {doc_count:,} documents")
    except Exception as exc:
        print(f"Error: cannot query index '{INDEX_NAME}': {exc}")
        print("Run ingest_1gb.py first to populate the index.")
        return

    generators = [
        gen for gen in ALL_QUERY_GENERATORS if args.suite == "all" or gen(build_rng(0))["suite"] == args.suite
    ]
    if args.types:
        type_set = set(args.types)
        rng = build_rng(0)
        generators = [gen for gen in generators if gen(rng)["name"] in type_set]
        if not generators:
            print("No matching query types. Use --list-types to see available types.")
            return

    selected_type_names = [gen(build_rng(0))["name"] for gen in generators]

    print("\nBenchmark config:")
    print(f"  Suite:        {args.suite}")
    print(f"  Query types:  {len(generators)}")
    print(f"  Queries/type: {args.queries} (+{args.warmup} warmup)")
    print(f"  Concurrency:  {args.concurrency}")
    print(f"  Seed:         {args.seed}")
    print(f"  Total:        {len(generators) * (args.queries + args.warmup):,} queries")

    memory_sampler = None
    server_pid = args.server_pid
    if server_pid is None and args.host in {"localhost", "127.0.0.1"}:
        server_pid = find_local_ferrissearch_pid()
    if server_pid is not None:
        memory_sampler = MemorySampler(server_pid, args.memory_sample_interval)
        memory_sampler.start()
        print(f"  Memory PID:   {server_pid} (sampling every {args.memory_sample_interval:.1f}s)")
    else:
        print("  Memory PID:   not tracking")

    if args.explain_sample:
        explain_samples(client, generators)

    if args.warmup > 0:
        print(f"\nWarming up ({args.warmup} queries per type)...", end="", flush=True)
        warmup_items = make_query_specs(generators, args.warmup, args.seed)
        if args.concurrency <= 1:
            for spec in warmup_items:
                run_single_query(client, spec)
        else:
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [pool.submit(run_single_query, client, spec) for spec in warmup_items]
                for future in as_completed(futures):
                    future.result()
        print(" done")

    print(f"\nRunning benchmark ({args.queries} queries per type)...\n")
    results_by_type = {}
    total_start = time.time()
    completed = 0
    total_to_run = len(generators) * args.queries

    work_items = make_query_specs(generators, args.queries, args.seed + 1)
    if args.concurrency <= 1:
        for spec in work_items:
            result = run_single_query(client, spec)
            results_by_type.setdefault(result["name"], []).append(result)
            completed += 1
            if completed % 25 == 0:
                elapsed = time.time() - total_start
                qps = completed / elapsed if elapsed > 0 else 0
                print(
                    f"\r  {completed:>6} / {total_to_run} queries ({qps:.1f} q/s)",
                    end="",
                    flush=True,
                )
    else:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [pool.submit(run_single_query, client, spec) for spec in work_items]
            for future in as_completed(futures):
                result = future.result()
                results_by_type.setdefault(result["name"], []).append(result)
                completed += 1
                if completed % 25 == 0:
                    elapsed = time.time() - total_start
                    qps = completed / elapsed if elapsed > 0 else 0
                    print(
                        f"\r  {completed:>6} / {total_to_run} queries ({qps:.1f} q/s)",
                        end="",
                        flush=True,
                    )

    total_elapsed = time.time() - total_start
    overall_qps = completed / total_elapsed if total_elapsed > 0 else 0
    if memory_sampler is not None:
        memory_sampler.stop()
    memory_summary = memory_sampler.summary() if memory_sampler is not None else None

    print(f"\r  {completed:>6} / {total_to_run} queries ({overall_qps:.1f} q/s)")
    print(f"\n{'=' * 108}")
    print(f"SQL BENCHMARK RESULTS — {INDEX_NAME} ({doc_count:,} docs)")
    print(f"{'=' * 108}\n")

    print_stats_table(results_by_type)
    print_execution_mode_summary(results_by_type)
    print_memory_summary(memory_summary)

    summary = build_summary(
        results_by_type,
        total_elapsed,
        doc_count,
        args,
        selected_type_names,
        memory_summary,
    )
    if args.json_out:
        write_summary_json(args.json_out, summary)
        print(f"\nWrote JSON summary to {args.json_out}")

    print(f"\n{'=' * 108}")
    print(f"  Total queries:   {completed:,}")
    print(f"  Total time:      {total_elapsed:.1f}s")
    print(f"  Throughput:      {overall_qps:,.1f} queries/sec")
    print(f"{'=' * 108}")


if __name__ == "__main__":
    main()