#!/usr/bin/env python3
"""Benchmark FerrisSearch terms aggregation with exact-result checks.

Runs against one already-started, task-owned FerrisSearch node. The harness
creates a new index, ingests a deterministic scalar-keyword dataset, force
merges it to one segment, and measures dense and sparse term dictionaries with
identical match-all and filtered queries.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import sys
import time

import requests

from compare_engines import (
    assert_equivalent,
    persist,
    request,
    summarize,
    validate_index_settings,
)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_cpu_list(value):
    cpus = set()
    for item in value.split(","):
        if "-" in item:
            start, end = (int(part) for part in item.split("-", 1))
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(item))
    return cpus


def process_snapshot(pid):
    status = dict(
        line.split(":", 1)
        for line in Path(f"/proc/{pid}/status").read_text().splitlines()
        if ":" in line
    )
    relative = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[1]
    cgroup = Path("/sys/fs/cgroup") / relative.lstrip("/")
    snapshot = {
        "pid": pid,
        "exe": os.readlink(f"/proc/{pid}/exe"),
        "allowed_cpus": sorted(os.sched_getaffinity(pid)),
        "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
        "cgroup": str(cgroup),
    }
    for name in ("memory.current", "memory.peak", "memory.max", "memory.swap.max"):
        path = cgroup / name
        if path.exists():
            snapshot[name.replace(".", "_")] = path.read_text().strip()
    return snapshot


def sparse_tag(event_id, unique_cardinality, hot_cardinality):
    if event_id < unique_cardinality:
        return f"sparse-unique-{event_id:05d}"
    return f"sparse-hot-{event_id % hot_cardinality:04d}"


def bulk_payload(
    start, count, dense_cardinality, sparse_cardinality, sparse_hot_cardinality
):
    lines = []
    for event_id in range(start, start + count):
        lines.append(
            json.dumps({"index": {"_id": str(event_id)}}, separators=(",", ":"))
        )
        lines.append(
            json.dumps(
                {
                    "event_id": event_id,
                    "dense_tag": f"dense-{event_id % dense_cardinality:04d}",
                    "sparse_tag": sparse_tag(
                        event_id, sparse_cardinality, sparse_hot_cardinality
                    ),
                },
                separators=(",", ":"),
            )
        )
    return ("\n".join(lines) + "\n").encode()


def expected_counts(documents, start, prefix, cardinality, width):
    counts = {f"{prefix}-{value:0{width}d}": 0 for value in range(cardinality)}
    for event_id in range(start, documents):
        counts[f"{prefix}-{event_id % cardinality:0{width}d}"] += 1
    return counts


def expected_sparse_counts(documents, start, unique_cardinality, hot_cardinality):
    counts = {
        f"sparse-hot-{value:04d}": 0 for value in range(hot_cardinality)
    }
    for event_id in range(max(start, unique_cardinality), documents):
        counts[f"sparse-hot-{event_id % hot_cardinality:04d}"] += 1
    return counts


def make_cases(args):
    filtered_start = args.documents // 4
    definitions = [
        ("dense_match_all", "dense_tag", "dense", args.dense_cardinality, 4, 0),
        (
            "dense_filtered",
            "dense_tag",
            "dense",
            args.dense_cardinality,
            4,
            filtered_start,
        ),
        (
            "sparse_match_all",
            "sparse_tag",
            "sparse",
            args.sparse_hot_cardinality,
            4,
            0,
        ),
        (
            "sparse_filtered",
            "sparse_tag",
            "sparse",
            args.sparse_hot_cardinality,
            4,
            filtered_start,
        ),
    ]
    cases = []
    for name, field, prefix, cardinality, width, start in definitions:
        query = (
            {"match_all": {}}
            if start == 0
            else {"range": {"event_id": {"gte": start}}}
        )
        cases.append(
            {
                "name": name,
                "body": {
                    "query": query,
                    "size": 0,
                    "aggs": {
                        "values": {
                            "terms": {"field": field, "size": cardinality}
                        }
                    },
                },
                "expected": {
                    "matched": args.documents - start,
                    "buckets": (
                        expected_sparse_counts(
                            args.documents,
                            start,
                            args.sparse_cardinality,
                            args.sparse_hot_cardinality,
                        )
                        if prefix == "sparse"
                        else expected_counts(
                            args.documents, start, prefix, cardinality, width
                        )
                    ),
                },
            }
        )
    return cases


def normalize_terms(body):
    if body.get("timed_out", False) or body["_shards"]["failed"] != 0:
        raise ValueError(f"Incomplete search response: {body}")
    total = body["hits"]["total"]
    if isinstance(total, dict):
        if total["relation"] != "eq":
            raise ValueError(f"Inexact total hits: {total}")
        total = total["value"]
    buckets = {}
    for bucket in body["aggregations"]["values"]["buckets"]:
        key = str(bucket["key"])
        if key in buckets:
            raise ValueError(f"Duplicate aggregation bucket: {key}")
        buckets[key] = bucket["doc_count"]
    return {"matched": total, "buckets": buckets}


def validate_bulk(body, count):
    if body["errors"] or len(body["items"]) != count:
        raise ValueError(f"Incomplete bulk response: {str(body)[:2000]}")
    for item in body["items"]:
        operation = item["index"]
        if "error" in operation or not 200 <= operation["status"] < 300:
            raise ValueError(f"Bulk item failed: {operation}")


def wait_for_force_merge(session, url, index):
    started = request(
        session, url, "POST", f"/{index}/_forcemerge?max_num_segments=1"
    )
    task_id = started["task"]["id"]
    deadline = time.monotonic() + 180
    while True:
        result = request(session, url, "GET", f"/_tasks/{task_id}")
        status = result["task"]["status"]
        if status == "completed":
            if result["_nodes"]["failed"] or any(
                node["failed_shards"] for node in result["nodes"]
            ):
                raise ValueError(f"Force merge failed: {result}")
            return result
        if status in ("failed", "unknown") or time.monotonic() >= deadline:
            raise ValueError(f"Force merge did not complete: {result}")
        time.sleep(0.2)


def run(args):
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite existing evidence: {args.output}")
    if not args.index.startswith("ferris-terms-"):
        raise ValueError("--index must start with ferris-terms-")
    if args.documents < args.sparse_cardinality + args.sparse_hot_cardinality * 4:
        raise ValueError("Need unique sparse terms plus four documents per hot term")
    if args.warmup < 1 or args.repetitions < 2:
        raise ValueError("Need at least one warmup and two measured rounds")
    if args.reuse_prepared_index and not args.prepared_data_sha256:
        raise ValueError("Reused indexes require --prepared-data-sha256")

    expected_server_cpus = parse_cpu_list(args.expected_server_cpus)
    expected_client_cpus = parse_cpu_list(args.expected_client_cpus)
    if expected_server_cpus & expected_client_cpus:
        raise ValueError("Server and client CPU sets must be disjoint")
    if set(os.sched_getaffinity(0)) != expected_client_cpus:
        raise ValueError("Client affinity does not match --expected-client-cpus")
    if set(os.sched_getaffinity(args.pid)) != expected_server_cpus:
        raise ValueError("Server affinity does not match --expected-server-cpus")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.trust_env = False
    url = args.url.rstrip("/")
    cases = make_cases(args)
    binary = args.binary.resolve()
    report = {
        "status": "failed",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv,
        "label": args.label,
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "binary": str(binary),
        "binary_sha256": sha256_file(binary),
        "prepared_data_sha256": args.prepared_data_sha256,
        "harness_sha256": sha256_file(Path(__file__)),
        "shared_oracle_sha256": sha256_file(Path(__file__).with_name("compare_engines.py")),
        "platform": platform.platform(),
        "cpu_model": next(
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "host_memory_bytes": int(
            next(
                line.split()[1]
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.startswith("MemTotal:")
            )
        )
        * 1024,
        "client_allowed_cpus": sorted(os.sched_getaffinity(0)),
        "server_before": process_snapshot(args.pid),
        "index": args.index,
        "documents": args.documents,
        "dense_cardinality": args.dense_cardinality,
        "sparse_cardinality": args.sparse_cardinality,
        "sparse_hot_cardinality": args.sparse_hot_cardinality,
        "warmup_rounds_per_case": args.warmup,
        "measured_rounds_per_case": args.repetitions,
        "conditions": {
            "dataset": "Deterministic scalar keyword values derived from event_id",
            "shards": 1,
            "replicas": 0,
            "refresh_interval_ms": 600000,
            "translog_durability": "request",
            "query_concurrency": 1,
            "query_state": (
                "Read-only warm queries after refresh, flush, and force merge"
                if args.force_merge
                else "Read-only warm queries after refresh and flush; segment count verified"
            ),
            "timing_boundary": "Client HTTP request plus JSON decoding; exact-result validation follows timing",
            "dense_threshold_under_test": 1024,
            "sparse_dictionary_terms": (
                args.sparse_cardinality + args.sparse_hot_cardinality
            ),
        },
        "cases": {
            case["name"]: {
                "request": case["body"],
                "expected_sha256": hashlib.sha256(
                    json.dumps(case["expected"], sort_keys=True).encode()
                ).hexdigest(),
                "warmup_latencies_ms": [],
                "measured_latencies_ms": [],
            }
            for case in cases
        },
    }
    if Path(report["server_before"]["exe"]).resolve() != binary:
        raise ValueError("Running server executable does not match --binary")
    if report["server_before"].get("memory_max") != str(args.expected_memory_max_bytes):
        raise ValueError("Server cgroup memory.max does not match the benchmark budget")
    if report["server_before"].get("memory_swap_max") != "0":
        raise ValueError("Server cgroup memory.swap.max must be zero")
    persist(args.output, report)

    try:
        report["root"] = request(session, url, "GET", "/")
        health = request(session, url, "GET", "/_cluster/health")
        report["health"] = health
        if not health["cluster_name"].startswith("terms-benchmark-"):
            raise ValueError(f"Refusing to use non-benchmark cluster: {health}")
        head = session.head(f"{url}/{args.index}", timeout=10)
        expected_head = 200 if args.reuse_prepared_index else 404
        if head.status_code != expected_head:
            raise ValueError(
                f"Benchmark index HEAD returned {head.status_code}; "
                f"expected {expected_head}"
            )

        creation = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval_ms": 600000,
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "event_id": {"type": "integer"},
                    "dense_tag": {"type": "keyword"},
                    "sparse_tag": {"type": "keyword"},
                },
            },
        }
        report["create_request"] = creation
        if args.reuse_prepared_index:
            report["ingestion"] = {
                "reused_prepared_index": True,
                "acknowledged_documents": args.documents,
            }
            report["force_merge"] = {
                "attempted": False,
                "reason": "prepared index reused for a reverse-order measurement",
            }
        else:
            request(session, url, "PUT", f"/{args.index}", json=creation)

            payload_digest = hashlib.sha256()
            ingestion_seconds = []
            for start in range(0, args.documents, args.batch_size):
                count = min(args.batch_size, args.documents - start)
                payload = bulk_payload(
                    start,
                    count,
                    args.dense_cardinality,
                    args.sparse_cardinality,
                    args.sparse_hot_cardinality,
                )
                payload_digest.update(payload)
                started = time.perf_counter()
                body = request(
                    session,
                    url,
                    "POST",
                    f"/{args.index}/_bulk",
                    data=payload,
                    headers={"Content-Type": "application/x-ndjson"},
                )
                ingestion_seconds.append(time.perf_counter() - started)
                validate_bulk(body, count)
            report["bulk_payload_sha256"] = payload_digest.hexdigest()
            report["ingestion"] = {
                "acknowledged_documents": args.documents,
                "batch_size": args.batch_size,
                "request_seconds": ingestion_seconds,
                "total_seconds": sum(ingestion_seconds),
                "documents_per_second": args.documents / sum(ingestion_seconds),
            }

            refresh = request(session, url, "POST", f"/{args.index}/_refresh")
            if refresh["_shards"]["failed"]:
                raise ValueError(f"Refresh failed: {refresh}")
            report["force_merge"] = (
                wait_for_force_merge(session, url, args.index)
                if args.force_merge
                else {
                    "attempted": False,
                    "reason": "not required; benchmark verifies the prepared segment count",
                }
            )
            flush = request(session, url, "POST", f"/{args.index}/_flush")
            if flush["_shards"]["failed"]:
                raise ValueError(f"Flush failed: {flush}")
        count = request(session, url, "GET", f"/{args.index}/_count")
        if count["count"] != args.documents or count["_shards"]["failed"]:
            raise ValueError(f"Document count mismatch: {count}")

        segments_response = session.get(f"{url}/_cat/segments", timeout=30)
        segments_response.raise_for_status()
        segments = [
            line.split()
            for line in segments_response.text.splitlines()
            if line.split() and line.split()[0] == args.index
        ]
        if sum(int(segment[3]) for segment in segments) != args.documents or (
            args.expected_segments > 0 and len(segments) != args.expected_segments
        ):
            raise ValueError(
                f"Unexpected prepared segment layout: {segments}"
            )
        report["prepared_segments"] = segments
        report["index_settings"] = request(
            session, url, "GET", f"/{args.index}/_settings"
        )
        validate_index_settings(report["index_settings"], args.index, "ferris")
        report["server_loaded"] = process_snapshot(args.pid)
        persist(args.output, report)

        rng = random.Random(args.seed)
        for phase, rounds in (("warmup", args.warmup), ("measured", args.repetitions)):
            for _ in range(rounds):
                order = list(cases)
                rng.shuffle(order)
                for case in order:
                    started = time.perf_counter()
                    body = request(
                        session,
                        url,
                        "POST",
                        f"/{args.index}/_search?request_cache=false",
                        json=case["body"],
                    )
                    elapsed_ms = (time.perf_counter() - started) * 1000
                    actual = normalize_terms(body)
                    assert_equivalent(actual, case["expected"], case["name"])
                    report["cases"][case["name"]][f"{phase}_latencies_ms"].append(
                        elapsed_ms
                    )
                    report["cases"][case["name"]]["last_result_sha256"] = (
                        hashlib.sha256(
                            json.dumps(actual, sort_keys=True).encode()
                        ).hexdigest()
                    )
            persist(args.output, report)

        for case in cases:
            result = report["cases"][case["name"]]
            result["summary"] = summarize(result["measured_latencies_ms"])
        report["server_after"] = process_snapshot(args.pid)
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        report["status"] = "completed"
        persist(args.output, report)
        for name, result in report["cases"].items():
            summary = result["summary"]
            print(
                f"{name}: p50={summary['p50_ms']:.3f} ms "
                f"p95={summary['p95_ms']:.3f} ms "
                f"mean={summary['mean_ms']:.3f} ms"
            )
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        report["failed_at"] = datetime.now(timezone.utc).isoformat()
        persist(args.output, report)
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--documents", type=int, default=50000)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--dense-cardinality", type=int, default=64)
    parser.add_argument("--sparse-cardinality", type=int, default=4096)
    parser.add_argument("--sparse-hot-cardinality", type=int, default=64)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--repetitions", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--expected-server-cpus", default="0-3")
    parser.add_argument("--expected-client-cpus", default="4-7")
    parser.add_argument(
        "--expected-memory-max-bytes", type=int, default=8 * 1024 * 1024 * 1024
    )
    parser.add_argument("--expected-segments", type=int, default=0)
    parser.add_argument("--force-merge", action="store_true")
    parser.add_argument("--reuse-prepared-index", action="store_true")
    parser.add_argument("--prepared-data-sha256")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
