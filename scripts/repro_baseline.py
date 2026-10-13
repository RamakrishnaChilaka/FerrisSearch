#!/usr/bin/env python3
"""Run reproducible local_shards and remote_store baselines.

The harness builds and starts an isolated FerrisSearch cluster, generates a
deterministic dataset, checks every query against a Python oracle, and writes
raw JSON plus a generated Markdown summary. Linux is required for procfs and
allocated-disk accounting.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import statistics
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit, urlunsplit

import requests


SERVICES = ("catalog", "checkout", "payments", "search")
REGIONS = ("eastus", "westus", "westeurope")
PROPERTIES = {
    "event_id": {"type": "integer"},
    "service": {"type": "keyword"},
    "region": {"type": "keyword"},
    "message": {"type": "text"},
    "duration_ms": {"type": "integer"},
}
OBJECT_METRICS = {
    "ferrissearch_object_store_requests_total",
    "ferrissearch_object_store_transfer_bytes_total",
}
PROCESS_METRICS = {
    "process_cpu_seconds_total",
    "process_resident_memory_bytes",
    "process_virtual_memory_bytes",
    "process_open_fds",
    "process_threads",
}
_THREAD_LOCAL = threading.local()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize_latencies(values):
    return {
        "samples": len(values),
        "min_ms": min(values),
        "p50_ms": statistics.median(values),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "mean_ms": statistics.mean(values),
        "max_ms": max(values),
    }


def generate_documents(count, seed):
    rng = random.Random(seed)
    documents = []
    for event_id in range(count):
        service = SERVICES[event_id % len(SERVICES)]
        region = REGIONS[(event_id // len(SERVICES)) % len(REGIONS)]
        duration_ms = 25 + rng.randrange(1975)
        documents.append(
            {
                "_id": f"event-{event_id:08d}",
                "event_id": event_id,
                "service": service,
                "region": region,
                "message": (
                    f"{service} timeout in {region}"
                    if event_id % 11 == 0
                    else f"{service} request completed in {region}"
                ),
                "duration_ms": duration_ms,
            }
        )
    return documents


def dataset_sha256(documents):
    digest = hashlib.sha256()
    for document in documents:
        digest.update(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def make_cases(documents):
    definitions = [
        ("match_all", {"match_all": {}}, None),
        ("service_checkout", {"term": {"service": "checkout"}}, None),
        ("duration_gte_1000", {"range": {"duration_ms": {"gte": 1000}}}, None),
        (
            "service_terms",
            {"match_all": {}},
            {"services": {"terms": {"field": "service", "size": len(SERVICES)}}},
        ),
        (
            "duration_stats",
            {"match_all": {}},
            {"duration": {"stats": {"field": "duration_ms"}}},
        ),
    ]
    cases = []
    for name, query, aggregations in definitions:
        if name == "match_all" or name in ("service_terms", "duration_stats"):
            selected = documents
        elif name == "service_checkout":
            selected = [doc for doc in documents if doc["service"] == "checkout"]
        else:
            selected = [doc for doc in documents if doc["duration_ms"] >= 1000]
        expected = {"matched": len(selected)}
        if name == "service_terms":
            expected["buckets"] = {
                service: sum(doc["service"] == service for doc in selected)
                for service in SERVICES
            }
        elif name == "duration_stats":
            values = [doc["duration_ms"] for doc in selected]
            expected["stats"] = {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "sum": sum(values),
                "avg": sum(values) / len(values),
            }
        body = {"query": query, "size": 0, "track_total_hits": True}
        if aggregations:
            body["aggs"] = aggregations
        cases.append({"name": name, "request": body, "expected": expected})
    return cases


def assert_equivalent(actual, expected, path="result"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            raise ValueError(f"{path}: fields differ: {actual!r} != {expected!r}")
        for key, value in expected.items():
            assert_equivalent(actual[key], value, f"{path}.{key}")
    elif isinstance(expected, (int, float)) and not isinstance(expected, bool):
        if not isinstance(actual, (int, float)) or not math.isclose(
            actual, expected, rel_tol=1e-10, abs_tol=1e-8
        ):
            raise ValueError(f"{path}: {actual!r} != {expected!r}")
    elif actual != expected:
        raise ValueError(f"{path}: {actual!r} != {expected!r}")


def normalize_search(body, case):
    if body.get("timed_out", False) or body["_shards"]["failed"] != 0:
        raise ValueError(f"Incomplete search response: {body}")
    total = body["hits"]["total"]
    if isinstance(total, dict):
        if total["relation"] != "eq":
            raise ValueError(f"Inexact total hits: {total}")
        total = total["value"]
    result = {"matched": total}
    if case["name"] == "service_terms":
        result["buckets"] = {
            str(bucket["key"]): bucket["doc_count"]
            for bucket in body["aggregations"]["services"]["buckets"]
        }
    elif case["name"] == "duration_stats":
        stats = body["aggregations"]["duration"]
        result["stats"] = {
            key: stats[key] for key in ("count", "min", "max", "sum", "avg")
        }
    return result


def session():
    current = getattr(_THREAD_LOCAL, "session", None)
    if current is None:
        current = requests.Session()
        current.trust_env = False
        _THREAD_LOCAL.session = current
    return current


def request_json(base_url, method, path, timeout=180, **kwargs):
    response = session().request(
        method, base_url.rstrip("/") + path, timeout=timeout, **kwargs
    )
    if not response.ok:
        raise RuntimeError(
            f"{method} {path}: HTTP {response.status_code}: {response.text[:2000]}"
        )
    body = response.json()
    if "error" in body:
        raise RuntimeError(f"{method} {path}: {body['error']}")
    return body


def parse_labels(text):
    labels = {}
    for match in re.finditer(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"', text):
        labels[match.group(1)] = bytes(match.group(2), "utf-8").decode("unicode_escape")
    return labels


def parse_metrics(text, wanted):
    metrics = {}
    pattern = re.compile(
        r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+"
        r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$"
    )
    for line in text.splitlines():
        match = pattern.match(line)
        if not match or match.group(1) not in wanted:
            continue
        labels = tuple(sorted(parse_labels(match.group(2) or "").items()))
        metrics[(match.group(1), labels)] = float(match.group(3))
    return metrics


def serialize_metrics(metrics):
    result = {}
    for (name, labels), value in sorted(metrics.items()):
        suffix = ",".join(f"{key}={item}" for key, item in labels)
        result[f"{name}{{{suffix}}}" if suffix else name] = value
    return result


def metric_delta(before, after):
    keys = before.keys() | after.keys()
    return {
        key: after.get(key, 0.0) - before.get(key, 0.0)
        for key in keys
        if after.get(key, 0.0) - before.get(key, 0.0) != 0
    }


def directory_size(path):
    logical = 0
    allocated = 0
    if not path.exists():
        return {"logical_bytes": 0, "allocated_bytes": 0}
    for entry in path.rglob("*"):
        if entry.is_file():
            info = entry.stat()
            logical += info.st_size
            allocated += info.st_blocks * 512
    return {"logical_bytes": logical, "allocated_bytes": allocated}


def host_metadata():
    cpu_model = "unknown"
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    memory_bytes = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                memory_bytes = int(line.split()[1]) * 1024
                break
    return {
        "platform": platform.platform(),
        "os": platform.freedesktop_os_release(),
        "cpu_model": cpu_model,
        "logical_cpus": os.cpu_count(),
        "host_memory_bytes": memory_bytes,
        "client_cpu_affinity": sorted(os.sched_getaffinity(0)),
    }


class Cluster:
    def __init__(self, root, binary, config, storage_uri):
        self.root = root
        self.binary = binary
        self.nodes = config["topology"]["nodes"]
        self.http_base = config["topology"]["http_port_base"]
        self.transport_base = config["topology"]["transport_port_base"]
        self.cluster_name = f"repro-baseline-{int(time.time())}"
        self.storage_uri = storage_uri
        self.processes = []

    @property
    def urls(self):
        return [f"http://127.0.0.1:{self.http_base + index}" for index in range(self.nodes)]

    def start(self):
        seeds = ",".join(
            f"127.0.0.1:{self.transport_base + index}"
            for index in range(self.nodes)
        )
        logs = self.root / "logs"
        logs.mkdir(parents=True)
        for index in range(self.nodes):
            node = index + 1
            data_dir = self.root / "data" / f"node-{node}"
            data_dir.mkdir(parents=True)
            environment = os.environ.copy()
            environment.update(
                {
                    "FERRISSEARCH_NODE_NAME": f"node-{node}",
                    "FERRISSEARCH_CLUSTER_NAME": self.cluster_name,
                    "FERRISSEARCH_HTTP_PORT": str(self.http_base + index),
                    "FERRISSEARCH_TRANSPORT_PORT": str(self.transport_base + index),
                    "FERRISSEARCH_DATA_DIR": str(data_dir),
                    "FERRISSEARCH_RAFT_NODE_ID": str(node),
                    "FERRISSEARCH_SEED_HOSTS": seeds,
                    "FERRISSEARCH_STORAGE_URI": self.storage_uri,
                    "FERRISSEARCH_TRANSLOG_DURABILITY": "request",
                    "FERRISSEARCH_SQL_APPROXIMATE_TOP_K": "false",
                }
            )
            log = (logs / f"node-{node}.log").open("wb")
            process = subprocess.Popen(
                [str(self.binary)], stdout=log, stderr=subprocess.STDOUT, env=environment
            )
            self.processes.append((process, log))
            time.sleep(3 if node == 1 else 1)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                health = request_json(self.urls[0], "GET", "/_cluster/health", timeout=5)
                if (
                    health["cluster_name"] == self.cluster_name
                    and health["number_of_nodes"] == self.nodes
                    and health["status"] == "green"
                ):
                    return health
            except (requests.RequestException, RuntimeError, KeyError):
                pass
            if any(process.poll() is not None for process, _ in self.processes):
                break
            time.sleep(1)
        raise RuntimeError(f"Cluster failed to become healthy; logs are under {logs}")

    def stop(self):
        for process, _ in self.processes:
            if process.poll() is None:
                process.terminate()
        for process, log in self.processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            log.close()

    def metric_snapshot(self, wanted):
        snapshots = {}
        for node, url in enumerate(self.urls, start=1):
            response = session().get(url + "/_metrics", timeout=30)
            response.raise_for_status()
            snapshots[f"node-{node}"] = parse_metrics(response.text, wanted)
        return snapshots

    def resource_snapshot(self):
        metrics = self.metric_snapshot(PROCESS_METRICS)
        return {
            node: {
                "metrics": serialize_metrics(values),
                "data_dir": directory_size(
                    self.root / "data" / node
                ),
            }
            for node, values in metrics.items()
        }


def validate_config(config):
    required = ("documents", "seed", "bulk_batch_size", "publish_batch_size")
    for name in required:
        if not isinstance(config.get("dataset", {}).get(name), int):
            raise ValueError(f"dataset.{name} must be an integer")
    execution = config.get("execution", {})
    for name in ("warmup_rounds", "measured_rounds", "concurrency"):
        if not isinstance(execution.get(name), int) or execution[name] < 1:
            raise ValueError(f"execution.{name} must be a positive integer")
    topology = config.get("topology", {})
    if topology.get("nodes", 0) < 1:
        raise ValueError("topology.nodes must be positive")
    if config["dataset"]["documents"] < 100:
        raise ValueError("dataset.documents must be at least 100")
    profiles = config.get("profiles")
    if profiles != ["local_shards", "remote_store"]:
        raise ValueError("profiles must be ['local_shards', 'remote_store']")


def ingest_local(url, index, documents, batch_size):
    samples = []
    payload_bytes = 0
    for start in range(0, len(documents), batch_size):
        lines = []
        batch = documents[start : start + batch_size]
        for document in batch:
            source = {key: value for key, value in document.items() if key != "_id"}
            lines.append(json.dumps({"index": {"_id": document["_id"]}}, separators=(",", ":")))
            lines.append(json.dumps(source, separators=(",", ":")))
        payload = ("\n".join(lines) + "\n").encode()
        payload_bytes += len(payload)
        started = time.perf_counter()
        body = request_json(
            url,
            "POST",
            f"/{index}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
        )
        samples.append(time.perf_counter() - started)
        if body["errors"] or len(body["items"]) != len(batch):
            raise ValueError(f"Incomplete local_shards bulk response: {body}")
        for item in body["items"]:
            operation = item["index"]
            if "error" in operation or not 200 <= operation["status"] < 300:
                raise ValueError(f"Failed local_shards bulk item: {operation}")
    refresh = request_json(url, "POST", f"/{index}/_refresh")
    if refresh["_shards"]["failed"]:
        raise ValueError(f"Refresh failed: {refresh}")
    return {
        "requests": len(samples),
        "payload_bytes": payload_bytes,
        "seconds": sum(samples),
        "documents_per_second": len(documents) / sum(samples),
        "request_seconds": samples,
    }


def ingest_remote(url, index, documents, batch_size):
    samples = []
    payload_bytes = 0
    generations = []
    for start in range(0, len(documents), batch_size):
        batch = documents[start : start + batch_size]
        payload = json.dumps({"docs": batch}, separators=(",", ":")).encode()
        payload_bytes += len(payload)
        started = time.perf_counter()
        body = request_json(
            url,
            "POST",
            f"/{index}/_remote_store/publish",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        samples.append(time.perf_counter() - started)
        if body["doc_count"] != len(batch):
            raise ValueError(f"Incomplete remote_store publication: {body}")
        generations.append(
            {"generation": body["generation"], "split_id": body["split_id"]}
        )
    return {
        "requests": len(samples),
        "payload_bytes": payload_bytes,
        "seconds": sum(samples),
        "documents_per_second": len(documents) / sum(samples),
        "request_seconds": samples,
        "published_generations": generations,
    }


def run_case(url, index, case):
    started = time.perf_counter()
    body = request_json(
        url,
        "POST",
        f"/{index}/_search?request_cache=false",
        json=case["request"],
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    actual = normalize_search(body, case)
    assert_equivalent(actual, case["expected"], case["name"])
    return elapsed_ms, actual


def run_rounds(url, index, cases, rounds, concurrency, seed):
    jobs = [case for _ in range(rounds) for case in cases]
    random.Random(seed).shuffle(jobs)
    samples = {case["name"]: [] for case in cases}
    started = time.perf_counter()
    if concurrency == 1:
        results = [(case, run_case(url, index, case)) for case in jobs]
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [(case, executor.submit(run_case, url, index, case)) for case in jobs]
            results = [(case, future.result()) for case, future in futures]
    wall_seconds = time.perf_counter() - started
    for case, (elapsed_ms, _) in results:
        samples[case["name"]].append(elapsed_ms)
    return samples, wall_seconds


def aggregate_metric_deltas(before, after):
    aggregate = {}
    per_node = {}
    for node in sorted(before):
        delta = metric_delta(before[node], after[node])
        per_node[node] = serialize_metrics(delta)
        for key, value in delta.items():
            aggregate[key] = aggregate.get(key, 0.0) + value
    return {"aggregate": serialize_metrics(aggregate), "per_node": per_node}


def profile_run(cluster, config, documents, cases, profile):
    execution = config["execution"]
    dataset = config["dataset"]
    index = f"fs008-{profile.replace('_', '-')}-{int(time.time())}"
    create = {
        "engine": profile,
        "settings": {
            "number_of_shards": config["topology"]["local_shards"]
            if profile == "local_shards"
            else 1,
            "number_of_replicas": config["topology"]["replicas"]
            if profile == "local_shards"
            else 0,
            "refresh_interval_ms": 600000,
        },
        "mappings": {"dynamic": "strict", "properties": PROPERTIES},
    }
    url = cluster.urls[0]
    request_json(url, "PUT", f"/{index}", json=create)
    deadline = time.monotonic() + 60
    while True:
        health = request_json(url, "GET", "/_cluster/health")
        if health["status"] == "green":
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Index {index} did not reach green health: {health}")
        time.sleep(0.5)
    metrics_before_ingest = cluster.metric_snapshot(OBJECT_METRICS)
    resources_before = cluster.resource_snapshot()
    if profile == "local_shards":
        ingestion = ingest_local(
            url, index, documents, dataset["bulk_batch_size"]
        )
    else:
        ingestion = ingest_remote(
            url, index, documents, dataset["publish_batch_size"]
        )
    count = request_json(url, "GET", f"/{index}/_count")
    if count["count"] != len(documents) or count["_shards"]["failed"]:
        raise ValueError(f"{profile} count mismatch: {count}")
    metrics_after_ingest = cluster.metric_snapshot(OBJECT_METRICS)
    resources_loaded = cluster.resource_snapshot()

    cold_samples = {}
    for case in cases:
        elapsed, actual = run_case(url, index, case)
        cold_samples[case["name"]] = {
            "latency_ms": elapsed,
            "result_sha256": hashlib.sha256(
                json.dumps(actual, sort_keys=True).encode()
            ).hexdigest(),
        }
    run_rounds(
        url,
        index,
        cases,
        execution["warmup_rounds"],
        execution["concurrency"],
        dataset["seed"] + (1 if profile == "local_shards" else 2),
    )
    metrics_before_measured = cluster.metric_snapshot(
        OBJECT_METRICS | PROCESS_METRICS
    )
    measured, wall_seconds = run_rounds(
        url,
        index,
        cases,
        execution["measured_rounds"],
        execution["concurrency"],
        dataset["seed"] + (3 if profile == "local_shards" else 4),
    )
    metrics_after_measured = cluster.metric_snapshot(
        OBJECT_METRICS | PROCESS_METRICS
    )
    metrics_after_profile = cluster.metric_snapshot(OBJECT_METRICS)
    resources_after = cluster.resource_snapshot()
    requests_count = sum(len(values) for values in measured.values())
    return {
        "index": index,
        "create_request": create,
        "ingestion": ingestion,
        "count_check": count,
        "cache_state": {
            "cold_preparation": (
                "First query per case on a fresh process-local reader/cache state; "
                "the local ingest path may already populate OS page cache"
            ),
            "measured": "Warm after one cold preparation query and configured warmup rounds",
        },
        "cold_preparation": cold_samples,
        "measured": {
            "wall_seconds": wall_seconds,
            "requests": requests_count,
            "requests_per_second": requests_count / wall_seconds,
            "cases": {
                name: {
                    "latencies_ms": values,
                    "summary": summarize_latencies(values),
                }
                for name, values in measured.items()
            },
        },
        "resources": {
            "before_ingest": resources_before,
            "loaded": resources_loaded,
            "after_measured": resources_after,
            "object_store_ingest_delta": aggregate_metric_deltas(
                metrics_before_ingest, metrics_after_ingest
            ),
            "object_store_profile_delta": aggregate_metric_deltas(
                metrics_before_ingest, metrics_after_profile
            ),
            "measured_metric_delta": aggregate_metric_deltas(
                metrics_before_measured, metrics_after_measured
            ),
        },
    }


def markdown_report(report):
    lines = [
        "# FerrisSearch Reproducible Baseline",
        "",
        f"- **Status:** `{report['status']}`",
        f"- **Commit:** `{report['source']['commit']}`",
        f"- **Generated:** {report.get('completed_at', report['started_at'])}",
        f"- **Build:** `{report['build']['profile']}`; features: "
        f"`{', '.join(report['build']['features']) or 'default'}`",
        f"- **Topology:** {report['topology']['nodes']} nodes, "
        f"{report['topology']['local_shards']} local shards, "
        f"{report['topology']['replicas']} replica(s)",
        f"- **Dataset:** {report['dataset']['documents']:,} deterministic documents; "
        f"SHA-256 `{report['dataset']['sha256']}`",
        f"- **Execution:** {report['execution']['warmup_rounds']} warmup rounds, "
        f"{report['execution']['measured_rounds']} measured rounds, "
        f"concurrency {report['execution']['concurrency']}",
        f"- **Object store:** `{report['object_store']['uri_redacted']}`",
        "",
        "Every measured response passed the checked-in Python correctness oracle.",
        "",
        "## Results",
        "",
        "| Profile | Ingest docs/s | Query req/s | Case | p50 ms | p95 ms | p99 ms |",
        "|---|---:|---:|---|---:|---:|---:|",
    ]
    for profile, result in report.get("profiles", {}).items():
        for case, case_result in result["measured"]["cases"].items():
            summary = case_result["summary"]
            lines.append(
                f"| `{profile}` | {result['ingestion']['documents_per_second']:.1f} | "
                f"{result['measured']['requests_per_second']:.1f} | `{case}` | "
                f"{summary['p50_ms']:.3f} | {summary['p95_ms']:.3f} | "
                f"{summary['p99_ms']:.3f} |"
            )
    lines.extend(
        [
            "",
            "## Resource Summary",
            "",
            "| Profile | Measured CPU seconds | RSS after bytes | Allocated node-disk bytes | Object GET/PUT requests | Object upload/download bytes |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for profile, result in report.get("profiles", {}).items():
        measured = result["resources"]["measured_metric_delta"]["aggregate"]
        after = result["resources"]["after_measured"]
        objects = result["resources"]["object_store_profile_delta"]["aggregate"]
        cpu_seconds = measured.get("process_cpu_seconds_total", 0)
        rss_bytes = sum(
            node["metrics"].get("process_resident_memory_bytes", 0)
            for node in after.values()
        )
        disk_bytes = sum(
            node["data_dir"]["allocated_bytes"] for node in after.values()
        )
        object_requests = sum(
            value
            for key, value in objects.items()
            if key.startswith("ferrissearch_object_store_requests_total{")
        )
        object_bytes = sum(
            value
            for key, value in objects.items()
            if key.startswith("ferrissearch_object_store_transfer_bytes_total{")
        )
        lines.append(
            f"| `{profile}` | {cpu_seconds:.3f} | {rss_bytes:.0f} | "
            f"{disk_bytes:.0f} | {object_requests:.0f} | {object_bytes:.0f} |"
        )
    object_disk = report.get("object_store", {}).get("disk")
    if object_disk is not None:
        lines.extend(
            [
                "",
                f"Shared object-store disk after the matrix: "
                f"{object_disk['logical_bytes']:.0f} logical bytes, "
                f"{object_disk['allocated_bytes']:.0f} allocated bytes.",
            ]
        )
    lines.extend(
        [
            "",
            "## Scope",
            "",
            "- Latency is client wall time for HTTP plus JSON decoding; oracle checking follows timing.",
            "- CPU, RSS, virtual memory, file descriptors, and threads come from each node's `/_metrics` endpoint.",
            "- Disk snapshots report logical and allocated bytes for each node data directory and the shared local object-store directory when applicable.",
            "- Object-store request and transfer values are FerrisSearch counter deltas. They are logical successful payload bytes, not provider billing bytes.",
            "- Results describe this exact run only; they are not production capacity or cross-engine equivalence claims.",
            "",
            "The adjacent JSON file contains requests, expected results, every latency sample, resource snapshots, and per-node metric deltas.",
            "",
        ]
    )
    return "\n".join(lines)


def redact_storage_uri(uri):
    if not uri.startswith("s3://"):
        return uri
    parsed = urlsplit(uri)
    host = parsed.hostname or "<redacted>"
    netloc = f"{host}:{parsed.port}" if parsed.port is not None else host
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


def sanitized_argv(argv):
    sanitized = []
    redact_next = False
    for argument in argv:
        if redact_next:
            sanitized.append(redact_storage_uri(argument))
            redact_next = False
        elif argument == "--storage-uri":
            sanitized.append(argument)
            redact_next = True
        elif argument.startswith("--storage-uri="):
            _, value = argument.split("=", 1)
            sanitized.append(f"--storage-uri={redact_storage_uri(value)}")
        else:
            sanitized.append(argument)
    return sanitized


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("scripts/benchmark_profiles/fs008.json")
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/release/ferrissearch"))
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--storage-uri",
        help="Shared file:// or preconfigured s3:// object-store URI; defaults inside output dir",
    )
    args = parser.parse_args()
    if platform.system() != "Linux":
        raise RuntimeError("The reproducible baseline harness currently requires Linux")
    repository = Path(__file__).resolve().parent.parent
    config_path = args.config.resolve()
    output_dir = args.output_dir.resolve()
    binary = args.binary.resolve()
    os.chdir(repository)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    source_dirty = bool(
        subprocess.check_output(["git", "status", "--short"], text=True).strip()
    )
    config = json.loads(config_path.read_text())
    validate_config(config)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Refusing to reuse non-empty output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "results.json"
    summary_path = output_dir / "summary.md"
    if not args.no_build:
        environment = os.environ.copy()
        environment["RUSTFLAGS"] = config["build"]["rustflags"]
        feature_args = (
            ["--features", ",".join(config["build"]["features"])]
            if config["build"]["features"]
            else []
        )
        subprocess.run(
            [
                "cargo",
                "build",
                "--release",
                *feature_args,
                *config["build"]["cargo_args"],
            ],
            check=True,
            env=environment,
        )
    if not binary.is_file():
        raise FileNotFoundError(f"FerrisSearch binary not found: {binary}")
    storage_uri = args.storage_uri
    object_dir = output_dir / "object-store"
    if storage_uri is None:
        object_dir.mkdir()
        storage_uri = object_dir.as_uri()
    run_root = output_dir / "cluster"
    cluster = Cluster(run_root, binary, config, storage_uri)
    documents = generate_documents(
        config["dataset"]["documents"], config["dataset"]["seed"]
    )
    cases = make_cases(documents)
    report = {
        "schema_version": 1,
        "status": "failed",
        "started_at": utc_now(),
        "argv": sanitized_argv(sys.argv),
        "source": {
            "commit": commit,
            "dirty": source_dirty,
            "harness_sha256": sha256_file(Path(__file__)),
            "config_sha256": sha256_file(config_path),
        },
        "build": {
            "profile": "release",
            "rustflags": config["build"]["rustflags"],
            "features": config["build"]["features"],
            "cargo_args": config["build"]["cargo_args"],
            "binary": str(binary),
            "binary_sha256": sha256_file(binary),
        },
        "host": host_metadata(),
        "topology": config["topology"],
        "dataset": {
            **config["dataset"],
            "description": "Deterministic synthetic service events generated by scripts/repro_baseline.py",
            "schema": PROPERTIES,
            "sha256": dataset_sha256(documents),
            "ingest_methods": {
                "local_shards": "OpenSearch-style NDJSON bulk API",
                "remote_store": "Dedicated immutable split publication API",
            },
        },
        "execution": {
            **config["execution"],
            "timing_boundary": "Client HTTP request plus JSON decoding; exact-result validation follows timing",
        },
        "object_store": {
            "uri_redacted": redact_storage_uri(storage_uri),
            "backend": "s3" if storage_uri.startswith("s3://") else "local_file",
            "external_s3_compatible_check": {
                "status": "pending" if storage_uri.startswith("s3://") else "skipped",
                "reason": (
                    "Harness is configured with an external s3:// backend"
                    if storage_uri.startswith("s3://")
                    else "Default reproducible run uses the shared local file backend"
                ),
            },
        },
        "correctness_oracle": {
            "kind": "Deterministic Python exact-result oracle",
            "cases": {
                case["name"]: {
                    "request": case["request"],
                    "expected": case["expected"],
                }
                for case in cases
            },
        },
        "profiles": {},
    }
    raw_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    try:
        report["cluster_health"] = cluster.start()
        for profile in config["profiles"]:
            print(f"Running {profile} baseline", flush=True)
            report["profiles"][profile] = profile_run(
                cluster, config, documents, cases, profile
            )
            raw_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        report["object_store"]["disk"] = (
            directory_size(object_dir) if object_dir.exists() else None
        )
        if storage_uri.startswith("s3://"):
            report["object_store"]["external_s3_compatible_check"] = {
                "status": "pass",
                "reason": "Both baseline profiles completed against the configured s3:// backend",
            }
        report["status"] = "completed"
        report["completed_at"] = utc_now()
    except Exception as error:
        if storage_uri.startswith("s3://"):
            report["object_store"]["external_s3_compatible_check"] = {
                "status": "failed",
                "reason": f"{type(error).__name__}: {error}",
            }
        report["error"] = f"{type(error).__name__}: {error}"
        report["failed_at"] = utc_now()
        raise
    finally:
        cluster.stop()
        raw_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        summary_path.write_text(markdown_report(report))


if __name__ == "__main__":
    main()
