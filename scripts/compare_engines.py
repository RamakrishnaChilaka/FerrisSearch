#!/usr/bin/env python3
"""Compare isolated FerrisSearch/OpenSearch nodes on synthetic incident events.

Requires requests. Creates a new benchmark index, never deletes an index, checks
every answer against a Python oracle, and preserves requests and latency samples.
See docs/benchmarks.md for node setup, scope, and interpretation.
"""

import argparse
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
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests


SERVICES = (
    "checkout", "payments", "catalog", "identity", "search", "inventory",
    "delivery", "billing", "notifications", "gateway", "cart", "recommendations",
)
REGIONS = ("eastus", "westus", "westeurope")
PROPERTIES = {
    "event_id": {"type": "integer"},
    "service": {"type": "keyword"},
    "region": {"type": "keyword"},
    "release": {"type": "keyword"},
    "status": {"type": "keyword"},
    "message": {"type": "text"},
    "duration_ms": {"type": "integer"},
}
FERRIS_PROFILE = {
    "FERRISSEARCH_TRANSLOG_DURABILITY": "request",
    "FERRISSEARCH_COLUMN_CACHE_SIZE_PERCENT": "3",
    "FERRISSEARCH_SQL_APPROXIMATE_TOP_K": "false",
}


def generate_documents(count, seed):
    rng = random.Random(seed)
    documents = []
    for event_id in range(count):
        service = rng.choice(SERVICES)
        region = rng.choice(REGIONS)
        release = "v2" if event_id >= count * 3 // 4 else "v1"
        incident = (
            release == "v2" and region == "eastus"
            and service in ("checkout", "payments")
        )
        failed = rng.random() < (0.45 if incident else 0.025)
        documents.append({
            "event_id": event_id,
            "service": service,
            "region": region,
            "release": release,
            "status": "error" if failed else "ok",
            "message": (
                f"request timeout while calling {service} upstream dependency"
                if failed else f"request completed successfully for {service}"
            ),
            "duration_ms": rng.randrange(500, 5000) if failed else rng.randrange(5, 500),
        })
    return documents


def make_cases(index, cohort_size=None):
    if cohort_size is not None and cohort_size <= 0:
        raise ValueError("A stable query cohort must contain documents")
    timeout = {"match": {"message": "timeout"}}
    filtered = {
        "bool": {
            "must": [timeout],
            "filter": [
                {"term": {"region": "eastus"}},
                {"range": {"duration_ms": {"gte": 1000}}},
            ],
        }
    }
    cases = [
        {"name": "find_timeouts", "kind": "hits", "selection": "timeouts",
         "query": timeout},
        {"name": "investigate_region", "kind": "hits", "selection": "region",
         "query": filtered},
        {"name": "find_slow_requests", "kind": "hits", "selection": "slow",
         "query": {"range": {"duration_ms": {"gte": 1000}}}},
        {"name": "service_counts_dsl", "kind": "terms", "selection": "all",
         "query": {"match_all": {}}},
        {"name": "timeout_duration_stats_dsl", "kind": "stats", "selection": "timeouts",
         "query": timeout},
        {"name": "service_analytics", "kind": "grouped", "selection": "all",
         "query": {"match_all": {}}, "where": ""},
        {"name": "incident_analytics", "kind": "grouped", "selection": "region",
         "query": filtered,
         "where": " WHERE text_match(message, 'timeout')"
                  " AND region = 'eastus' AND duration_ms >= 1000"},
    ]
    for case in cases:
        if cohort_size is not None:
            case["query"] = {
                "bool": {
                    "must": [case["query"]],
                    "filter": [{"range": {"event_id": {"lt": cohort_size}}}],
                }
            }
            if case["kind"] == "grouped":
                joiner = " AND " if case["where"] else " WHERE "
                case["where"] += f"{joiner}event_id < {cohort_size}"
        body = {"query": case["query"], "size": 0, "track_total_hits": True}
        if case["kind"] == "hits":
            body.update({"size": 20, "sort": [{"event_id": "desc"}]})
        elif case["kind"] == "terms":
            body["aggs"] = {"services": {"terms": {"field": "service", "size": len(SERVICES)}}}
        elif case["kind"] == "stats":
            body["aggs"] = {"duration": {"stats": {"field": "duration_ms"}}}
        else:
            body["aggs"] = {
                "services": {
                    "terms": {"field": "service", "size": len(SERVICES)},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "duration_ms"}},
                        "max_duration": {"max": {"field": "duration_ms"}},
                    },
                }
            }
        search_path = f"/{index}/_search?request_cache=false"
        case["opensearch"] = {"path": search_path, "body": body}
        if case["kind"] == "grouped":
            case["ferris"] = {
                "path": f"/{index}/_sql",
                "body": {
                    "query": (
                        "SELECT service, COUNT(*) AS events,"
                        " AVG(duration_ms) AS avg_duration,"
                        f' MAX(duration_ms) AS max_duration FROM "{index}"'
                        f'{case["where"]} GROUP BY service ORDER BY service ASC'
                    )
                },
            }
        else:
            case["ferris"] = {"path": search_path, "body": body}
    return cases


def selected(document, selection):
    if selection == "all":
        return True
    if selection == "timeouts":
        return "timeout" in document["message"].split()
    if selection == "region":
        return (
            "timeout" in document["message"].split()
            and document["region"] == "eastus" and document["duration_ms"] >= 1000
        )
    if selection == "slow":
        return document["duration_ms"] >= 1000
    raise ValueError(f"Unknown selection: {selection}")


def oracle(documents, case):
    matching = [doc for doc in documents if selected(doc, case["selection"])]
    if not matching:
        raise ValueError(f"No matching documents for {case['name']}; increase --documents")
    kind = case["kind"]
    if kind == "hits":
        rows = sorted(matching, key=lambda doc: doc["event_id"], reverse=True)[:20]
    elif kind == "stats":
        values = [doc["duration_ms"] for doc in matching]
        rows = {
            "count": len(values), "sum": sum(values), "min": min(values),
            "max": max(values), "avg": sum(values) / len(values),
        }
    else:
        groups = defaultdict(list)
        for doc in matching:
            groups[doc["service"]].append(doc["duration_ms"])
        rows = []
        for service, values in sorted(groups.items()):
            row = {"service": service, "events": len(values)}
            if kind == "grouped":
                row.update({
                    "avg_duration": sum(values) / len(values),
                    "max_duration": max(values),
                })
            rows.append(row)
    return {"matched": len(matching), "rows": rows}


def normalize(body, case, engine):
    if body.get("timed_out", False) or body["_shards"]["failed"] != 0:
        raise ValueError(f"Incomplete response: {body}")
    if case["kind"] == "grouped" and engine == "ferris":
        if body["approximate_top_k"] or body["truncated"]:
            raise ValueError(f"Non-exact SQL response: {body}")
        if body["execution_mode"] != "tantivy_grouped_partials":
            raise ValueError(f"Unexpected SQL execution mode: {body['execution_mode']}")
        return {
            "matched": body["matched_hits"],
            "rows": sorted(body["rows"], key=lambda row: row["service"]),
        }
    total = body["hits"]["total"]
    if isinstance(total, dict):
        if total["relation"] != "eq":
            raise ValueError(f"Inexact hit count: {total}")
        total = total["value"]
    if case["kind"] == "hits":
        rows = [hit["_source"] for hit in body["hits"]["hits"]]
    elif case["kind"] == "stats":
        rows = body["aggregations"]["duration"]
    else:
        rows = []
        for bucket in body["aggregations"]["services"]["buckets"]:
            row = {"service": bucket["key"], "events": bucket["doc_count"]}
            if case["kind"] == "grouped":
                row.update({
                    "avg_duration": bucket["avg_duration"]["value"],
                    "max_duration": bucket["max_duration"]["value"],
                })
            rows.append(row)
        rows.sort(key=lambda row: row["service"])
    return {"matched": total, "rows": rows}


def assert_equivalent(actual, expected, path="result"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            raise ValueError(f"{path}: different fields: {actual!r} vs {expected!r}")
        for key, value in expected.items():
            assert_equivalent(actual[key], value, f"{path}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ValueError(f"{path}: different row counts")
        for index, (left, right) in enumerate(zip(actual, expected)):
            assert_equivalent(left, right, f"{path}[{index}]")
    elif isinstance(expected, (float, int)) and not isinstance(expected, bool):
        if not isinstance(actual, (float, int)) or not math.isclose(
            actual, expected, rel_tol=1e-10, abs_tol=1e-8
        ):
            raise ValueError(f"{path}: {actual!r} != {expected!r}")
    elif actual != expected:
        raise ValueError(f"{path}: {actual!r} != {expected!r}")


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(values):
    return {
        "samples": len(values),
        "p50_ms": statistics.median(values),
        "p95_ms": percentile(values, 0.95),
        "mean_ms": statistics.mean(values),
        "min_ms": min(values),
        "max_ms": max(values),
    }


def validate_runtime_profile(affinities, client_cpus, opensearch_node, ferris_environment):
    ferris_cpus = set(affinities["ferris"])
    opensearch_cpus = set(affinities["opensearch"])
    client_cpus = set(client_cpus)
    if not ferris_cpus or ferris_cpus != opensearch_cpus:
        raise ValueError("Both engines must have the same non-empty CPU affinity")
    if not client_cpus or ferris_cpus & client_cpus:
        raise ValueError("Pin the client to non-empty CPUs disjoint from the servers")
    heap_bytes = opensearch_node["jvm"]["mem"]["heap_max_in_bytes"]
    if heap_bytes != 1073741824:
        raise ValueError("This benchmark profile requires a verified 1 GiB OpenSearch heap")
    processors = int(opensearch_node["settings"]["node"]["processors"])
    if processors != len(opensearch_cpus):
        raise ValueError("OpenSearch node.processors must match its CPU affinity")
    if ferris_environment != FERRIS_PROFILE:
        raise ValueError("FerrisSearch must use the documented durability/cache/exactness environment")
    return {
        "server_cpu_ids": sorted(ferris_cpus),
        "client_cpu_ids": sorted(client_cpus),
        "opensearch_heap_bytes": heap_bytes,
        "opensearch_node_processors": processors,
        "ferris_environment": ferris_environment,
    }


def validate_index_settings(body, index, engine, refresh_ms=600000):
    settings = body[index]["settings"]["index"]
    if int(settings["number_of_shards"]) != 1 or int(settings["number_of_replicas"]) != 0:
        raise ValueError(f"{engine} must have one primary shard and zero replicas")
    if engine == "ferris":
        if settings["engine"] != "local_shards" or settings["refresh_interval_ms"] != refresh_ms:
            raise ValueError("FerrisSearch index settings do not match the benchmark profile")
    elif (
        settings["refresh_interval"] != f"{refresh_ms // 1000}s"
        or settings["translog"]["durability"] != "request"
        or str(settings["requests"]["cache"]["enable"]).lower() != "false"
    ):
        raise ValueError("OpenSearch refresh/durability/request-cache settings do not match")


def request(session, base_url, method, path, timeout=180, **kwargs):
    response = session.request(method, base_url + path, timeout=timeout, **kwargs)
    if not response.ok:
        raise RuntimeError(f"{method} {path}: HTTP {response.status_code}: {response.text[:2000]}")
    body = response.json()
    if "error" in body:
        raise RuntimeError(f"{method} {path}: {body['error']}")
    return body


def snapshot(run_dir, engine):
    pid = int((run_dir / f"{engine}.pid").read_text().strip())
    status = dict(
        line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines()
        if ":" in line
    )
    stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    data_dir = run_dir / f"{engine}-data"
    logical_bytes = 0
    allocated_bytes = 0
    for path in data_dir.rglob("*"):
        if path.is_file():
            info = path.stat()
            logical_bytes += info.st_size
            allocated_bytes += info.st_blocks * 512
    return {
        "pid": pid,
        "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
        "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        "allowed_cpus": status["Cpus_allowed_list"].strip(),
        "data_logical_bytes": logical_bytes,
        "data_allocated_bytes": allocated_bytes,
    }


def persist(path, report):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def run(args):
    if not re.fullmatch(r"ferris-compare-[a-z0-9-]+", args.index):
        raise ValueError("--index must start with ferris-compare- and contain lowercase letters/digits/hyphens")
    if args.documents < 1000 or args.warmup < 1 or args.repetitions < 2:
        raise ValueError("Need at least 1000 documents, one warmup round, and two measured rounds")
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite existing evidence: {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    urls = {"ferris": args.ferris_url.rstrip("/"), "opensearch": args.opensearch_url.rstrip("/")}
    sessions = {engine: requests.Session() for engine in urls}
    for session in sessions.values():
        session.trust_env = False
    previous = json.loads(args.resume_from.read_text()) if args.resume_from else None
    if previous is not None:
        for key, value in (("documents", args.documents), ("seed", args.seed), ("index", args.index)):
            if previous[key] != value:
                raise ValueError(f"Cannot resume with different {key}")
    report = {
        "status": "failed",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ferris_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "platform": platform.platform(),
        "cpu_model": next(
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "host_memory_bytes": int(
            next(line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines()
                 if line.startswith("MemTotal:"))
        ) * 1024,
        "client_allowed_cpus": sorted(os.sched_getaffinity(0)),
        "documents": args.documents,
        "seed": args.seed,
        "index": args.index,
        "warmup_rounds_per_case": args.warmup,
        "measured_rounds_per_case": args.repetitions,
        "concurrency": 1,
        "conditions": {
            "dataset": "Deterministic synthetic application events; not production traffic",
            "shards": 1, "replicas": 0, "translog_durability": "request",
            "refresh_interval_seconds": 600,
            "query_state": "Read-only, warm caches, force-merged to one segment on each engine",
            "opensearch_request_cache": False,
            "opensearch_distribution": "Official minimal/core tarball, no optional plugins",
            "opensearch_heap_bytes": 1073741824,
            "ferris_column_cache_percent_of_host_ram": 3,
            "ferris_sql_approximate_top_k": False,
            "timing_boundary": "Client HTTP request plus JSON decoding, excluding oracle validation",
            "resource_scope": "Process RSS snapshots, not peak/required RAM; data dirs include WAL/metadata",
            "cost_scope": "No production cost, availability, or sustained-load claim",
        },
        "engines": {},
    }
    if previous is not None:
        if previous["ferris_commit"] != report["ferris_commit"]:
            raise ValueError("Cannot resume after the FerrisSearch source revision changed")
        report["resumed_from"] = str(args.resume_from)
        report["previous_run_status"] = previous["status"]
    try:
        for engine, url in urls.items():
            info = request(sessions[engine], url, "GET", "/")
            health = request(sessions[engine], url, "GET", "/_cluster/health")
            if not health["cluster_name"].startswith(f"{engine}-comparison-"):
                raise ValueError(f"Refusing to use a non-benchmark cluster: {health}")
            head = sessions[engine].head(f"{url}/{args.index}", timeout=10)
            prior_engine = previous["engines"].get(engine, {}) if previous is not None else {}
            resuming_ingested = "ingestion" in prior_engine
            expected_status = 200 if resuming_ingested else 404
            if head.status_code != expected_status:
                raise ValueError(f"Index must be new; HEAD returned {head.status_code} on {engine}")
            if resuming_ingested and info != prior_engine["info"]:
                raise ValueError(f"{engine} identity changed since ingestion")
            report["engines"][engine] = {
                "info": info,
                "resources_empty": (
                    prior_engine["resources_empty"] if resuming_ingested
                    else snapshot(args.run_dir, engine)
                ),
            }
        current_resources = {engine: snapshot(args.run_dir, engine) for engine in urls}
        node_info = request(
            sessions["opensearch"], urls["opensearch"], "GET",
            "/_nodes/jvm,process,settings?filter_path=nodes.*.jvm.mem.heap_max_in_bytes,"
            "nodes.*.settings.node.processors,nodes.*.process.id",
        )
        nodes = list(node_info["nodes"].values())
        if len(nodes) != 1 or nodes[0]["process"]["id"] != current_resources["opensearch"]["pid"]:
            raise ValueError("The OpenSearch PID must identify the single benchmark node")
        ferris_pid = current_resources["ferris"]["pid"]
        ferris_environment = {}
        # Retain only these non-secret benchmark controls, never the full environment.
        for entry in Path(f"/proc/{ferris_pid}/environ").read_bytes().split(b"\0"):
            key, _, value = entry.partition(b"=")
            name = key.decode()
            if name in FERRIS_PROFILE:
                ferris_environment[name] = value.decode()
        report["runtime_verification"] = validate_runtime_profile(
            {engine: os.sched_getaffinity(info["pid"]) for engine, info in current_resources.items()},
            os.sched_getaffinity(0), nodes[0], ferris_environment,
        )
        report["conditions"]["opensearch_heap_bytes"] = (
            report["runtime_verification"]["opensearch_heap_bytes"]
        )
        print(f"Generating {args.documents:,} synthetic events", flush=True)
        documents = generate_documents(args.documents, args.seed)
        cases = make_cases(args.index)
        report["cases"] = {
            case["name"]: {"requests": {engine: case[engine] for engine in urls},
                           "expected": oracle(documents, case)}
            for case in cases
        }
        batches = []
        checksum = hashlib.sha256()
        for start in range(0, len(documents), 5000):
            lines = []
            for document in documents[start:start + 5000]:
                lines.append(json.dumps({"index": {"_id": str(document["event_id"])}}, separators=(",", ":")))
                lines.append(json.dumps(document, separators=(",", ":")))
            payload = ("\n".join(lines) + "\n").encode()
            checksum.update(payload)
            batches.append((min(5000, len(documents) - start), payload))
        report["bulk_payload_sha256"] = checksum.hexdigest()
        report["bulk_payload_bytes"] = sum(len(payload) for _, payload in batches)
        if previous is not None and previous["bulk_payload_sha256"] != checksum.hexdigest():
            raise ValueError("Cannot resume with a different generated dataset")
        persist(args.output, report)
        for engine, url in urls.items():
            settings = {"number_of_shards": 1, "number_of_replicas": 0}
            if engine == "ferris":
                settings["refresh_interval_ms"] = 600000
            else:
                settings.update({
                    "refresh_interval": "600s", "translog.durability": "request",
                    "requests.cache.enable": False,
                })
            creation = {"settings": settings, "mappings": {"dynamic": "strict", "properties": PROPERTIES}}
            report["engines"][engine]["create_request"] = creation
            prior_engine = previous["engines"].get(engine, {}) if previous is not None else {}
            resuming_ingested = "ingestion" in prior_engine
            if resuming_ingested:
                if creation != prior_engine["create_request"]:
                    raise ValueError(f"Cannot resume {engine} with different index settings")
                report["engines"][engine]["ingestion"] = prior_engine["ingestion"]
                print(f"Resuming acknowledged {engine} ingestion from prior evidence", flush=True)
            else:
                request(sessions[engine], url, "PUT", f"/{args.index}", json=creation)
            samples = []
            if not resuming_ingested:
                print(f"Ingesting {engine}", flush=True)
            for batch_size, payload in ([] if resuming_ingested else batches):
                started = time.perf_counter()
                body = request(
                    sessions[engine], url, "POST", f"/{args.index}/_bulk",
                    data=payload, headers={"Content-Type": "application/x-ndjson"},
                )
                elapsed = time.perf_counter() - started
                if body["errors"] or len(body["items"]) != batch_size:
                    raise ValueError(f"{engine} bulk failed: {body}")
                for item in body["items"]:
                    operation = item["index"]
                    if not 200 <= operation["status"] < 300 or "error" in operation:
                        raise ValueError(f"{engine} bulk item failed: {operation}")
                samples.append(elapsed)
            if not resuming_ingested:
                report["engines"][engine]["ingestion"] = {
                    "acknowledged_documents": args.documents,
                    "bulk_request_seconds": sum(samples),
                    "documents_per_second": args.documents / sum(samples),
                    "batch_seconds": samples,
                    "scope": "Single ingestion pass; excludes dataset generation, refresh, merge, and flush",
                }
            request(sessions[engine], url, "POST", f"/{args.index}/_refresh")
            time.sleep(2)
            merged = request(
                sessions[engine], url, "POST", f"/{args.index}/_forcemerge?max_num_segments=1"
            )
            if engine == "ferris":
                task_id = merged["task"]["id"]
                deadline = time.monotonic() + 180
                while True:
                    task_response = request(sessions[engine], url, "GET", f"/_tasks/{task_id}")
                    task = task_response["task"]
                    if task["status"] == "completed":
                        if task_response["_nodes"]["failed"] or any(
                            node["failed_shards"] for node in task_response["nodes"]
                        ):
                            raise ValueError(f"Force merge failed: {task_response}")
                        break
                    if task["status"] in ("failed", "unknown") or time.monotonic() >= deadline:
                        raise ValueError(f"Force merge did not complete: {task}")
                    time.sleep(0.2)
            elif merged["_shards"]["failed"]:
                raise ValueError(f"Force merge failed: {merged}")
            flushed = request(sessions[engine], url, "POST", f"/{args.index}/_flush")
            if flushed["_shards"]["failed"]:
                raise ValueError(f"Flush failed: {flushed}")
            count = request(sessions[engine], url, "GET", f"/{args.index}/_count")
            if count["count"] != args.documents or count["_shards"]["failed"]:
                raise ValueError(f"{engine} document count mismatch: {count}")
            if engine == "ferris":
                response = sessions[engine].get(f"{url}/_cat/segments", timeout=30)
                response.raise_for_status()
                segments = [
                    line.split() for line in response.text.splitlines()
                    if line.split() and line.split()[0] == args.index
                ]
                if len(segments) != 1 or int(segments[0][3]) != args.documents:
                    raise ValueError(f"Expected one complete FerrisSearch segment: {segments}")
            else:
                segments = request(sessions[engine], url, "GET", f"/{args.index}/_segments")
                copies = segments["indices"][args.index]["shards"]["0"]
                if len(copies) != 1 or copies[0]["num_search_segments"] != 1:
                    raise ValueError(f"Expected one OpenSearch shard/segment: {segments}")
            report["engines"][engine]["prepared_segments"] = segments
            report["engines"][engine]["resources_loaded"] = snapshot(args.run_dir, engine)
            report["engines"][engine]["index_settings"] = request(
                sessions[engine], url, "GET", f"/{args.index}/_settings"
            )
            validate_index_settings(report["engines"][engine]["index_settings"], args.index, engine)
            persist(args.output, report)
            rate = report["engines"][engine]["ingestion"]["documents_per_second"]
            print(f"{engine}: {rate:,.0f} docs/s, count verified", flush=True)
        rng = random.Random(args.seed + 1)
        for phase, rounds in (("warmup", args.warmup), ("measured", args.repetitions)):
            print(f"{phase}: {rounds} rounds per case and engine", flush=True)
            before = {engine: snapshot(args.run_dir, engine) for engine in urls}
            for round_number in range(rounds):
                order = list(cases)
                rng.shuffle(order)
                for case in order:
                    engines = list(urls)
                    rng.shuffle(engines)
                    for engine in engines:
                        specification = case[engine]
                        started = time.perf_counter()
                        body = request(
                            sessions[engine], urls[engine], "POST",
                            specification["path"], json=specification["body"],
                        )
                        milliseconds = (time.perf_counter() - started) * 1000
                        result = normalize(body, case, engine)
                        expected = report["cases"][case["name"]]["expected"]
                        assert_equivalent(result, expected, f"{engine}.{case['name']}")
                        entry = report["cases"][case["name"]].setdefault(engine, {})
                        entry["last_result"] = result
                        entry.setdefault(f"{phase}_latencies_ms", []).append(milliseconds)
                if (round_number + 1) % 10 == 0:
                    print(f"{phase}: {round_number + 1}/{rounds}", flush=True)
                    persist(args.output, report)
            for engine in urls:
                after = snapshot(args.run_dir, engine)
                report["engines"][engine][f"resources_after_{phase}"] = after
                report["engines"][engine][f"{phase}_cpu_seconds"] = (
                    after["cpu_seconds"] - before[engine]["cpu_seconds"]
                )
        for name, result in report["cases"].items():
            for engine in urls:
                result[engine]["summary"] = summarize(result[engine]["measured_latencies_ms"])
            ferris = result["ferris"]["summary"]
            opensearch = result["opensearch"]["summary"]
            result["opensearch_over_ferris_p50"] = opensearch["p50_ms"] / ferris["p50_ms"]
            print(
                f"{name}: Ferris p50/p95={ferris['p50_ms']:.2f}/{ferris['p95_ms']:.2f} ms; "
                f"OpenSearch={opensearch['p50_ms']:.2f}/{opensearch['p95_ms']:.2f} ms",
                flush=True,
            )
        report["status"] = "completed"
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
    finally:
        persist(args.output, report)
        for session in sessions.values():
            session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ferris-url", default="http://127.0.0.1:29200")
    parser.add_argument("--opensearch-url", default="http://127.0.0.1:29201")
    parser.add_argument("--documents", type=int, default=250000)
    parser.add_argument("--seed", type=int, default=20260921)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--repetitions", type=int, default=50)
    parser.add_argument("--index", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume-from", type=Path, help="Resume fully acknowledged ingestion from a prior report")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
