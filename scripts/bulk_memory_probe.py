#!/usr/bin/env python3
"""Exercise real chunked bulk HTTP ingestion with measured child-node RSS."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import traceback

import requests

from repro_baseline import host_metadata, request_json, sha256_file

MIB = 1024 * 1024
CHUNK_BYTES = 64 * 1024
INDEX = "bulk-memory-probe"
METRICS = (
    "ferrissearch_bulk_active_requests",
    "ferrissearch_bulk_spool_reserved_bytes",
    "ferrissearch_bulk_staged_input_bytes_total",
    "ferrissearch_bulk_execution_batches_total",
)


def source_hash(root):
    digest = hashlib.sha256()
    files = [root / "Cargo.toml", root / "Cargo.lock", root / "build.rs"]
    files += list((root / "src").rglob("*.rs"))
    files += list((root / "proto").glob("*.proto"))
    for path in sorted(files):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def process_memory(pid):
    values = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, text = line.partition(":")
        if key in {"VmRSS", "VmHWM", "RssAnon", "RssFile"}:
            values[key] = int(text.split()[0]) * 1024
    if "VmRSS" not in values or "VmHWM" not in values:
        raise RuntimeError(f"Missing RSS/high-water data for PID {pid}")
    return values


def read_metrics(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request("GET", "/_metrics")
        response = connection.getresponse()
        text = response.read().decode()
        if response.status != 200:
            raise RuntimeError(f"Metrics failed: {response.status}: {text}")
        values = {}
        for line in text.splitlines():
            if not line.startswith("#"):
                name, _, value = line.partition(" ")
                if name in METRICS:
                    values[name] = int(float(value))
        missing = set(METRICS) - values.keys()
        if missing:
            raise RuntimeError(f"Missing bulk metrics: {sorted(missing)}")
        return values
    finally:
        connection.close()


def request_body(documents, padding_bytes, stats):
    padding = b"z" * padding_bytes
    for value in range(documents):
        segments = (
            b'{"index":{"_id":"only"}}\n',
            f'{{"value":{value},"padding":"'.encode(),
            padding,
            b'","body":"probe"}\n',
        )
        for segment in segments:
            for offset in range(0, len(segment), CHUNK_BYTES):
                chunk = segment[offset : offset + CHUNK_BYTES]
                stats["input_bytes"] += len(chunk)
                yield chunk


class Node:
    def __init__(self, binary, root, repository):
        self.root = root
        self.root.mkdir()
        self.log = (root / "node.log").open("wb")
        self.process = None
        self.reservations = [socket.socket(), socket.socket()]
        for reservation in self.reservations:
            reservation.bind(("127.0.0.1", 0))
        self.http_port, self.transport_port = [
            reservation.getsockname()[1] for reservation in self.reservations
        ]
        self.url = f"http://127.0.0.1:{self.http_port}"
        self.environment = {
            "FERRISSEARCH_NODE_NAME": "bulk-probe-node",
            "FERRISSEARCH_CLUSTER_NAME": f"fs014-{os.getpid()}-{self.http_port}",
            "FERRISSEARCH_HTTP_PORT": str(self.http_port),
            "FERRISSEARCH_TRANSPORT_PORT": str(self.transport_port),
            "FERRISSEARCH_DATA_DIR": str(root / "data"),
            "FERRISSEARCH_RAFT_NODE_ID": "1",
            "FERRISSEARCH_SEED_HOSTS": f"127.0.0.1:{self.transport_port}",
            "FERRISSEARCH_TRANSLOG_DURABILITY": "request",
            "FERRISSEARCH_COLUMN_CACHE_SIZE_PERCENT": "0",
            "FERRISSEARCH_SQL_APPROXIMATE_TOP_K": "false",
            "FERRISSEARCH_BULK_MAX_CONCURRENT_REQUESTS": "2",
            "FERRISSEARCH_BULK_MAX_REQUEST_BYTES": str(8 * 1024**3),
            "FERRISSEARCH_BULK_MAX_SPOOL_BYTES": str(16 * 1024**3),
            "FERRISSEARCH_BULK_IDLE_TIMEOUT_MS": "30000",
        }
        environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("FERRISSEARCH_")
        }
        environment.update(self.environment)
        environment["RUST_LOG"] = "warn"
        for reservation in self.reservations:
            reservation.close()
        self.process = subprocess.Popen(
            [str(binary)], cwd=repository, env=environment,
            stdout=self.log, stderr=subprocess.STDOUT,
        )

    def ready(self):
        deadline = time.monotonic() + 90
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Node exited during startup; see {self.root / 'node.log'}")
            try:
                health = request_json(self.url, "GET", "/_cluster/health", timeout=2)
                if health["number_of_nodes"] == 1 and health["status"] == "green":
                    return
                last_error = f"Not ready: {health}"
            except (RuntimeError, requests.RequestException, OSError) as error:
                last_error = error
            time.sleep(0.1)
        raise RuntimeError(f"Node readiness deadline expired: {last_error}")

    def stop(self):
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=15)
        finally:
            self.log.close()


def run_case(args, root, mib, repository):
    node = Node(args.binary, root, repository)
    stop = threading.Event()
    samples = []
    started = time.monotonic()
    sampler = None
    executor = None
    stats = {"input_bytes": 0}
    baseline = None
    phase = "startup"
    try:
        node.ready()
        request_json(node.url, "PUT", f"/{INDEX}", json={
            "settings": {
                "number_of_shards": 1, "number_of_replicas": 0,
                "refresh_interval_ms": 3600000, "flush_threshold_bytes": 0,
            },
            "mappings": {
                "dynamic": False, "properties": {"value": {"type": "integer"}},
            },
        })
        baseline = process_memory(node.process.pid)

        def sample():
            while not stop.is_set():
                snapshot = {
                    "elapsed_seconds": time.monotonic() - started,
                    "memory": process_memory(node.process.pid),
                    "bulk": read_metrics(node.http_port),
                }
                samples.append(snapshot)
                if snapshot["memory"]["VmRSS"] - baseline["VmRSS"] > args.extra_rss_mib * MIB:
                    node.process.terminate()
                    raise AssertionError(
                        f"Live RSS increment exceeds {args.extra_rss_mib} MiB; stopped probe PID {node.process.pid}"
                    )
                stop.wait(0.1)

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="bulk-memory-sampler")
        sampler = executor.submit(sample)
        documents = math.ceil(mib * MIB / args.padding_bytes)
        phase = "ingest"
        connection = http.client.HTTPConnection("127.0.0.1", node.http_port, timeout=1800)
        begin = time.monotonic()
        try:
            connection.request(
                "POST", f"/{INDEX}/_bulk?refresh=true",
                body=request_body(documents, args.padding_bytes, stats),
                headers={"Content-Type": "application/x-ndjson"}, encode_chunked=True,
            )
            response = connection.getresponse()
            encoded = response.read(64 * MIB + 1)
            if len(encoded) > 64 * MIB:
                raise RuntimeError("Probe response exceeds its client-side 64 MiB limit")
            value = json.loads(encoded)
            if response.status != 200 or value["errors"]:
                raise RuntimeError(f"Bulk failed: {response.status}: {str(value)[:2000]}")
        finally:
            connection.close()
        items = value["items"]
        phase = "oracle"
        if len(items) != documents:
            raise AssertionError(f"Missing/reordered responses: {len(items)} != {documents}")
        term = items[0]["index"]["_primary_term"]
        if term <= 0:
            raise AssertionError("Primary term is not positive")
        for position, envelope in enumerate(items):
            item = envelope["index"]
            expected = {
                "_id": "only", "_seq_no": position, "_version": position + 1,
                "_primary_term": term, "status": 201 if position == 0 else 200,
                "_shards": {"total": 1, "successful": 1, "failed": 0},
                "forced_refresh": True,
            }
            for field, wanted in expected.items():
                if item.get(field) != wanted:
                    raise AssertionError(f"Item {position}/{field}: {item.get(field)!r} != {wanted!r}")
        document = request_json(node.url, "GET", f"/{INDEX}/_doc/only?realtime=false")
        if (
            document["_source"]["value"] != documents - 1
            or len(document["_source"]["padding"]) != args.padding_bytes
            or document["_seq_no"] != documents - 1
            or document["_version"] != documents
        ):
            raise AssertionError("Last-document source/version/sequence/refresh oracle failed")
        count = request_json(node.url, "GET", f"/{INDEX}/_count")
        if count["count"] != 1:
            raise AssertionError(f"Expected one overwritten document, got {count['count']}")
        deadline = time.monotonic() + 30
        while True:
            final_bulk = read_metrics(node.http_port)
            if final_bulk[METRICS[0]] == 0 and final_bulk[METRICS[1]] == 0:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"Bulk admission/spool resources leaked: {final_bulk}")
            time.sleep(0.1)
        stop.set()
        sampler.result(timeout=15)
        final = process_memory(node.process.pid)
        peak = max([final["VmHWM"]] + [sample["memory"]["VmHWM"] for sample in samples])
        if peak - baseline["VmRSS"] > args.extra_rss_mib * MIB:
            raise AssertionError(
                f"Peak RSS increment {peak - baseline['VmRSS']} exceeds {args.extra_rss_mib} MiB"
            )
        if final_bulk[METRICS[2]] != stats["input_bytes"]:
            raise AssertionError(f"Staged byte count disagrees with generated body: {final_bulk}")
        phase = "pass"
        return {
            "requested_mib": mib, "documents": documents, **stats,
            "response_bytes": len(encoded), "elapsed_seconds": time.monotonic() - begin,
            "baseline_memory": baseline, "final_memory": final, "peak_rss_bytes": peak,
            "extra_peak_rss_bytes": peak - baseline["VmRSS"], "samples": samples,
            "final_bulk": final_bulk, "node_environment": node.environment,
            "oracle": "pass",
        }
    finally:
        failure = "".join(traceback.format_exception(*sys.exc_info())) if sys.exc_info()[0] else None
        stop.set()
        try:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
        finally:
            try:
                node.stop()
            finally:
                (root / "resources.json").write_text(json.dumps({
                    "phase": phase, "failure": failure, "input": stats,
                    "baseline": baseline, "samples": samples, "node_environment": node.environment,
                    "sampling_failure": (
                        "".join(traceback.format_exception(sampler.exception()))
                        if sampler is not None and sampler.done() and sampler.exception() is not None
                        else None
                    ),
                }, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/ferrissearch"))
    parser.add_argument("--profile", choices=("debug", "release"), default="debug")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mib", type=int, nargs="+", default=[256, 2048])
    parser.add_argument("--padding-bytes", type=int, default=512 * 1024)
    parser.add_argument("--extra-rss-mib", type=int, default=384)
    parser.add_argument("--growth-rss-mib", type=int, default=128)
    args = parser.parse_args()
    if (
        not all(1 <= mib <= 4096 for mib in args.mib)
        or not 64 * 1024 <= args.padding_bytes <= 16 * MIB
        or args.extra_rss_mib <= 0 or args.growth_rss_mib <= 0
    ):
        parser.error("Positive RSS bounds, 1..4096 MiB inputs, and 64 KiB..16 MiB padding required")
    args.binary = args.binary.resolve(strict=True)
    repository = Path(__file__).resolve().parents[1]
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": "running", "profile": args.profile, "features": [],
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip(),
        "worktree_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=repository, text=True),
        "source_sha256": source_hash(repository), "binary_sha256": sha256_file(args.binary),
        "harness_sha256": sha256_file(Path(__file__)),
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "host": host_metadata(), "topology": {"nodes": 1, "shards": 1, "replicas": 0},
        "dataset": {"padding_bytes": args.padding_bytes, "same_document_overwrites": True},
        "limits": {"extra_rss_mib": args.extra_rss_mib, "growth_rss_mib": args.growth_rss_mib},
        "cache": "fresh node per case; column cache disabled; filesystem caches not dropped",
        "samples_scope": "child-node RSS only; excludes client RSS and kernel page cache",
        "cases": [],
    }
    try:
        for mib in args.mib:
            case = run_case(args, output / f"case-{mib}mib", mib, repository)
            report["cases"].append(case)
            print(
                f"{mib} MiB: {case['input_bytes']} input bytes, {case['documents']} items, "
                f"extra peak RSS {case['extra_peak_rss_bytes'] / MIB:.2f} MiB",
                flush=True,
            )
            (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        peaks = [case["extra_peak_rss_bytes"] for case in report["cases"]]
        if max(peaks) - min(peaks) > args.growth_rss_mib * MIB:
            raise AssertionError(f"RSS growth across logical input sizes exceeds {args.growth_rss_mib} MiB")
        report["status"] = "pass"
    finally:
        if sys.exc_info()[0] is not None:
            report["status"] = "failed"
            report["failure"] = "".join(traceback.format_exception(*sys.exc_info()))
        report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
