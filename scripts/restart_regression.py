#!/usr/bin/env python3

"""Run the flush + restart regression workflow against three real FerrisSearch nodes.

Default flow:
1. Launch 3 Raft-backed nodes with isolated data dirs
2. Create a 3-shard index
3. Bulk index 2,000,000 docs
4. Flush the index
5. Kill and restart all 3 nodes
6. Verify the index UUID is unchanged, shard dirs still exist, and document count matches

This script uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


INDEX_NAME = "restart-regression"
CLUSTER_NAME = "restart-regression-cluster"
DOC_BODY = (
    "restart regression payload restart regression payload restart regression payload "
    "restart regression payload restart regression payload restart regression payload "
    "restart regression payload restart regression payload"
)
DELETE_REASONS = (
    "reason=legacy_publish_state_removed_index",
    "reason=api_delete_index",
    "reason=transport_delete_index_rpc",
    "reason=orphan_cleanup_unknown_uuid",
)


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request_json(url: str, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(url, data=body, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def wait_for(predicate, timeout_s: float, message: str) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise RuntimeError(message)


@dataclass
class NodeProcess:
    name: str
    data_dir: Path
    log_path: Path
    http_port: int
    transport_port: int
    raft_node_id: int
    binary: Path
    process: subprocess.Popen | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.http_port}"

    def spawn(self, seed_hosts: str) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        for key in list(env):
            if key.startswith("FERRISSEARCH_"):
                env.pop(key, None)

        env.update(
            {
                "RUST_LOG": "info",
                "FERRISSEARCH_NODE_NAME": self.name,
                "FERRISSEARCH_CLUSTER_NAME": CLUSTER_NAME,
                "FERRISSEARCH_HTTP_PORT": str(self.http_port),
                "FERRISSEARCH_TRANSPORT_PORT": str(self.transport_port),
                "FERRISSEARCH_DATA_DIR": str(self.data_dir),
                "FERRISSEARCH_SEED_HOSTS": seed_hosts,
                "FERRISSEARCH_RAFT_NODE_ID": str(self.raft_node_id),
                "FERRISSEARCH_COLUMN_CACHE_SIZE_PERCENT": "0",
            }
        )

        log_file = self.log_path.open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(self.binary)],
            cwd=Path(__file__).resolve().parent.parent,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        wait_for(self.is_ready, 60, f"{self.name} did not become ready\n{self.log_tail()}")

    def is_ready(self) -> bool:
        self.ensure_running()
        try:
            status, _ = request_json(f"{self.base_url}/")
            return status == 200
        except Exception:
            return False

    def ensure_running(self) -> None:
        if self.process is None:
            raise RuntimeError(f"{self.name} is not running")
        rc = self.process.poll()
        if rc is not None:
            raise RuntimeError(f"{self.name} exited with status {rc}\n{self.log_tail()}")

    def stop(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.process = None

    def log_tail(self, max_lines: int = 80) -> str:
        if not self.log_path.exists():
            return ""
        lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-max_lines:])


class RestartWorkflow:
    def __init__(self, binary: Path, docs: int, batch_size: int, keep_data: bool, data_root: Path | None) -> None:
        self.binary = binary
        self.docs = docs
        self.batch_size = batch_size
        self.keep_data = keep_data
        self._owns_data_root = data_root is None
        self.data_root = data_root
        if self.data_root is None:
            self.data_root = Path(tempfile.mkdtemp(prefix="ferris-restart-regression-"))
        self.nodes = self._build_nodes()
        self.seed_hosts = ",".join(f"127.0.0.1:{node.transport_port}" for node in self.nodes)

    def _build_nodes(self) -> list[NodeProcess]:
        assert self.data_root is not None
        return [
            NodeProcess(
                name=f"node-{idx}",
                data_dir=self.data_root / f"node-{idx}",
                log_path=self.data_root / f"node-{idx}.log",
                http_port=reserve_port(),
                transport_port=reserve_port(),
                raft_node_id=idx,
                binary=self.binary,
            )
            for idx in range(1, 4)
        ]

    def start_cluster(self) -> None:
        first, *rest = self.nodes
        first.spawn(self.seed_hosts)
        self.wait_for_single_node_bootstrap(first)
        for node in rest:
            node.spawn(self.seed_hosts)
        self.wait_for_cluster(index_required=False)

    def restart_cluster(self) -> None:
        for node in self.nodes:
            node.stop()
        time.sleep(0.5)
        first, *rest = self.nodes
        first.spawn(self.seed_hosts)
        self.wait_for_single_node_bootstrap(first)
        for node in rest:
            node.spawn(self.seed_hosts)
        self.wait_for_cluster(index_required=True)

    def wait_for_single_node_bootstrap(self, node: NodeProcess) -> None:
        def predicate() -> bool:
            node.ensure_running()
            try:
                status, state = request_json(f"{node.base_url}/_cluster/state")
                return (
                    status == 200
                    and state.get("master_node") == node.name
                    and node.name in state.get("nodes", {})
                )
            except Exception:
                return False

        wait_for(predicate, 60, f"{node.name} did not finish single-node bootstrap\n{node.log_tail()}")

    def wait_for_cluster(self, index_required: bool) -> dict:
        last_error = None
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                for node in self.nodes:
                    node.ensure_running()
                status, state = request_json(f"{self.nodes[0].base_url}/_cluster/state")
                if status == 200 and len(state.get("nodes", {})) >= 3:
                    if not index_required or INDEX_NAME in state.get("indices", {}):
                        return state
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            time.sleep(0.25)
        raise RuntimeError(f"cluster did not become ready: {last_error}\n{self.logs_summary()}")

    def create_index(self) -> str:
        status, body = request_json(
            f"{self.nodes[0].base_url}/{INDEX_NAME}",
            method="PUT",
            body=json.dumps(
                {
                    "settings": {
                        "number_of_shards": 3,
                        "number_of_replicas": 0,
                        "refresh_interval_ms": 60000,
                    },
                    "mappings": {
                        "properties": {
                            "title": {"type": "text"},
                            "author": {"type": "keyword"},
                            "payload": {"type": "text"},
                            "n": {"type": "integer"},
                        }
                    },
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        if status != 200 or not body.get("acknowledged"):
            raise RuntimeError(f"create index failed: {body}")
        state = self.wait_for_cluster(index_required=True)
        return state["indices"][INDEX_NAME]["uuid"]

    def bulk_index(self) -> None:
        for batch_start in range(0, self.docs, self.batch_size):
            batch_end = min(batch_start + self.batch_size, self.docs)
            lines = []
            for doc_id in range(batch_start, batch_end):
                lines.append(json.dumps({"index": {"_id": f"doc-{doc_id}"}}))
                lines.append(
                    json.dumps(
                        {
                            "title": f"restart regression doc {doc_id}",
                            "author": f"author-{doc_id % 17}",
                            "payload": DOC_BODY,
                            "n": doc_id,
                        }
                    )
                )
            body = ("\n".join(lines) + "\n").encode("utf-8")
            status, response = request_json(
                f"{self.nodes[0].base_url}/{INDEX_NAME}/_bulk",
                method="POST",
                body=body,
                headers={"Content-Type": "application/x-ndjson"},
            )
            if status != 200 or response.get("errors"):
                raise RuntimeError(f"bulk index failed at batch {batch_start}: {response}")
            if batch_start and batch_start % (self.batch_size * 20) == 0:
                print(f"Indexed {batch_end}/{self.docs} docs", flush=True)

    def flush(self) -> None:
        status, body = request_json(
            f"{self.nodes[0].base_url}/{INDEX_NAME}/_flush",
            method="POST",
            body=b"{}",
            headers={"Content-Type": "application/json"},
        )
        if status != 200:
            raise RuntimeError(f"flush failed: {body}")
        if body.get("_shards", {}).get("successful") != 3:
            raise RuntimeError(f"flush did not succeed on all shard copies: {body}")

    def wait_for_count(self, expected: int) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                status, body = request_json(f"{self.nodes[0].base_url}/{INDEX_NAME}/_count")
                if status == 200 and body.get("count") == expected:
                    return
            except Exception:
                pass
            time.sleep(0.25)
        raise RuntimeError(f"count did not converge to {expected}\n{self.logs_summary()}")

    def assert_shard_dirs(self, state: dict, index_uuid: str) -> None:
        routing = state["indices"][INDEX_NAME]["shard_routing"]
        for node in self.nodes:
            expected_shards = []
            for shard_id, shard_route in routing.items():
                if shard_route.get("primary") == node.name or node.name in shard_route.get("replicas", []):
                    expected_shards.append(int(shard_id))
            for shard_id in expected_shards:
                shard_dir = node.data_dir / index_uuid / f"shard_{shard_id}"
                if not shard_dir.exists():
                    raise RuntimeError(
                        f"missing shard dir {shard_dir} after workflow\n{self.logs_summary()}"
                    )

    def assert_no_destructive_delete_logs(self) -> None:
        for node in self.nodes:
            log_text = node.log_path.read_text(encoding="utf-8", errors="replace")
            for reason in DELETE_REASONS:
                if reason in log_text:
                    raise RuntimeError(
                        f"unexpected destructive delete reason {reason} in {node.name} log\n{node.log_tail(120)}"
                    )

    def logs_summary(self) -> str:
        return "\n".join(
            f"===== {node.name} =====\n{node.log_tail()}" for node in self.nodes
        )

    def cleanup(self) -> None:
        for node in self.nodes:
            node.stop()
        if self._owns_data_root and self.data_root and not self.keep_data:
            shutil.rmtree(self.data_root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("target/debug/ferrissearch"),
        help="FerrisSearch binary to launch (default: target/debug/ferrissearch)",
    )
    parser.add_argument(
        "--docs",
        type=int,
        default=2_000_000,
        help="Number of docs to bulk index before flush + restart (default: 2000000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Bulk batch size (default: 1000)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Persist node data and logs under this directory instead of a temp dir",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep the temp data/log directory after success or failure",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.binary.exists():
        print(
            f"FerrisSearch binary not found at {args.binary}. Build it first with: cargo build --bin ferrissearch",
            file=sys.stderr,
        )
        return 2

    workflow = RestartWorkflow(
        binary=args.binary.resolve(),
        docs=args.docs,
        batch_size=args.batch_size,
        keep_data=args.keep_data,
        data_root=args.data_root.resolve() if args.data_root else None,
    )
    print(f"Using data root: {workflow.data_root}")

    try:
        workflow.start_cluster()
        index_uuid = workflow.create_index()
        print(f"Created index {INDEX_NAME} with uuid {index_uuid}")
        workflow.bulk_index()
        workflow.flush()
        state_before = workflow.wait_for_cluster(index_required=True)
        workflow.assert_shard_dirs(state_before, index_uuid)
        workflow.restart_cluster()
        state_after = workflow.wait_for_cluster(index_required=True)
        if state_after["indices"][INDEX_NAME]["uuid"] != index_uuid:
            raise RuntimeError(
                f"index uuid changed across restart: before={index_uuid} after={state_after['indices'][INDEX_NAME]['uuid']}"
            )
        workflow.assert_shard_dirs(state_after, index_uuid)
        workflow.wait_for_count(args.docs)
        workflow.assert_no_destructive_delete_logs()
    except Exception as exc:  # noqa: BLE001
        print(f"Regression failed: {exc}", file=sys.stderr)
        print(workflow.logs_summary(), file=sys.stderr)
        if workflow.data_root:
            print(f"Logs and data kept at: {workflow.data_root}", file=sys.stderr)
        workflow.keep_data = True
        workflow.cleanup()
        return 1

    print("Restart regression passed")
    print(f"Logs and data root: {workflow.data_root}")
    workflow.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())