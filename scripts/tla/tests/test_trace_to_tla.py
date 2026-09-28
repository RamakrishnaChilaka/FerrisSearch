from __future__ import annotations

import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts" / "tla" / "trace_to_tla.py"
SPEC = importlib.util.spec_from_file_location("trace_to_tla", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
trace_to_tla = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = trace_to_tla
SPEC.loader.exec_module(trace_to_tla)


def checkpoints() -> dict[str, None]:
    return {"processed": None, "persisted": None, "max_seq_no": None}


def start_record() -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": 0,
        "event": "trace_start",
        "test": "unit",
        "durability": "request",
        "initial_prefix_through": None,
        "nodes": [{"node": "n1", "incarnation": 0}],
        "shard_state": {
            "index_uuid": "idx",
            "shard": 0,
            "primary": "n1",
            "term": 1,
            "activated": True,
            "in_sync": [],
            "copies": [
                {
                    "node": "n1",
                    "allocation": 1,
                    "exists": True,
                    "fence_term": 1,
                    "fence_max_seq_no": None,
                    "checkpoints": checkpoints(),
                }
            ],
        },
    }


def end_record(step: int, count: int) -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": step,
        "event": "trace_end",
        "outcome": "completed",
        "quiescent": True,
        "records_before_end": count,
    }


def route_record() -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": 1,
        "event": "client_write_routed",
        "node": "n1",
        "incarnation": 0,
        "index_uuid": "idx",
        "shard": 0,
        "allocation": 1,
        "peer": "n1",
        "peer_incarnation": 0,
        "peer_allocation": 1,
        "request_id": "w0",
        "term": 1,
        "seq_no": None,
        "doc": "d",
        "op": "index",
        "content_hash": "a" * 64,
        "origin": None,
        "outcome": "routed",
        "checkpoints": checkpoints(),
    }


class TraceConverterTests(unittest.TestCase):
    def write_trace(self, records: list[dict[str, object]]) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "trace.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        return path

    def test_empty_trace_generates_module(self) -> None:
        path = self.write_trace([start_record(), end_record(1, 1)])
        trace = trace_to_tla.load_trace(path)
        rendered = trace_to_tla.render_trace_input(trace)
        self.assertIn("MODULE TraceInput", rendered)
        self.assertIn("Trace == <<>>", rendered)

    def test_unknown_schema_version_fails_loudly(self) -> None:
        start = start_record()
        start["schema"] = "ferrissearch.d1.trace/v2"
        path = self.write_trace([start, end_record(1, 1)])
        with self.assertRaisesRegex(
            trace_to_tla.TraceSchemaError, "unsupported schema"
        ):
            trace_to_tla.load_trace(path)

    def test_unknown_event_fails_loudly(self) -> None:
        event = route_record()
        event["event"] = "future_event"
        path = self.write_trace([start_record(), event, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown event"):
            trace_to_tla.load_trace(path)

    def test_unknown_field_fails_loudly(self) -> None:
        event = route_record()
        event["future_field"] = 1
        path = self.write_trace([start_record(), event, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown field"):
            trace_to_tla.load_trace(path)

    def test_unknown_outcome_fails_loudly(self) -> None:
        event = route_record()
        event["outcome"] = "maybe"
        path = self.write_trace([start_record(), event, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown .* outcome"):
            trace_to_tla.load_trace(path)

    def test_operation_identity_cannot_change_content(self) -> None:
        first = {
            **route_record(),
            "event": "wal_appended",
            "request_id": "w0",
            "term": 1,
            "seq_no": 0,
            "origin": "primary",
            "outcome": "appended",
            "durable": True,
        }
        second = copy.deepcopy(first)
        second["step"] = 2
        second["content_hash"] = "b" * 64
        path = self.write_trace(
            [start_record(), first, second, end_record(3, 3)]
        )
        with self.assertRaisesRegex(
            trace_to_tla.TraceSchemaError, "changed content"
        ):
            trace_to_tla.load_trace(path)


if __name__ == "__main__":
    unittest.main()
