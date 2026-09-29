from __future__ import annotations

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


def start_record() -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": 0,
        "event": "trace_start",
        "test": "unit",
        "durability": "request",
        "nodes": [
            {"node": "p", "incarnation": 0},
            {"node": "r", "incarnation": 0},
        ],
        "shard_state": {
            "index_uuid": "idx",
            "shard": 0,
            "primary": "p",
            "term": 1,
            "activated": True,
            "in_sync": ["r"],
            "copies": [
                {"node": "p", "allocation": 1, "exists": True, "fence_term": 1},
                {"node": "r", "allocation": 2, "exists": True, "fence_term": 1},
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
        "quiescent": False,
        "records_before_end": count,
    }


def route_record(step: int = 1) -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": step,
        "event": "client_write_routed",
        "node": "p",
        "index_uuid": "idx",
        "shard": 0,
        "request_id": "w0",
        "target_node": "p",
        "doc": "d",
        "op": "index",
        "content_hash": "a" * 64,
    }

def absent_copy_state(step: int = 2) -> dict[str, object]:
    return {
        "schema": trace_to_tla.SCHEMA,
        "run_id": "unit",
        "step": step,
        "event": "copy_state",
        "node": "p",
        "index_uuid": "idx",
        "shard": 0,
        "allocation": 1,
        "reason": "trace_end",
        "documents": [
            {
                "doc": "d",
                "state": "absent",
                "seq_no": None,
                "term": None,
                "content_hash": None,
            }
        ],
    }


class TraceConverterTests(unittest.TestCase):
    def write_trace(self, records: list[dict[str, object]]) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "trace.jsonl"
        path.write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
            encoding="utf-8",
        )
        return path

    def test_minimal_prefix_generates_module_and_config(self) -> None:
        path = self.write_trace(
            [start_record(), route_record(), absent_copy_state(), end_record(3, 3)]
        )
        trace = trace_to_tla.load_trace(path)
        module, config = trace_to_tla.render(trace)
        self.assertIn("MODULE TraceInput", module)
        self.assertIn("Trace == <<", module)
        self.assertIn("SPECIFICATION TraceSpec", config)

    def test_v3_is_rejected(self) -> None:
        start = start_record()
        start["schema"] = "ferrissearch.d1.trace/v3"
        path = self.write_trace([start, end_record(1, 1)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unsupported schema"):
            trace_to_tla.load_trace(path)

    def test_unknown_event_is_rejected(self) -> None:
        event = route_record()
        event["event"] = "future_event"
        path = self.write_trace([start_record(), event, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown event"):
            trace_to_tla.load_trace(path)

    def test_unknown_field_is_rejected(self) -> None:
        event = route_record()
        event["future_field"] = 1
        path = self.write_trace([start_record(), event, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown field"):
            trace_to_tla.load_trace(path)

    def test_steps_must_be_consecutive(self) -> None:
        event = route_record(step=2)
        path = self.write_trace([start_record(), event, end_record(3, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "step must be consecutive"):
            trace_to_tla.load_trace(path)

    def test_copy_state_cannot_invent_an_operation(self) -> None:
        copy_state = {
            "schema": trace_to_tla.SCHEMA,
            "run_id": "unit",
            "step": 1,
            "event": "copy_state",
            "node": "p",
            "index_uuid": "idx",
            "shard": 0,
            "allocation": 1,
            "reason": "quiescent",
            "documents": [
                {
                    "doc": "d",
                    "state": "live",
                    "seq_no": 0,
                    "term": 1,
                    "content_hash": "a" * 64,
                }
            ],
        }
        path = self.write_trace([start_record(), copy_state, end_record(2, 2)])
        with self.assertRaisesRegex(trace_to_tla.TraceSchemaError, "unknown operation identity"):
            trace_to_tla.load_trace(path)


if __name__ == "__main__":
    unittest.main()
