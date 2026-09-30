from __future__ import annotations

import importlib.util
import copy
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "scripts" / "tla" / "check_d1_trace_invariants.py"
SPEC = importlib.util.spec_from_file_location("check_d1_trace_invariants", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checker
SPEC.loader.exec_module(checker)

FIXTURES = ROOT / "specs" / "tla" / "trace" / "v4"


class D1TraceInvariantTests(unittest.TestCase):
    @staticmethod
    def insert_before_end(
        events: list[dict[str, object]],
        inserted: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        result = copy.deepcopy(events[:-1]) + inserted + [copy.deepcopy(events[-1])]
        for step, event in enumerate(result):
            event["step"] = step
        result[-1]["records_before_end"] = len(result) - 1
        return result

    def test_response_checkpoint_may_lag_current_replica_checkpoint(self) -> None:
        events = checker.load_trace(
            FIXTURES / "b1-bulk-batch-final-response-checkpoint.jsonl"
        )
        checker.check_trace(events)

    def test_response_checkpoint_may_not_overstate_replica_checkpoint(self) -> None:
        events = checker.load_trace(
            FIXTURES / "invalid-replica-response-overstates-persisted.jsonl"
        )
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(events)
        self.assertEqual(caught.exception.step, 15)
        self.assertEqual(caught.exception.event, "replica_result")

    def test_in_sync_stale_ack_without_newer_version_is_rejected(self) -> None:
        events = checker.load_trace(FIXTURES / "invalid-combined-arrival-order.jsonl")
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(events)
        self.assertEqual(caught.exception.step, 51)
        self.assertEqual(caught.exception.event, "operation_processed")

    def test_client_ack_without_wal_or_apply_is_rejected(self) -> None:
        events = checker.load_trace(FIXTURES / "valid-concurrent-order.jsonl")
        routed = next(event for event in events if event["event"] == "client_write_routed")
        orphan_request = {
            **copy.deepcopy(routed),
            "request_id": "orphan-request",
        }
        orphan_result = {
            "schema": checker.SCHEMA,
            "run_id": events[0]["run_id"],
            "step": 0,
            "event": "client_result",
            "node": routed["node"],
            "index_uuid": routed["index_uuid"],
            "shard": routed["shard"],
            "request_id": "orphan-request",
            "outcome": "acknowledged",
            "failure_stage": None,
        }
        mutated = self.insert_before_end(events, [orphan_request, orphan_result])
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(mutated)
        self.assertEqual(caught.exception.event, "client_result")
        self.assertIn("no primary apply", str(caught.exception))

    def test_write_accepted_below_fence_is_rejected(self) -> None:
        events = copy.deepcopy(
            checker.load_trace(FIXTURES / "valid-concurrent-order.jsonl")
        )
        wal_index = next(
            index
            for index, event in enumerate(events)
            if event["event"] == "wal_appended"
            and event["origin"] == "live_replication"
        )
        wal = events[wal_index]
        fence = {
            "schema": checker.SCHEMA,
            "run_id": events[0]["run_id"],
            "step": 0,
            "event": "fence_persisted",
            "node": wal["node"],
            "index_uuid": wal["index_uuid"],
            "shard": wal["shard"],
            "allocation": wal["allocation"],
            "term": wal["term"] + 1,
            "fence_max_seq_no": None,
            "reason": "replication",
        }
        mutated = events[:wal_index] + [fence] + events[wal_index:]
        for step, event in enumerate(mutated):
            event["step"] = step
        mutated[-1]["records_before_end"] = len(mutated) - 1
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(mutated)
        self.assertEqual(caught.exception.event, "wal_appended")
        self.assertIn("below the durable fence", str(caught.exception))

    def test_applied_content_mismatch_is_rejected(self) -> None:
        events = copy.deepcopy(
            checker.load_trace(FIXTURES / "valid-concurrent-order.jsonl")
        )
        replica_events = [
            event
            for event in events
            if event["event"] in {"wal_appended", "operation_processed"}
            and event.get("origin") == "live_replication"
            and event["seq_no"] == 0
        ]
        self.assertTrue(replica_events)
        for event in replica_events:
            event["doc"] = "different-doc"
            event["content_hash"] = "f" * 64
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(events)
        self.assertIn("identity changed", str(caught.exception))

    def test_unobservable_mid_batch_maximum_is_rejected(self) -> None:
        events = checker.load_trace(
            FIXTURES / "invalid-unobservable-mid-batch-maximum.jsonl"
        )
        with self.assertRaises(checker.InvariantViolation) as caught:
            checker.check_trace(events)
        self.assertIn("batch maximum differs", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
