from __future__ import annotations

import importlib.util
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


if __name__ == "__main__":
    unittest.main()
