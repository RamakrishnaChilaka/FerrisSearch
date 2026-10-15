from __future__ import annotations

import copy
import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "check_write_authority_trace", ROOT / "scripts/tla/check_write_authority_trace.py"
)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = checker
SPEC.loader.exec_module(checker)


def authority(term: int = 2, activation: int = 5) -> dict:
    return {
        "index_uuid": "uuid-1", "shard_id": 0, "primary_node_id": "p",
        "primary_allocation_id": 1, "primary_term": term, "activation_id": activation,
    }


def canonical(events: list, current: dict, previous: dict | None = None, replica: bool = False) -> dict:
    history = {
        "activation_id": current["activation_id"], "primary_term": current["primary_term"],
        "max_seq_no": None, "digest": [current["primary_term"]] * 32,
    }

    def emit(kind, details):
        events.append({"step": len(events), "event": kind, "authority": current, "details": details})

    emit("canonical_promoting", {"state_version": 7, "previous_history": previous})
    for node, allocation in [("p", 1)] + ([("q", 2)] if replica else []):
        identity = {"node_id": node, "allocation_id": allocation}
        emit("canonical_frozen", {
            "copy": identity, "fence_term": current["primary_term"], "persisted_next": 0, "max_next": 0,
        })
        emit("canonical_sealed", {"copy": identity, "history": history})
    emit("canonical_active", {
        "history": history, "eligible_replicas": [{"node_id": "q", "allocation_id": 2}] if replica else [],
    })
    return history


def witness() -> dict:
    current = authority()
    events = []
    canonical(events, current, replica=True)
    exclusion = {
        "authority": current, "captured_state_version": 8,
        "transition_id": "00000000-0000-4000-8000-000000000001",
        "failed_copies": [{"node_id": "q", "allocation_id": 2}],
    }
    for kind in ("exclusion_closed", "exclusion_journaled", "exclusion_proposed", "exclusion_committed"):
        events.append({"step": len(events), "event": kind, "authority": current,
                       "details": {"exclusion": exclusion}})
    events.append({
        "step": len(events), "event": "exclusion_settled", "authority": current,
        "details": {
            "exclusion": exclusion, "accepted": True,
            "observed": {
                "version": 9, "index_uuid": "uuid-1", "primary": "p", "primary_allocation": 1,
                "primary_term": 2, "activation_id": 5, "phase": "active", "primary_unavailable": False,
                "failed_copies": [{"node_id": "q", "allocation_id": None, "authoritative": False}],
            },
        },
    })
    events.append({
        "step": len(events), "event": "write_certified", "authority": current,
        "details": {
            "minimum": 1, "eligible_proven": 1,
            "copies": [
                {"node_id": "p", "allocation_id": 1, "primary": True, "acknowledged": True},
                {"node_id": "q", "allocation_id": 2, "primary": False, "acknowledged": False},
            ],
        },
    })
    return {
        "schema": checker.SCHEMA, "profile": "serialized-authority-witness", "run_id": "test",
        "index_uuid": "uuid-1", "shard": 0, "nodes": ["p", "q"], "events": events,
    }


class WriteAuthorityTraceTests(unittest.TestCase):
    @staticmethod
    def remove(document, kind):
        document["events"] = [event for event in document["events"] if event["event"] != kind]
        for step, event in enumerate(document["events"]):
            event["step"] = step

    def test_valid_canonical_and_observed_exclusion(self):
        checker.validate(witness())

    def test_proposal_requires_durable_journal(self):
        document = witness()
        self.remove(document, "exclusion_journaled")
        with self.assertRaisesRegex(ValueError, "proposal precedes durable journal"):
            checker.validate(document)

    def test_acceptance_must_be_exact_committed_result(self):
        document = witness()
        self.remove(document, "exclusion_committed")
        with self.assertRaisesRegex(ValueError, "neither exact committed"):
            checker.validate(document)

    def test_stale_absence_cannot_settle_debt(self):
        document = witness()
        settled = document["events"][-2]["details"]
        settled["accepted"] = False
        settled["observed"]["version"] = 7
        with self.assertRaisesRegex(ValueError, "neither exact committed"):
            checker.validate(document)

    def test_observed_reassignment_settles_only_the_old_allocation(self):
        document = witness()
        settled = document["events"][-2]["details"]
        settled["accepted"] = False
        settled["observed"]["failed_copies"][0].update(allocation_id=3, authoritative=True)
        checker.validate(document)
        document["events"][-1]["details"]["copies"][1]["allocation_id"] = 3
        with self.assertRaisesRegex(ValueError, "no observed exact exclusion"):
            checker.validate(document)

    def test_committed_but_unsettled_debt_still_blocks_certification(self):
        document = witness()
        self.remove(document, "exclusion_settled")
        with self.assertRaisesRegex(ValueError, "revoked authority or debt"):
            checker.validate(document)

    def test_minimum_floor_cannot_be_bypassed(self):
        document = witness()
        document["events"][-1]["details"]["minimum"] = 2
        with self.assertRaisesRegex(ValueError, "eligible-copy floor"):
            checker.validate(document)

    def test_every_eligible_copy_needs_the_exact_canonical_seal(self):
        document = witness()
        document["events"] = [
            event for event in document["events"]
            if not (event["event"] == "canonical_sealed" and event["details"]["copy"]["node_id"] == "q")
        ]
        for step, event in enumerate(document["events"]):
            event["step"] = step
        with self.assertRaisesRegex(ValueError, "matching seal"):
            checker.validate(document)

    def test_late_old_epoch_revocation_does_not_revoke_new_authority(self):
        document = witness()
        previous = copy.deepcopy(document["events"][2]["details"]["history"])
        current = authority(3, 10)
        canonical(document["events"], current, previous)
        document["events"].append({
            "step": len(document["events"]), "event": "old_authority_revoked",
            "authority": authority(), "details": {},
        })
        document["events"].append({
            "step": len(document["events"]), "authority": current, "event": "write_certified",
            "details": {
                "minimum": 1, "eligible_proven": 1,
                "copies": [{"node_id": "p", "allocation_id": 1, "primary": True, "acknowledged": True}],
            },
        })
        checker.validate(document)

    def test_malformed_types_and_unknown_fields_fail_loudly(self):
        for mutate in (
            lambda doc: doc["events"][0]["authority"].update(activation_id=True),
            lambda doc: doc["events"][0]["details"].update(extra=True),
            lambda doc: doc["events"][0].update(event="unknown"),
            lambda doc: doc["events"][0].update(event=1),
            lambda doc: doc.update(events=[None]),
            lambda doc: doc.update(events=[]),
        ):
            document = witness()
            mutate(document)
            with self.assertRaises(ValueError):
                checker.validate(document)


if __name__ == "__main__":
    unittest.main()
