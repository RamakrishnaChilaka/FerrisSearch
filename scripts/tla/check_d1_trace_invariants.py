#!/usr/bin/env python3
"""Independent safety checks for emitted D1 protocol traces."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA = "ferrissearch.d1.trace/v4"


class InvariantViolation(Exception):
    def __init__(self, step: int, event: str, message: str) -> None:
        super().__init__(message)
        self.step = step
        self.event = event


@dataclass(frozen=True)
class Identity:
    term: int
    seq_no: int
    doc: str | None
    op: str
    content_hash: str


@dataclass
class CopyState:
    processed: set[int] = field(default_factory=set)
    persisted: set[int] = field(default_factory=set)
    max_seq_no: int | None = None
    documents: dict[str, tuple[str, int, int, str]] = field(default_factory=dict)
    identities: dict[int, Identity] = field(default_factory=dict)
    fence_term: int = 0
    last_effect_step: int = 0


def fail(event: dict[str, Any], message: str) -> None:
    raise InvariantViolation(event["step"], event["event"], message)


def checkpoint(values: set[int]) -> int | None:
    next_seq = 0
    while next_seq in values:
        next_seq += 1
    return next_seq - 1 if next_seq else None


def expected_checkpoints(copy: CopyState) -> dict[str, int | None]:
    return {
        "processed": checkpoint(copy.processed),
        "persisted": checkpoint(copy.persisted),
        "max_seq_no": copy.max_seq_no,
    }


def identity(event: dict[str, Any]) -> Identity:
    return Identity(
        term=event["term"],
        seq_no=event["seq_no"],
        doc=event["doc"],
        op=event["op"],
        content_hash=event["content_hash"],
    )


def check_checkpoint_event(event: dict[str, Any], copy: CopyState) -> None:
    expected = expected_checkpoints(copy)
    if event["checkpoints"] != expected:
        fail(
            event,
            f"gap-aware checkpoints are {event['checkpoints']}, expected {expected}",
        )


def apply_observed_operation(
    event: dict[str, Any],
    copy: CopyState,
    wal_durable: dict[tuple[str, str], bool],
    replay: bool = False,
) -> None:
    observed = identity(event)
    previous = copy.identities.get(observed.seq_no)
    if previous is not None and previous != observed:
        if event["outcome"] != "collision":
            fail(
                event,
                "different operation identity at an already processed sequence was accepted",
            )
        check_checkpoint_event(event, copy)
        return
    if event["outcome"] == "collision":
        if previous is None or previous == observed:
            fail(event, "collision does not conflict with an existing sequence identity")
        check_checkpoint_event(event, copy)
        return

    completes = event["outcome"] in {"applied_newer", "stale", "noop"}
    if event["outcome"] == "redelivery":
        if previous is None or observed.seq_no not in copy.processed:
            fail(event, "redelivery names a sequence that was not already processed")
    elif completes:
        copy.identities.setdefault(observed.seq_no, observed)
        copy.processed.add(observed.seq_no)
        copy.max_seq_no = max(copy.max_seq_no or 0, observed.seq_no)
        durable = replay or wal_durable.get((event["node"], event["receipt_id"]), False)
        if durable:
            copy.persisted.add(observed.seq_no)

    if event["outcome"] == "applied_newer" and observed.doc is not None:
        current = copy.documents.get(observed.doc)
        if current is not None and current[1] > observed.seq_no:
            fail(
                event,
                f"applied seq_no {observed.seq_no} over newer document seq_no {current[1]}",
            )
        state = "deleted" if observed.op == "delete" else "live"
        copy.documents[observed.doc] = (
            state,
            observed.seq_no,
            observed.term,
            observed.content_hash,
        )
    check_checkpoint_event(event, copy)


def normalized_documents(
    documents: list[dict[str, Any]],
) -> dict[str, tuple[str, int | None, int | None, str | None]]:
    return {
        document["doc"]: (
            document["state"],
            document["seq_no"],
            document["term"],
            document["content_hash"],
        )
        for document in documents
    }


def load_trace(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                raise InvariantViolation(
                    line_number - 1, "json", f"invalid JSON: {error}"
                ) from error
            if event.get("schema") != SCHEMA:
                raise InvariantViolation(
                    event.get("step", line_number - 1),
                    event.get("event", "schema"),
                    f"unsupported schema {event.get('schema')!r}",
                )
            if event.get("step") != len(events):
                raise InvariantViolation(
                    event.get("step", line_number - 1),
                    event.get("event", "step"),
                    f"non-consecutive step, expected {len(events)}",
                )
            events.append(event)
    if not events or events[0].get("event") != "trace_start":
        raise InvariantViolation(0, "trace_start", "trace_start is missing")
    if events[-1].get("event") != "trace_end":
        raise InvariantViolation(len(events), "trace_end", "trace_end is missing")
    return events


def check_trace(events: list[dict[str, Any]]) -> None:
    start = events[0]
    nodes = {entry["node"]: entry["incarnation"] for entry in start["nodes"]}
    alive = {node: True for node in nodes}
    copies = {
        entry["node"]: CopyState(fence_term=entry["fence_term"])
        for entry in start["shard_state"]["copies"]
        if entry["exists"]
    }
    primary = start["shard_state"]["primary"]
    in_sync = set(start["shard_state"]["in_sync"])
    requests: dict[str, dict[str, Any]] = {}
    request_operations: dict[str, Identity] = {}
    request_status: dict[str, str] = {}
    receipt_identity: dict[str, Identity] = {}
    wal_durable: dict[tuple[str, str], bool] = {}
    messages: dict[str, dict[str, Any]] = {}
    required_messages: dict[str, set[str]] = {}
    message_results: dict[str, str] = {}
    pending_commits: dict[str, dict[str, Any]] = {}
    persisted_commits: dict[str, dict[str, Any]] = {}
    final_copy_states: dict[str, tuple[int, dict[str, tuple[Any, ...]]]] = {}

    for event in events[1:-1]:
        kind = event["event"]
        node = event.get("node")
        if node in copies and kind not in {"copy_state", "routing_view"}:
            copies[node].last_effect_step = event["step"]

        if kind == "client_write_routed":
            request_id = event["request_id"]
            if request_id in requests:
                fail(event, f"request_id {request_id} was reused")
            requests[request_id] = event
            request_status[request_id] = "routed"
        elif kind == "wal_appended":
            observed = identity(event)
            prior = receipt_identity.setdefault(event["receipt_id"], observed)
            if prior != observed:
                fail(event, "receipt identity changed at WAL append")
            wal_durable[(event["node"], event["receipt_id"])] = event["durable"]
        elif kind == "operation_processed":
            copy = copies.setdefault(event["node"], CopyState())
            observed = identity(event)
            prior = receipt_identity.setdefault(event["receipt_id"], observed)
            if prior != observed:
                fail(event, "receipt identity changed at operation processing")
            request_id = event["request_id"]
            if event["origin"] == "primary" and request_id is not None:
                request_operations[request_id] = observed
                request_status[request_id] = "replicating"
            apply_observed_operation(event, copy, wal_durable)
            if event["origin"] == "live_replication":
                for message in messages.values():
                    if (
                        message["target"] == event["node"]
                        and message["identity"] == observed
                        and message["phase"] == "request"
                    ):
                        message["phase"] = (
                            "nack" if event["outcome"] in {"collision", "apply_failed"} else "ack"
                        )
                        break
        elif kind == "primary_replication_started":
            request_id = event["request_id"]
            required = set()
            for target in event["required_replicas"]:
                message_id = target["message_id"]
                if message_id in messages:
                    fail(event, f"message_id {message_id} was reused")
                messages[message_id] = {
                    "source": event["node"],
                    "target": target["node"],
                    "identity": Identity(
                        event["term"],
                        event["seq_no"],
                        request_operations[request_id].doc,
                        request_operations[request_id].op,
                        request_operations[request_id].content_hash,
                    ),
                    "phase": "request",
                }
                required.add(message_id)
            required_messages[request_id] = required
        elif kind == "replica_result":
            message_id = event["message_id"]
            message = messages.get(message_id)
            if message is None:
                fail(event, f"replica result references unknown message {message_id}")
            if event["message_phase"] != (message["phase"] or "none"):
                fail(event, "replica result message phase does not match observed delivery")
            message_results[message_id] = event["outcome"]
            message["phase"] = None
            if event["outcome"] in {"acknowledged", "failed"}:
                expected_persisted = checkpoint(copies[event["replica"]].persisted)
                if event["persisted_checkpoint"] != expected_persisted:
                    fail(
                        event,
                        "replica response persisted checkpoint does not match applied state",
                    )
        elif kind == "client_result":
            request_id = event["request_id"]
            if event["outcome"] == "acknowledged":
                missing = [
                    message_id
                    for message_id in required_messages.get(request_id, set())
                    if message_results.get(message_id) != "acknowledged"
                ]
                if missing:
                    fail(
                        event,
                        f"request acknowledged before replica acknowledgements: {sorted(missing)}",
                    )
                request_status[request_id] = "acknowledged"
            else:
                request_status[request_id] = "failed"
        elif kind == "fence_persisted":
            copy = copies.setdefault(event["node"], CopyState())
            if event["term"] < copy.fence_term:
                fail(
                    event,
                    f"fence regressed from {copy.fence_term} to {event['term']}",
                )
            copy.fence_term = event["term"]
        elif kind == "promotion_noop_fill":
            copy = copies.setdefault(event["node"], CopyState())
            for noop in event["noops"]:
                observed = Identity(
                    term=event["term"],
                    seq_no=noop["seq_no"],
                    doc=None,
                    op="noop",
                    content_hash=noop["content_hash"],
                )
                prior = copy.identities.get(observed.seq_no)
                if prior is not None and prior != observed:
                    fail(event, "promotion NoOp collides with an existing local identity")
                copy.identities[observed.seq_no] = observed
                receipt_identity[noop["receipt_id"]] = observed
                copy.processed.add(observed.seq_no)
                copy.persisted.add(observed.seq_no)
                copy.max_seq_no = max(copy.max_seq_no or 0, observed.seq_no)
            check_checkpoint_event(event, copy)
        elif kind == "promotion_noop_replication_started":
            observed = receipt_identity.get(event["receipt_id"])
            if observed is None:
                fail(event, "promotion NoOp send references an unknown receipt")
            message_id = event["message_id"]
            if message_id in messages:
                fail(event, f"message_id {message_id} was reused")
            messages[message_id] = {
                "source": event["node"],
                "target": event["replica"],
                "identity": observed,
                "phase": "request",
            }
        elif kind == "promotion_noop_result":
            message_id = event["message_id"]
            message = messages.get(message_id)
            if message is None:
                fail(event, f"NoOp result references unknown message {message_id}")
            if event["message_phase"] != (message["phase"] or "none"):
                fail(event, "NoOp result phase does not match observed delivery")
            message["phase"] = None
        elif kind == "commit_captured":
            copy = copies.setdefault(event["node"], CopyState())
            check_checkpoint_event(event, copy)
            pending_commits[event["commit_id"]] = {
                "node": event["node"],
                "checkpoints": event["checkpoints"],
                "documents": dict(copy.documents),
                "identities": dict(copy.identities),
            }
        elif kind == "commit_persisted":
            capture = pending_commits.pop(event["commit_id"], None)
            if capture is None or capture["node"] != event["node"]:
                fail(event, "commit persistence has no matching capture")
            persisted_commits[event["node"]] = capture
        elif kind == "node_crashed":
            expected_failed = sorted(
                request_id
                for request_id, status in request_status.items()
                if status == "replicating"
                and request_operations.get(request_id) is not None
                and requests[request_id]["target_node"] == event["node"]
            )
            if event["failed_request_ids"] != expected_failed:
                fail(event, "crash failed_request_ids do not match active requests")
            expected_dropped = []
            for message_id, message in messages.items():
                phase = message["phase"]
                if phase is None:
                    continue
                destination = message["target"] if phase == "request" else message["source"]
                if destination == event["node"]:
                    expected_dropped.append(
                        {"message_id": message_id, "message_phase": phase}
                    )
                    message["phase"] = None
            expected_dropped.sort(key=lambda item: (item["message_id"], item["message_phase"]))
            if event["dropped_messages"] != expected_dropped:
                fail(event, "crash dropped_messages do not match in-flight messages")
            for request_id in expected_failed:
                request_status[request_id] = "failed"
            alive[event["node"]] = False
        elif kind == "node_restarted":
            node = event["node"]
            expected_incarnation = nodes[node] + 1
            if event["incarnation"] != expected_incarnation:
                fail(event, f"restart incarnation must be {expected_incarnation}")
            nodes[node] = expected_incarnation
            alive[node] = True
            copy = copies.setdefault(node, CopyState())
            persisted = persisted_commits.get(node)
            if persisted is not None and event["checkpoints"] != persisted["checkpoints"]:
                fail(event, "restart checkpoints differ from the last persisted commit")
            processed_checkpoint = event["checkpoints"]["processed"]
            persisted_checkpoint = event["checkpoints"]["persisted"]
            copy.processed = (
                set(range(processed_checkpoint + 1))
                if processed_checkpoint is not None
                else set()
            )
            copy.persisted = (
                set(range(persisted_checkpoint + 1))
                if persisted_checkpoint is not None
                else set()
            )
            copy.max_seq_no = event["checkpoints"]["max_seq_no"]
            copy.documents = (
                dict(persisted["documents"]) if persisted is not None else {}
            )
            copy.identities = (
                {
                    seq_no: observed
                    for seq_no, observed in persisted["identities"].items()
                    if seq_no in copy.processed
                }
                if persisted is not None
                else {}
            )
        elif kind == "replay_started":
            check_checkpoint_event(event, copies[event["node"]])
        elif kind == "replay_entry":
            if event["outcome"] == "skip_committed":
                check_checkpoint_event(event, copies[event["node"]])
            else:
                apply_observed_operation(
                    event, copies[event["node"]], wal_durable, replay=True
                )
        elif kind == "routing_promoted":
            primary = event["new_primary"]
            in_sync = set(event["in_sync"])
        elif kind == "in_sync_removed":
            in_sync = set(event["in_sync"])
        elif kind == "copy_state":
            observed = normalized_documents(event["documents"])
            expected = {
                doc: (state, seq_no, term, content_hash)
                for doc, (state, seq_no, term, content_hash) in copies[
                    event["node"]
                ].documents.items()
            }
            all_docs = set(observed) | set(expected)
            for doc in all_docs:
                actual = observed.get(doc, ("absent", None, None, None))
                wanted = expected.get(doc, ("absent", None, None, None))
                if actual != wanted:
                    fail(
                        event,
                        f"copy_state for {event['node']} document {doc!r} is {actual}, expected {wanted}",
                    )
            final_copy_states[event["node"]] = (event["step"], observed)

    authoritative = {primary, *in_sync}
    for node in sorted(authoritative):
        if not alive.get(node, False):
            continue
        if node not in final_copy_states:
            raise InvariantViolation(
                events[-1]["step"],
                "trace_end",
                f"available authoritative node {node} has no final copy_state",
            )
        copy_step, _ = final_copy_states[node]
        if copy_step <= copies[node].last_effect_step:
            raise InvariantViolation(
                events[-1]["step"],
                "trace_end",
                f"copy_state for {node} precedes its last state-changing event",
            )

    if authoritative:
        reference_node = primary
        reference = final_copy_states.get(reference_node)
        if reference is None and alive.get(reference_node, False):
            raise InvariantViolation(
                events[-1]["step"],
                "trace_end",
                f"primary {reference_node} has no final copy_state",
            )
        if reference is not None:
            for node in sorted(in_sync):
                if not alive.get(node, False):
                    continue
                replica = final_copy_states.get(node)
                if replica is None:
                    continue
                if replica[1] != reference[1]:
                    raise InvariantViolation(
                        replica[0],
                        "copy_state",
                        f"in-sync replica {node} does not converge with primary {primary}",
                    )

    acknowledged = [
        request_id
        for request_id, status in request_status.items()
        if status == "acknowledged" and request_id in request_operations
    ]
    for node in sorted(authoritative):
        if not alive.get(node, False) or node not in final_copy_states:
            continue
        documents = final_copy_states[node][1]
        for request_id in acknowledged:
            operation = request_operations[request_id]
            if operation.doc is None:
                continue
            final = documents.get(operation.doc, ("absent", None, None, None))
            final_seq = final[1]
            if final_seq is None or final_seq < operation.seq_no:
                raise InvariantViolation(
                    final_copy_states[node][0],
                    "copy_state",
                    f"acknowledged request {request_id} at seq_no {operation.seq_no} is lost on {node}",
                )
            if final_seq == operation.seq_no:
                expected_state = "deleted" if operation.op == "delete" else "live"
                expected = (
                    expected_state,
                    operation.seq_no,
                    operation.term,
                    operation.content_hash,
                )
                if final != expected:
                    raise InvariantViolation(
                        final_copy_states[node][0],
                        "copy_state",
                        f"acknowledged request {request_id} has final identity {final}, expected {expected}",
                    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    try:
        check_trace(load_trace(args.trace))
    except InvariantViolation as error:
        print(
            f"Invariant violation at step {error.step} "
            f"(event {error.event}): {error}",
            file=sys.stderr,
        )
        return 1
    print(f"D1 trace invariants passed: {args.trace}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
