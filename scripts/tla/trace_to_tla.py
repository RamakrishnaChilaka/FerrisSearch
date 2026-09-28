#!/usr/bin/env python3
"""Validate FerrisSearch D1 JSONL and generate TraceInput.tla."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SCHEMA = "ferrissearch.d1.trace/v1"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")

START_FIELDS = {
    "schema",
    "run_id",
    "step",
    "event",
    "test",
    "durability",
    "initial_prefix_through",
    "nodes",
    "shard_state",
}
END_FIELDS = {
    "schema",
    "run_id",
    "step",
    "event",
    "outcome",
    "quiescent",
    "records_before_end",
}
COMMON_FIELDS = {
    "schema",
    "run_id",
    "step",
    "event",
    "node",
    "incarnation",
    "index_uuid",
    "shard",
    "allocation",
    "peer",
    "peer_incarnation",
    "peer_allocation",
    "request_id",
    "term",
    "seq_no",
    "doc",
    "op",
    "content_hash",
    "origin",
    "outcome",
    "checkpoints",
}
EVENT_EXTRA_FIELDS = {
    "client_write_routed": set(),
    "primary_assigned": {"routing_version", "required_replicas"},
    "client_result": {"failure_stage"},
    "wal_appended": {"durable"},
    "operation_applied": {"operation_processed", "operation_persisted"},
    "checkpoint_changed": {"previous_checkpoints", "cause"},
    "replica_received": {"rejection"},
    "replica_result": set(),
    "fence_persisted": {"fence_max_seq_no"},
    "commit_persisted": {"term_state", "commit_context"},
    "wal_truncated": {
        "truncate_through",
        "retained_min_seq_no",
        "retained_max_seq_no",
    },
    "node_crashed": set(),
    "node_restarted": set(),
    "replay_started": {"replay_id"},
    "replay_entry": {"replay_id", "replay_ordinal"},
    "replay_finished": {"replay_id", "entries_examined"},
    "routing_promoted": set(),
    "primary_activated": set(),
    "recovery_started": {"session_id", "snapshot_next_seq_no"},
    "recovery_installed": {
        "session_id",
        "snapshot_next_seq_no",
        "barrier_next_seq_no",
    },
    "recovery_membership": {"session_id"},
}
OUTCOMES = {
    "client_write_routed": {"routed"},
    "primary_assigned": {"assigned"},
    "client_result": {"acknowledged", "failed"},
    "wal_appended": {"appended"},
    "operation_applied": {
        "applied_newer",
        "stale",
        "redelivery",
        "noop",
        "collision",
        "apply_failed",
    },
    "checkpoint_changed": {"changed", "restored"},
    "replica_received": {"accepted", "rejected"},
    "replica_result": {"acknowledged", "failed", "timeout", "dropped"},
    "fence_persisted": {"raised"},
    "commit_persisted": {"persisted"},
    "wal_truncated": {"completed"},
    "node_crashed": {"unclean", "clean"},
    "node_restarted": {"started"},
    "replay_started": {"started"},
    "replay_entry": {
        "skip_committed",
        "applied_newer",
        "stale",
        "redelivery",
        "noop",
        "collision",
        "apply_failed",
    },
    "replay_finished": {"completed", "failed"},
    "routing_promoted": {"committed"},
    "primary_activated": {"activated"},
    "recovery_started": {"started"},
    "recovery_installed": {"pending_membership"},
    "recovery_membership": {"admitted", "promoted", "rejected", "unknown"},
}
ORIGINS = {
    "primary",
    "live_replication",
    "replay",
    "recovery",
    "promotion_noop_fill",
}
OPERATION_EVENTS = {
    "primary_assigned",
    "wal_appended",
    "operation_applied",
    "replica_received",
    "replica_result",
    "replay_entry",
}
CHECKPOINT_FIELDS = {"processed", "persisted", "max_seq_no"}


class TraceSchemaError(ValueError):
    """A schema error with an actionable line number."""


@dataclass(frozen=True)
class LoadedTrace:
    start: dict[str, Any]
    events: list[dict[str, Any]]
    end: dict[str, Any]


def fail(line: int, message: str) -> None:
    raise TraceSchemaError(f"line {line}: {message}")


def require_exact_fields(
    value: dict[str, Any], expected: set[str], line: int, context: str
) -> None:
    actual = set(value)
    unknown = sorted(actual - expected)
    missing = sorted(expected - actual)
    if unknown:
        fail(line, f"{context} has unknown field(s): {', '.join(unknown)}")
    if missing:
        fail(line, f"{context} is missing field(s): {', '.join(missing)}")


def require_bool(value: Any, line: int, field: str) -> bool:
    if not isinstance(value, bool):
        fail(line, f"{field} must be a boolean")
    return value


def require_str(value: Any, line: int, field: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value:
        fail(line, f"{field} must be a non-empty string")
    return value


def require_int(
    value: Any,
    line: int,
    field: str,
    *,
    nullable: bool = False,
    positive: bool = False,
) -> int | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        fail(line, f"{field} must be an integer")
    minimum = 1 if positive else 0
    if value < minimum:
        comparator = "positive" if positive else "non-negative"
        fail(line, f"{field} must be {comparator}")
    return value


def validate_checkpoints(value: Any, line: int, field: str) -> dict[str, int | None]:
    if not isinstance(value, dict):
        fail(line, f"{field} must be an object")
    require_exact_fields(value, CHECKPOINT_FIELDS, line, field)
    processed = require_int(value["processed"], line, f"{field}.processed", nullable=True)
    persisted = require_int(value["persisted"], line, f"{field}.persisted", nullable=True)
    maximum = require_int(value["max_seq_no"], line, f"{field}.max_seq_no", nullable=True)
    if persisted is not None and processed is None:
        fail(line, f"{field}.persisted requires a processed checkpoint")
    if processed is not None and maximum is None:
        fail(line, f"{field}.processed requires max_seq_no")
    if persisted is not None and processed is not None and persisted > processed:
        fail(line, f"{field}.persisted exceeds processed")
    if processed is not None and maximum is not None and processed > maximum:
        fail(line, f"{field}.processed exceeds max_seq_no")
    return {"processed": processed, "persisted": persisted, "max_seq_no": maximum}


def validate_nodes(value: Any, line: int) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) < 1:
        fail(line, "nodes must be a non-empty array")
    seen: set[str] = set()
    nodes: list[dict[str, Any]] = []
    for offset, node in enumerate(value):
        if not isinstance(node, dict):
            fail(line, f"nodes[{offset}] must be an object")
        require_exact_fields(node, {"node", "incarnation"}, line, f"nodes[{offset}]")
        node_id = require_str(node["node"], line, f"nodes[{offset}].node")
        incarnation = require_int(
            node["incarnation"], line, f"nodes[{offset}].incarnation"
        )
        assert node_id is not None and incarnation is not None
        if node_id in seen:
            fail(line, f"duplicate node {node_id!r}")
        seen.add(node_id)
        nodes.append({"node": node_id, "incarnation": incarnation})
    return nodes


def validate_start(value: dict[str, Any], line: int) -> dict[str, Any]:
    require_exact_fields(value, START_FIELDS, line, "trace_start")
    if value["schema"] != SCHEMA:
        fail(line, f"unsupported schema {value['schema']!r}; expected {SCHEMA!r}")
    require_str(value["run_id"], line, "run_id")
    if require_int(value["step"], line, "step") != 0:
        fail(line, "trace_start.step must be zero")
    if value["event"] != "trace_start":
        fail(line, "first record must be trace_start")
    require_str(value["test"], line, "test")
    if value["durability"] not in {"request", "async"}:
        fail(line, "durability must be 'request' or 'async'")
    prefix = require_int(
        value["initial_prefix_through"],
        line,
        "initial_prefix_through",
        nullable=True,
    )
    nodes = validate_nodes(value["nodes"], line)
    node_ids = {node["node"] for node in nodes}

    shard = value["shard_state"]
    if not isinstance(shard, dict):
        fail(line, "shard_state must be an object")
    require_exact_fields(
        shard,
        {
            "index_uuid",
            "shard",
            "primary",
            "term",
            "activated",
            "in_sync",
            "copies",
        },
        line,
        "shard_state",
    )
    require_str(shard["index_uuid"], line, "shard_state.index_uuid")
    require_int(shard["shard"], line, "shard_state.shard")
    primary = require_str(shard["primary"], line, "shard_state.primary")
    if primary not in node_ids:
        fail(line, "shard_state.primary is not in nodes")
    require_int(shard["term"], line, "shard_state.term", positive=True)
    require_bool(shard["activated"], line, "shard_state.activated")
    in_sync = shard["in_sync"]
    if not isinstance(in_sync, list) or any(not isinstance(node, str) for node in in_sync):
        fail(line, "shard_state.in_sync must be an array of node IDs")
    if len(in_sync) != len(set(in_sync)):
        fail(line, "shard_state.in_sync contains duplicates")
    if primary in in_sync or not set(in_sync).issubset(node_ids):
        fail(line, "shard_state.in_sync must contain only non-primary declared nodes")

    copies = shard["copies"]
    if not isinstance(copies, list) or len(copies) != len(nodes):
        fail(line, "shard_state.copies must contain exactly one entry per node")
    copy_nodes: set[str] = set()
    for offset, copy in enumerate(copies):
        if not isinstance(copy, dict):
            fail(line, f"shard_state.copies[{offset}] must be an object")
        require_exact_fields(
            copy,
            {
                "node",
                "allocation",
                "exists",
                "fence_term",
                "fence_max_seq_no",
                "checkpoints",
            },
            line,
            f"shard_state.copies[{offset}]",
        )
        node_id = require_str(copy["node"], line, f"shard_state.copies[{offset}].node")
        if node_id not in node_ids or node_id in copy_nodes:
            fail(line, f"invalid or duplicate copy node {node_id!r}")
        copy_nodes.add(node_id)
        require_int(
            copy["allocation"],
            line,
            f"shard_state.copies[{offset}].allocation",
            positive=True,
        )
        exists = require_bool(
            copy["exists"], line, f"shard_state.copies[{offset}].exists"
        )
        require_int(
            copy["fence_term"],
            line,
            f"shard_state.copies[{offset}].fence_term",
        )
        require_int(
            copy["fence_max_seq_no"],
            line,
            f"shard_state.copies[{offset}].fence_max_seq_no",
            nullable=True,
        )
        checkpoints = validate_checkpoints(
            copy["checkpoints"], line, f"shard_state.copies[{offset}].checkpoints"
        )
        expected = prefix if exists else None
        if checkpoints != {
            "processed": expected,
            "persisted": expected,
            "max_seq_no": expected,
        }:
            fail(
                line,
                "initial copy checkpoints must equal initial_prefix_through "
                "for existing copies and be null for missing copies",
            )
    return value


def validate_required_replicas(
    value: Any, line: int, node_ids: set[str], incarnations: dict[str, int]
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        fail(line, "required_replicas must be an array")
    result: list[dict[str, Any]] = []
    previous = ""
    seen: set[str] = set()
    for offset, replica in enumerate(value):
        if not isinstance(replica, dict):
            fail(line, f"required_replicas[{offset}] must be an object")
        require_exact_fields(
            replica,
            {"node", "incarnation", "allocation"},
            line,
            f"required_replicas[{offset}]",
        )
        node = require_str(replica["node"], line, f"required_replicas[{offset}].node")
        incarnation = require_int(
            replica["incarnation"], line, f"required_replicas[{offset}].incarnation"
        )
        allocation = require_int(
            replica["allocation"],
            line,
            f"required_replicas[{offset}].allocation",
            positive=True,
        )
        assert node is not None and incarnation is not None and allocation is not None
        if node not in node_ids or node in seen:
            fail(line, f"invalid or duplicate required replica {node!r}")
        if previous and node <= previous:
            fail(line, "required_replicas must be sorted by node ID")
        if incarnations[node] != incarnation:
            fail(line, f"required replica {node!r} has a stale incarnation")
        previous = node
        seen.add(node)
        result.append(
            {"node": node, "incarnation": incarnation, "allocation": allocation}
        )
    return result


def validate_term_state(value: Any, line: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(line, "term_state must be an object")
    require_exact_fields(
        value,
        {
            "current_term",
            "max_seq_no_at_term_start",
            "processed_in_current_term_below_start_max",
        },
        line,
        "term_state",
    )
    require_int(value["current_term"], line, "term_state.current_term", positive=True)
    maximum = require_int(
        value["max_seq_no_at_term_start"],
        line,
        "term_state.max_seq_no_at_term_start",
        nullable=True,
    )
    ranges = value["processed_in_current_term_below_start_max"]
    if not isinstance(ranges, list):
        fail(line, "term_state.processed_in_current_term_below_start_max must be an array")
    previous_end: int | None = None
    for offset, item in enumerate(ranges):
        if not isinstance(item, dict):
            fail(line, f"term_state range {offset} must be an object")
        require_exact_fields(item, {"start", "end"}, line, f"term_state range {offset}")
        start = require_int(item["start"], line, f"term_state range {offset}.start")
        end = require_int(item["end"], line, f"term_state range {offset}.end")
        assert start is not None and end is not None
        if start > end:
            fail(line, f"term_state range {offset} starts after it ends")
        if previous_end is not None and start <= previous_end + 1:
            fail(line, "term_state ranges must be sorted, disjoint, and non-adjacent")
        if maximum is None or end > maximum:
            fail(line, "term_state range exceeds max_seq_no_at_term_start")
        previous_end = end
    return value


def validate_protocol_event(
    value: dict[str, Any],
    line: int,
    start: dict[str, Any],
    incarnations: dict[str, int],
    alive: dict[str, bool],
) -> dict[str, Any]:
    event = value.get("event")
    if event not in EVENT_EXTRA_FIELDS:
        fail(line, f"unknown event {event!r}")
    require_exact_fields(
        value, COMMON_FIELDS | EVENT_EXTRA_FIELDS[event], line, event
    )
    if value["schema"] != SCHEMA:
        fail(line, f"unsupported schema {value['schema']!r}; expected {SCHEMA!r}")
    if value["run_id"] != start["run_id"]:
        fail(line, "run_id does not match trace_start")
    require_int(value["step"], line, "step")
    node_ids = {node["node"] for node in start["nodes"]}
    node = require_str(value["node"], line, "node")
    if node not in node_ids:
        fail(line, f"node {node!r} is not declared")
    incarnation = require_int(value["incarnation"], line, "incarnation")
    assert incarnation is not None
    if event == "node_restarted":
        if alive[node]:
            fail(line, f"node {node!r} restarted while still alive")
        if incarnation != incarnations[node] + 1:
            fail(line, f"node {node!r} restart incarnation must increment by one")
    elif incarnation != incarnations[node]:
        fail(line, f"node {node!r} has unexpected incarnation {incarnation}")
    elif not alive[node] and event != "node_restarted":
        fail(line, f"node {node!r} emitted after crashing in this incarnation")

    shard_state = start["shard_state"]
    if value["index_uuid"] != shard_state["index_uuid"]:
        fail(line, "index_uuid does not match trace_start")
    if value["shard"] != shard_state["shard"]:
        fail(line, "shard does not match trace_start")
    require_int(value["allocation"], line, "allocation", nullable=True, positive=True)
    peer = require_str(value["peer"], line, "peer", nullable=True)
    if peer is not None and peer not in node_ids:
        fail(line, f"peer {peer!r} is not declared")
    require_int(
        value["peer_incarnation"], line, "peer_incarnation", nullable=True
    )
    require_int(
        value["peer_allocation"],
        line,
        "peer_allocation",
        nullable=True,
        positive=True,
    )
    require_str(value["request_id"], line, "request_id", nullable=True)
    require_int(value["term"], line, "term", nullable=True, positive=True)
    require_int(value["seq_no"], line, "seq_no", nullable=True)
    require_str(value["doc"], line, "doc", nullable=True)
    if value["op"] not in {None, "index", "delete", "noop"}:
        fail(line, "op must be index, delete, noop, or null")
    content_hash = require_str(
        value["content_hash"], line, "content_hash", nullable=True
    )
    if content_hash is not None and not HASH_RE.fullmatch(content_hash):
        fail(line, "content_hash must be 64 lowercase hexadecimal characters")
    if value["origin"] not in ORIGINS | {None}:
        fail(line, f"unknown origin {value['origin']!r}")
    if value["outcome"] not in OUTCOMES[event]:
        fail(line, f"unknown {event} outcome {value['outcome']!r}")
    validate_checkpoints(value["checkpoints"], line, "checkpoints")

    if event in OPERATION_EVENTS:
        for field in ("term", "seq_no", "op", "content_hash"):
            if value[field] is None:
                fail(line, f"{event}.{field} must not be null")
        if value["op"] == "noop":
            if value["doc"] is not None:
                fail(line, f"{event}.doc must be null for a NoOp")
        elif value["doc"] is None:
            fail(line, f"{event}.doc must not be null")

    if event == "client_write_routed":
        for field in (
            "peer",
            "peer_allocation",
            "request_id",
            "term",
            "doc",
            "op",
            "content_hash",
        ):
            if value[field] is None:
                fail(line, f"{event}.{field} must not be null")
    elif event == "primary_assigned":
        if value["request_id"] is None or value["origin"] != "primary":
            fail(line, "primary_assigned requires request_id and origin=primary")
        require_int(value["routing_version"], line, "routing_version")
        value["required_replicas"] = validate_required_replicas(
            value["required_replicas"], line, node_ids, incarnations
        )
    elif event == "client_result":
        if value["request_id"] is None:
            fail(line, "client_result.request_id must not be null")
        allowed_stages = {
            None,
            "routing",
            "activation",
            "validation",
            "primary_apply",
            "replication",
        }
        if value["failure_stage"] not in allowed_stages:
            fail(line, "client_result.failure_stage is invalid")
        if value["outcome"] == "acknowledged" and value["failure_stage"] is not None:
            fail(line, "acknowledged client_result must have null failure_stage")
        if value["outcome"] == "failed" and value["failure_stage"] is None:
            fail(line, "failed client_result requires failure_stage")
    elif event == "wal_appended":
        require_bool(value["durable"], line, "durable")
    elif event == "operation_applied":
        require_bool(value["operation_processed"], line, "operation_processed")
        require_bool(value["operation_persisted"], line, "operation_persisted")
        if value["origin"] == "replay":
            fail(line, "replay planner outcomes must use replay_entry")
    elif event == "checkpoint_changed":
        validate_checkpoints(value["previous_checkpoints"], line, "previous_checkpoints")
        if value["cause"] not in {
            "apply",
            "commit",
            "replay",
            "recovery",
            "restart",
        }:
            fail(line, "checkpoint_changed.cause is invalid")
    elif event == "replica_received":
        if value["peer"] is None or value["peer_incarnation"] is None:
            fail(line, "replica_received requires peer and peer_incarnation")
        if value["origin"] != "live_replication":
            fail(line, "replica_received requires origin=live_replication")
        allowed = {None} if value["outcome"] == "accepted" else {
            "uuid",
            "allocation",
            "term",
            "recovery_install",
            "validation",
            "storage",
        }
        if value["rejection"] not in allowed:
            fail(line, "replica_received.rejection does not match outcome")
    elif event == "fence_persisted":
        require_int(
            value["fence_max_seq_no"],
            line,
            "fence_max_seq_no",
            nullable=True,
        )
    elif event == "commit_persisted":
        validate_term_state(value["term_state"], line)
        if value["commit_context"] not in {
            "refresh",
            "flush",
            "force_merge",
            "replay",
            "recovery",
        }:
            fail(line, "commit_context is invalid")
    elif event == "wal_truncated":
        require_int(value["truncate_through"], line, "truncate_through")
        retained_min = require_int(
            value["retained_min_seq_no"],
            line,
            "retained_min_seq_no",
            nullable=True,
        )
        retained_max = require_int(
            value["retained_max_seq_no"],
            line,
            "retained_max_seq_no",
            nullable=True,
        )
        if (retained_min is None) != (retained_max is None):
            fail(line, "retained WAL minimum and maximum must both be null or both present")
        if (
            retained_min is not None
            and retained_max is not None
            and retained_min > retained_max
        ):
            fail(line, "retained WAL minimum exceeds maximum")
    elif event in {"replay_started", "replay_entry", "replay_finished"}:
        require_str(value["replay_id"], line, "replay_id")
        if event == "replay_entry":
            require_int(value["replay_ordinal"], line, "replay_ordinal")
            if value["origin"] != "replay":
                fail(line, "replay_entry requires origin=replay")
        elif event == "replay_finished":
            require_int(value["entries_examined"], line, "entries_examined")
    elif event == "recovery_started":
        require_str(value["session_id"], line, "session_id")
        require_int(value["snapshot_next_seq_no"], line, "snapshot_next_seq_no")
    elif event == "recovery_installed":
        require_str(value["session_id"], line, "session_id")
        require_int(value["snapshot_next_seq_no"], line, "snapshot_next_seq_no")
        require_int(value["barrier_next_seq_no"], line, "barrier_next_seq_no")
    elif event == "recovery_membership":
        require_str(value["session_id"], line, "session_id")

    if event == "node_crashed":
        if not alive[node]:
            fail(line, f"node {node!r} crashed twice")
        alive[node] = False
    elif event == "node_restarted":
        incarnations[node] = incarnation
        alive[node] = True
    return value


def load_trace(path: Path) -> LoadedTrace:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            if not raw_line.endswith("\n") and raw_line:
                fail(line_number, "JSONL record is missing a terminating newline")
            text = raw_line.strip()
            if not text:
                fail(line_number, "blank lines are not permitted")
            try:
                value = json.loads(text)
            except json.JSONDecodeError as error:
                fail(line_number, f"invalid JSON: {error.msg}")
            if not isinstance(value, dict):
                fail(line_number, "record must be a JSON object")
            records.append(value)
    if len(records) < 2:
        raise TraceSchemaError("trace must contain trace_start and trace_end")

    start = validate_start(records[0], 1)
    node_ids = [node["node"] for node in start["nodes"]]
    incarnations = {node["node"]: node["incarnation"] for node in start["nodes"]}
    alive = {node: True for node in node_ids}
    previous_step = 0
    events: list[dict[str, Any]] = []
    operation_identity: dict[tuple[int, int], tuple[str | None, str, str]] = {}
    requests: dict[str, dict[str, Any]] = {}
    accepted_receives: set[tuple[str, tuple[int, int]]] = set()
    successful_applies: set[tuple[str, tuple[int, int]]] = set()
    replica_acks: dict[str, set[str]] = {}
    replay_ordinals: dict[tuple[str, str], int] = {}

    for index, value in enumerate(records[1:-1], 2):
        event = validate_protocol_event(
            value, index, start, incarnations, alive
        )
        step = event["step"]
        if step <= previous_step:
            fail(index, "step must be strictly increasing")
        previous_step = step

        if event["term"] is not None and event["seq_no"] is not None:
            identity = (event["term"], event["seq_no"])
            content = (event["doc"], event["op"], event["content_hash"])
            previous = operation_identity.setdefault(identity, content)
            if previous != content:
                fail(index, f"operation identity {identity} changed content")
        else:
            identity = None

        if event["event"] == "client_write_routed":
            request_id = event["request_id"]
            assert request_id is not None
            if request_id in requests:
                fail(index, f"request_id {request_id!r} was reused")
            requests[request_id] = {"required": set(), "key": None, "acks": set()}
        elif event["event"] == "primary_assigned":
            request_id = event["request_id"]
            assert request_id is not None and identity is not None
            if request_id not in requests:
                fail(index, f"primary_assigned references unknown request {request_id!r}")
            requests[request_id]["key"] = identity
            requests[request_id]["required"] = {
                replica["node"] for replica in event["required_replicas"]
            }
            replica_acks.setdefault(request_id, set())
        elif event["event"] == "replica_received" and event["outcome"] == "accepted":
            assert identity is not None
            accepted_receives.add((event["node"], identity))
        elif event["event"] == "operation_applied":
            assert identity is not None
            if event["outcome"] in {"applied_newer", "stale", "redelivery", "noop"}:
                successful_applies.add((event["node"], identity))
        elif event["event"] == "replica_result" and event["outcome"] == "acknowledged":
            assert identity is not None and event["peer"] is not None
            if (event["peer"], identity) not in accepted_receives:
                fail(index, "replica acknowledgement has no accepted receive")
            if (event["peer"], identity) not in successful_applies:
                fail(index, "replica acknowledgement has no successful planner outcome")
            matching = [
                request_id
                for request_id, request in requests.items()
                if request["key"] == identity
            ]
            if len(matching) != 1:
                fail(index, "replica acknowledgement cannot be matched to one request")
            replica_acks[matching[0]].add(event["peer"])
        elif event["event"] == "client_result":
            request_id = event["request_id"]
            assert request_id is not None
            if request_id not in requests:
                fail(index, f"client_result references unknown request {request_id!r}")
            if event["outcome"] == "acknowledged":
                missing = requests[request_id]["required"] - replica_acks.get(
                    request_id, set()
                )
                if missing:
                    fail(
                        index,
                        "client acknowledgement is missing replica acknowledgement(s): "
                        + ", ".join(sorted(missing)),
                    )
        elif event["event"] == "replay_entry":
            replay_key = (event["node"], event["replay_id"])
            expected = replay_ordinals.get(replay_key, 0)
            if event["replay_ordinal"] != expected:
                fail(index, f"replay ordinal must be {expected}")
            replay_ordinals[replay_key] = expected + 1
        events.append(event)

    end_line = len(records)
    end = records[-1]
    require_exact_fields(end, END_FIELDS, end_line, "trace_end")
    if end["schema"] != SCHEMA:
        fail(end_line, f"unsupported schema {end['schema']!r}; expected {SCHEMA!r}")
    if end["run_id"] != start["run_id"]:
        fail(end_line, "trace_end.run_id does not match trace_start")
    end_step = require_int(end["step"], end_line, "trace_end.step")
    assert end_step is not None
    if end_step <= previous_step:
        fail(end_line, "trace_end.step must be greater than every protocol step")
    if end["event"] != "trace_end" or end["outcome"] != "completed":
        fail(end_line, "last record must be completed trace_end")
    require_bool(end["quiescent"], end_line, "trace_end.quiescent")
    if require_int(
        end["records_before_end"], end_line, "trace_end.records_before_end"
    ) != len(records) - 1:
        fail(end_line, "trace_end.records_before_end is incorrect")
    return LoadedTrace(start=start, events=events, end=end)


def checkpoint_next(value: int | None) -> int:
    return 0 if value is None else value + 1


def tla_set(values: Iterable[str]) -> str:
    values = list(values)
    return "{}" if not values else "{" + ", ".join(values) + "}"


def tla_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def function_case(domain: str, variable: str, values: dict[str, str]) -> str:
    items = list(values.items())
    if not items:
        return f"[{variable} \\in {domain} |-> 0]"
    branches = " [] ".join(
        f"{variable} = {key} -> {value}" for key, value in items[:-1]
    )
    if branches:
        branches += f" [] OTHER -> {items[-1][1]}"
    else:
        branches = items[-1][1]
    if len(items) == 1:
        return f"[{variable} \\in {domain} |-> {items[0][1]}]"
    return f"[{variable} \\in {domain} |-> CASE {branches}]"


class TokenMap:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.values: dict[str, str] = {}

    def token(self, value: str) -> str:
        if value not in self.values:
            self.values[value] = tla_string(f"{self.prefix}{len(self.values)}")
        return self.values[value]

    def tokens(self) -> list[str]:
        return list(self.values.values())


def expand_ranges(ranges: list[dict[str, int]]) -> set[int]:
    values: set[int] = set()
    for item in ranges:
        values.update(range(item["start"], item["end"] + 1))
    return values


def render_trace_input(trace: LoadedTrace) -> str:
    node_tokens = TokenMap("N")
    doc_tokens = TokenMap("D")
    request_tokens = TokenMap("R")
    hash_tokens = TokenMap("H")
    replay_tokens = TokenMap("P")
    op_tokens: dict[tuple[int, int], str] = {}
    op_content: dict[tuple[int, int], tuple[str | None, str, str]] = {}

    for node in trace.start["nodes"]:
        node_tokens.token(node["node"])
    for event in trace.events:
        if event["doc"] is not None:
            doc_tokens.token(event["doc"])
        if event["request_id"] is not None:
            request_tokens.token(event["request_id"])
        if event["content_hash"] is not None:
            hash_tokens.token(event["content_hash"])
        if "replay_id" in event:
            replay_tokens.token(event["replay_id"])
        if event["term"] is not None and event["seq_no"] is not None:
            identity = (event["term"], event["seq_no"])
            op_tokens.setdefault(identity, tla_string(f"K{len(op_tokens)}"))
            op_content.setdefault(
                identity,
                (event["doc"], event["op"], event["content_hash"]),
            )

    nodes = node_tokens.tokens()
    docs = doc_tokens.tokens()
    requests = request_tokens.tokens()
    hashes = hash_tokens.tokens()
    replay_ids = replay_tokens.tokens()
    op_keys = list(op_tokens.values())

    def node_token(value: str | None) -> str:
        return tla_string("NO_NODE") if value is None else node_tokens.token(value)

    def doc_token(value: str | None) -> str:
        return tla_string("NO_DOC") if value is None else doc_tokens.token(value)

    def request_token(value: str | None) -> str:
        return tla_string("NO_REQUEST") if value is None else request_tokens.token(value)

    def hash_token(value: str | None) -> str:
        return tla_string("NO_HASH") if value is None else hash_tokens.token(value)

    def replay_token(value: str | None) -> str:
        return tla_string("NO_REPLAY") if value is None else replay_tokens.token(value)

    def op_token(event: dict[str, Any]) -> str:
        if event["term"] is None or event["seq_no"] is None:
            return tla_string("NO_KEY")
        return op_tokens[(event["term"], event["seq_no"])]

    max_values = [
        trace.start["initial_prefix_through"]
        if trace.start["initial_prefix_through"] is not None
        else 0
    ]
    for copy in trace.start["shard_state"]["copies"]:
        max_values.extend(
            value
            for value in (
                copy["fence_max_seq_no"],
                copy["checkpoints"]["max_seq_no"],
            )
            if value is not None
        )
    for event in trace.events:
        max_values.extend(
            value
            for value in (
                event["seq_no"],
                event["checkpoints"]["max_seq_no"],
                event.get("fence_max_seq_no"),
                event.get("truncate_through"),
            )
            if value is not None
        )
        if "term_state" in event:
            maximum = event["term_state"]["max_seq_no_at_term_start"]
            if maximum is not None:
                max_values.append(maximum)
    max_seq = max(max_values)

    shard = trace.start["shard_state"]
    copies = {copy["node"]: copy for copy in shard["copies"]}
    initial_alive = {
        node_tokens.token(node["node"]): "TRUE" for node in trace.start["nodes"]
    }
    initial_incarnation = {
        node_tokens.token(node["node"]): str(node["incarnation"])
        for node in trace.start["nodes"]
    }
    initial_allocation = {
        node_tokens.token(node): str(copy["allocation"])
        for node, copy in copies.items()
    }
    initial_exists = {
        node_tokens.token(node): "TRUE" if copy["exists"] else "FALSE"
        for node, copy in copies.items()
    }
    initial_fence_term = {
        node_tokens.token(node): str(copy["fence_term"])
        for node, copy in copies.items()
    }
    initial_fence_max = {
        node_tokens.token(node): str(checkpoint_next(copy["fence_max_seq_no"]))
        for node, copy in copies.items()
    }
    initial_processed_next = {
        node_tokens.token(node): str(checkpoint_next(copy["checkpoints"]["processed"]))
        for node, copy in copies.items()
    }
    initial_persisted_next = {
        node_tokens.token(node): str(checkpoint_next(copy["checkpoints"]["persisted"]))
        for node, copy in copies.items()
    }
    initial_max_next = {
        node_tokens.token(node): str(checkpoint_next(copy["checkpoints"]["max_seq_no"]))
        for node, copy in copies.items()
    }
    initial_processed = {
        node_tokens.token(node): (
            "{}"
            if copy["checkpoints"]["processed"] is None
            else f"0..{copy['checkpoints']['processed']}"
        )
        for node, copy in copies.items()
    }
    initial_persisted = {
        node_tokens.token(node): (
            "{}"
            if copy["checkpoints"]["persisted"] is None
            else f"0..{copy['checkpoints']['persisted']}"
        )
        for node, copy in copies.items()
    }
    initial_activated = {
        node_tokens.token(node["node"]): (
            str(shard["term"])
            if shard["activated"] and node["node"] == shard["primary"]
            else "0"
        )
        for node in trace.start["nodes"]
    }

    op_term = {
        token: str(identity[0]) for identity, token in op_tokens.items()
    }
    op_seq = {
        token: str(identity[1]) for identity, token in op_tokens.items()
    }
    op_doc = {
        op_tokens[identity]: doc_token(content[0])
        for identity, content in op_content.items()
    }
    op_kind = {
        op_tokens[identity]: tla_string(content[1])
        for identity, content in op_content.items()
    }
    op_hash = {
        op_tokens[identity]: hash_token(content[2])
        for identity, content in op_content.items()
    }

    event_records: list[str] = []
    for event in trace.events:
        required = event.get("required_replicas", [])
        required_nodes = {
            node_tokens.token(replica["node"]) for replica in required
        }
        required_allocations = {
            node_tokens.token(node["node"]): "0" for node in trace.start["nodes"]
        }
        required_incarnations = dict(required_allocations)
        for replica in required:
            token = node_tokens.token(replica["node"])
            required_allocations[token] = str(replica["allocation"])
            required_incarnations[token] = str(replica["incarnation"])

        checkpoints = event["checkpoints"]
        previous = event.get(
            "previous_checkpoints",
            {"processed": None, "persisted": None, "max_seq_no": None},
        )
        term_state = event.get(
            "term_state",
            {
                "current_term": 0,
                "max_seq_no_at_term_start": None,
                "processed_in_current_term_below_start_max": [],
            },
        )
        term_processed = expand_ranges(
            term_state["processed_in_current_term_below_start_max"]
        )
        fields = [
            ("step", str(event["step"])),
            ("kind", tla_string(event["event"])),
            ("node", node_token(event["node"])),
            ("incarnation", str(event["incarnation"])),
            ("allocation", str(event["allocation"] or 0)),
            ("peer", node_token(event["peer"])),
            ("peerIncarnation", str(event["peer_incarnation"] or 0)),
            ("peerAllocation", str(event["peer_allocation"] or 0)),
            ("request", request_token(event["request_id"])),
            ("term", str(event["term"] or 0)),
            ("seq", str(event["seq_no"] or 0)),
            ("key", op_token(event)),
            ("doc", doc_token(event["doc"])),
            ("op", tla_string(event["op"] or "none")),
            ("hash", hash_token(event["content_hash"])),
            ("origin", tla_string(event["origin"] or "none")),
            ("outcome", tla_string(event["outcome"])),
            ("cpProcessedNext", str(checkpoint_next(checkpoints["processed"]))),
            ("cpPersistedNext", str(checkpoint_next(checkpoints["persisted"]))),
            ("cpMaxNext", str(checkpoint_next(checkpoints["max_seq_no"]))),
            (
                "operationProcessed",
                "TRUE" if event.get("operation_processed", False) else "FALSE",
            ),
            (
                "operationPersisted",
                "TRUE" if event.get("operation_persisted", False) else "FALSE",
            ),
            ("durable", "TRUE" if event.get("durable", False) else "FALSE"),
            ("required", tla_set(sorted(required_nodes))),
            (
                "requiredAllocation",
                function_case(
                    "Nodes", "requiredNode", required_allocations
                ),
            ),
            (
                "requiredIncarnation",
                function_case(
                    "Nodes", "requiredNode", required_incarnations
                ),
            ),
            ("routingVersion", str(event.get("routing_version", 0))),
            ("failureStage", tla_string(event.get("failure_stage") or "none")),
            ("rejection", tla_string(event.get("rejection") or "none")),
            ("fenceMaxNext", str(checkpoint_next(event.get("fence_max_seq_no")))),
            (
                "prevProcessedNext",
                str(checkpoint_next(previous["processed"])),
            ),
            (
                "prevPersistedNext",
                str(checkpoint_next(previous["persisted"])),
            ),
            ("prevMaxNext", str(checkpoint_next(previous["max_seq_no"]))),
            ("cause", tla_string(event.get("cause") or "none")),
            ("termStateCurrent", str(term_state["current_term"])),
            (
                "termStateMaxNext",
                str(checkpoint_next(term_state["max_seq_no_at_term_start"])),
            ),
            (
                "termStateProcessed",
                tla_set(str(value) for value in sorted(term_processed)),
            ),
            (
                "commitContext",
                tla_string(event.get("commit_context") or "none"),
            ),
            (
                "truncateThroughNext",
                str(checkpoint_next(event.get("truncate_through"))),
            ),
            ("replayId", replay_token(event.get("replay_id"))),
            ("replayOrdinal", str(event.get("replay_ordinal", 0))),
            ("entriesExamined", str(event.get("entries_examined", 0))),
            ("snapshotNext", str(event.get("snapshot_next_seq_no", 0))),
            ("barrierNext", str(event.get("barrier_next_seq_no", 0))),
        ]
        rendered = ",\n      ".join(f"{name} |-> {value}" for name, value in fields)
        event_records.append("    [" + rendered + "]")

    trace_body = (
        "<<>>"
        if not event_records
        else "<<\n" + ",\n".join(event_records) + "\n>>"
    )
    event_kinds = {tla_string(event["event"]) for event in trace.events}

    return f"""---------------------------- MODULE TraceInput ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

Nodes == {tla_set(nodes)}
Docs == {tla_set(docs)}
Requests == {tla_set(requests)}
Hashes == {tla_set(hashes)}
ReplayIds == {tla_set(replay_ids)}
OpKeys == {tla_set(op_keys)}
EventKinds == {tla_set(sorted(event_kinds))}
MaxSeqBound == {max_seq}
Seqs == 0..MaxSeqBound

InitialPrimary == {node_tokens.token(shard["primary"])}
InitialPrimaryTerm == {shard["term"]}
InitialInSync == {tla_set(node_tokens.token(node) for node in shard["in_sync"])}
InitialAlive == {function_case("Nodes", "node", initial_alive)}
InitialIncarnation == {function_case("Nodes", "node", initial_incarnation)}
InitialAllocation == {function_case("Nodes", "node", initial_allocation)}
InitialCopyExists == {function_case("Nodes", "node", initial_exists)}
InitialActivatedTerm == {function_case("Nodes", "node", initial_activated)}
InitialFenceTerm == {function_case("Nodes", "node", initial_fence_term)}
InitialFenceMaxNext == {function_case("Nodes", "node", initial_fence_max)}
InitialProcessed == {function_case("Nodes", "node", initial_processed)}
InitialPersisted == {function_case("Nodes", "node", initial_persisted)}
InitialProcessedNext == {function_case("Nodes", "node", initial_processed_next)}
InitialPersistedNext == {function_case("Nodes", "node", initial_persisted_next)}
InitialMaxNext == {function_case("Nodes", "node", initial_max_next)}
InitialDocKey == [node \\in Nodes |-> [doc \\in Docs |-> "NO_KEY"]]
InitialDocSeqNext == [node \\in Nodes |-> [doc \\in Docs |-> 0]]

OpTerm == {function_case("OpKeys", "key", op_term)}
OpSeq == {function_case("OpKeys", "key", op_seq)}
OpDoc == {function_case("OpKeys", "key", op_doc)}
OpKind == {function_case("OpKeys", "key", op_kind)}
OpHash == {function_case("OpKeys", "key", op_hash)}

Trace == {trace_body}

=============================================================================
"""


def convert(trace_path: Path, output_path: Path) -> LoadedTrace:
    trace = load_trace(trace_path)
    rendered = render_trace_input(trace)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(output_path)
    return trace


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate D1 JSONL and generate a TLC TraceInput module"
    )
    parser.add_argument("trace", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="path to write TraceInput.tla",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        trace = convert(args.trace, args.output)
    except (OSError, TraceSchemaError) as error:
        print(f"trace conversion failed: {error}", file=sys.stderr)
        return 2
    print(
        f"converted {len(trace.events)} protocol events from {args.trace} "
        f"to {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
