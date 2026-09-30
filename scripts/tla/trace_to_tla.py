#!/usr/bin/env python3
"""Validate schema-v4 D1 JSONL and generate TLC trace inputs."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA = "ferrissearch.d1.trace/v4"
HASH_RE = re.compile(r"^[0-9a-f]{64}$")

START_FIELDS = {
    "schema",
    "run_id",
    "step",
    "event",
    "test",
    "durability",
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
EVENT_FIELDS = {
    "client_write_routed": {
        "node",
        "index_uuid",
        "shard",
        "request_id",
        "target_node",
        "doc",
        "op",
        "content_hash",
    },
    "wal_appended": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "request_id",
        "receipt_id",
        "term",
        "seq_no",
        "doc",
        "op",
        "content_hash",
        "origin",
        "durable",
    },
    "operation_processed": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "request_id",
        "receipt_id",
        "term",
        "seq_no",
        "doc",
        "op",
        "content_hash",
        "origin",
        "outcome",
        "checkpoints",
    },
    "primary_replication_started": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "source_incarnation",
        "request_id",
        "receipt_id",
        "term",
        "seq_no",
        "required_replicas",
        "routing_version",
    },
    "replica_received": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "source_node",
        "source_incarnation",
        "message_id",
        "receipt_id",
        "term",
        "seq_no",
        "doc",
        "op",
        "content_hash",
    },
    "replica_rejected": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "message_id",
        "receipt_id",
        "term",
        "seq_no",
        "reason",
    },
    "replica_result": {
        "node",
        "index_uuid",
        "shard",
        "request_id",
        "receipt_id",
        "message_id",
        "replica",
        "replica_incarnation",
        "outcome",
        "message_phase",
        "persisted_checkpoint",
    },
    "client_result": {
        "node",
        "index_uuid",
        "shard",
        "request_id",
        "outcome",
        "failure_stage",
    },
    "fence_persisted": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "term",
        "fence_max_seq_no",
        "reason",
    },
    "commit_captured": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "commit_id",
        "checkpoints",
        "term_state",
    },
    "commit_persisted": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "commit_id",
    },
    "wal_truncated": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "truncate_through",
    },
    "node_crashed": {
        "node",
        "incarnation",
        "outcome",
        "failed_request_ids",
        "dropped_messages",
    },
    "node_restarted": {
        "node",
        "incarnation",
        "index_uuid",
        "shard",
        "allocation",
        "checkpoints",
    },
    "replay_started": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "replay_id",
        "checkpoints",
    },
    "replay_entry": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "replay_id",
        "ordinal",
        "receipt_id",
        "term",
        "seq_no",
        "doc",
        "op",
        "content_hash",
        "outcome",
        "checkpoints",
    },
    "replay_finished": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "replay_id",
        "outcome",
    },
    "copy_state": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "reason",
        "documents",
    },
    "routing_view": {
        "node",
        "index_uuid",
        "shard",
        "primary",
        "term",
        "in_sync",
        "allocations",
        "initialized",
    },
    "routing_promoted": {
        "emitter",
        "index_uuid",
        "shard",
        "new_primary",
        "term",
        "in_sync",
    },
    "primary_activated": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "term",
    },
    "promotion_noop_fill": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "batch_id",
        "term",
        "noops",
        "checkpoints",
    },
    "promotion_noop_replication_started": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "source_incarnation",
        "batch_id",
        "receipt_id",
        "message_id",
        "term",
        "seq_no",
        "content_hash",
        "replica",
        "replica_allocation",
        "replica_incarnation",
    },
    "promotion_noop_received": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "source_node",
        "source_incarnation",
        "batch_id",
        "receipt_id",
        "message_id",
        "term",
        "seq_no",
        "content_hash",
    },
    "promotion_noop_result": {
        "node",
        "index_uuid",
        "shard",
        "allocation",
        "batch_id",
        "receipt_id",
        "message_id",
        "term",
        "seq_no",
        "replica",
        "replica_incarnation",
        "outcome",
        "message_phase",
        "persisted_checkpoint",
    },
    "in_sync_removed": {
        "emitter",
        "index_uuid",
        "shard",
        "removed_node",
        "removed_allocation",
        "in_sync",
    },
    "recovery_snapshot": {
        "source_node",
        "target_node",
        "index_uuid",
        "shard",
        "session_id",
        "snapshot_next_seq_no",
        "processed_seqs",
        "documents",
    },
    "recovery_started": {
        "source_node",
        "target_node",
        "index_uuid",
        "shard",
        "allocation",
        "session_id",
    },
    "recovery_installed": {
        "source_node",
        "target_node",
        "index_uuid",
        "shard",
        "allocation",
        "session_id",
        "snapshot_next_seq_no",
    },
    "recovery_barrier": {
        "source_node",
        "target_node",
        "index_uuid",
        "shard",
        "allocation",
        "session_id",
        "barrier_next_seq_no",
        "processed_seqs",
    },
    "recovery_membership": {
        "source_node",
        "target_node",
        "index_uuid",
        "shard",
        "allocation",
        "session_id",
        "outcome",
    },
}
EVENT_OPTIONAL_FIELDS = {
    "operation_processed": {"batch_max_seq_no"},
    "replay_entry": {"batch_max_seq_no"},
}
COMMON_FIELDS = {"schema", "run_id", "step", "event"}
CHECKPOINT_FIELDS = {"processed", "persisted", "max_seq_no"}
TERM_STATE_FIELDS = {
    "current_term",
    "max_seq_no_at_term_start",
    "processed_in_current_term_below_start_max",
}
COPY_DOCUMENT_FIELDS = {
    "doc",
    "state",
    "seq_no",
    "term",
    "content_hash",
}
NOOP_FIELDS = {"receipt_id", "seq_no", "content_hash"}
CRASH_MESSAGE_FIELDS = {"message_id", "message_phase"}


class TraceSchemaError(ValueError):
    pass


@dataclass(frozen=True)
class LoadedTrace:
    start: dict[str, Any]
    events: list[dict[str, Any]]
    end: dict[str, Any]
    request_ids: dict[str, int]
    identities: dict[tuple[int, int], dict[str, Any]]
    profile: str


def fail(line: int, message: str) -> None:
    raise TraceSchemaError(f"line {line}: {message}")


def exact_fields(
    value: dict[str, Any],
    expected: set[str],
    line: int,
    optional: set[str] | None = None,
) -> None:
    optional = optional or set()
    unknown = sorted(set(value) - expected - optional)
    missing = sorted(expected - set(value))
    if unknown:
        fail(line, f"unknown field(s): {', '.join(unknown)}")
    if missing:
        fail(line, f"missing field(s): {', '.join(missing)}")


def string(value: Any, line: int, field: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value:
        fail(line, f"{field} must be a non-empty string")
    return value


def integer(
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
    if value < (1 if positive else 0):
        fail(line, f"{field} must be {'positive' if positive else 'non-negative'}")
    return value


def boolean(value: Any, line: int, field: str) -> bool:
    if not isinstance(value, bool):
        fail(line, f"{field} must be a boolean")
    return value


def checkpoints(value: Any, line: int) -> dict[str, int | None]:
    if not isinstance(value, dict):
        fail(line, "checkpoints must be an object")
    exact_fields(value, CHECKPOINT_FIELDS, line)
    processed = integer(value["processed"], line, "processed", nullable=True)
    persisted = integer(value["persisted"], line, "persisted", nullable=True)
    maximum = integer(value["max_seq_no"], line, "max_seq_no", nullable=True)
    if persisted is not None and processed is None:
        fail(line, "persisted checkpoint requires processed checkpoint")
    if processed is not None and maximum is None:
        fail(line, "processed checkpoint requires max_seq_no")
    if persisted is not None and processed is not None and persisted > processed:
        fail(line, "persisted checkpoint exceeds processed checkpoint")
    if processed is not None and maximum is not None and processed > maximum:
        fail(line, "processed checkpoint exceeds max_seq_no")
    return {"processed": processed, "persisted": persisted, "max_seq_no": maximum}


def validate_start(value: dict[str, Any], line: int) -> dict[str, Any]:
    exact_fields(value, START_FIELDS, line)
    if value["schema"] != SCHEMA:
        fail(line, f"unsupported schema {value['schema']!r}; expected {SCHEMA!r}")
    string(value["run_id"], line, "run_id")
    if integer(value["step"], line, "step") != 0 or value["event"] != "trace_start":
        fail(line, "first record must be trace_start at step 0")
    string(value["test"], line, "test")
    if value["durability"] not in {"request", "async"}:
        fail(line, "durability must be request or async")
    nodes = value["nodes"]
    if not isinstance(nodes, list) or len(nodes) < 2 or len(nodes) > 3:
        fail(line, "trace validation supports two or three nodes")
    seen: set[str] = set()
    for item in nodes:
        if not isinstance(item, dict):
            fail(line, "node entry must be an object")
        exact_fields(item, {"node", "incarnation"}, line)
        node = string(item["node"], line, "node")
        integer(item["incarnation"], line, "incarnation")
        assert node is not None
        if node in seen:
            fail(line, f"duplicate node {node!r}")
        seen.add(node)

    shard = value["shard_state"]
    if not isinstance(shard, dict):
        fail(line, "shard_state must be an object")
    exact_fields(
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
    )
    string(shard["index_uuid"], line, "index_uuid")
    integer(shard["shard"], line, "shard")
    primary = string(shard["primary"], line, "primary")
    if primary not in seen:
        fail(line, "primary is not declared")
    if integer(shard["term"], line, "term", positive=True) != 1:
        fail(line, "trace validation currently starts at term 1")
    if not boolean(shard["activated"], line, "activated"):
        fail(line, "trace validation starts with an activated primary")
    if (
        not isinstance(shard["in_sync"], list)
        or not set(shard["in_sync"]).issubset(seen - {primary})
    ):
        fail(line, "in_sync must contain declared non-primary nodes")
    copies = shard["copies"]
    if not isinstance(copies, list) or len(copies) != len(nodes):
        fail(line, "copies must contain every node")
    copy_nodes: set[str] = set()
    for copy in copies:
        if not isinstance(copy, dict):
            fail(line, "copy entry must be an object")
        exact_fields(copy, {"node", "allocation", "exists", "fence_term"}, line)
        node = string(copy["node"], line, "copy.node")
        if node not in seen or node in copy_nodes:
            fail(line, "copy node is invalid or duplicated")
        copy_nodes.add(node)
        integer(copy["allocation"], line, "allocation", positive=True)
        if not boolean(copy["exists"], line, "exists"):
            fail(line, "trace validation starts with every declared copy present")
        if integer(copy["fence_term"], line, "fence_term") != 1:
            fail(line, "trace validation copies start fenced at term 1")
    return value


def validate_document(value: Any, line: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(line, "copy-state document must be an object")
    exact_fields(value, COPY_DOCUMENT_FIELDS, line)
    string(value["doc"], line, "document.doc")
    if value["state"] not in {"absent", "live", "deleted"}:
        fail(line, "document.state is invalid")
    seq = integer(value["seq_no"], line, "document.seq_no", nullable=True)
    term = integer(value["term"], line, "document.term", nullable=True, positive=True)
    digest = string(value["content_hash"], line, "document.content_hash", nullable=True)
    if value["state"] == "absent":
        if any(item is not None for item in (seq, term, digest)):
            fail(line, "absent document must have null identity fields")
    else:
        if seq is None or term is None or digest is None:
            fail(line, "live/deleted document requires seq_no, term, and content_hash")
        if not HASH_RE.fullmatch(digest):
            fail(line, "document.content_hash must be lowercase SHA-256")
    return value


def validate_event(
    value: dict[str, Any],
    line: int,
    start: dict[str, Any],
    expected_step: int,
) -> dict[str, Any]:
    event = value.get("event")
    if event not in EVENT_FIELDS:
        fail(line, f"unknown event {event!r}")
    exact_fields(
        value,
        COMMON_FIELDS | EVENT_FIELDS[event],
        line,
        EVENT_OPTIONAL_FIELDS.get(event),
    )
    if value["schema"] != SCHEMA:
        fail(line, f"unsupported schema {value['schema']!r}; expected {SCHEMA!r}")
    if value["run_id"] != start["run_id"]:
        fail(line, "run_id does not match trace_start")
    if integer(value["step"], line, "step") != expected_step:
        fail(line, f"step must be consecutive; expected {expected_step}")

    nodes = {item["node"] for item in start["nodes"]}
    shard = start["shard_state"]
    if "node" in value and value["node"] not in nodes:
        fail(line, "node is not declared")
    if "index_uuid" in value and value["index_uuid"] != shard["index_uuid"]:
        fail(line, "index_uuid does not match trace_start")
    if "shard" in value and value["shard"] != shard["shard"]:
        fail(line, "shard does not match trace_start")
    for field in ("allocation",):
        if field in value:
            integer(value[field], line, field, positive=True)
    for field in ("term",):
        if field in value:
            integer(value[field], line, field, positive=True)
    for field in (
        "seq_no",
        "ordinal",
        "routing_version",
        "truncate_through",
        "source_incarnation",
        "replica_incarnation",
        "replica_allocation",
    ):
        if field in value:
            integer(value[field], line, field)
    for field in (
        "request_id",
        "receipt_id",
        "commit_id",
        "replay_id",
        "batch_id",
        "message_id",
    ):
        if field in value:
            string(value[field], line, field, nullable=field == "request_id")
    if "content_hash" in value:
        digest = string(value["content_hash"], line, "content_hash")
        assert digest is not None
        if not HASH_RE.fullmatch(digest):
            fail(line, "content_hash must be lowercase SHA-256")
    if "checkpoints" in value:
        checkpoints(value["checkpoints"], line)
    if "batch_max_seq_no" in value:
        batch_max_seq_no = integer(
            value["batch_max_seq_no"],
            line,
            "batch_max_seq_no",
            nullable=True,
        )
        if batch_max_seq_no != value["checkpoints"]["max_seq_no"]:
            fail(line, "batch_max_seq_no must equal checkpoints.max_seq_no")
    if "durable" in value:
        boolean(value["durable"], line, "durable")

    if event == "client_write_routed":
        if value["target_node"] not in nodes:
            fail(line, "target_node is not declared")
        if value["op"] not in {"index", "delete"}:
            fail(line, "client op must be index or delete")
    elif event in {"wal_appended", "operation_processed"}:
        if value["origin"] not in {
            "primary",
            "live_replication",
            "recovery",
            "promotion",
        }:
            fail(line, "origin is invalid")
        if value["op"] not in {"index", "delete", "noop"}:
            fail(line, "operation kind is invalid")
        if value["op"] == "noop":
            if value["doc"] is not None:
                fail(line, "NoOp doc must be null")
        else:
            string(value["doc"], line, "doc")
        if event == "operation_processed" and value["outcome"] not in {
            "applied_newer",
            "stale",
            "redelivery",
            "noop",
            "collision",
            "apply_failed",
        }:
            fail(line, "operation outcome is invalid")
    elif event == "primary_replication_started":
        required = value["required_replicas"]
        if not isinstance(required, list):
            fail(line, "required_replicas must be an array")
        previous = ""
        for replica in required:
            if not isinstance(replica, dict):
                fail(line, "required replica must be an object")
            exact_fields(
                replica,
                {"node", "allocation", "incarnation", "message_id"},
                line,
            )
            node = string(replica["node"], line, "required node")
            if node not in nodes or node <= previous:
                fail(line, "required_replicas must be unique and sorted")
            previous = node
            integer(replica["allocation"], line, "required allocation", positive=True)
            integer(replica["incarnation"], line, "required incarnation")
            string(replica["message_id"], line, "required message_id")
    elif event == "replica_received":
        if value["source_node"] not in nodes:
            fail(line, "source_node is not declared")
        integer(value["source_incarnation"], line, "source_incarnation")
    elif event == "replica_rejected":
        if value["reason"] not in {
            "quarantined",
            "term_fence",
            "identity_mismatch",
            "recovery_gate",
            "copy_unavailable",
            "batch_rejected",
            "apply_failure",
        }:
            fail(line, "replica rejection reason is invalid")
    elif event == "replica_result":
        if value["replica"] not in nodes:
            fail(line, "replica is not declared")
        if value["outcome"] not in {
            "acknowledged",
            "failed",
            "dropped",
            "timeout",
        }:
            fail(line, "replica_result outcome is invalid")
        if value["message_phase"] not in {"request", "ack", "nack", "none"}:
            fail(line, "replica_result message_phase is invalid")
        if (
            value["outcome"] == "acknowledged"
            and value["message_phase"] != "ack"
        ):
            fail(line, "acknowledged replica result requires ack phase")
        if value["outcome"] == "failed" and value["message_phase"] != "nack":
            fail(line, "failed replica result requires nack phase")
        integer(
            value["persisted_checkpoint"],
            line,
            "persisted_checkpoint",
            nullable=True,
        )
    elif event == "client_result":
        if value["outcome"] not in {"acknowledged", "failed"}:
            fail(line, "client_result outcome is invalid")
        if value["outcome"] == "acknowledged" and value["failure_stage"] is not None:
            fail(line, "acknowledged client result has failure_stage")
        if value["outcome"] == "failed" and value["failure_stage"] is None:
            fail(line, "failed client result requires failure_stage")
    elif event == "fence_persisted":
        integer(
            value["fence_max_seq_no"],
            line,
            "fence_max_seq_no",
            nullable=True,
        )
        if value["reason"] not in {"replication", "activation", "recovery"}:
            fail(line, "fence reason is invalid")
    elif event == "commit_captured":
        state = value["term_state"]
        if not isinstance(state, dict):
            fail(line, "term_state must be an object")
        exact_fields(state, TERM_STATE_FIELDS, line)
        integer(state["current_term"], line, "current_term", positive=True)
        integer(
            state["max_seq_no_at_term_start"],
            line,
            "max_seq_no_at_term_start",
            nullable=True,
        )
        ranges = state["processed_in_current_term_below_start_max"]
        if not isinstance(ranges, list):
            fail(line, "processed term ranges must be an array")
        previous_end: int | None = None
        for interval in ranges:
            if not isinstance(interval, dict):
                fail(line, "processed term range must be an object")
            exact_fields(interval, {"start", "end"}, line)
            start = integer(interval["start"], line, "term range start")
            end = integer(interval["end"], line, "term range end")
            assert start is not None and end is not None
            if start > end:
                fail(line, "processed term range start exceeds end")
            if previous_end is not None and start <= previous_end + 1:
                fail(line, "processed term ranges must be normalized")
            maximum = state["max_seq_no_at_term_start"]
            if maximum is None or end > maximum:
                fail(line, "processed term range exceeds term-start maximum")
            previous_end = end
    elif event == "node_crashed":
        integer(value["incarnation"], line, "incarnation")
        if value["outcome"] not in {"clean", "unclean"}:
            fail(line, "crash outcome is invalid")
        if (
            not isinstance(value["failed_request_ids"], list)
            or value["failed_request_ids"]
            != sorted(set(value["failed_request_ids"]))
        ):
            fail(line, "failed_request_ids must be unique and sorted")
        for request_id in value["failed_request_ids"]:
            string(request_id, line, "failed_request_ids item")
        if not isinstance(value["dropped_messages"], list):
            fail(line, "dropped_messages must be an array")
        dropped_order: list[tuple[str, str]] = []
        for dropped in value["dropped_messages"]:
            if not isinstance(dropped, dict):
                fail(line, "dropped message must be an object")
            exact_fields(dropped, CRASH_MESSAGE_FIELDS, line)
            message_id = string(
                dropped["message_id"], line, "dropped message_id"
            )
            phase = string(
                dropped["message_phase"], line, "dropped message_phase"
            )
            if phase not in {"request", "ack", "nack"}:
                fail(line, "dropped message phase is invalid")
            assert message_id is not None and phase is not None
            dropped_order.append((message_id, phase))
        if dropped_order != sorted(set(dropped_order)):
            fail(line, "dropped_messages must be unique and sorted")
    elif event == "node_restarted":
        integer(value["incarnation"], line, "incarnation")
    elif event == "replay_entry":
        if value["outcome"] not in {
            "skip_committed",
            "applied_newer",
            "stale",
            "redelivery",
            "noop",
        }:
            fail(line, "replay outcome is invalid")
    elif event == "replay_finished":
        if value["outcome"] not in {"completed", "failed"}:
            fail(line, "replay_finished outcome is invalid")
    elif event == "copy_state":
        if value["reason"] not in {
            "quiescent",
            "replay",
            "admission",
            "trace_end",
        }:
            fail(line, "copy_state reason is invalid")
        if not isinstance(value["documents"], list):
            fail(line, "copy_state.documents must be an array")
        documents = [validate_document(item, line) for item in value["documents"]]
        if [item["doc"] for item in documents] != sorted(
            item["doc"] for item in documents
        ):
            fail(line, "copy_state documents must be sorted by doc")
    elif event == "routing_view":
        if value["primary"] not in nodes:
            fail(line, "routing_view primary is not declared")
        integer(value["term"], line, "routing_view term", positive=True)
        boolean(value["initialized"], line, "routing_view initialized")
        if (
            not isinstance(value["in_sync"], list)
            or not set(value["in_sync"]).issubset(nodes - {value["primary"]})
        ):
            fail(line, "routing_view in_sync is invalid")
        if not isinstance(value["allocations"], list):
            fail(line, "routing_view allocations must be an array")
        allocation_nodes: set[str] = set()
        for item in value["allocations"]:
            if not isinstance(item, dict):
                fail(line, "routing allocation must be an object")
            exact_fields(item, {"node", "allocation"}, line)
            if item["node"] not in nodes or item["node"] in allocation_nodes:
                fail(line, "routing allocation node is invalid or duplicated")
            allocation_nodes.add(item["node"])
            integer(item["allocation"], line, "routing allocation")
        if allocation_nodes != nodes:
            fail(line, "routing_view must include every allocation")
    elif event == "routing_promoted":
        if value["emitter"] not in nodes or value["new_primary"] not in nodes:
            fail(line, "routing promotion nodes are not declared")
        integer(value["term"], line, "routing promotion term", positive=True)
        if not isinstance(value["in_sync"], list):
            fail(line, "routing promotion in_sync must be an array")
    elif event == "primary_activated":
        integer(value["term"], line, "activation term", positive=True)
    elif event == "promotion_noop_fill":
        integer(value["term"], line, "noop-fill term", positive=True)
        if not isinstance(value["noops"], list):
            fail(line, "noops must be an array")
        sequences: list[int] = []
        receipts: set[str] = set()
        for noop in value["noops"]:
            if not isinstance(noop, dict):
                fail(line, "NoOp entry must be an object")
            exact_fields(noop, NOOP_FIELDS, line)
            receipt = string(noop["receipt_id"], line, "NoOp receipt_id")
            sequence = integer(noop["seq_no"], line, "NoOp seq_no")
            digest = string(noop["content_hash"], line, "NoOp content_hash")
            assert receipt is not None and sequence is not None and digest is not None
            if receipt in receipts:
                fail(line, "NoOp receipt_id was duplicated")
            if not HASH_RE.fullmatch(digest):
                fail(line, "NoOp content_hash must be lowercase SHA-256")
            receipts.add(receipt)
            sequences.append(sequence)
        if sequences != sorted(set(sequences)):
            fail(line, "NoOps must be unique and sorted by seq_no")
    elif event == "promotion_noop_replication_started":
        if value["replica"] not in nodes:
            fail(line, "promotion NoOp replica is not declared")
        integer(
            value["replica_allocation"],
            line,
            "replica_allocation",
            positive=True,
        )
    elif event == "promotion_noop_received":
        if value["source_node"] not in nodes:
            fail(line, "promotion NoOp source is not declared")
    elif event == "promotion_noop_result":
        if value["replica"] not in nodes:
            fail(line, "promotion NoOp replica is not declared")
        if value["outcome"] not in {
            "acknowledged",
            "failed",
            "dropped",
            "timeout",
        }:
            fail(line, "promotion NoOp result outcome is invalid")
        if value["message_phase"] not in {"request", "ack", "nack", "none"}:
            fail(line, "promotion NoOp result message_phase is invalid")
        if (
            value["outcome"] == "acknowledged"
            and value["message_phase"] != "ack"
        ):
            fail(line, "acknowledged promotion NoOp requires ack phase")
        if value["outcome"] == "failed" and value["message_phase"] != "nack":
            fail(line, "failed promotion NoOp requires nack phase")
        integer(
            value["persisted_checkpoint"],
            line,
            "persisted_checkpoint",
            nullable=True,
        )
    elif event == "in_sync_removed":
        if value["emitter"] not in nodes or value["removed_node"] not in nodes:
            fail(line, "in-sync removal nodes are not declared")
        integer(
            value["removed_allocation"],
            line,
            "removed_allocation",
            positive=True,
        )
        if not isinstance(value["in_sync"], list):
            fail(line, "in_sync_removed.in_sync must be an array")
    elif event == "recovery_snapshot":
        if value["source_node"] not in nodes or value["target_node"] not in nodes:
            fail(line, "recovery snapshot nodes are not declared")
        string(value["session_id"], line, "session_id")
        integer(value["snapshot_next_seq_no"], line, "snapshot_next_seq_no")
        if (
            not isinstance(value["processed_seqs"], list)
            or any(
                isinstance(item, bool) or not isinstance(item, int) or item < 0
                for item in value["processed_seqs"]
            )
        ):
            fail(line, "processed_seqs must be non-negative integers")
        if not isinstance(value["documents"], list):
            fail(line, "recovery snapshot documents must be an array")
        for item in value["documents"]:
            validate_document(item, line)
    elif event in {"recovery_started", "recovery_installed", "recovery_barrier", "recovery_membership"}:
        if value["source_node"] not in nodes or value["target_node"] not in nodes:
            fail(line, "recovery nodes are not declared")
        string(value["session_id"], line, "session_id")
        if event == "recovery_installed":
            integer(value["snapshot_next_seq_no"], line, "snapshot_next_seq_no")
        elif event == "recovery_barrier":
            integer(value["barrier_next_seq_no"], line, "barrier_next_seq_no")
            if not isinstance(value["processed_seqs"], list):
                fail(line, "recovery barrier processed_seqs must be an array")
        elif event == "recovery_membership" and value["outcome"] not in {
            "admitted",
            "promoted",
            "rejected",
            "unknown",
        }:
            fail(line, "recovery membership outcome is invalid")
    return value


def identity(event: dict[str, Any]) -> tuple[int, int] | None:
    if "term" not in event or "seq_no" not in event:
        return None
    return (event["term"], event["seq_no"])


def load_trace(path: Path) -> LoadedTrace:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            if not raw.endswith("\n"):
                fail(line_number, "JSONL line must end with newline")
            if not raw.strip():
                fail(line_number, "blank lines are not permitted")
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as error:
                fail(line_number, f"invalid JSON: {error.msg}")
            if not isinstance(value, dict):
                fail(line_number, "record must be an object")
            records.append(value)
    if len(records) < 2:
        raise TraceSchemaError("trace must contain trace_start and trace_end")
    start = validate_start(records[0], 1)
    events = [
        validate_event(record, line, start, line - 1)
        for line, record in enumerate(records[1:-1], 2)
    ]
    kinds = {event["event"] for event in events}
    has_recovery = bool(kinds & {
        "recovery_snapshot",
        "recovery_started",
        "recovery_installed",
        "recovery_barrier",
        "recovery_membership",
    })
    has_collision = "in_sync_removed" in kinds or any(
        event["event"] == "operation_processed"
        and event.get("outcome") == "collision"
        for event in events
    )
    has_authority = bool(
        kinds
        & {
            "routing_promoted",
            "primary_activated",
            "promotion_noop_fill",
            "promotion_noop_replication_started",
            "promotion_noop_received",
            "promotion_noop_result",
        }
    )
    has_data = bool(
        kinds
        & {
            "replica_received",
            "replica_rejected",
            "replica_result",
            "commit_captured",
            "commit_persisted",
            "node_restarted",
            "replay_started",
            "replay_entry",
            "replay_finished",
            "wal_truncated",
            "promotion_noop_received",
            "promotion_noop_result",
        }
    )
    has_client_write = "client_write_routed" in kinds
    if has_recovery and (
        has_authority
        or has_collision
        or bool(
            kinds
            & {
                "commit_captured",
                "commit_persisted",
                "wal_truncated",
                "node_crashed",
                "node_restarted",
                "replay_started",
                "replay_entry",
                "replay_finished",
                "promotion_noop_fill",
                "promotion_noop_replication_started",
                "promotion_noop_received",
                "promotion_noop_result",
            }
        )
    ):
        profile = "d1-full"
    elif has_recovery:
        profile = "d1-recovery"
    elif has_collision and has_client_write:
        profile = "d1-combined"
    elif has_collision:
        profile = "d1-collision"
    elif has_authority and has_data:
        profile = "d1-combined"
    elif has_authority:
        profile = "d1-authority"
    else:
        profile = "d1-core"

    profile_events = {
        "d1-core": {
            "client_write_routed",
            "wal_appended",
            "operation_processed",
            "primary_replication_started",
            "replica_received",
            "replica_rejected",
            "replica_result",
            "client_result",
            "fence_persisted",
            "commit_captured",
            "commit_persisted",
            "wal_truncated",
            "node_crashed",
            "node_restarted",
            "replay_started",
            "replay_entry",
            "replay_finished",
            "routing_view",
            "copy_state",
        },
        "d1-authority": {
            "node_crashed",
            "routing_promoted",
            "routing_view",
            "fence_persisted",
            "primary_activated",
            "client_write_routed",
            "wal_appended",
            "operation_processed",
            "primary_replication_started",
            "client_result",
            "copy_state",
        },
        "d1-combined": {
            "client_write_routed",
            "wal_appended",
            "operation_processed",
            "primary_replication_started",
            "replica_received",
            "replica_rejected",
            "replica_result",
            "client_result",
            "fence_persisted",
            "commit_captured",
            "commit_persisted",
            "wal_truncated",
            "node_crashed",
            "node_restarted",
            "replay_started",
            "replay_entry",
            "replay_finished",
            "routing_view",
            "routing_promoted",
            "in_sync_removed",
            "promotion_noop_fill",
            "promotion_noop_replication_started",
            "promotion_noop_received",
            "promotion_noop_result",
            "primary_activated",
            "copy_state",
        },
        "d1-full": {
            "client_write_routed",
            "wal_appended",
            "operation_processed",
            "primary_replication_started",
            "replica_received",
            "replica_rejected",
            "replica_result",
            "client_result",
            "fence_persisted",
            "commit_captured",
            "commit_persisted",
            "wal_truncated",
            "node_crashed",
            "node_restarted",
            "replay_started",
            "replay_entry",
            "replay_finished",
            "routing_view",
            "routing_promoted",
            "in_sync_removed",
            "promotion_noop_fill",
            "promotion_noop_replication_started",
            "promotion_noop_received",
            "promotion_noop_result",
            "primary_activated",
            "recovery_snapshot",
            "recovery_started",
            "recovery_installed",
            "recovery_barrier",
            "recovery_membership",
            "copy_state",
        },
        "d1-collision": {
            "wal_appended",
            "operation_processed",
            "routing_promoted",
            "routing_view",
            "fence_persisted",
            "in_sync_removed",
            "copy_state",
        },
        "d1-recovery": {
            "client_write_routed",
            "wal_appended",
            "operation_processed",
            "primary_replication_started",
            "replica_received",
            "replica_rejected",
            "replica_result",
            "client_result",
            "routing_view",
            "recovery_snapshot",
            "recovery_started",
            "recovery_installed",
            "recovery_barrier",
            "recovery_membership",
            "copy_state",
        },
    }
    for event in events:
        if event["event"] not in profile_events[profile]:
            fail(
                event["step"] + 1,
                f"event {event['event']!r} is outside inferred composition {profile}",
            )
    end = records[-1]
    exact_fields(end, END_FIELDS, len(records))
    if end["schema"] != SCHEMA:
        fail(len(records), "trace_end schema does not match v4")
    if end["run_id"] != start["run_id"]:
        fail(len(records), "trace_end run_id does not match")
    if integer(end["step"], len(records), "step") != len(records) - 1:
        fail(len(records), "trace_end step must be consecutive")
    if end["event"] != "trace_end" or end["outcome"] != "completed":
        fail(len(records), "last record must be completed trace_end")
    boolean(end["quiescent"], len(records), "quiescent")
    if end["records_before_end"] != len(records) - 1:
        fail(len(records), "records_before_end is incorrect")

    request_ids: dict[str, int] = {}
    identities: dict[tuple[int, int], dict[str, Any]] = {}
    routed: dict[str, dict[str, Any]] = {}
    receipts: dict[str, tuple[int, int]] = {}
    wal_by_receipt: dict[str, dict[str, Any]] = {}
    process_by_receipt: dict[str, dict[str, Any]] = {}
    wal_by_copy: set[tuple[str, str]] = set()
    processed_by_copy: set[tuple[str, str]] = set()
    required_by_request: dict[str, set[str]] = {}
    acked_by_request: dict[str, set[str]] = {}
    request_state: dict[str, str] = {}
    request_primary: dict[str, str] = {}
    commits: dict[str, dict[str, Any]] = {}
    replay_active: dict[str, str] = {}
    replay_ordinal: dict[str, int] = {}
    replay_next_position: dict[str, int] = {}
    last_copy_state = -1
    copy_state_steps: dict[str, int] = {}
    available = {
        start["shard_state"]["primary"],
        *start["shard_state"]["in_sync"],
    }
    current_primary = start["shard_state"]["primary"]
    last_non_observation_step = 0
    initial_allocations = {
        item["node"]: item["allocation"]
        for item in start["shard_state"]["copies"]
    }
    latest_views = {
        item["node"]: {
            "primary": start["shard_state"]["primary"],
            "term": start["shard_state"]["term"],
            "in_sync": set(start["shard_state"]["in_sync"]),
            "allocations": dict(initial_allocations),
            "initialized": start["shard_state"]["activated"],
        }
        for item in start["nodes"]
    }
    node_alive = {item["node"]: True for item in start["nodes"]}
    activated_terms = {item["node"]: 0 for item in start["nodes"]}
    if start["shard_state"]["activated"]:
        activated_terms[start["shard_state"]["primary"]] = start["shard_state"][
            "term"
        ]
    node_incarnations = {
        item["node"]: item["incarnation"] for item in start["nodes"]
    }
    message_attempts: dict[str, dict[str, Any]] = {}
    received_message_ids: set[str] = set()
    attempt_by_receipt_target: dict[tuple[str, str], str] = {}
    noop_batches: dict[str, dict[str, Any]] = {}
    wal_entries_by_node: dict[str, list[str]] = {
        item["node"]: [] for item in start["nodes"]
    }
    initial_term = start["shard_state"]["term"]
    initial_fence_terms = {
        item["node"]: item["fence_term"]
        for item in start["shard_state"]["copies"]
    }
    term_state = {
        item["node"]: {
            "current_term": initial_fence_terms.get(item["node"], initial_term),
            "max_seq_no_at_term_start": None,
            "processed": set(),
        }
        for item in start["nodes"]
    }
    persisted_term_state = {
        node: {
            "current_term": state["current_term"],
            "max_seq_no_at_term_start": state["max_seq_no_at_term_start"],
            "processed": set(state["processed"]),
        }
        for node, state in term_state.items()
    }
    durable_fence = {
        node: {
            "term": initial_fence_terms.get(node, initial_term),
            "max_seq_no": None,
        }
        for node in term_state
    }
    copy_max_seq_no: dict[str, int | None] = {
        item["node"]: None for item in start["nodes"]
    }
    recovery_term_state: dict[str, dict[str, Any]] = {}

    def register_identity(
        ident: tuple[int, int],
        content: dict[str, Any],
        line: int,
    ) -> None:
        if ident in identities and identities[ident] != content:
            fail(line, f"operation identity {ident} changed content")
        identities.setdefault(ident, content)

    def register_receipt(
        receipt: str,
        ident: tuple[int, int] | None,
        line: int,
    ) -> None:
        if ident is None:
            fail(line, f"receipt {receipt!r} has no operation identity")
        if receipt in receipts and receipts[receipt] != ident:
            fail(line, f"receipt {receipt!r} changed operation identity")
        receipts.setdefault(receipt, ident)

    def register_attempt(
        attempt: dict[str, Any],
        line: int,
    ) -> None:
        message_id = attempt["message_id"]
        key = (attempt["receipt_id"], attempt["target"])
        if message_id in message_attempts:
            fail(line, f"message_id {message_id!r} was reused")
        if key in attempt_by_receipt_target:
            previous = message_attempts[attempt_by_receipt_target[key]]
            if previous["phase"] is not None:
                fail(
                    line,
                    "receipt/target already has an in-flight transport attempt: "
                    f"{attempt['receipt_id']!r}/{attempt['target']!r}",
                )
        message_attempts[message_id] = attempt
        attempt_by_receipt_target[key] = message_id

    def attempt_destination(attempt: dict[str, Any]) -> str | None:
        phase = attempt["phase"]
        if phase == "request":
            return attempt["target"]
        if phase in {"ack", "nack"}:
            return attempt["source"]
        return None

    def clear_write_attempts(request: str) -> None:
        for attempt in message_attempts.values():
            if attempt["type"] == "write" and attempt["request_id"] == request:
                attempt["phase"] = None

    def copy_term_state(state: dict[str, Any]) -> dict[str, Any]:
        return {
            "current_term": state["current_term"],
            "max_seq_no_at_term_start": state["max_seq_no_at_term_start"],
            "processed": set(state["processed"]),
        }

    def reset_term_state(node: str) -> None:
        persisted = persisted_term_state[node]
        fence = durable_fence[node]
        if fence["term"] > persisted["current_term"]:
            term_state[node] = {
                "current_term": fence["term"],
                "max_seq_no_at_term_start": fence["max_seq_no"],
                "processed": set(),
            }
        else:
            term_state[node] = copy_term_state(persisted)

    def mark_term_processed(
        node: str,
        primary_term: int,
        seq_no: int,
        outcome: str,
    ) -> None:
        if outcome not in {"applied_newer", "stale", "noop"}:
            return
        state = term_state[node]
        if primary_term < state["current_term"]:
            return
        if primary_term > state["current_term"]:
            state["current_term"] = primary_term
            state["max_seq_no_at_term_start"] = copy_max_seq_no[node]
            state["processed"] = set()
        maximum = state["max_seq_no_at_term_start"]
        if maximum is not None and seq_no <= maximum:
            state["processed"].add(seq_no)
        current_maximum = copy_max_seq_no[node]
        if current_maximum is None or seq_no > current_maximum:
            copy_max_seq_no[node] = seq_no

    def normalized_ranges(values: set[int]) -> list[dict[str, int]]:
        ranges: list[dict[str, int]] = []
        for value in sorted(values):
            if ranges and value == ranges[-1]["end"] + 1:
                ranges[-1]["end"] = value
            else:
                ranges.append({"start": value, "end": value})
        return ranges

    for event in events:
        kind = event["event"]
        line = event["step"] + 1
        if kind not in {"copy_state", "routing_view"}:
            last_non_observation_step = event["step"]
        request = event.get("request_id")
        ident = identity(event)
        if ident is not None and "content_hash" in event:
            operation_kind = event.get("op")
            if kind.startswith("promotion_noop_"):
                operation_kind = "noop"
            register_identity(
                ident,
                {
                    "doc": event.get("doc"),
                    "op": operation_kind,
                    "content_hash": event["content_hash"],
                },
                line,
            )

        if kind == "client_write_routed":
            assert request is not None
            if request in request_ids:
                fail(line, f"request_id {request!r} was reused")
            request_ids[request] = len(request_ids) + 1
            routed[request] = event
            acked_by_request[request] = set()
            request_state[request] = "Routed"

        elif kind == "wal_appended":
            receipt = event["receipt_id"]
            if (
                event["origin"] == "primary"
                and request not in routed
                and not (request is None and event["op"] == "noop")
            ):
                fail(line, "primary WAL append has no routed request")
            register_receipt(receipt, ident, line)
            copy_key = (receipt, event["node"])
            if copy_key in wal_by_copy:
                fail(line, "WAL receipt was appended twice on one copy")
            wal_by_receipt.setdefault(receipt, event)
            wal_by_copy.add(copy_key)
            wal_entries_by_node[event["node"]].append(receipt)
            if event["origin"] == "live_replication":
                message_id = attempt_by_receipt_target.get(copy_key)
                if message_id is None and profile != "d1-collision":
                    fail(line, "live-replication WAL append has no message attempt")
                if message_id is not None:
                    event["_message_id"] = message_id

        elif kind == "operation_processed":
            event["_batch_max_seq_no"] = event.get(
                "batch_max_seq_no", event["checkpoints"]["max_seq_no"]
            )
            receipt = event["receipt_id"]
            register_receipt(receipt, ident, line)
            if copy_key := attempt_by_receipt_target.get(
                (receipt, event["node"])
            ):
                event["_message_id"] = copy_key
            if (
                (receipt, event["node"]) not in wal_by_copy
                and event["outcome"] not in {"redelivery", "collision"}
            ):
                fail(line, "processed operation has no WAL append")
            if (
                event["origin"] == "live_replication"
                and "_message_id" not in event
                and profile != "d1-collision"
            ):
                fail(line, "processed live operation has no message attempt")
            if (
                event["origin"] == "live_replication"
                and profile != "d1-collision"
                and event["_message_id"] not in received_message_ids
            ):
                fail(line, "processed live operation has no replica receive event")
            process_by_receipt[receipt] = event
            processed_by_copy.add((receipt, event["node"]))
            mark_term_processed(
                event["node"],
                event["term"],
                event["seq_no"],
                event["outcome"],
            )
            if event["origin"] == "primary":
                if request not in routed and not (
                    request is None and event["op"] == "noop"
                ):
                    fail(line, "primary process has no routed request")
                if request is not None:
                    request_state[request] = "Replicating"
                    request_primary[request] = event["node"]
            if "_message_id" in event:
                attempt = message_attempts[event["_message_id"]]
                if attempt["phase"] != "request":
                    fail(line, "processed operation does not own a request message")
                attempt["phase"] = (
                    "nack"
                    if event["outcome"] in {"collision", "apply_failed"}
                    else "ack"
                )

        elif kind == "primary_replication_started":
            if request not in routed:
                fail(line, "replication start has no routed request")
            receipt = event["receipt_id"]
            if receipt not in wal_by_receipt or receipt not in process_by_receipt:
                fail(line, "replication start lacks WAL/apply observations")
            if receipts[receipt] != ident:
                fail(line, "replication receipt identity changed")
            if event["source_incarnation"] != node_incarnations[event["node"]]:
                fail(line, "replication source_incarnation is stale")
            required_by_request[request] = {
                item["node"] for item in event["required_replicas"]
            }
            view = latest_views[event["node"]]
            if required_by_request[request] != view["in_sync"]:
                fail(
                    line,
                    "required_replicas must equal the primary node's in-sync view",
                )
            required_message_ids: list[str] = []
            for replica in event["required_replicas"]:
                if replica["allocation"] != view["allocations"][replica["node"]]:
                    fail(
                        line,
                        "required replica allocation does not match the primary view",
                    )
                if replica["incarnation"] != node_incarnations[replica["node"]]:
                    fail(line, "required replica incarnation is stale")
                required_message_ids.append(replica["message_id"])
                register_attempt(
                    {
                        "type": "write",
                        "message_id": replica["message_id"],
                        "request_id": request,
                        "receipt_id": receipt,
                        "term": event["term"],
                        "seq_no": event["seq_no"],
                        "source": event["node"],
                        "source_incarnation": event["source_incarnation"],
                        "target": replica["node"],
                        "target_incarnation": replica["incarnation"],
                        "target_allocation": replica["allocation"],
                        "phase": "request",
                    },
                    line,
                )
            event["_required_message_ids"] = required_message_ids

        elif kind == "replica_received":
            message_id = event["message_id"]
            attempt = message_attempts.get(message_id)
            if attempt is None or attempt["type"] != "write":
                fail(line, "replica receipt names an unknown write message")
            if message_id in received_message_ids:
                fail(line, "replica receipt was emitted twice")
            if (
                attempt["phase"] != "request"
                or attempt["receipt_id"] != event["receipt_id"]
                or attempt["target"] != event["node"]
                or attempt["source"] != event["source_node"]
                or attempt["source_incarnation"] != event["source_incarnation"]
                or attempt["term"] != event["term"]
                or attempt["seq_no"] != event["seq_no"]
                or attempt["target_allocation"] != event["allocation"]
            ):
                fail(line, "replica receipt does not match its message attempt")
            event["_message_id"] = message_id
            received_message_ids.add(message_id)

        elif kind == "replica_rejected":
            message_id = event["message_id"]
            attempt = message_attempts.get(message_id)
            if attempt is None:
                fail(line, "replica rejection names an unknown message")
            if (
                attempt["phase"] != "request"
                or attempt["receipt_id"] != event["receipt_id"]
                or attempt["target"] != event["node"]
                or attempt["term"] != event["term"]
                or attempt["seq_no"] != event["seq_no"]
                or attempt["target_allocation"] != event["allocation"]
            ):
                fail(line, "replica rejection does not match its message attempt")
            attempt["phase"] = "nack"
            event["_message_id"] = message_id

        elif kind == "replica_result":
            if request not in required_by_request:
                fail(line, "replica result has no replication start")
            message_id = event["message_id"]
            attempt = message_attempts.get(message_id)
            if attempt is None or attempt["type"] != "write":
                fail(line, "replica result names an unknown write message")
            if (
                attempt["request_id"] != request
                or attempt["receipt_id"] != event["receipt_id"]
                or attempt["target"] != event["replica"]
                or attempt["target_incarnation"] != event["replica_incarnation"]
            ):
                fail(line, "replica result does not match its message attempt")
            if event["message_phase"] == "none":
                if attempt["phase"] is not None:
                    fail(line, "replica result claims no message while one is in flight")
            elif attempt["phase"] != event["message_phase"]:
                fail(line, "replica result message phase does not match")
            if event["outcome"] == "acknowledged":
                receipt = event["receipt_id"]
                if (receipt, event["replica"]) not in processed_by_copy:
                    fail(line, "replica ack has no processed operation")
                acked_by_request[request].add(event["replica"])
            if event["outcome"] == "failed":
                clear_write_attempts(request)
                request_state[request] = "Failed"
            else:
                attempt["phase"] = None
            event["_message_id"] = message_id

        elif kind == "client_result":
            assert request is not None
            if event["failure_stage"] == "version_conflict" and any(
                item["event"] == "wal_appended"
                and item.get("origin") == "primary"
                and item.get("request_id") == request
                for item in events
            ):
                fail(line, "version conflict has a primary WAL append")
            if event["outcome"] == "acknowledged":
                if request not in required_by_request:
                    fail(line, "client ack has no replication start")
                missing = required_by_request[request] - acked_by_request[request]
                if missing:
                    fail(
                        line,
                        "client ack is missing required replica(s): "
                        + ", ".join(sorted(missing)),
                    )
                if start["durability"] == "request":
                    receipt = next(
                        item["receipt_id"]
                        for item in events
                        if item["event"] == "primary_replication_started"
                        and item["request_id"] == request
                    )
                    primary_wals = [
                        item
                        for item in events
                        if item["event"] == "wal_appended"
                        and item["receipt_id"] == receipt
                        and item["origin"] == "primary"
                    ]
                    if len(primary_wals) != 1 or not primary_wals[0]["durable"]:
                        fail(line, "request-durable primary WAL is not durable")
                    for replica in required_by_request[request]:
                        replica_wals = [
                            item
                            for item in events
                            if item["event"] == "wal_appended"
                            and item["receipt_id"] == receipt
                            and item["node"] == replica
                        ]
                        if len(replica_wals) != 1 or not replica_wals[0]["durable"]:
                            fail(
                                line,
                                "request-durable replica WAL is not durable: "
                                f"{replica}",
                            )
                request_state[request] = "Acked"
            else:
                request_state[request] = "Failed"
                clear_write_attempts(request)

        elif kind == "fence_persisted":
            fence = durable_fence[event["node"]]
            if event["term"] > fence["term"]:
                fence["term"] = event["term"]
                fence["max_seq_no"] = event["fence_max_seq_no"]
            state = term_state[event["node"]]
            if event["term"] > state["current_term"]:
                state["current_term"] = event["term"]
                state["max_seq_no_at_term_start"] = event["fence_max_seq_no"]
                state["processed"] = set()

        elif kind == "commit_captured":
            state = term_state[event["node"]]
            expected_term_state = {
                "current_term": state["current_term"],
                "max_seq_no_at_term_start": state[
                    "max_seq_no_at_term_start"
                ],
                "processed_in_current_term_below_start_max": normalized_ranges(
                    state["processed"]
                ),
            }
            if event["term_state"] != expected_term_state:
                fail(
                    line,
                    "commit_captured term_state does not match traced copy state: "
                    f"observed={event['term_state']!r}, "
                    f"expected={expected_term_state!r}",
                )
            commits[event["commit_id"]] = event

        elif kind == "commit_persisted":
            if event["commit_id"] not in commits:
                fail(line, "commit_persisted has no captured boundary")
            captured = commits[event["commit_id"]]["term_state"]
            persisted_term_state[event["node"]] = {
                "current_term": captured["current_term"],
                "max_seq_no_at_term_start": captured[
                    "max_seq_no_at_term_start"
                ],
                "processed": {
                    seq_no
                    for interval in captured[
                        "processed_in_current_term_below_start_max"
                    ]
                    for seq_no in range(interval["start"], interval["end"] + 1)
                },
            }

        elif kind == "promotion_noop_fill":
            batch_id = event["batch_id"]
            if batch_id in noop_batches:
                fail(line, f"promotion NoOp batch {batch_id!r} was reused")
            noops: dict[str, dict[str, Any]] = {}
            physical_effects: list[bool] = []
            for noop in event["noops"]:
                noop_ident = (event["term"], noop["seq_no"])
                content = {
                    "doc": None,
                    "op": "noop",
                    "content_hash": noop["content_hash"],
                }
                register_identity(noop_ident, content, line)
                register_receipt(noop["receipt_id"], noop_ident, line)
                copy_key = (noop["receipt_id"], event["node"])
                has_wal = copy_key in wal_by_copy
                has_process = copy_key in processed_by_copy
                if has_wal != has_process:
                    fail(
                        line,
                        "promotion NoOp fill has only one physical apply effect",
                    )
                if has_wal:
                    wal_event = wal_by_receipt[noop["receipt_id"]]
                    process_event = process_by_receipt[noop["receipt_id"]]
                    if (
                        wal_event["node"] != event["node"]
                        or wal_event["origin"] not in {"primary", "promotion"}
                        or process_event["node"] != event["node"]
                        or process_event["origin"] not in {"primary", "promotion"}
                        or process_event["outcome"] != "noop"
                    ):
                        fail(
                            line,
                            "promotion NoOp physical effects do not match the fill",
                        )
                else:
                    wal_by_receipt.setdefault(
                        noop["receipt_id"],
                        {
                            "node": event["node"],
                            "origin": "promotion",
                            "durable": True,
                        },
                    )
                    wal_by_copy.add(copy_key)
                    processed_by_copy.add(copy_key)
                    wal_entries_by_node[event["node"]].append(noop["receipt_id"])
                mark_term_processed(
                    event["node"],
                    event["term"],
                    noop["seq_no"],
                    "noop",
                )
                physical_effects.append(has_wal)
                noops[noop["receipt_id"]] = noop
            copy_max_seq_no[event["node"]] = event["checkpoints"]["max_seq_no"]
            if any(physical_effects) and not all(physical_effects):
                fail(line, "promotion NoOp fill mixes summary and physical effects")
            event["_fill_physical"] = bool(physical_effects and physical_effects[0])
            noop_batches[batch_id] = {
                "event": event,
                "node": event["node"],
                "term": event["term"],
                "noops": noops,
                "send_targets": None,
                "send_allocations": None,
                "candidate_targets": None,
                "candidate_allocations": None,
                "message_ids": [],
            }

        elif kind == "promotion_noop_replication_started":
            batch = noop_batches.get(event["batch_id"])
            if batch is None:
                fail(line, "promotion NoOp send has no fill batch")
            noop = batch["noops"].get(event["receipt_id"])
            if noop is None:
                fail(line, "promotion NoOp send has an unknown receipt")
            view = latest_views[event["node"]]
            if batch["send_targets"] is None:
                if (
                    not node_alive[event["node"]]
                    or activated_terms[event["node"]] != event["term"]
                    or view["primary"] != event["node"]
                    or view["term"] != event["term"]
                    or not view["initialized"]
                ):
                    fail(line, "promotion NoOp send precedes a valid activation")
                batch["send_targets"] = set(view["in_sync"])
                batch["send_allocations"] = {
                    target: view["allocations"][target]
                    for target in view["in_sync"]
                }
            if (
                batch["node"] != event["node"]
                or batch["term"] != event["term"]
                or noop["seq_no"] != event["seq_no"]
                or noop["content_hash"] != event["content_hash"]
                or event["source_incarnation"] != node_incarnations[event["node"]]
                or event["replica"] not in batch["send_targets"]
                or event["replica_allocation"]
                != batch["send_allocations"][event["replica"]]
                or event["replica_incarnation"]
                != node_incarnations[event["replica"]]
            ):
                fail(line, "promotion NoOp send does not match its fill/view")
            register_attempt(
                {
                    "type": "noop",
                    "message_id": event["message_id"],
                    "request_id": None,
                    "receipt_id": event["receipt_id"],
                    "term": event["term"],
                    "seq_no": event["seq_no"],
                    "source": event["node"],
                    "source_incarnation": event["source_incarnation"],
                    "target": event["replica"],
                    "target_incarnation": event["replica_incarnation"],
                    "target_allocation": event["replica_allocation"],
                    "phase": "request",
                },
                line,
            )
            batch["message_ids"].append(event["message_id"])
            event["_message_id"] = event["message_id"]

        elif kind == "promotion_noop_received":
            message_id = event["message_id"]
            attempt = message_attempts.get(message_id)
            if attempt is None or attempt["type"] != "noop":
                fail(line, "promotion NoOp receipt names an unknown message")
            if message_id in received_message_ids:
                fail(line, "promotion NoOp receipt was emitted twice")
            if (
                attempt["phase"] != "request"
                or attempt["receipt_id"] != event["receipt_id"]
                or attempt["target"] != event["node"]
                or attempt["source"] != event["source_node"]
                or attempt["source_incarnation"] != event["source_incarnation"]
                or attempt["term"] != event["term"]
                or attempt["seq_no"] != event["seq_no"]
                or attempt["target_allocation"] != event["allocation"]
            ):
                fail(line, "promotion NoOp receipt does not match its message")
            event["_message_id"] = message_id
            received_message_ids.add(message_id)

        elif kind == "promotion_noop_result":
            message_id = event["message_id"]
            attempt = message_attempts.get(message_id)
            if attempt is None or attempt["type"] != "noop":
                fail(line, "promotion NoOp result names an unknown message")
            if (
                attempt["receipt_id"] != event["receipt_id"]
                or attempt["source"] != event["node"]
                or attempt["target"] != event["replica"]
                or attempt["target_incarnation"] != event["replica_incarnation"]
                or attempt["term"] != event["term"]
                or attempt["seq_no"] != event["seq_no"]
            ):
                fail(line, "promotion NoOp result does not match its message")
            if event["message_phase"] == "none":
                if attempt["phase"] is not None:
                    fail(line, "promotion NoOp result claims no in-flight message")
            elif attempt["phase"] != event["message_phase"]:
                fail(line, "promotion NoOp result message phase does not match")
            if (
                event["outcome"] == "acknowledged"
                and (event["receipt_id"], event["replica"])
                not in processed_by_copy
            ):
                fail(line, "promotion NoOp ack has no processed operation")
            attempt["phase"] = None
            event["_message_id"] = message_id

        elif kind == "replay_started":
            reset_term_state(event["node"])
            copy_max_seq_no[event["node"]] = event["checkpoints"]["max_seq_no"]
            replay_active[event["node"]] = event["replay_id"]
            replay_ordinal[event["node"]] = 0
            replay_next_position[event["node"]] = 1

        elif kind == "replay_entry":
            event["_batch_max_seq_no"] = event.get(
                "batch_max_seq_no", event["checkpoints"]["max_seq_no"]
            )
            if replay_active.get(event["node"]) != event["replay_id"]:
                fail(line, "replay_entry has no active replay")
            expected = replay_ordinal[event["node"]]
            if event["ordinal"] != expected:
                fail(line, f"replay ordinal must be {expected}")
            receipt = event["receipt_id"]
            if receipt not in receipts:
                fail(line, "replay_entry names an unknown WAL receipt")
            if receipts[receipt] != ident:
                fail(line, "replay_entry receipt identity changed")
            try:
                position = wal_entries_by_node[event["node"]].index(receipt) + 1
            except ValueError:
                fail(line, "replay_entry receipt is absent from the copy WAL")
            event["_wal_position"] = position
            replay_next_position[event["node"]] = position + 1
            replay_ordinal[event["node"]] = expected + 1
            mark_term_processed(
                event["node"],
                event["term"],
                event["seq_no"],
                event["outcome"],
            )

        elif kind == "replay_finished":
            if replay_active.get(event["node"]) != event["replay_id"]:
                fail(line, "replay_finished has no active replay")
            replay_active.pop(event["node"])
            event["_wal_position"] = (
                len(wal_entries_by_node[event["node"]]) + 1
                if event["outcome"] == "completed"
                else replay_next_position[event["node"]]
            )
            if event["outcome"] == "completed" and event["node"] in {
                current_primary,
                *start["shard_state"]["in_sync"],
            }:
                available.add(event["node"])
            elif event["outcome"] == "failed":
                available.discard(event["node"])

        elif kind == "copy_state":
            for document in event["documents"]:
                if document["state"] == "absent":
                    continue
                document_ident = (document["term"], document["seq_no"])
                if document_ident not in identities:
                    fail(
                        line,
                        "copy_state references unknown operation identity "
                        f"{document_ident}",
                    )
                operation = identities[document_ident]
                expected_kind = (
                    "delete" if document["state"] == "deleted" else "index"
                )
                if (
                    operation["doc"] != document["doc"]
                    or operation["op"] != expected_kind
                    or operation["content_hash"] != document["content_hash"]
                ):
                    fail(
                        line,
                        "copy_state document does not match the traced operation",
                    )
            last_copy_state = event["step"]
            copy_state_steps[event["node"]] = event["step"]

        elif kind == "routing_view":
            latest_views[event["node"]] = {
                "primary": event["primary"],
                "term": event["term"],
                "in_sync": set(event["in_sync"]),
                "allocations": {
                    item["node"]: item["allocation"]
                    for item in event["allocations"]
                },
                "initialized": event["initialized"],
            }
            for batch in noop_batches.values():
                if (
                    batch["node"] == event["node"]
                    and batch["term"] == event["term"]
                    and batch["send_targets"] is None
                    and activated_terms[event["node"]] == event["term"]
                ):
                    if event["primary"] == event["node"] and event["initialized"]:
                        batch["candidate_targets"] = set(event["in_sync"])
                        batch["candidate_allocations"] = {
                            item["node"]: item["allocation"]
                            for item in event["allocations"]
                            if item["node"] in event["in_sync"]
                        }
                    else:
                        batch["candidate_targets"] = None
                        batch["candidate_allocations"] = None

        elif kind == "node_crashed":
            if event["incarnation"] != node_incarnations[event["node"]]:
                fail(line, "node_crashed incarnation is stale")
            expected_failed = sorted(
                request_id
                for request_id, state in request_state.items()
                if state == "Replicating"
                and request_primary.get(request_id) == event["node"]
            )
            if event["failed_request_ids"] != expected_failed:
                fail(line, "node_crashed failed_request_ids do not match")
            expected_dropped = sorted(
                (
                    {
                        "message_id": message_id,
                        "message_phase": attempt["phase"],
                    }
                    for message_id, attempt in message_attempts.items()
                    if attempt_destination(attempt) == event["node"]
                ),
                key=lambda item: (item["message_id"], item["message_phase"]),
            )
            if event["dropped_messages"] != expected_dropped:
                fail(line, "node_crashed dropped_messages do not match")
            for dropped in expected_dropped:
                message_attempts[dropped["message_id"]]["phase"] = None
            for request_id in expected_failed:
                request_state[request_id] = "Failed"
            available.discard(event["node"])
            node_alive[event["node"]] = False
            activated_terms[event["node"]] = 0

        elif kind == "node_restarted":
            if event["incarnation"] != node_incarnations[event["node"]] + 1:
                fail(line, "node_restarted incarnation must advance by one")
            node_incarnations[event["node"]] = event["incarnation"]
            available.discard(event["node"])
            node_alive[event["node"]] = True
            activated_terms[event["node"]] = 0
            reset_term_state(event["node"])
            copy_max_seq_no[event["node"]] = event["checkpoints"]["max_seq_no"]

        elif kind == "routing_promoted":
            current_primary = event["new_primary"]
            available = set(event["in_sync"])
            promoted_view = latest_views[event["emitter"]]
            promoted_view["primary"] = event["new_primary"]
            promoted_view["term"] = event["term"]
            promoted_view["in_sync"] = set(event["in_sync"])
            if profile == "d1-collision":
                available.add(current_primary)

        elif kind == "primary_activated":
            available.add(event["node"])
            latest_views[event["node"]]["term"] = event["term"]
            activated_terms[event["node"]] = event["term"]
            view = latest_views[event["node"]]
            for batch in noop_batches.values():
                if (
                    batch["node"] == event["node"]
                    and batch["term"] == event["term"]
                    and batch["send_targets"] is None
                    and view["primary"] == event["node"]
                    and view["initialized"]
                ):
                    batch["candidate_targets"] = set(view["in_sync"])
                    batch["candidate_allocations"] = {
                        target: view["allocations"][target]
                        for target in view["in_sync"]
                    }

        elif kind == "in_sync_removed":
            available.discard(event["removed_node"])
            latest_views[event["emitter"]]["in_sync"] = set(event["in_sync"])

        elif kind == "recovery_snapshot":
            recovery_term_state[event["session_id"]] = copy_term_state(
                term_state[event["source_node"]]
            )

        elif kind == "recovery_installed":
            snapshot_term_state = recovery_term_state.get(event["session_id"])
            if snapshot_term_state is None:
                fail(line, "recovery install has no captured source term state")
            target = event["target_node"]
            term_state[target] = copy_term_state(snapshot_term_state)
            persisted_term_state[target] = copy_term_state(snapshot_term_state)
            durable_fence[target] = {
                "term": snapshot_term_state["current_term"],
                "max_seq_no": snapshot_term_state[
                    "max_seq_no_at_term_start"
                ],
            }
            copy_max_seq_no[target] = (
                event["snapshot_next_seq_no"] - 1
                if event["snapshot_next_seq_no"] > 0
                else None
            )

        elif kind == "recovery_membership":
            if event["outcome"] == "admitted":
                available.add(event["target_node"])
            elif event["outcome"] == "promoted":
                current_primary = event["target_node"]
                available.add(event["target_node"])

    for batch in noop_batches.values():
        expected_targets = batch["send_targets"]
        if expected_targets is None:
            expected_targets = (
                set(batch["candidate_targets"] or set())
                if node_alive[batch["node"]]
                else set()
            )
        expected_pairs = {
            (receipt, target)
            for receipt in batch["noops"]
            for target in expected_targets
        }
        actual_pairs = {
            (attempt["receipt_id"], attempt["target"])
            for attempt in message_attempts.values()
            if attempt["type"] == "noop"
            and attempt["message_id"] in batch["message_ids"]
        }
        if actual_pairs != expected_pairs:
            fail(
                batch["event"]["step"] + 1,
                "promotion NoOp sends must cover every filled sequence and "
                "in-sync replica",
            )
        batch["event"]["_required_message_ids"] = []

    if last_copy_state != (events[-1]["step"] if events else -1):
        fail(len(records), "trace must end with copy_state")
    if end["quiescent"]:
        if replay_active:
            fail(len(records), "quiescent trace has unfinished replay")
        missing_states = sorted(
            node
            for node in available
            if copy_state_steps.get(node, -1) <= last_non_observation_step
        )
        if missing_states:
            fail(
                len(records),
                "quiescent trace lacks final copy_state for available node(s): "
                + ", ".join(missing_states),
            )
    return LoadedTrace(start, events, end, request_ids, identities, profile)


def token_map(values: list[str], prefix: str) -> dict[str, str]:
    return {value: f'"{prefix}{index}"' for index, value in enumerate(values)}


def tla_set(values: list[str]) -> str:
    return "{}" if not values else "{" + ", ".join(values) + "}"


def cp_next(value: int | None) -> int:
    return 0 if value is None else value + 1


def function(domain: str, variable: str, values: dict[str, str], default: str) -> str:
    branches = [
        f"{variable} = {key} -> {value}" for key, value in values.items()
    ]
    if not branches:
        return f"[{variable} \\in {domain} |-> {default}]"
    return (
        f"[{variable} \\in {domain} |-> CASE "
        + " [] ".join(branches)
        + f" [] OTHER -> {default}]"
    )


def render(trace: LoadedTrace) -> tuple[str, str]:
    nodes_raw = [item["node"] for item in trace.start["nodes"]]
    node = token_map(nodes_raw, "N")
    docs_raw = sorted(
        {
            item["doc"]
            for item in trace.events
            if item.get("doc") is not None
        }
        | {
            document["doc"]
            for item in trace.events
            if item["event"] == "copy_state"
            for document in item["documents"]
        }
    )
    while len(docs_raw) < 2:
        docs_raw.append(f"__dummy_{len(docs_raw)}")
    doc = token_map(docs_raw, "D")
    requests = trace.request_ids
    primary_raw = trace.start["shard_state"]["primary"]
    replica_raw = trace.start["shard_state"]["in_sync"][0]
    allocation = {
        item["node"]: item["allocation"]
        for item in trace.start["shard_state"]["copies"]
    }
    preserve_allocation_ids = trace.profile == "d1-full"
    allocation_values: dict[str, dict[int, int]] = {
        raw: {
            allocation[raw]: (
                allocation[raw] if preserve_allocation_ids else 1
            )
        }
        for raw in nodes_raw
    }

    def register_allocation(raw_node: str | None, raw_value: int | None) -> None:
        if raw_node is None or raw_value is None or raw_value == 0:
            return
        values = allocation_values[raw_node]
        if raw_value not in values:
            values[raw_value] = (
                raw_value if preserve_allocation_ids else len(values) + 1
            )

    for event in trace.events:
        register_allocation(event.get("node"), event.get("allocation"))
        register_allocation(event.get("removed_node"), event.get("removed_allocation"))
        register_allocation(event.get("replica"), event.get("replica_allocation"))
        for replica in event.get("required_replicas", []):
            register_allocation(replica["node"], replica["allocation"])
        for assigned in event.get("allocations", []):
            register_allocation(assigned["node"], assigned["allocation"])

    def abstract_allocation(raw_node: str | None, raw_value: int | None) -> int:
        if raw_node is None or raw_value is None or raw_value == 0:
            return 0
        return allocation_values[raw_node][raw_value]

    identity_to_write: dict[tuple[int, int], int] = {}
    for event in trace.events:
        if event["event"] == "primary_replication_started":
            identity_to_write[(event["term"], event["seq_no"])] = requests[
                event["request_id"]
            ]

    def write_id(event: dict[str, Any]) -> int:
        request = event.get("request_id")
        if request is not None and request in requests:
            return requests[request]
        ident = identity(event)
        return 0 if ident is None else identity_to_write.get(ident, 0)

    message_attempts: dict[str, dict[str, Any]] = {}
    for event in trace.events:
        if event["event"] == "primary_replication_started":
            for replica in event["required_replicas"]:
                message_attempts[replica["message_id"]] = {
                    "type": "write",
                    "request_id": event["request_id"],
                    "receipt_id": event["receipt_id"],
                    "term": event["term"],
                    "seq_no": event["seq_no"],
                    "source": event["node"],
                    "source_incarnation": event["source_incarnation"],
                    "target": replica["node"],
                    "target_incarnation": replica["incarnation"],
                    "target_allocation": replica["allocation"],
                }
        elif event["event"] == "promotion_noop_replication_started":
            message_attempts[event["message_id"]] = {
                "type": "noop",
                "request_id": None,
                "receipt_id": event["receipt_id"],
                "term": event["term"],
                "seq_no": event["seq_no"],
                "source": event["node"],
                "source_incarnation": event["source_incarnation"],
                "target": event["replica"],
                "target_incarnation": event["replica_incarnation"],
                "target_allocation": event["replica_allocation"],
            }

    def message_record(message_id: str, phase: str) -> str:
        attempt = message_attempts[message_id]
        is_noop = attempt["type"] == "noop"
        kind = {
            ("write", "request"): "Replicate",
            ("write", "ack"): "ReplicaAck",
            ("write", "nack"): "ReplicaNack",
            ("noop", "request"): "ReplicateNoOp",
            ("noop", "ack"): "NoOpAck",
            ("noop", "nack"): "NoOpNack",
        }[(attempt["type"], phase)]
        if phase == "request":
            source = attempt["source"]
            target = attempt["target"]
            source_incarnation = attempt["source_incarnation"]
            target_incarnation = attempt["target_incarnation"]
        else:
            source = attempt["target"]
            target = attempt["source"]
            source_incarnation = attempt["target_incarnation"]
            target_incarnation = attempt["source_incarnation"]
        operation = (
            0
            if is_noop
            else requests[attempt["request_id"]]
        )
        return (
            "[kind |-> "
            + json.dumps(kind)
            + ", write |-> "
            + str(operation)
            + ", from |-> "
            + node[source]
            + ", to |-> "
            + node[target]
            + ", seq |-> "
            + str(attempt["seq_no"])
            + ", fromEpoch |-> "
            + str(source_incarnation)
            + ", toEpoch |-> "
            + str(target_incarnation)
            + ", term |-> "
            + str(attempt["term"])
            + ', indexUuid |-> "INDEX_UUID", targetAllocation |-> '
            + str(
                abstract_allocation(
                    attempt["target"], attempt["target_allocation"]
                )
            )
            + "]"
        )

    fence_by_node: dict[str, int] = {
        raw: 1 for raw in nodes_raw
    }
    wal_by_receipt = {
        item["receipt_id"]: item
        for item in trace.events
        if item["event"] == "wal_appended"
        and item["origin"] == "primary"
    }
    process_by_receipt = {
        item["receipt_id"]: item
        for item in trace.events
        if item["event"] == "operation_processed"
        and item["origin"] == "primary"
    }
    receive_by_receipt = {
        item["receipt_id"]: item
        for item in trace.events
        if item["event"] == "replica_received"
    }
    commit_capture: dict[str, dict[str, Any]] = {}
    trace_records: list[str] = []
    for event in trace.events:
        kind = event["event"]
        checkpoints_value = event.get(
            "checkpoints",
            {"processed": None, "persisted": None, "max_seq_no": None},
        )
        durable_observed = event.get("durable", False)
        if kind == "primary_replication_started":
            receipt = event["receipt_id"]
            checkpoints_value = process_by_receipt[receipt]["checkpoints"]
            durable_observed = wal_by_receipt[receipt]["durable"]
        if kind == "fence_persisted":
            fence_by_node[event["node"]] = event["term"]
        if kind == "commit_captured":
            commit_capture[event["commit_id"]] = event
        if kind == "commit_persisted":
            checkpoints_value = commit_capture[event["commit_id"]]["checkpoints"]

        required = []
        if kind == "primary_replication_started":
            required = [node[item["node"]] for item in event["required_replicas"]]

        copy_doc_value = {doc[raw]: "0" for raw in docs_raw}
        copy_doc_seq = {doc[raw]: "0" for raw in docs_raw}
        copy_doc_term = {doc[raw]: "0" for raw in docs_raw}
        if kind == "copy_state":
            for observed in event["documents"]:
                token = doc[observed["doc"]]
                if observed["state"] != "absent":
                    ident = (observed["term"], observed["seq_no"])
                    operation = identity_to_write.get(ident, 0)
                    copy_doc_value[token] = str(operation)
                    copy_doc_seq[token] = str(observed["seq_no"] + 1)
                    copy_doc_term[token] = str(observed["term"])
        snapshot_doc_value = {doc[raw]: "0" for raw in docs_raw}
        for observed in event.get("documents", []):
            if observed["state"] == "absent":
                continue
            ident = (observed["term"], observed["seq_no"])
            snapshot_doc_value[doc[observed["doc"]]] = str(
                identity_to_write.get(ident, 0)
            )

        event_node = event.get("node")
        peer_raw = (
            event.get("target_node")
            or event.get("source_node")
            or event.get("replica")
        )
        message_id = event.get("_message_id")
        if message_id is not None:
            attempt = message_attempts[message_id]
            peer_raw = (
                attempt["target"]
                if kind
                in {
                    "replica_result",
                    "promotion_noop_replication_started",
                    "promotion_noop_result",
                }
                else attempt["source"]
            )
        message_phase = "none"
        if kind in {"replica_result", "promotion_noop_result"}:
            message_phase = event["message_phase"]
        elif message_id is not None:
            message_phase = "request"
        has_transport_message = (
            message_id is not None and message_phase != "none"
        )
        transport_message = (
            message_record(message_id, message_phase)
            if has_transport_message
            else "NoTraceMessage"
        )
        required_message_ids = event.get("_required_message_ids", [])
        required_messages = [
            message_record(required_id, "request")
            for required_id in required_message_ids
        ]
        crash_dropped_messages = [
            message_record(
                dropped["message_id"], dropped["message_phase"]
            )
            for dropped in event.get("dropped_messages", [])
        ]
        crash_failed_writes = [
            str(requests[request_id])
            for request_id in event.get("failed_request_ids", [])
        ]
        operation_doc = event.get("doc")
        operation_kind = event.get("op")
        write_kind = (
            "Put" if operation_kind == "index" else
            "Delete" if operation_kind == "delete" else "Put"
        )
        fence_observed = 0
        receipt = event.get("receipt_id")
        if receipt is not None:
            matching_fences = [
                item
                for item in trace.events
                if item["event"] == "fence_persisted"
                and item["node"] == event_node
                and item["step"] < event["step"]
            ]
            if matching_fences:
                fence_observed = matching_fences[-1]["term"]

        fields = {
            "step": str(event["step"]),
            "kind": json.dumps(kind),
            "node": node.get(event_node, '"NO_NODE"'),
            "peer": node.get(peer_raw, '"NO_NODE"'),
            "writeId": str(write_id(event)),
            "doc": doc.get(operation_doc, doc[docs_raw[0]]),
            "writeKind": json.dumps(write_kind),
            "origin": json.dumps(event.get("origin", "none")),
            "outcome": json.dumps(event.get("outcome", "none")),
            "reason": json.dumps(event.get("reason", "none")),
            "seq": str(event.get("seq_no", 0)),
            "term": str(event.get("term", 0)),
            "allocation": str(
                abstract_allocation(event_node, event.get("allocation"))
            ),
            "required": tla_set(required),
            "preWalVersionConflict": (
                "TRUE"
                if event["event"] == "client_result"
                and event["outcome"] == "failed"
                and event["failure_stage"] == "version_conflict"
                else "FALSE"
            ),
            "requiredMessages": tla_set(required_messages),
            "hasTransportMessage": (
                "TRUE" if has_transport_message else "FALSE"
            ),
            "transportMessage": transport_message,
            "messagePhase": json.dumps(message_phase),
            "crashFailedWrites": tla_set(crash_failed_writes),
            "crashDroppedMessages": tla_set(crash_dropped_messages),
            "processedNext": str(cp_next(checkpoints_value["processed"])),
            "persistedNext": str(cp_next(checkpoints_value["persisted"])),
            "maxNext": str(cp_next(checkpoints_value["max_seq_no"])),
            "durable": "TRUE" if durable_observed else "FALSE",
            "fillPhysical": (
                "TRUE" if event.get("_fill_physical", False) else "FALSE"
            ),
            "fenceObservedTerm": str(fence_observed),
            "fenceMaxNext": str(cp_next(event.get("fence_max_seq_no"))),
            "truncateNext": str(cp_next(event.get("truncate_through"))),
            "docValue": function("TraceDocs", "doc", copy_doc_value, "0"),
            "docSeqNext": function("TraceDocs", "doc", copy_doc_seq, "0"),
            "docTerm": function("TraceDocs", "doc", copy_doc_term, "0"),
            "emitter": node.get(event.get("emitter"), '"NO_NODE"'),
            "newPrimary": node.get(event.get("new_primary"), '"NO_NODE"'),
            "viewPrimary": node.get(event.get("primary"), '"NO_NODE"'),
            "viewTerm": str(event.get("term", 0)),
            "viewInSync": tla_set(
                [node[item] for item in event.get("in_sync", [])]
            ),
            "viewAllocations": function(
                "TraceNodes",
                "viewNode",
                {
                    node[item["node"]]: str(
                        abstract_allocation(item["node"], item["allocation"])
                    )
                    for item in event.get("allocations", [])
                },
                "0",
            ),
            "initialized": (
                "TRUE" if event.get("initialized", False) else "FALSE"
            ),
            "removedNode": node.get(
                event.get("removed_node"), '"NO_NODE"'
            ),
            "removedAllocation": str(
                abstract_allocation(
                    event.get("removed_node"),
                    event.get("removed_allocation"),
                )
            ),
            "source": node.get(
                event.get("source_node"), '"NO_NODE"'
            ),
            "target": node.get(
                event.get("target_node"), '"NO_NODE"'
            ),
            "snapshotNext": str(event.get("snapshot_next_seq_no", 0)),
            "barrierNext": str(event.get("barrier_next_seq_no", 0)),
            "observedProcessed": tla_set(
                [
                    str(item)
                    for item in (
                        event.get("processed_seqs", [])
                        or [
                            noop["seq_no"]
                            for noop in event.get("noops", [])
                        ]
                    )
                ]
            ),
            "snapshotDocValue": function(
                "TraceDocs", "doc", snapshot_doc_value, "0"
            ),
            "resultPersistedNext": str(
                cp_next(event.get("persisted_checkpoint"))
            ),
            "walPosition": str(event.get("_wal_position", 0)),
        }
        body = ",\n      ".join(f"{key} |-> {value}" for key, value in fields.items())
        trace_records.append("    [" + body + "]")

    trace_body = (
        "<<>>" if not trace_records else "<<\n" + ",\n".join(trace_records) + "\n>>"
    )
    max_writes = max(1, len(requests))
    max_term = max(
        [trace.start["shard_state"]["term"]]
        + [
            value
            for event in trace.events
            for value in (
                event.get("term"),
                event.get("primary_term"),
            )
            if isinstance(value, int)
        ]
        + [
            document["term"]
            for event in trace.events
            for document in event.get("documents", [])
            if isinstance(document.get("term"), int)
        ]
    )
    max_crashes = sum(
        event["event"] == "node_crashed" for event in trace.events
    )
    max_recoveries = sum(
        event["event"] == "recovery_snapshot" for event in trace.events
    )
    max_allocations = max(
        value
        for values in allocation_values.values()
        for value in values.values()
    )
    raft_events = sum(
        event["event"]
        in {
            "routing_promoted",
            "in_sync_removed",
            "primary_activated",
            "recovery_membership",
        }
        for event in trace.events
    )
    max_raft_entries = max(raft_events + 1, 1)
    max_pending_raft = 2 if raft_events else 0
    max_messages = max(
        6,
        max_writes * max(1, len(nodes_raw) - 1) * 2,
        len(message_attempts) * 2,
    )
    max_view_lag = max(max_raft_entries, 2)
    validator_hidden_steps = {
        "d1-core": 3,
        "d1-authority": 4,
        "d1-combined": 4,
        "d1-full": 8,
        "d1-collision": 0,
        "d1-recovery": 8,
    }[trace.profile]
    d1_fault_mode = (
        "D1Fixed" if trace.start["durability"] == "request" else "D1Async"
    )

    module = f"""---------------------------- MODULE TraceInput ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

MaxHiddenSteps == {validator_hidden_steps}
RequestDurability == {"TRUE" if trace.start["durability"] == "request" else "FALSE"}
TraceNodes == {tla_set([node[item] for item in nodes_raw])}
TraceDocs == {tla_set([doc[item] for item in docs_raw])}
TraceInitialPrimary == {node[primary_raw]}
TraceInitialInSync == {tla_set([node[item] for item in trace.start["shard_state"]["in_sync"]])}
TraceQuiescent == {"TRUE" if trace.end["quiescent"] else "FALSE"}
TraceCombined == {"TRUE" if trace.profile in {"d1-combined", "d1-full"} else "FALSE"}
NoTraceMessage ==
    [kind |-> "Replicate",
     write |-> 1,
     from |-> {node[primary_raw]},
     to |-> {node[primary_raw]},
     seq |-> 0,
     fromEpoch |-> 0,
     toEpoch |-> 0,
     term |-> 0,
     indexUuid |-> "INDEX_UUID",
     targetAllocation |-> 0]
Trace == {trace_body}

=============================================================================
"""

    profile = trace.profile
    if profile == "d1-recovery":
        initial_out_of_sync = (
            len(nodes_raw) - 1
            != len(trace.start["shard_state"]["in_sync"])
        )
        config = f"""CONSTANTS
    Nodes = {tla_set([node[item] for item in nodes_raw])}
    Docs = {tla_set([doc[item] for item in docs_raw])}
    MaxWrites = {max_writes}
    MaxCrashes = {max_crashes}
    MaxPartitions = 0
    MaxRecoveries = {max(1, max_recoveries)}
    MaxTerm = {max_term}
    MaxMessages = {max_messages}
    MaxViewLag = {max_view_lag}
    MaxAllocationId = {max_allocations}
    MaxRaftEntries = {max_raft_entries}
    MaxPendingRaft = 2
    FaultMode = "D1Fixed"
    InitialOutOfSync = {"TRUE" if initial_out_of_sync else "FALSE"}
    InitialInitialized = TRUE
    EnableRecovery = TRUE
    AllocationIds = TRUE
    ReplicaFencing = TRUE
    DurableReplicaFence = TRUE
    AllowedWriteKinds = {{"Put", "Delete"}}
    EnableRecoveryFailures = FALSE
    RestorePendingOnRestart = TRUE
    PrimaryNode = {node[primary_raw]}
    ReplicaNode = {node[replica_raw]}
    DocX = {doc[docs_raw[0]]}
    DocY = {doc[docs_raw[1]]}

SPECIFICATION TraceRecoverySpec

INVARIANT TraceRecoveryTypeOK
INVARIANT TraceRecoverySafety
INVARIANT TraceRecoveryNotAccepted
"""
    elif profile == "d1-collision":
        if len(nodes_raw) != 3:
            raise TraceSchemaError("d1-collision requires three nodes")
        config = f"""CONSTANTS
    P = {node[primary_raw]}
    R1 = {node[trace.start["shard_state"]["in_sync"][0]]}
    R2 = {node[trace.start["shard_state"]["in_sync"][1]]}
    CollisionMode = "TermAware"
    CollisionSeq = {max(
        event.get("seq_no", 0)
        for event in trace.events
    )}

SPECIFICATION TraceCollisionSpec

INVARIANT TraceCollisionTypeOK
INVARIANT TraceCollisionSafety
INVARIANT TraceCollisionNotAccepted
"""
    elif profile == "d1-authority":
        initial_out_of_sync = (
            len(nodes_raw) - 1
            != len(trace.start["shard_state"]["in_sync"])
        )
        config = f"""CONSTANTS
    Nodes = {tla_set([node[item] for item in nodes_raw])}
    Docs = {tla_set([doc[item] for item in docs_raw])}
    MaxWrites = {max_writes}
    MaxCrashes = {max(1, max_crashes)}
    MaxPartitions = 1
    MaxRecoveries = 0
    MaxTerm = {max_term}
    MaxMessages = {max_messages}
    MaxViewLag = {max_view_lag}
    MaxAllocationId = {max_allocations}
    MaxRaftEntries = {max_raft_entries}
    MaxPendingRaft = 2
    FaultMode = "C2"
    InitialOutOfSync = {"TRUE" if initial_out_of_sync else "FALSE"}
    InitialInitialized = TRUE
    EnableRecovery = FALSE
    AllocationIds = TRUE
    ReplicaFencing = TRUE
    DurableReplicaFence = TRUE
    AllowedWriteKinds = {{"Put", "Delete"}}
    EnableRecoveryFailures = FALSE
    RestorePendingOnRestart = TRUE

SPECIFICATION TraceAuthoritySpec

INVARIANT TraceAuthorityTypeOK
INVARIANT TraceAuthoritySafety
INVARIANT TraceAuthorityNotAccepted
"""
    else:
        recovery_enabled = profile == "d1-full"
        config = f"""CONSTANTS
    Nodes = {tla_set([node[item] for item in nodes_raw])}
    Docs = {tla_set([doc[item] for item in docs_raw])}
    MaxWrites = {max_writes}
    MaxCrashes = {max_crashes}
    MaxPartitions = 0
    MaxRecoveries = {max(1, max_recoveries) if recovery_enabled else 0}
    MaxTerm = {max_term}
    MaxMessages = {max_messages}
    MaxViewLag = {max_view_lag}
    MaxAllocationId = {max_allocations}
    MaxRaftEntries = {max_raft_entries}
    MaxPendingRaft = {max_pending_raft}
    FaultMode = "{d1_fault_mode}"
    InitialOutOfSync = {"TRUE" if len(trace.start["shard_state"]["in_sync"]) != len(nodes_raw) - 1 else "FALSE"}
    InitialInitialized = TRUE
    EnableRecovery = {"TRUE" if recovery_enabled else "FALSE"}
    AllocationIds = TRUE
    ReplicaFencing = TRUE
    DurableReplicaFence = TRUE
    AllowedWriteKinds = {{"Put", "Delete"}}
    EnableRecoveryFailures = FALSE
    RestorePendingOnRestart = TRUE
    PrimaryNode = {node[primary_raw]}
    ReplicaNode = {node[replica_raw]}
    DocX = {doc[docs_raw[0]]}
    DocY = {doc[docs_raw[1]]}

SPECIFICATION TraceSpec

INVARIANT TraceTypeOK
INVARIANT TraceCoreSafety
INVARIANT TraceNotAccepted
"""
    return module, config


def convert(
    trace_path: Path,
    module_path: Path,
    config_path: Path,
    profile_path: Path | None = None,
) -> LoadedTrace:
    trace = load_trace(trace_path)
    module, config = render(trace)
    module_path.write_text(module, encoding="utf-8")
    config_path.write_text(config, encoding="utf-8")
    if profile_path is not None:
        profile_path.write_text(trace.profile + "\n", encoding="utf-8")
    return trace


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--config-output", type=Path, required=True)
    parser.add_argument("--profile-output", type=Path)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        trace = convert(
            args.trace,
            args.output,
            args.config_output,
            args.profile_output,
        )
    except (OSError, TraceSchemaError) as error:
        print(f"trace conversion failed: {error}", file=sys.stderr)
        return 2
    print(f"converted {len(trace.events)} schema-v4 events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
