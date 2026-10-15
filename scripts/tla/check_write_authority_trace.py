#!/usr/bin/env python3
"""Check externally serialized FS013 authority witness observations."""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA = "ferrissearch.write-authority.trace/v1"
AUTHORITY_FIELDS = {
    "index_uuid", "shard_id", "primary_node_id", "primary_allocation_id",
    "primary_term", "activation_id",
}


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def fields(value: Any, expected: set[str]) -> None:
    require(isinstance(value, dict) and set(value) == expected,
            f"expected exact fields {sorted(expected)}")


def integer(value: Any, *, positive: bool = False) -> None:
    require(type(value) is int and int(positive) <= value <= 2**64 - 1,
            "expected a bounded positive integer" if positive else "expected a bounded nonnegative integer")


def history(value: Any, authority: dict[str, Any], *, previous: bool = False) -> None:
    fields(value, {"activation_id", "primary_term", "max_seq_no", "digest"})
    for name in ("activation_id", "primary_term"):
        integer(value[name], positive=True)
        require(value[name] < authority[name] if previous else value[name] == authority[name],
                "canonical history does not match its activation")
    if value["max_seq_no"] is not None:
        integer(value["max_seq_no"])
    digest = value["digest"]
    require(isinstance(digest, list) and len(digest) == 32
            and all(type(byte) is int and 0 <= byte <= 255 for byte in digest)
            and any(digest), "invalid canonical history digest")


def copy_identity(value: Any, nodes: set[str]) -> tuple[str, int]:
    fields(value, {"node_id", "allocation_id"})
    require(value["node_id"] in nodes, "copy node is not declared")
    integer(value["allocation_id"], positive=True)
    return value["node_id"], value["allocation_id"]


@dataclass
class Intent:
    value: dict[str, Any]
    journaled: bool = False
    proposed: bool = False
    committed: bool = False
    settled: bool = False


def definitive_observation(exclusion: dict[str, Any], observed: Any, nodes: set[str]) -> bool:
    fields(observed, {
        "version", "index_uuid", "primary", "primary_allocation", "primary_term",
        "activation_id", "phase", "primary_unavailable", "failed_copies",
    })
    integer(observed["version"])
    for name in ("primary_allocation", "primary_term", "activation_id"):
        if observed[name] is not None:
            integer(observed[name], positive=True)
    require(observed["phase"] in (None, "inactive", "promoting", "active"),
            "invalid observed authority phase")
    require(observed["primary_unavailable"] is None
            or type(observed["primary_unavailable"]) is bool, "invalid availability observation")
    require(observed["index_uuid"] is None
            or (isinstance(observed["index_uuid"], str) and bool(observed["index_uuid"])),
            "invalid observed index UUID")
    require(observed["primary"] is None or observed["primary"] in nodes,
            "invalid observed primary")
    require(isinstance(observed["failed_copies"], list), "missing observed failed copies")
    current = {}
    for copy in observed["failed_copies"]:
        fields(copy, {"node_id", "allocation_id", "authoritative"})
        require(copy["node_id"] in nodes and copy["node_id"] not in current,
                "invalid or duplicate observed failed copy")
        if copy["allocation_id"] is not None:
            integer(copy["allocation_id"], positive=True)
        require(type(copy["authoritative"]) is bool, "invalid authoritative-copy observation")
        current[copy["node_id"]] = copy
    require(set(current) == {copy["node_id"] for copy in exclusion["failed_copies"]},
            "observed allocations do not match the captured failed set")
    if observed["version"] < exclusion["captured_state_version"]:
        return False
    authority = exclusion["authority"]
    if observed["index_uuid"] != authority["index_uuid"] or observed["primary_term"] is None:
        return True
    if observed["primary_term"] < authority["primary_term"]:
        return False
    if (
        observed["primary"] != authority["primary_node_id"]
        or observed["primary_allocation"] != authority["primary_allocation_id"]
        or observed["primary_term"] != authority["primary_term"]
        or observed["activation_id"] != authority["activation_id"]
        or observed["phase"] != "active"
    ):
        return True
    return all(
        not current[copy["node_id"]]["authoritative"]
        or current[copy["node_id"]]["allocation_id"] != copy["allocation_id"]
        for copy in exclusion["failed_copies"]
    )


def validate(document: Any) -> None:
    fields(document, {"schema", "profile", "run_id", "index_uuid", "shard", "nodes", "events"})
    require(document["schema"] == SCHEMA, "unsupported write-authority trace schema")
    require(document["profile"] == "serialized-authority-witness", "unsupported authority profile")
    for name in ("run_id", "index_uuid"):
        require(isinstance(document[name], str) and bool(document[name]), f"missing {name}")
    integer(document["shard"])
    require(document["shard"] <= 2**32 - 1, "shard exceeds u32")
    require(isinstance(document["nodes"], list)
            and all(isinstance(node, str) and node for node in document["nodes"]),
            "invalid declared nodes")
    nodes = set(document["nodes"])
    require(bool(nodes) and len(nodes) == len(document["nodes"]), "duplicate or absent declared nodes")
    require(isinstance(document["events"], list) and bool(document["events"]),
            "an authority witness requires observations")
    phases, current, frozen, seals, fences, debt, removed = {}, {}, {}, {}, {}, {}, {}
    revoked = set()
    intents: dict[str, Intent] = {}
    for step, event in enumerate(document["events"]):
        try:
            fields(event, {"step", "event", "authority", "details"})
            require(type(event["step"]) is int and event["step"] == step, "non-consecutive step")
            authority = event["authority"]
            fields(authority, AUTHORITY_FIELDS)
            integer(authority["shard_id"])
            require(authority["index_uuid"] == document["index_uuid"]
                    and authority["shard_id"] == document["shard"], "foreign shard authority")
            require(authority["primary_node_id"] in nodes, "undeclared primary")
            for name in ("primary_allocation_id", "primary_term", "activation_id"):
                integer(authority[name], positive=True)
            key = tuple(authority[name] for name in sorted(AUTHORITY_FIELDS))
            source = authority["primary_node_id"], authority["primary_allocation_id"]
            kind, details = event["event"], event["details"]
            require(isinstance(kind, str), "event name must be a string")
            if kind == "canonical_promoting":
                fields(details, {"state_version", "previous_history"})
                integer(details["state_version"], positive=True)
                if details["previous_history"] is not None:
                    history(details["previous_history"], authority, previous=True)
                phases[key] = "promoting"
                current[source[0]] = key
            elif kind == "canonical_frozen":
                fields(details, {"copy", "fence_term", "persisted_next", "max_next"})
                require(phases.get(key) == "promoting", "freeze has no committed PROMOTING authority")
                copy = copy_identity(details["copy"], nodes)
                integer(details["fence_term"], positive=True)
                integer(details["persisted_next"])
                integer(details["max_next"])
                require(details["fence_term"] == authority["primary_term"]
                        and fences.get(copy, 0) <= details["fence_term"], "invalid durable copy fence")
                require(details["persisted_next"] <= details["max_next"], "invalid frozen watermarks")
                fences[copy] = details["fence_term"]
                frozen[key, copy] = details["max_next"]
            elif kind == "canonical_sealed":
                fields(details, {"copy", "history"})
                copy = copy_identity(details["copy"], nodes)
                require((key, copy) in frozen, "seal has no exact frozen process authority")
                history(details["history"], authority)
                end = 0 if details["history"]["max_seq_no"] is None else details["history"]["max_seq_no"] + 1
                require(end >= frozen[key, copy], "seal loses protected local history")
                seals[key, copy] = details["history"]
            elif kind == "canonical_active":
                fields(details, {"history", "eligible_replicas"})
                history(details["history"], authority)
                require(phases.get(key) == "promoting", "ACTIVE has no committed reservation")
                require(seals.get((key, source)) == details["history"], "ACTIVE has no durable local seal")
                require(isinstance(details["eligible_replicas"], list), "missing eligible replicas")
                seen = {source[0]}
                for value in details["eligible_replicas"]:
                    copy = copy_identity(value, nodes)
                    require(copy[0] not in seen, "duplicate or primary eligible replica")
                    seen.add(copy[0])
                    require(seals.get((key, copy)) == details["history"], "eligible copy has no matching seal")
                phases[key] = "active"
                current[source[0]] = key
            elif kind.startswith("exclusion_"):
                expected = {"exclusion", "accepted", "observed"} if kind == "exclusion_settled" else {"exclusion"}
                fields(details, expected)
                exclusion = details["exclusion"]
                fields(exclusion, {"authority", "captured_state_version", "transition_id", "failed_copies"})
                require(exclusion["authority"] == authority, "exclusion has a foreign source authority")
                integer(exclusion["captured_state_version"], positive=True)
                transition = exclusion["transition_id"]
                require(isinstance(transition, str) and str(uuid.UUID(transition)) == transition,
                        "transition is not a canonical UUID")
                require(isinstance(exclusion["failed_copies"], list)
                        and bool(exclusion["failed_copies"]), "exclusion has no failed copies")
                failed = [copy_identity(copy, nodes) for copy in exclusion["failed_copies"]]
                require(len({copy[0] for copy in failed}) == len(failed)
                        and source[0] not in {copy[0] for copy in failed}, "invalid failed-copy set")
                if kind == "exclusion_closed":
                    require(phases.get(key) == "active", "debt has no previously ACTIVE epoch")
                    require(transition not in intents, "transition ID was reused")
                    intents[transition] = Intent(exclusion)
                    debt.setdefault(key, set()).add(transition)
                    continue
                require(transition in intents and intents[transition].value == exclusion,
                        "exclusion differs from its admission-closing intent")
                intent = intents[transition]
                if kind == "exclusion_journaled":
                    require(not intent.settled, "settled intent was journaled again")
                    intent.journaled = True
                elif kind == "exclusion_proposed":
                    require(intent.journaled, "proposal precedes durable journal")
                    intent.proposed = True
                elif kind == "exclusion_committed":
                    require(intent.proposed, "commit has no journaled conditional proposal")
                    intent.committed = True
                elif kind == "exclusion_settled":
                    require(intent.journaled, "settlement has no durable intent")
                    require(type(details["accepted"]) is bool, "invalid accepted observation")
                    observed = definitive_observation(exclusion, details["observed"], nodes)
                    require(intent.committed if details["accepted"] else observed,
                            "settlement has neither exact committed acceptance nor definitive applied evidence")
                    intent.settled = True
                    debt.setdefault(key, set()).discard(transition)
                    removed.setdefault(key, set()).update(failed)
                else:
                    raise ValueError(f"unknown event {kind!r}")
            elif kind == "old_authority_revoked":
                fields(details, set())
                revoked.add(key)
            elif kind == "write_certified":
                fields(details, {"minimum", "eligible_proven", "copies"})
                integer(details["minimum"], positive=True)
                integer(details["eligible_proven"], positive=True)
                require(phases.get(key) == "active" and current.get(source[0]) == key,
                        "write certificate has no current ACTIVE authority")
                require(key not in revoked and not debt.get(key), "write certificate has revoked authority or debt")
                require(fences.get(source) == authority["primary_term"], "write certificate has no exact durable fence")
                require(isinstance(details["copies"], list), "missing copy proofs")
                seen, primary, proven = set(), 0, 0
                for copy in details["copies"]:
                    fields(copy, {"node_id", "allocation_id", "primary", "acknowledged"})
                    identity = copy_identity({name: copy[name] for name in ("node_id", "allocation_id")}, nodes)
                    require(identity[0] not in seen, "duplicate copy proof")
                    seen.add(identity[0])
                    require(type(copy["primary"]) is bool and type(copy["acknowledged"]) is bool,
                            "invalid copy-proof outcome")
                    if copy["primary"]:
                        primary += 1
                        require(identity == source and copy["acknowledged"], "invalid primary proof")
                    proven += int(copy["acknowledged"])
                    require(copy["acknowledged"] or identity in removed.get(key, set()),
                            "failed copy has no observed exact exclusion")
                require(primary == 1 and proven >= details["eligible_proven"] >= details["minimum"],
                        "certificate does not meet the eligible-copy floor")
            else:
                raise ValueError(f"unknown event {kind!r}")
        except (ValueError, TypeError, KeyError) as error:
            name = event.get("event", "?") if isinstance(event, dict) else "?"
            raise ValueError(f"step {step} ({name}): {error}") from error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    try:
        validate(json.loads(args.trace.read_text(encoding="utf-8")))
    except (OSError, ValueError) as error:
        print(f"Write-authority witness rejected: {error}", file=sys.stderr)
        return 1
    print(f"Serialized write-authority witness passed: {args.trace}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
