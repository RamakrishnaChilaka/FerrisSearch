#!/usr/bin/env python3
"""Validate the OpenSearch compatibility inventory against the Axum router."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "docs" / "opensearch-compatibility.json"
ROUTER_PATH = ROOT / "src" / "api" / "mod.rs"
ALLOWED_CLASSIFICATIONS = {
    "supported",
    "partial",
    "intentionally_different",
    "unsupported",
}


def route_calls(source: str) -> list[str]:
    calls: list[str] = []
    cursor = 0
    marker = ".route("
    while True:
        start = source.find(marker, cursor)
        if start < 0:
            return calls
        index = start + len(marker)
        depth = 1
        in_string = False
        escaped = False
        while index < len(source) and depth:
            char = source[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
            elif char == '"':
                in_string = True
            elif char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            index += 1
        if depth:
            raise ValueError("unterminated .route(...) call in src/api/mod.rs")
        calls.append(source[start + len(marker) : index - 1])
        cursor = index


def routed_endpoints(source: str) -> set[tuple[str, str]]:
    routes: set[tuple[str, str]] = set()
    for call in route_calls(source):
        path_match = re.match(r'\s*"([^"]+)"\s*,', call)
        if path_match is None:
            raise ValueError(f"could not parse route path from: {call[:80]!r}")
        path = path_match.group(1)
        methods = {
            method.upper()
            for method in re.findall(r"\b(get|post|put|delete|head)\s*\(", call)
        }
        if not methods:
            raise ValueError(f"could not parse route method for {path}")
        routes.update((method, path) for method in methods)
    return routes


def evidence_path(reference: str) -> Path:
    relative = reference.split("#", 1)[0]
    return ROOT / relative


def github_heading_slug(heading: str) -> str:
    normalized = re.sub(r"<[^>]+>", "", heading.strip().lower())
    normalized = re.sub(r"[^\w\- ]", "", normalized)
    return normalized.replace(" ", "-")


def validate_evidence(reference: str) -> str | None:
    path = evidence_path(reference)
    if not path.exists():
        return f"missing evidence path {reference!r}"
    _, separator, fragment = reference.partition("#")
    if not separator or not fragment:
        return None

    text = path.read_text()
    if path.suffix == ".md":
        headings = {
            github_heading_slug(line.lstrip("#").strip())
            for line in text.splitlines()
            if re.match(r"^#{1,6} ", line)
        }
        if fragment not in headings:
            return f"missing evidence heading {reference!r}"
        return None

    symbol = fragment.rsplit("::", 1)[-1]
    if symbol not in text:
        return f"missing evidence symbol {reference!r}"
    return None


def main() -> int:
    matrix = json.loads(MATRIX_PATH.read_text())
    errors: list[str] = []

    if matrix.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if not matrix.get("matrix_version"):
        errors.append("matrix_version is required")
    if not matrix.get("source_baseline"):
        errors.append("source_baseline is required")

    documented: set[tuple[str, str]] = set()
    seen_ids: set[str] = set()
    for endpoint in matrix.get("endpoints", []):
        endpoint_id = endpoint.get("id")
        if not endpoint_id or endpoint_id in seen_ids:
            errors.append(f"endpoint id is missing or duplicated: {endpoint_id!r}")
        seen_ids.add(endpoint_id)

        classification = endpoint.get("classification")
        if classification not in ALLOWED_CLASSIFICATIONS - {"unsupported"}:
            errors.append(
                f"{endpoint_id}: routed endpoint has invalid classification {classification!r}"
            )

        path = endpoint.get("path")
        methods = endpoint.get("methods", [])
        if not path or not methods:
            errors.append(f"{endpoint_id}: path and methods are required")
        for method in methods:
            documented.add((method.upper(), path))

        for reference in endpoint.get("evidence", []):
            if error := validate_evidence(reference):
                errors.append(f"{endpoint_id}: {error}")
        if not any(
            reference.startswith("tests/")
            or "/tests.rs" in reference
            or "_tests.rs" in reference
            or "#tests::" in reference
            for reference in endpoint.get("evidence", [])
        ):
            errors.append(f"{endpoint_id}: routed endpoint requires a test reference")

    unsupported: set[tuple[str, str]] = set()
    for endpoint in matrix.get("important_unsupported", []):
        endpoint_id = endpoint.get("id")
        if not endpoint_id or endpoint_id in seen_ids:
            errors.append(f"unsupported endpoint id is missing or duplicated: {endpoint_id!r}")
        seen_ids.add(endpoint_id)
        if endpoint.get("classification") != "unsupported":
            errors.append(f"{endpoint_id}: unsupported entry must use classification 'unsupported'")
        path = endpoint.get("path")
        for method in endpoint.get("methods", []):
            unsupported.add((method.upper(), path))
        for reference in endpoint.get("evidence", []):
            if error := validate_evidence(reference):
                errors.append(f"{endpoint_id}: {error}")

    actual = routed_endpoints(ROUTER_PATH.read_text())
    for method, path in sorted(actual - documented):
        errors.append(f"router endpoint missing from matrix: {method} {path}")
    for method, path in sorted(documented - actual):
        errors.append(f"matrix endpoint is not routed: {method} {path}")
    for method, path in sorted(actual & unsupported):
        errors.append(f"unsupported endpoint is actually routed: {method} {path}")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1

    print(
        f"OpenSearch compatibility matrix covers {len(actual)} routed method/path pairs "
        f"and {len(unsupported)} important unsupported pairs"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
