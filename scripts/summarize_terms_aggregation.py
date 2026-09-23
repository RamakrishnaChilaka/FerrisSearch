#!/usr/bin/env python3
"""Validate and summarize paired terms-aggregation benchmark reports."""

import argparse
import json
from pathlib import Path

from compare_engines import persist, summarize


def load_completed(path):
    report = json.loads(path.read_text())
    if report.get("status") != "completed":
        raise ValueError(f"Incomplete benchmark report: {path}")
    return report


def require_same(label, values):
    first = values[0]
    if any(value != first for value in values[1:]):
        raise ValueError(f"Mismatched {label}")
    return first


def reduction(before, after):
    return (before - after) / before * 100


def run(args):
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite existing summary: {args.output}")
    if len(args.before) != len(args.after):
        raise ValueError("Provide the same number of before and after reports")

    preparation = load_completed(args.preparation)
    before = [load_completed(path) for path in args.before]
    after = [load_completed(path) for path in args.after]
    reports = before + after

    documents = require_same("document count", [report["documents"] for report in reports])
    dense_cardinality = require_same(
        "dense cardinality", [report["dense_cardinality"] for report in reports]
    )
    sparse_cardinality = require_same(
        "sparse cardinality", [report["sparse_cardinality"] for report in reports]
    )
    sparse_hot_cardinality = require_same(
        "sparse hot cardinality",
        [report["sparse_hot_cardinality"] for report in reports],
    )
    prepared_data_sha256 = require_same(
        "prepared data hash", [report["prepared_data_sha256"] for report in reports]
    )
    prepared_segments = require_same(
        "prepared segments", [sorted(report["prepared_segments"]) for report in reports]
    )
    conditions = require_same(
        "benchmark conditions", [report["conditions"] for report in reports]
    )
    harness_sha256 = require_same(
        "harness hash", [report["harness_sha256"] for report in reports]
    )
    before_binary_sha256 = require_same(
        "baseline binary hash", [report["binary_sha256"] for report in before]
    )
    after_binary_sha256 = require_same(
        "modified binary hash", [report["binary_sha256"] for report in after]
    )
    if before_binary_sha256 == after_binary_sha256:
        raise ValueError("Before and after binary hashes must differ")
    platform_name = require_same(
        "platform", [report["platform"] for report in reports]
    )
    cpu_model = require_same(
        "CPU model", [report["cpu_model"] for report in reports]
    )
    host_memory_bytes = require_same(
        "host memory", [report["host_memory_bytes"] for report in reports]
    )
    client_allowed_cpus = require_same(
        "client CPU affinity", [report["client_allowed_cpus"] for report in reports]
    )
    server_allowed_cpus = require_same(
        "server CPU affinity",
        [report["server_before"]["allowed_cpus"] for report in reports],
    )
    memory_max = require_same(
        "server memory limit",
        [report["server_before"]["memory_max"] for report in reports],
    )
    memory_swap_max = require_same(
        "server swap limit",
        [report["server_before"]["memory_swap_max"] for report in reports],
    )

    case_names = require_same(
        "case names", [sorted(report["cases"]) for report in reports]
    )
    cases = {}
    for case_name in case_names:
        request_body = require_same(
            f"{case_name} request",
            [report["cases"][case_name]["request"] for report in reports],
        )
        expected_sha256 = require_same(
            f"{case_name} expected result",
            [report["cases"][case_name]["expected_sha256"] for report in reports],
        )
        result_sha256 = require_same(
            f"{case_name} actual result",
            [report["cases"][case_name]["last_result_sha256"] for report in reports],
        )
        before_latencies = [
            latency
            for report in before
            for latency in report["cases"][case_name]["measured_latencies_ms"]
        ]
        after_latencies = [
            latency
            for report in after
            for latency in report["cases"][case_name]["measured_latencies_ms"]
        ]
        before_summary = summarize(before_latencies)
        after_summary = summarize(after_latencies)
        paired_runs = []
        for before_report, after_report in zip(before, after):
            before_run = before_report["cases"][case_name]["summary"]
            after_run = after_report["cases"][case_name]["summary"]
            paired_runs.append(
                {
                    "before_label": before_report["label"],
                    "after_label": after_report["label"],
                    "before": before_run,
                    "after": after_run,
                    "p50_reduction_percent": reduction(
                        before_run["p50_ms"], after_run["p50_ms"]
                    ),
                    "p95_reduction_percent": reduction(
                        before_run["p95_ms"], after_run["p95_ms"]
                    ),
                    "mean_reduction_percent": reduction(
                        before_run["mean_ms"], after_run["mean_ms"]
                    ),
                }
            )
        cases[case_name] = {
            "request": request_body,
            "expected_sha256": expected_sha256,
            "actual_sha256": result_sha256,
            "before_pooled": before_summary,
            "after_pooled": after_summary,
            "p50_reduction_percent": reduction(
                before_summary["p50_ms"], after_summary["p50_ms"]
            ),
            "p95_reduction_percent": reduction(
                before_summary["p95_ms"], after_summary["p95_ms"]
            ),
            "mean_reduction_percent": reduction(
                before_summary["mean_ms"], after_summary["mean_ms"]
            ),
            "paired_runs": paired_runs,
        }

    summary = {
        "status": "completed",
        "date": "2026-09-22",
        "scope": "FerrisSearch string terms aggregation dense counter with sparse control",
        "before_reports": [str(path) for path in args.before],
        "after_reports": [str(path) for path in args.after],
        "preparation_report": str(args.preparation),
        "baseline_binary_sha256": before_binary_sha256,
        "modified_binary_sha256": after_binary_sha256,
        "harness_sha256": harness_sha256,
        "prepared_data_sha256": prepared_data_sha256,
        "bulk_payload_sha256": preparation["bulk_payload_sha256"],
        "toolchain": {
            "rustc": args.rustc_version,
            "cargo": args.cargo_version,
        },
        "host": {
            "platform": platform_name,
            "cpu_model": cpu_model,
            "logical_cpus": args.host_logical_cpus,
            "host_memory_bytes": host_memory_bytes,
            "server_allowed_cpus": server_allowed_cpus,
            "client_allowed_cpus": client_allowed_cpus,
            "memory_max": memory_max,
            "memory_swap_max": memory_swap_max,
        },
        "documents": documents,
        "dense_cardinality": dense_cardinality,
        "sparse_unique_cardinality": sparse_cardinality,
        "sparse_hot_cardinality": sparse_hot_cardinality,
        "prepared_segments": prepared_segments,
        "conditions": conditions,
        "correctness": {
            "all_requests_checked": True,
            "before_after_requests_identical": True,
            "before_after_results_identical": True,
            "prepared_data_byte_identical": True,
        },
        "cases": cases,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    persist(args.output, summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, nargs="+", required=True)
    parser.add_argument("--after", type=Path, nargs="+", required=True)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rustc-version", required=True)
    parser.add_argument("--cargo-version", required=True)
    parser.add_argument("--host-logical-cpus", type=int, required=True)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
