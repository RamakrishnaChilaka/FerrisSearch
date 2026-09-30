#!/usr/bin/env python3
"""Capture and validate deterministic randomized D1 protocol traces."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass


ROOT = Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/tla/check_d1_trace_invariants.py"
VALIDATOR = ROOT / "scripts/tla/validate_trace.sh"
TIME = Path("/usr/bin/time")
MODES = {
    "correct": "none",
    "arrival-order": "arrival-order",
    "seq-only-redelivery": "seq-only-redelivery",
}
TIME_PATTERN = re.compile(
    r"(?P<label>[A-Z_]+)_RAW elapsed=(?P<elapsed>[0-9.]+) "
    r"user=(?P<user>[0-9.]+) sys=(?P<sys>[0-9.]+) "
    r"maxrss_kb=(?P<rss>[0-9]+) exit=(?P<exit>[0-9]+)"
)


@dataclass(frozen=True)
class TimedResult:
    status: int
    elapsed: float
    output: str


@dataclass(frozen=True)
class SeedResult:
    seed: int
    passed: bool
    events: int
    faults: dict[str, int]
    controls: list[str]
    checker_status: int
    checker_elapsed: float
    tla_status: int
    tla_elapsed: float
    log: Path


def parse_seeds(value: str) -> list[int]:
    seeds: list[int] = []
    for token in (item.strip() for item in value.split(",")):
        if not token:
            continue
        if "..=" in token:
            start_raw, end_raw = token.split("..=", 1)
            start = int(start_raw)
            end = int(end_raw)
            if start > end:
                raise ValueError(f"reversed seed range: {token}")
            seeds.extend(range(start, end + 1))
        else:
            seeds.append(int(token))
    if not seeds:
        raise ValueError("seed list is empty")
    if len(set(seeds)) != len(seeds):
        raise ValueError("seed list contains duplicates")
    return seeds


def run_timed(
    label: str,
    command: list[str],
    *,
    env: dict[str, str],
    cwd: Path = ROOT,
) -> TimedResult:
    format_string = (
        f"{label}_RAW elapsed=%e user=%U sys=%S maxrss_kb=%M exit=%x"
    )
    completed = subprocess.run(
        [str(TIME), "-f", format_string, *command],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    matches = [
        match
        for match in TIME_PATTERN.finditer(completed.stdout)
        if match.group("label") == label
    ]
    if not matches:
        raise RuntimeError(
            f"{label} command did not emit raw /usr/bin/time output:\n"
            f"{completed.stdout}"
        )
    elapsed = float(matches[-1].group("elapsed"))
    return TimedResult(completed.returncode, elapsed, completed.stdout)


def trace_paths(output_dir: Path, mode: str, seed: int) -> tuple[Path, Path]:
    trace = output_dir / f"d1-random-{mode}-{seed}.jsonl"
    return trace, trace.with_suffix(".schedule.json")


def schedule_summary(schedule_path: Path) -> tuple[dict[str, int], list[str]]:
    schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
    faults = {
        "delay": 0,
        "drop_request": 0,
        "drop_response": 0,
        "hold_until_applied": 0,
    }
    for fault in schedule["faults"]:
        faults[fault["action"]["kind"]] += 1
    controls = list(schedule["control_faults"])
    for control in controls:
        faults[control] = faults.get(control, 0) + 1
    return faults, controls


def validate_seed(
    *,
    seed: int,
    mode: str,
    output_dir: Path,
    tla_timeout: int,
    tla_heap: str,
) -> SeedResult:
    trace, schedule = trace_paths(output_dir, mode, seed)
    if not trace.is_file() or not schedule.is_file():
        raise RuntimeError(f"seed {seed} is missing its trace or schedule")

    log = output_dir / f"seed-{seed:06d}.validation.log"
    env = os.environ.copy()
    checker_command = [sys.executable, str(CHECKER), str(trace)]
    checker = run_timed("CHECKER", checker_command, env=env)
    tla_env = env | {
        "TLA_TRACE_TIMEOUT_SECONDS": str(tla_timeout),
        "TLA_TRACE_HEAP": tla_heap,
    }
    tla_command = [str(VALIDATOR), str(trace)]
    tla = run_timed("TLA", tla_command, env=tla_env)

    expected = 0 if mode == "correct" else 1
    passed = checker.status == expected and tla.status == expected
    events = sum(1 for _ in trace.open(encoding="utf-8"))
    faults, controls = schedule_summary(schedule)
    log.write_text(
        "\n".join(
            [
                f"$ {' '.join(checker_command)}",
                checker.output.rstrip(),
                f"CHECKER_STATUS={checker.status}",
                f"$ {' '.join(tla_command)}",
                tla.output.rstrip(),
                f"TLA_STATUS={tla.status}",
                f"SEED_VERDICT={'PASS' if passed else 'FAIL'}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return SeedResult(
        seed=seed,
        passed=passed,
        events=events,
        faults=faults,
        controls=controls,
        checker_status=checker.status,
        checker_elapsed=checker.elapsed,
        tla_status=tla.status,
        tla_elapsed=tla.elapsed,
        log=log,
    )


def capture(
    *,
    seeds: list[int],
    mode: str,
    output_dir: Path,
    cargo_target_dir: Path,
) -> None:
    env = os.environ.copy()
    env.pop("D1_TRACE_OUTPUT", None)
    env.update(
        {
            "CARGO_TARGET_DIR": str(cargo_target_dir),
            "D1_TRACE_SEEDS": ",".join(str(seed) for seed in seeds),
            "D1_TRACE_MUTATION": MODES[mode],
            "D1_TRACE_OUTPUT_DIR": str(output_dir),
            "RUST_TEST_THREADS": "1",
        }
    )
    command = [
        "cargo",
        "test",
        "--manifest-path",
        str(ROOT / "Cargo.toml"),
        "--features",
        "protocol-trace",
        "--test",
        "d1_protocol_trace",
        "randomized_three_node_fault_trace",
        "--",
        "--exact",
        "--nocapture",
    ]
    result = run_timed("CAPTURE", command, env=env)
    capture_log = output_dir / "capture.log"
    capture_log.write_text(
        "\n".join(
            [
                f"$ {' '.join(command)}",
                result.output.rstrip(),
                f"CAPTURE_STATUS={result.status}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(result.output, end="")
    if result.status != 0:
        raise RuntimeError(
            f"randomized trace capture failed; see {capture_log}"
        )


def validate_all(
    *,
    seeds: list[int],
    mode: str,
    output_dir: Path,
    jobs: int,
    tla_timeout: int,
    tla_heap: str,
) -> list[SeedResult]:
    results: dict[int, SeedResult] = {}
    stop_after_failure = mode == "correct"
    seed_iter = iter(seeds)
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
        pending: dict[concurrent.futures.Future[SeedResult], int] = {}

        def submit_next() -> bool:
            try:
                seed = next(seed_iter)
            except StopIteration:
                return False
            future = executor.submit(
                validate_seed,
                seed=seed,
                mode=mode,
                output_dir=output_dir,
                tla_timeout=tla_timeout,
                tla_heap=tla_heap,
            )
            pending[future] = seed
            return True

        for _ in range(jobs):
            if not submit_next():
                break

        stop = False
        while pending:
            done, _ = concurrent.futures.wait(
                pending,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                seed = pending.pop(future)
                try:
                    result = future.result()
                except Exception as error:
                    result = SeedResult(
                        seed=seed,
                        passed=False,
                        events=0,
                        faults={},
                        controls=[],
                        checker_status=-1,
                        checker_elapsed=0.0,
                        tla_status=-1,
                        tla_elapsed=0.0,
                        log=output_dir / f"seed-{seed:06d}.validation.log",
                    )
                    result.log.write_text(
                        f"SEED_VERDICT=FAIL\nerror={error}\n",
                        encoding="utf-8",
                    )
                results[seed] = result
                if stop_after_failure and not result.passed:
                    stop = True
            while not stop and len(pending) < jobs and submit_next():
                pass
            if stop:
                for future in pending:
                    future.cancel()
                for future, seed in list(pending.items()):
                    if future.cancelled():
                        pending.pop(future)

    return [results[seed] for seed in sorted(results)]


def percentile_summary(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    return statistics.median(values), max(values)


def print_summary(
    *,
    requested: list[int],
    mode: str,
    results: list[SeedResult],
    output_dir: Path,
) -> bool:
    for result in results:
        print(
            json.dumps(
                {
                    "seed": result.seed,
                    "verdict": "pass" if result.passed else "fail",
                    "events": result.events,
                    "faults": result.faults,
                    "control_faults": result.controls,
                    "checker_status": result.checker_status,
                    "checker_seconds": result.checker_elapsed,
                    "tla_status": result.tla_status,
                    "tla_seconds": result.tla_elapsed,
                    "raw_log": str(result.log),
                },
                sort_keys=True,
            )
        )

    checker_p50, checker_max = percentile_summary(
        [result.checker_elapsed for result in results]
    )
    tla_p50, tla_max = percentile_summary(
        [result.tla_elapsed for result in results]
    )
    summary = {
        "mode": mode,
        "requested_seeds": len(requested),
        "validated_seeds": len(results),
        "passed_seeds": sum(result.passed for result in results),
        "checker_seconds_p50": checker_p50,
        "checker_seconds_max": checker_max,
        "tla_seconds_p50": tla_p50,
        "tla_seconds_max": tla_max,
        "output_dir": str(output_dir),
    }
    if mode != "correct" and results:
        summary["checker_detection_rate"] = sum(
            result.checker_status == 1 for result in results
        ) / len(results)
        summary["tla_detection_rate"] = sum(
            result.tla_status == 1 for result in results
        ) / len(results)
    print("SWEEP_SUMMARY " + json.dumps(summary, sort_keys=True))

    if len(results) != len(requested):
        return False
    if not all(result.passed for result in results):
        return False
    if mode != "correct":
        return (
            summary["checker_detection_rate"] >= 0.80
            and summary["tla_detection_rate"] >= 0.80
        )
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=sorted(MODES),
        default="correct",
    )
    parser.add_argument("--seeds", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--jobs",
        type=int,
        default=min(2, os.cpu_count() or 1),
    )
    parser.add_argument("--tla-timeout", type=int, default=300)
    parser.add_argument("--tla-heap", default="2g")
    parser.add_argument(
        "--cargo-target-dir",
        type=Path,
        default=Path(
            os.environ.get("CARGO_TARGET_DIR", ROOT / "target")
        ),
    )
    parser.add_argument("--skip-capture", action="store_true")
    args = parser.parse_args()

    try:
        seeds = parse_seeds(args.seeds)
    except ValueError as error:
        parser.error(str(error))
    if args.jobs < 1 or args.jobs > 2:
        parser.error("--jobs must be 1 or 2")
    if args.tla_timeout < 1:
        parser.error("--tla-timeout must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not args.skip_capture:
            capture(
                seeds=seeds,
                mode=args.mode,
                output_dir=args.output_dir,
                cargo_target_dir=args.cargo_target_dir,
            )
        results = validate_all(
            seeds=seeds,
            mode=args.mode,
            output_dir=args.output_dir,
            jobs=args.jobs,
            tla_timeout=args.tla_timeout,
            tla_heap=args.tla_heap,
        )
    except (OSError, RuntimeError) as error:
        print(f"sweep failed: {error}", file=sys.stderr)
        return 1

    return 0 if print_summary(
        requested=seeds,
        mode=args.mode,
        results=results,
        output_dir=args.output_dir,
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
