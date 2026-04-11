#!/usr/bin/env bash

set -euo pipefail

HOST="${1:-http://localhost:9200}"
RUNS="${RUNS:-3}"
OUT_DIR="${OUT_DIR:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ -n "$OUT_DIR" ]]; then
    mkdir -p "$OUT_DIR"
fi

now_ns() {
  "$PYTHON_BIN" -c 'import time; print(time.perf_counter_ns())'
}

run_query() {
    local slug="$1"
    local label="$2"
    local query="$3"

    local tmp
    tmp="$(mktemp)"
    local best_file
    best_file="$(mktemp)"
    local best_ms=""
    local best_body=""
    local request_body
    request_body="$("$PYTHON_BIN" -c 'import json, sys; print(json.dumps({"query": sys.argv[1], "analyze": True}))' "$query")"

    for run in $(seq 1 "$RUNS"); do
      local started ended elapsed body
      started="$(now_ns)"
        body="$(curl -sS "$HOST/nyc-taxis/_sql/explain" \
            -H 'Content-Type: application/json' \
        -d "$request_body")"
      ended="$(now_ns)"
        elapsed=$(( (ended - started) / 1000000 ))
        printf '%s\n' "$body" >> "$tmp"
        if [[ -z "$best_ms" || "$elapsed" -lt "$best_ms" ]]; then
            best_ms="$elapsed"
            best_body="$body"
        fi
    done

    printf '\n=== %s ===\n' "$label"
    printf 'best_wall_ms=%s over %s run(s)\n' "$best_ms" "$RUNS"
    printf '%s\n' "$best_body" > "$best_file"
    "$PYTHON_BIN" - "$best_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    payload = json.load(fh)
timings = payload.get("timings", {})
grouped = timings.get("grouped_merge") or {}
summary = {
    "execution_mode": payload.get("execution_mode"),
    "matched_hits": payload.get("matched_hits"),
    "row_count": payload.get("row_count"),
    "planning_ms": timings.get("planning_ms"),
    "search_ms": timings.get("search_ms"),
    "merge_ms": timings.get("merge_ms"),
    "datafusion_ms": timings.get("datafusion_ms"),
    "total_ms": timings.get("total_ms"),
    "grouped_merge": grouped,
}
print(json.dumps(summary, indent=2))
PY

    if [[ -n "$OUT_DIR" ]]; then
        mv "$best_file" "$OUT_DIR/$slug.best.json"
        mv "$tmp" "$OUT_DIR/$slug.all.jsonl"
    else
        rm -f "$tmp" "$best_file"
    fi
}

run_query \
  "carrier-market-share" \
  "Q1 Carrier Market Share" \
  "SELECT hvfhs_license_num, COUNT(*) AS rides, SUM(base_passenger_fare) AS revenue, AVG(base_passenger_fare) AS avg_fare FROM \"nyc-taxis\" GROUP BY hvfhs_license_num ORDER BY rides DESC"

run_query \
  "pickup-zones-topk" \
  "Q2 Top Pickup Zones" \
  "SELECT PULocationID, COUNT(*) AS rides, AVG(base_passenger_fare) AS avg_fare, SUM(base_passenger_fare) AS revenue FROM \"nyc-taxis\" GROUP BY PULocationID ORDER BY rides DESC LIMIT 5"

run_query \
  "routes-topk" \
  "Q3 Top Routes" \
  "SELECT PULocationID, DOLocationID, COUNT(*) AS rides, AVG(base_passenger_fare) AS avg_fare, SUM(base_passenger_fare) AS revenue FROM \"nyc-taxis\" GROUP BY PULocationID, DOLocationID ORDER BY rides DESC LIMIT 10"

run_query \
  "airport-corridor" \
  "Q4 Airport Corridor" \
  "SELECT hvfhs_license_num, PULocationID, DOLocationID, COUNT(*) AS rides, AVG(base_passenger_fare) AS avg_fare, AVG(driver_pay) AS avg_driver_pay FROM \"nyc-taxis\" WHERE PULocationID IN (132, 138) GROUP BY hvfhs_license_num, PULocationID, DOLocationID ORDER BY rides DESC LIMIT 10"

run_query \
  "wav-headline" \
  "Q5 WAV Headline" \
  "SELECT hvfhs_license_num, COUNT(*) AS rides, AVG(base_passenger_fare) AS avg_fare, AVG(driver_pay) AS avg_driver_pay, AVG(driver_pay - base_passenger_fare) AS avg_premium FROM \"nyc-taxis\" WHERE wav_request_flag = 'Y' AND wav_match_flag = 'Y' GROUP BY hvfhs_license_num ORDER BY rides DESC"

run_query \
  "margin-gap" \
  "Q6 Margin Gap" \
  "SELECT hvfhs_license_num, COUNT(*) AS rides, AVG(base_passenger_fare - driver_pay) AS avg_spread, AVG((base_passenger_fare - driver_pay) / base_passenger_fare) AS spread_ratio FROM \"nyc-taxis\" WHERE base_passenger_fare > 0 GROUP BY hvfhs_license_num ORDER BY avg_spread DESC"

printf '\nFrozen NYC taxi hybrid benchmark suite completed against %s\n' "$HOST"
