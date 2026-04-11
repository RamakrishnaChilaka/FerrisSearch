#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "$ROOT_DIR/scripts/load_nyc_taxis.sh" \
  --start-year 2025 \
  --end-year 2025 \
  --start-month 1 \
  --end-month 1 \
  --target-rows 20940373 \
  --http-base-port 19200 \
  --transport-base-port 19300 \
  --cluster-data-dir "$ROOT_DIR/data/nyc_taxis_20m_bench_cluster" \
  --log-dir "$ROOT_DIR/logs/nyc_taxis_20m_bench" \
  --bench-script "$ROOT_DIR/scripts/nyc_taxi_hybrid_benchmark.sh" \
  "$@"
