#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_VENV="$ROOT_DIR/.venv"
PYTHON_BIN="$PYTHON_VENV/bin/python"
PIP_BIN="$PYTHON_VENV/bin/pip"

TARGET_ROWS=100000000
DATA_YEAR=2025
START_MONTH=1
END_MONTH=12
HTTP_HOST="127.0.0.1"
HTTP_BASE_PORT=9200
TRANSPORT_BASE_PORT=9300
NODES=3
SHARDS=12
BATCH_SIZE=20000
PARQUET_BATCH=100000
WORKERS=""
DATASET_DIR="$ROOT_DIR/test_workload_data/nyc_taxis_100m"
CLUSTER_DATA_DIR="$ROOT_DIR/data/nyc_taxis_100m_cluster"
LOG_DIR="$ROOT_DIR/logs/nyc_taxis_100m"
SKIP_BENCH=0
FORCE_BUILD=0
FORCE_DOWNLOAD=0
START_CLUSTER=1
CLUSTER_STARTED_BY_US=0

cleanup() {
    local exit_code=$?
    if [[ "$CLUSTER_STARTED_BY_US" -eq 1 && -f "$LOG_DIR/pids.txt" ]]; then
        log "Cleaning up cluster processes"
        while read -r pid; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
            fi
        done < "$LOG_DIR/pids.txt"
    fi
    exit "$exit_code"
}

trap cleanup EXIT

usage() {
    cat <<'EOF'
Usage: bash scripts/load_nyc_taxis_100m.sh [options]

End-to-end workflow for a home machine:
1. Build the release binary if needed
2. Start or reuse a 3-node local FerrisSearch cluster
3. Download enough NYC TLC FHVHV monthly parquet files to reach 100M rows
4. Ingest exactly 100M rows into the nyc-taxis index
5. Run the NYC taxi SQL benchmark queries

Options:
  --target-rows N           Rows to ingest (default: 100000000)
  --year YYYY               TLC year to download from first (default: 2025)
  --start-month M           First month to consider, 1-12 (default: 1)
  --end-month M             Last month to consider, 1-12 (default: 12)
  --workers N               Bulk worker threads (default: min(nproc, 16))
  --batch-size N            Bulk docs per batch (default: 20000)
  --parquet-batch N         PyArrow rows per parquet batch (default: 100000)
  --shards N                Index shard count (default: 12)
  --dataset-dir PATH        Download/cache directory for parquet files
  --cluster-data-dir PATH   Local data dir root for the temporary 3-node cluster
  --log-dir PATH            Log directory for cluster node logs
  --http-base-port PORT     Base HTTP port for node-1 (default: 9200)
  --transport-base-port P   Base transport port for node-1 (default: 9300)
  --skip-bench              Load data but skip the benchmark query script
  --no-start-cluster        Reuse an already-running cluster instead of starting one
  --force-build             Rebuild the release binary even if it already exists
  --force-download          Re-download parquet files even if cached locally
  --help                    Show this help
EOF
}

log() {
    printf '[nyc-100m] %s\n' "$*"
}

die() {
    printf '[nyc-100m] ERROR: %s\n' "$*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target-rows)
            TARGET_ROWS="$2"
            shift 2
            ;;
        --year)
            DATA_YEAR="$2"
            shift 2
            ;;
        --start-month)
            START_MONTH="$2"
            shift 2
            ;;
        --end-month)
            END_MONTH="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --parquet-batch)
            PARQUET_BATCH="$2"
            shift 2
            ;;
        --shards)
            SHARDS="$2"
            shift 2
            ;;
        --dataset-dir)
            DATASET_DIR="$2"
            shift 2
            ;;
        --cluster-data-dir)
            CLUSTER_DATA_DIR="$2"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --http-base-port)
            HTTP_BASE_PORT="$2"
            shift 2
            ;;
        --transport-base-port)
            TRANSPORT_BASE_PORT="$2"
            shift 2
            ;;
        --skip-bench)
            SKIP_BENCH=1
            shift
            ;;
        --no-start-cluster)
            START_CLUSTER=0
            shift
            ;;
        --force-build)
            FORCE_BUILD=1
            shift
            ;;
        --force-download)
            FORCE_DOWNLOAD=1
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            die "Unknown option: $1"
            ;;
    esac
done

if [[ -z "$WORKERS" ]]; then
    if command -v nproc >/dev/null 2>&1; then
        CPU_COUNT="$(nproc)"
        if (( CPU_COUNT > 16 )); then
            WORKERS=16
        else
            WORKERS="$CPU_COUNT"
        fi
    else
        WORKERS=8
    fi
fi

BASE_URL="http://${HTTP_HOST}:${HTTP_BASE_PORT}"
PID_FILE="$LOG_DIR/pids.txt"

mkdir -p "$DATASET_DIR" "$CLUSTER_DATA_DIR" "$LOG_DIR"

ensure_python_env() {
    if [[ ! -x "$PYTHON_BIN" ]]; then
        log "Creating Python virtual environment at $PYTHON_VENV"
        python3 -m venv "$PYTHON_VENV"
    fi

    local missing
    missing="$($PYTHON_BIN - <<'PY'
missing = []
for module, package in [
    ("pyarrow", "pyarrow"),
    ("opensearchpy", "opensearch-py"),
]:
    try:
        __import__(module)
    except Exception:
        missing.append(package)
print(" ".join(missing))
PY
)"

    if [[ -n "$missing" ]]; then
        log "Installing Python packages into .venv: $missing"
        "$PIP_BIN" install --upgrade pip
        "$PIP_BIN" install $missing
    fi
}

build_release_binary() {
    if [[ ! -x "$ROOT_DIR/target/release/ferrissearch" || "$FORCE_BUILD" -eq 1 ]]; then
        log "Building release binary"
        (cd "$ROOT_DIR" && RUSTFLAGS="-C target-cpu=native" cargo build --release)
    fi
}

cluster_is_healthy() {
    curl -sSf "$BASE_URL/_cluster/state" >/dev/null 2>&1
}

wait_for_cluster() {
    local attempts=0
    while (( attempts < 120 )); do
        if cluster_is_healthy; then
            local node_count
            node_count="$(curl -s "$BASE_URL/_cluster/state" | "$PYTHON_BIN" - <<'PY'
import json, sys
state = json.load(sys.stdin)
print(len(state.get("nodes", {})))
PY
)"
            if [[ "$node_count" -ge "$NODES" ]]; then
                return 0
            fi
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    return 1
}

start_cluster_if_needed() {
    if cluster_is_healthy; then
        log "Reusing existing cluster at $BASE_URL"
        return 0
    fi

    if [[ "$START_CLUSTER" -ne 1 ]]; then
        die "No running cluster found at $BASE_URL and --no-start-cluster was set"
    fi

    rm -rf "$CLUSTER_DATA_DIR"
    mkdir -p "$CLUSTER_DATA_DIR"
    : > "$PID_FILE"
    CLUSTER_STARTED_BY_US=1

    for node_id in $(seq 1 "$NODES"); do
        local http_port=$((HTTP_BASE_PORT + node_id - 1))
        local transport_port=$((TRANSPORT_BASE_PORT + node_id - 1))
        local node_data_dir="$CLUSTER_DATA_DIR/node-$node_id"
        mkdir -p "$node_data_dir"
        log "Starting node-$node_id on HTTP $http_port / transport $transport_port"
        (
            cd "$ROOT_DIR"
            FERRISSEARCH_NODE_NAME="node-$node_id" \
            FERRISSEARCH_HTTP_PORT="$http_port" \
            FERRISSEARCH_TRANSPORT_PORT="$transport_port" \
            FERRISSEARCH_DATA_DIR="$node_data_dir" \
            FERRISSEARCH_RAFT_NODE_ID="$node_id" \
            FERRISSEARCH_SEED_HOSTS="${HTTP_HOST}:${TRANSPORT_BASE_PORT},${HTTP_HOST}:$((TRANSPORT_BASE_PORT + 1)),${HTTP_HOST}:$((TRANSPORT_BASE_PORT + 2))" \
            ./target/release/ferrissearch > "$LOG_DIR/node-$node_id.log" 2>&1 &
            echo $! >> "$PID_FILE"
        )
        if [[ "$node_id" -eq 1 ]]; then
            sleep 5
        else
            sleep 2
        fi
    done

    log "Waiting for $NODES-node cluster to become healthy"
    wait_for_cluster || die "Cluster did not become healthy; check logs under $LOG_DIR"
}

parquet_rows() {
    local parquet_path="$1"
    "$PYTHON_BIN" - "$parquet_path" <<'PY'
import sys
import pyarrow.parquet as pq

print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)
PY
}

download_parquets() {
    local cumulative_rows=0
    PARQUET_FILES=()

    for month in $(seq -w "$START_MONTH" "$END_MONTH"); do
        local file_name="fhvhv_tripdata_${DATA_YEAR}-${month}.parquet"
        local local_path="$DATASET_DIR/$file_name"
        local remote_url="https://d37ci6vzurychx.cloudfront.net/trip-data/$file_name"

        if [[ "$FORCE_DOWNLOAD" -eq 1 || ! -f "$local_path" ]]; then
            log "Downloading $file_name"
            curl -fL "$remote_url" -o "$local_path.tmp"
            mv "$local_path.tmp" "$local_path"
        else
            log "Using cached $file_name"
        fi

        local row_count
        row_count="$(parquet_rows "$local_path")"
        cumulative_rows=$((cumulative_rows + row_count))
        PARQUET_FILES+=("$local_path")
        log "$file_name -> ${row_count} rows (cumulative: ${cumulative_rows})"

        if (( cumulative_rows >= TARGET_ROWS )); then
            break
        fi
    done

    if (( cumulative_rows < TARGET_ROWS )); then
        die "Only found ${cumulative_rows} rows from ${DATA_YEAR}-${START_MONTH}..${DATA_YEAR}-${END_MONTH}; need ${TARGET_ROWS}"
    fi
}

run_ingest() {
    log "Ingesting ${TARGET_ROWS} NYC taxi rows into nyc-taxis"
    NYC_TAXI_HOST="$HTTP_HOST" \
    NYC_TAXI_PORT="$HTTP_BASE_PORT" \
    NYC_TAXI_TARGET_ROWS="$TARGET_ROWS" \
    NYC_TAXI_WORKERS="$WORKERS" \
    NYC_TAXI_BATCH_SIZE="$BATCH_SIZE" \
    NYC_TAXI_PARQUET_BATCH="$PARQUET_BATCH" \
    NYC_TAXI_SHARDS="$SHARDS" \
    "$PYTHON_BIN" - "${PARQUET_FILES[@]}" <<'PY'
import os
import queue
import statistics
import sys
import threading
import time
import json

import pyarrow.parquet as pq
from opensearchpy import OpenSearch, helpers

INDEX_NAME = "nyc-taxis"
TIMESTAMP_COLS = {
    "request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime"
}
MAPPINGS = {
    "properties": {
        "hvfhs_license_num": {"type": "keyword"},
        "dispatching_base_num": {"type": "keyword"},
        "originating_base_num": {"type": "keyword"},
        "request_datetime": {"type": "keyword"},
        "on_scene_datetime": {"type": "keyword"},
        "pickup_datetime": {"type": "keyword"},
        "dropoff_datetime": {"type": "keyword"},
        "PULocationID": {"type": "integer"},
        "DOLocationID": {"type": "integer"},
        "trip_miles": {"type": "float"},
        "trip_time": {"type": "integer"},
        "base_passenger_fare": {"type": "float"},
        "tolls": {"type": "float"},
        "bcf": {"type": "float"},
        "sales_tax": {"type": "float"},
        "congestion_surcharge": {"type": "float"},
        "airport_fee": {"type": "float"},
        "tips": {"type": "float"},
        "driver_pay": {"type": "float"},
        "shared_request_flag": {"type": "keyword"},
        "shared_match_flag": {"type": "keyword"},
        "access_a_ride_flag": {"type": "keyword"},
        "wav_request_flag": {"type": "keyword"},
        "wav_match_flag": {"type": "keyword"},
        "cbd_congestion_fee": {"type": "float"},
    }
}

host = os.environ["NYC_TAXI_HOST"]
port = int(os.environ["NYC_TAXI_PORT"])
target_rows = int(os.environ["NYC_TAXI_TARGET_ROWS"])
workers = int(os.environ["NYC_TAXI_WORKERS"])
batch_size = int(os.environ["NYC_TAXI_BATCH_SIZE"])
parquet_batch = int(os.environ["NYC_TAXI_PARQUET_BATCH"])
shards = int(os.environ["NYC_TAXI_SHARDS"])
parquet_files = sys.argv[1:]


def batch_to_bulk_actions(table, start_id):
    columns = {name: table.column(name).to_pylist() for name in table.column_names}
    actions = []
    for index in range(table.num_rows):
        doc = {}
        for name, col_data in columns.items():
            value = col_data[index]
            if value is None:
                continue
            if name in TIMESTAMP_COLS and hasattr(value, "isoformat"):
                doc[name] = value.isoformat()
            else:
                doc[name] = value
        actions.append({"_index": INDEX_NAME, "_id": str(start_id + index), "_source": doc})
    return actions


class IngestStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ingested = 0
        self.errors = 0
        self.batch_latencies = []
        self.logged_bulk_error_batches = 0
        self.logged_exceptions = 0

    def add(self, success, errors, latency_ms):
        with self.lock:
            self.ingested += success
            self.errors += errors
            self.batch_latencies.append(latency_ms)

    def should_log_bulk_errors(self):
        with self.lock:
            if self.logged_bulk_error_batches >= 5:
                return None
            self.logged_bulk_error_batches += 1
            return self.logged_bulk_error_batches

    def should_log_exception(self):
        with self.lock:
            if self.logged_exceptions >= 5:
                return None
            self.logged_exceptions += 1
            return self.logged_exceptions

    def snapshot(self):
        with self.lock:
            return self.ingested, self.errors


def worker_fn(work_queue, stats):
    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        use_ssl=False,
        verify_certs=False,
        timeout=120,
        max_retries=3,
        retry_on_timeout=True,
    )
    while True:
        item = work_queue.get()
        if item is None:
            work_queue.task_done()
            break
        start = time.perf_counter()
        try:
            success, errs = helpers.bulk(client, item, raise_on_error=False)
            err_count = len(errs) if isinstance(errs, list) else errs
            stats.add(success, err_count, (time.perf_counter() - start) * 1000)
            if isinstance(errs, list) and errs:
                sample_no = stats.should_log_bulk_errors()
                if sample_no is not None:
                    print(f"\nBulk error sample {sample_no} ({len(errs)} item errors in batch):")
                    for err in errs[:3]:
                        print(json.dumps(err, indent=2, default=str))
        except Exception:
            stats.add(0, len(item), (time.perf_counter() - start) * 1000)
            sample_no = stats.should_log_exception()
            if sample_no is not None:
                exc_type, exc, _ = sys.exc_info()
                exc_name = exc_type.__name__ if exc_type is not None else "Exception"
                print(f"\nBulk exception sample {sample_no}: {exc_name}: {exc}")
        work_queue.task_done()


client = OpenSearch(
    hosts=[{"host": host, "port": port}],
    use_ssl=False,
    verify_certs=False,
    timeout=120,
)
info = client.info()
print(f"Connected to {info['name']} v{info['version']}")

available_rows = 0
file_infos = []
for path in parquet_files:
    rows = pq.ParquetFile(path).metadata.num_rows
    file_infos.append((path, rows))
    available_rows += rows

total_rows = min(available_rows, target_rows)
print(f"Parquet files: {len(file_infos)}")
print(f"Rows available: {available_rows:,}; ingesting {total_rows:,}")
print(f"Config: workers={workers}, batch_size={batch_size}, parquet_batch={parquet_batch}, shards={shards}")

if client.indices.exists(index=INDEX_NAME):
    print(f"Index '{INDEX_NAME}' already exists, deleting...")
    client.indices.delete(index=INDEX_NAME)

client.indices.create(
    index=INDEX_NAME,
    body={
        "settings": {"number_of_shards": shards, "number_of_replicas": 0},
        "mappings": MAPPINGS,
    },
)

stats = IngestStats()
work_queue = queue.Queue(maxsize=workers * 2)
threads = []
for _ in range(workers):
    thread = threading.Thread(target=worker_fn, args=(work_queue, stats), daemon=True)
    thread.start()
    threads.append(thread)

start = time.time()
doc_id = 0
produced = 0

for file_index, (path, row_count) in enumerate(file_infos, start=1):
    if produced >= total_rows:
        break
    file_target = min(row_count, total_rows - produced)
    print(f"\nProcessing file {file_index}/{len(file_infos)}: {path} ({file_target:,}/{row_count:,} rows)")
    parquet_file = pq.ParquetFile(path)
    file_produced = 0

    for batch_table in parquet_file.iter_batches(batch_size=parquet_batch):
        if produced >= total_rows:
            break
        remaining = total_rows - produced
        take = min(batch_table.num_rows, remaining)
        if take < batch_table.num_rows:
            batch_table = batch_table.slice(0, take)
        actions = batch_to_bulk_actions(batch_table, doc_id)
        doc_id += len(actions)
        produced += len(actions)
        file_produced += len(actions)

        for offset in range(0, len(actions), batch_size):
            work_queue.put(actions[offset:offset + batch_size])

        ingested, errors = stats.snapshot()
        elapsed = time.time() - start
        rate = ingested / elapsed if elapsed > 0 else 0
        pct = (ingested / total_rows) * 100 if total_rows else 100.0
        file_pct = (file_produced / file_target) * 100 if file_target else 100.0
        print(
            f"\r  {ingested:>12,} / {total_rows:,} docs ({pct:5.1f}%) | current file {file_pct:5.1f}% | {rate:,.0f} docs/s | errors: {errors} | q: {work_queue.qsize()}",
            end="",
            flush=True,
        )

work_queue.join()
for _ in threads:
    work_queue.put(None)
for thread in threads:
    thread.join(timeout=10)

ingested, errors = stats.snapshot()
elapsed = time.time() - start
rate = ingested / elapsed if elapsed > 0 else 0

print(f"\n\n{'=' * 60}")
print("NYC Taxi ingestion complete!")
print(f"  Documents: {ingested:,} indexed, {errors:,} errors")
print(f"  Workers:   {workers}")
print(f"  Time:      {elapsed:.1f}s")
print(f"  Rate:      {rate:,.0f} docs/s")

latencies = sorted(stats.batch_latencies)
if latencies:
    count = len(latencies)
    def pct(p):
        k = (count - 1) * (p / 100)
        floor = int(k)
        ceil = min(floor + 1, count - 1)
        return latencies[floor] + (k - floor) * (latencies[ceil] - latencies[floor])
    print(f"\n  Bulk batch latency ({count:,} batches of {batch_size} docs):")
    print(f"    Min:  {latencies[0]:>8.1f} ms")
    print(f"    Avg:  {statistics.mean(latencies):>8.1f} ms")
    print(f"    p50:  {pct(50):>8.1f} ms")
    print(f"    p95:  {pct(95):>8.1f} ms")
    print(f"    p99:  {pct(99):>8.1f} ms")
    print(f"    Max:  {latencies[-1]:>8.1f} ms")

client.indices.refresh(index=INDEX_NAME)
resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
print(f"Verified: {resp['hits']['total']['value']:,} docs in index")
PY
}

run_benchmarks() {
    if [[ "$SKIP_BENCH" -eq 1 ]]; then
        log "Skipping benchmark queries"
        return 0
    fi

    log "Running NYC taxi benchmark queries"
    (cd "$ROOT_DIR" && bash scripts/nyc_taxi_queries.sh "$BASE_URL")
}

print_final_summary() {
    cat <<EOF

[nyc-100m] Done.
[nyc-100m] Cluster URL: $BASE_URL
[nyc-100m] Dataset cache: $DATASET_DIR
[nyc-100m] Cluster data: $CLUSTER_DATA_DIR
[nyc-100m] Node logs: $LOG_DIR
[nyc-100m] Index: nyc-taxis
EOF

    if [[ -f "$PID_FILE" ]]; then
        echo "[nyc-100m] Started cluster PIDs are recorded in $PID_FILE"
        echo "[nyc-100m] To stop the cluster later: xargs kill < $PID_FILE"
    fi
}

ensure_python_env
build_release_binary
start_cluster_if_needed
download_parquets
run_ingest
run_benchmarks
print_final_summary
