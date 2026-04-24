#!/usr/bin/env bash

# End-to-end remote_store smoke run against RustFS + an isolated 3-node
# release cluster. Uses temp dirs and non-default ports so it can run beside
# an existing local cluster without clobbering ./data/node-*.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUSTFS_SCRIPT="${ROOT_DIR}/scripts/rustfs_dev.sh"
BINARY="${ROOT_DIR}/target/release/ferrissearch"

NODE_COUNT=3
HTTP_BASE="${SMOKE_HTTP_BASE:-19200}"
TRANSPORT_BASE="${SMOKE_TRANSPORT_BASE:-19300}"
INDEX_NAME="${SMOKE_INDEX_NAME:-warmremote}"
REMOTE_PREFIX="${SMOKE_REMOTE_PREFIX:-live-smoke-$(date +%s)}"
CLUSTER_NAME="${SMOKE_CLUSTER_NAME:-ferrissearch-smoke}"

KEEP_CLUSTER=0
KEEP_RUSTFS=0
STARTED_RUSTFS=0
PIDS=()

SMOKE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/ferris-remote-store-smoke.XXXXXX")"
LOG_DIR="${SMOKE_ROOT}/logs"
DATA_ROOT="${SMOKE_ROOT}/data"
mkdir -p "${LOG_DIR}" "${DATA_ROOT}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--keep-cluster] [--keep-rustfs] [--prefix PREFIX]

Runs an isolated RustFS-backed remote_store smoke test using release binaries.

Options:
  --keep-cluster   Leave the smoke cluster running after success/failure
  --keep-rustfs    Leave RustFS running if this script started it
  --prefix PREFIX  Override the remote object-store prefix

Environment overrides:
  SMOKE_HTTP_BASE
  SMOKE_TRANSPORT_BASE
  SMOKE_INDEX_NAME
  SMOKE_REMOTE_PREFIX
  SMOKE_CLUSTER_NAME
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-cluster)
      KEEP_CLUSTER=1
      shift
      ;;
    --keep-rustfs)
      KEEP_RUSTFS=1
      shift
      ;;
    --prefix)
      if [[ $# -lt 2 ]]; then
        echo "error: --prefix requires a value" >&2
        exit 2
      fi
      REMOTE_PREFIX="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

fail() {
  echo "error: $*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found on PATH: $1"
  fi
}

show_logs() {
  for log_file in "${LOG_DIR}"/node-*.log; do
    [[ -f "${log_file}" ]] || continue
    echo
    echo "==== ${log_file} (tail) ====" >&2
    tail -n 40 "${log_file}" >&2 || true
  done
}

stop_cluster() {
  if [[ ${#PIDS[@]} -eq 0 ]]; then
    return
  fi

  for pid in "${PIDS[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
  wait "${PIDS[@]}" 2>/dev/null || true
}

cleanup() {
  local exit_code=$?
  if [[ ${exit_code} -ne 0 ]]; then
    show_logs
  fi

  if [[ ${KEEP_CLUSTER} -eq 0 ]]; then
    stop_cluster
  fi

  if [[ ${STARTED_RUSTFS} -eq 1 && ${KEEP_RUSTFS} -eq 0 ]]; then
    "${RUSTFS_SCRIPT}" down >/dev/null 2>&1 || true
  fi

  echo
  echo "smoke artifacts preserved under ${SMOKE_ROOT}"
  if [[ ${KEEP_CLUSTER} -eq 1 ]]; then
    echo "smoke cluster still running on http://127.0.0.1:${HTTP_BASE}"
  fi
  exit ${exit_code}
}

trap cleanup EXIT INT TERM

build_release_binary() {
  echo "Building release binary with target-cpu=native..."
  (
    cd "${ROOT_DIR}"
    RUSTFLAGS="-C target-cpu=native" cargo build --release
  )
}

rustfs_ready() {
  curl -fsS -o /dev/null "http://127.0.0.1:9100/" 2>/dev/null
}

ensure_rustfs() {
  if rustfs_ready; then
    echo "Reusing running RustFS on http://127.0.0.1:9100"
  else
    echo "Starting RustFS dev server..."
    "${RUSTFS_SCRIPT}" up >/dev/null
    STARTED_RUSTFS=1
  fi

  eval "$("${RUSTFS_SCRIPT}" env)"
  : "${FERRIS_REMOTE_STORE_BUCKET:?rustfs env did not export FERRIS_REMOTE_STORE_BUCKET}"
}

wait_for_cluster_health() {
  local url="http://127.0.0.1:${HTTP_BASE}/_cluster/health"
  local health
  for _attempt in $(seq 1 60); do
    health="$(curl -sS --max-time 5 "${url}" 2>/dev/null || true)"
    if [[ "${health}" == *'"status":"green"'* && "${health}" == *'"number_of_nodes":3'* ]]; then
      echo "${health}"
      return 0
    fi
    sleep 1
  done

  fail "cluster did not reach green status on ${url}"
}

start_cluster() {
  local seeds=""
  for i in $(seq 1 "${NODE_COUNT}"); do
    if [[ -n "${seeds}" ]]; then
      seeds+=","
    fi
    seeds+="127.0.0.1:$((TRANSPORT_BASE + i - 1))"
  done

  for i in $(seq 1 "${NODE_COUNT}"); do
    local http_port=$((HTTP_BASE + i - 1))
    local transport_port=$((TRANSPORT_BASE + i - 1))
    local data_dir="${DATA_ROOT}/node-${i}"
    local log_file="${LOG_DIR}/node-${i}.log"
    mkdir -p "${data_dir}"

    echo "Starting smoke node ${i} on HTTP ${http_port} / transport ${transport_port}"
    FERRISSEARCH_NODE_NAME="node-${i}" \
    FERRISSEARCH_CLUSTER_NAME="${CLUSTER_NAME}" \
    FERRISSEARCH_HTTP_PORT="${http_port}" \
    FERRISSEARCH_TRANSPORT_PORT="${transport_port}" \
    FERRISSEARCH_DATA_DIR="${data_dir}" \
    FERRISSEARCH_RAFT_NODE_ID="${i}" \
    FERRISSEARCH_SEED_HOSTS="${seeds}" \
    FERRISSEARCH_STORAGE_URI="s3://${FERRIS_REMOTE_STORE_BUCKET}/${REMOTE_PREFIX}" \
    "${BINARY}" >"${log_file}" 2>&1 &
    PIDS+=("$!")

    if [[ ${i} -eq 1 ]]; then
      sleep 3
    else
      sleep 1
    fi
  done
}

request_json() {
  local method=$1
  local url=$2
  local body=${3-}
  local response

  if [[ -n "${body}" ]]; then
    response="$(curl --max-time 20 -sS -X "${method}" "${url}" -H 'Content-Type: application/json' -d "${body}" -w $'\n%{http_code}')"
  else
    response="$(curl --max-time 20 -sS -X "${method}" "${url}" -w $'\n%{http_code}')"
  fi

  RESPONSE_BODY="${response%$'\n'*}"
  RESPONSE_STATUS="${response##*$'\n'}"
}

assert_status() {
  local expected=$1
  local context=$2
  if [[ "${RESPONSE_STATUS}" != "${expected}" ]]; then
    echo "${RESPONSE_BODY}" >&2
    fail "${context}: expected HTTP ${expected}, got ${RESPONSE_STATUS}"
  fi
}

assert_contains() {
  local needle=$1
  local context=$2
  if [[ "${RESPONSE_BODY}" != *"${needle}"* ]]; then
    echo "${RESPONSE_BODY}" >&2
    fail "${context}: response did not contain ${needle}"
  fi
}

extract_json_string() {
  local key=$1
  sed -n "s/.*\"${key}\":\"\([^\"]*\)\".*/\1/p" <<<"${RESPONSE_BODY}"
}

wait_for_path() {
  local path=$1
  for _attempt in $(seq 1 20); do
    if [[ -e "${path}" ]]; then
      return 0
    fi
    sleep 1
  done
  fail "timed out waiting for path ${path}"
}

require_command curl
require_command find
require_command sed

echo "Smoke root: ${SMOKE_ROOT}"
echo "Remote prefix: ${REMOTE_PREFIX}"
echo "Isolated HTTP ports: ${HTTP_BASE}-$((HTTP_BASE + NODE_COUNT - 1))"
echo "Isolated transport ports: ${TRANSPORT_BASE}-$((TRANSPORT_BASE + NODE_COUNT - 1))"

ensure_rustfs
build_release_binary
start_cluster

health="$(wait_for_cluster_health)"
echo "Cluster health: ${health}"

request_json PUT "http://127.0.0.1:$((HTTP_BASE + 1))/${INDEX_NAME}" '{"engine":"remote_store"}'
assert_status 200 "create remote_store index"

request_json POST "http://127.0.0.1:$((HTTP_BASE + 2))/${INDEX_NAME}/_remote_store/publish" '{"docs":[{"_id":"doc-1","title":"warm remote store hit","body":"first published split for rustfs"},{"_id":"doc-2","title":"warm remote store follow-up","body":"second published split for rustfs"}]}'
assert_status 200 "publish remote_store split"
assert_contains '"doc_count":2' "publish remote_store split"
split_id="$(extract_json_string split_id)"
[[ -n "${split_id}" ]] || fail "publish response did not include split_id"
echo "Published split_id: ${split_id}"

request_json POST "http://127.0.0.1:$((HTTP_BASE + 1))/${INDEX_NAME}/_search" '{"query":{"match_all":{}},"size":10}'
assert_status 200 "POST /_search match_all"
assert_contains '"successful":1' "POST /_search match_all"
assert_contains '"value":2' "POST /_search match_all"

request_json GET "http://127.0.0.1:${HTTP_BASE}/${INDEX_NAME}/_search?q=warm"
assert_status 200 "GET /_search?q=warm"
assert_contains '"successful":1' "GET /_search?q=warm"
assert_contains '"value":2' "GET /_search?q=warm"

request_json POST "http://127.0.0.1:$((HTTP_BASE + 2))/${INDEX_NAME}/_count" '{"query":{"match_all":{}}}'
assert_status 200 "POST /_count match_all"
assert_contains '"count":2' "POST /_count match_all"
assert_contains '"successful":1' "POST /_count match_all"

request_json POST "http://127.0.0.1:${HTTP_BASE}/${INDEX_NAME}/_remote_store/verify" '{}'
assert_status 200 "POST /_remote_store/verify"
assert_contains '"ok_count":1' "POST /_remote_store/verify"
assert_contains '"mismatch_count":0' "POST /_remote_store/verify"

bucket_root="${ROOT_DIR}/.rustfs-dev/data/${FERRIS_REMOTE_STORE_BUCKET}/${REMOTE_PREFIX}"
wait_for_path "${bucket_root}"
index_uuid_path="$(find "${bucket_root}" -mindepth 1 -maxdepth 1 -type d | head -n 1 || true)"
[[ -n "${index_uuid_path}" ]] || fail "could not resolve published index UUID under ${bucket_root}"
index_uuid="$(basename "${index_uuid_path}")"

wait_for_path "${bucket_root}/${index_uuid}/manifest.current.json/xl.meta"
wait_for_path "${bucket_root}/${index_uuid}/manifests/000000000001.json/xl.meta"
wait_for_path "${bucket_root}/${index_uuid}/splits/${split_id}/bundle/xl.meta"

cache_done="$(find "${DATA_ROOT}" -path "*/_remote_store_cache/splits/${index_uuid}/${split_id}/.done" -type f | head -n 1 || true)"
[[ -n "${cache_done}" ]] || fail "did not find hydrated local cache marker for split ${split_id}"

echo
echo "Remote object layout confirmed under: ${bucket_root}/${index_uuid}"
echo "Warm cache marker confirmed at: ${cache_done}"
echo "RustFS console: http://127.0.0.1:9101/rustfs/console/browser/?bucket=${FERRIS_REMOTE_STORE_BUCKET}&key=${REMOTE_PREFIX}%2F"
echo "Smoke run completed successfully."