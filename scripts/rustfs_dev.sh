#!/usr/bin/env bash
# RustFS single-node dev server for remote_store S3 backend testing.
#
# Usage:
#   scripts/rustfs_dev.sh up        # start container + create default bucket
#   scripts/rustfs_dev.sh down      # stop + remove container (keeps data)
#   scripts/rustfs_dev.sh status    # container state + connection info
#   scripts/rustfs_dev.sh clean     # down + wipe data/logs dirs
#   scripts/rustfs_dev.sh env       # print `export ...` lines for S3 env vars
#
# Endpoints after `up`:
#   S3 API   : http://127.0.0.1:9100
#   Console  : http://127.0.0.1:9101
#   Access   : rustfsadmin / rustfsadmin
#   Bucket   : ferrissearch-dev (auto-created)
#
# Ports picked to avoid clashing with FerrisSearch (9200/9300) and any
# existing MinIO (9000/9001) the user might be running elsewhere.

set -euo pipefail

CONTAINER_NAME="ferris-rustfs-dev"
IMAGE="rustfs/rustfs:latest"
HOST_API_PORT=9100
HOST_CONSOLE_PORT=9101
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
DEFAULT_BUCKET="ferrissearch-dev"
REGION="us-east-1"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${ROOT_DIR}/.rustfs-dev"
DATA_DIR="${STATE_DIR}/data"
LOGS_DIR="${STATE_DIR}/logs"

endpoint_url() {
  echo "http://127.0.0.1:${HOST_API_PORT}"
}

print_env() {
  cat <<EOF
export AWS_ACCESS_KEY_ID="${ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${SECRET_KEY}"
export AWS_REGION="${REGION}"
export AWS_ENDPOINT_URL="$(endpoint_url)"
export AWS_ENDPOINT_URL_S3="$(endpoint_url)"
export FERRIS_REMOTE_STORE_BUCKET="${DEFAULT_BUCKET}"
EOF
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found on PATH" >&2
    exit 1
  fi
  # WSL can have a /usr/bin/docker shim that just prints a "enable WSL integration"
  # message and exits non-zero. Force an actual daemon probe before continuing so
  # we fail loudly instead of silently skipping `docker run`.
  if ! docker info >/dev/null 2>&1; then
    echo "error: 'docker info' failed. Is the Docker daemon running and WSL integration enabled?" >&2
    docker info 2>&1 | sed 's/^/  /' >&2 || true
    exit 1
  fi
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"
}

wait_for_ready() {
  local max=30
  for i in $(seq 1 "${max}"); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${HOST_API_PORT}/" 2>/dev/null \
       || curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:${HOST_API_PORT}/" 2>/dev/null | grep -q '^[234]'; then
      return 0
    fi
    sleep 1
  done
  echo "error: rustfs did not become ready on 127.0.0.1:${HOST_API_PORT} within ${max}s" >&2
  docker logs --tail 40 "${CONTAINER_NAME}" >&2 || true
  return 1
}

create_default_bucket() {
  # Use the AWS CLI inside a throwaway container so the host doesn't need one.
  # --network host so the container can reach 127.0.0.1:${HOST_API_PORT}.
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID="${ACCESS_KEY}" \
    -e AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" \
    -e AWS_REGION="${REGION}" \
    amazon/aws-cli:latest \
    s3api create-bucket \
      --bucket "${DEFAULT_BUCKET}" \
      --endpoint-url "$(endpoint_url)" \
      >/dev/null 2>&1 || true
  # Idempotent: ignore "BucketAlreadyOwnedByYou".
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID="${ACCESS_KEY}" \
    -e AWS_SECRET_ACCESS_KEY="${SECRET_KEY}" \
    -e AWS_REGION="${REGION}" \
    amazon/aws-cli:latest \
    s3api head-bucket \
      --bucket "${DEFAULT_BUCKET}" \
      --endpoint-url "$(endpoint_url)" \
      >/dev/null
}

cmd_up() {
  require_docker
  mkdir -p "${DATA_DIR}" "${LOGS_DIR}"
  # RustFS container runs as UID 10001.
  if [[ "$(stat -c '%u' "${DATA_DIR}")" != "10001" || "$(stat -c '%u' "${LOGS_DIR}")" != "10001" ]]; then
    sudo chown -R 10001:10001 "${DATA_DIR}" "${LOGS_DIR}"
  fi

  if container_running; then
    echo "rustfs already running as ${CONTAINER_NAME}"
  elif container_exists; then
    echo "starting existing container ${CONTAINER_NAME}..."
    docker start "${CONTAINER_NAME}" >/dev/null
  else
    echo "creating container ${CONTAINER_NAME} from ${IMAGE}..."
    if ! docker run -d \
      --name "${CONTAINER_NAME}" \
      -p "${HOST_API_PORT}:9000" \
      -p "${HOST_CONSOLE_PORT}:9001" \
      -v "${DATA_DIR}:/data" \
      -v "${LOGS_DIR}:/logs" \
      -e "RUSTFS_ACCESS_KEY=${ACCESS_KEY}" \
      -e "RUSTFS_SECRET_KEY=${SECRET_KEY}" \
      "${IMAGE}" >/dev/null; then
      echo "error: 'docker run' failed" >&2
      exit 1
    fi
  fi

  echo -n "waiting for rustfs to be ready..."
  wait_for_ready && echo " ok"
  echo -n "creating bucket ${DEFAULT_BUCKET}... "
  create_default_bucket && echo "ok"

  echo
  echo "rustfs dev server is up."
  echo "  s3 api    : $(endpoint_url)"
  echo "  console   : http://127.0.0.1:${HOST_CONSOLE_PORT}  (rustfsadmin / rustfsadmin)"
  echo "  bucket    : ${DEFAULT_BUCKET}"
  echo
  echo "to export env vars for aws cli / sdks:"
  echo "  eval \"\$(scripts/rustfs_dev.sh env)\""
}

cmd_down() {
  require_docker
  if container_running; then
    docker stop "${CONTAINER_NAME}" >/dev/null
  fi
  if container_exists; then
    docker rm "${CONTAINER_NAME}" >/dev/null
  fi
  echo "rustfs container removed (data preserved under ${STATE_DIR})"
}

cmd_clean() {
  cmd_down
  if [[ -d "${STATE_DIR}" ]]; then
    # Owned by 10001 from container; remove with sudo.
    sudo rm -rf "${STATE_DIR}"
  fi
  echo "wiped ${STATE_DIR}"
}

cmd_status() {
  require_docker
  if container_running; then
    echo "state    : running"
  elif container_exists; then
    echo "state    : stopped"
  else
    echo "state    : not created"
    return 0
  fi
  echo "name     : ${CONTAINER_NAME}"
  echo "s3 api   : $(endpoint_url)"
  echo "console  : http://127.0.0.1:${HOST_CONSOLE_PORT}"
  echo "bucket   : ${DEFAULT_BUCKET}"
  echo "data dir : ${DATA_DIR}"
}

cmd_env() {
  print_env
}

main() {
  local cmd="${1:-up}"
  case "${cmd}" in
    up)     cmd_up ;;
    down)   cmd_down ;;
    clean)  cmd_clean ;;
    status) cmd_status ;;
    env)    cmd_env ;;
    *)
      echo "unknown command: ${cmd}" >&2
      echo "usage: $0 {up|down|clean|status|env}" >&2
      exit 2
      ;;
  esac
}

main "$@"
