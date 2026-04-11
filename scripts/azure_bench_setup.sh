#!/usr/bin/env bash
#
# azure_bench_setup.sh — One-shot setup for a fresh Azure VM (Ubuntu 22.04/24.04)
# to benchmark FerrisSearch with the full 243M NYC taxi dataset.
#
# Designed for: Standard_D32s_v5 or similar (32 vCPU, 128 GB RAM)
#
# Usage:
#   # SSH into the Azure VM, then:
#   curl -fsSL https://raw.githubusercontent.com/RamakrishnaChilaka/FerrisSearch/main/scripts/azure_bench_setup.sh | bash
#
#   # Or if you already cloned:
#   cd FerrisSearch
#   bash scripts/azure_bench_setup.sh
#
# What this does:
#   1. Install OS packages (build-essential, clang, protobuf, python3, etc.)
#   2. Set up NVMe RAID0 array across local SSDs → /mnt/bench
#   3. Clone the repo (if running standalone via curl | bash)
#   4. Install Rust toolchain (stable)
#   5. Create Python venv and install dependencies
#   6. Tune kernel parameters for high-throughput ingestion
#   7. Build FerrisSearch release binary with target-cpu=native
#
# After this completes, run:
#   cd /mnt/bench/FerrisSearch
#   DATASET_DIR="$PWD/test_workload_data/nyc_taxis_243m"
#   CLUSTER_DATA_DIR="$PWD/data/nyc_taxis_243m_cluster"
#   LOG_DIR="$PWD/logs/nyc_taxis_243m"
#   bash scripts/load_nyc_taxis.sh \
#     --start-year 2022 \
#     --end-year 2024 \
#     --shards 24 \
#     --workers 24 \
#     --batch-size 25000 \
#     --parquet-batch 200000 \
#     # benchmark defaults in load_nyc_taxis.sh:
#     # translog_durability=async, translog_sync_interval_ms=5000,
#     # refresh_interval_ms=60000, flush_threshold_bytes=4294967296
#     --http-base-port 19200 \
#     --transport-base-port 19300 \
#     --dataset-dir "$DATASET_DIR" \
#     --cluster-data-dir "$CLUSTER_DATA_DIR" \
#     --log-dir "$LOG_DIR"

set -euo pipefail

log() { printf '\033[1;36m[setup]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[setup] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

BENCH_MOUNT="/mnt/bench"
RAID_DEV="/dev/md0"
REPO_URL="https://github.com/RamakrishnaChilaka/FerrisSearch.git"
ROOT_DIR=""
BENCH_STORAGE_LABEL=""

# ---------------------------------------------------------------------------
# 1. OS packages
# ---------------------------------------------------------------------------
log "Installing system packages..."
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    clang \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    libssl-dev \
    python3 \
    python3-venv \
    python3-pip \
    curl \
    git \
    jq \
    htop \
    sysstat \
    numactl \
    mdadm \
    xfsprogs

# ---------------------------------------------------------------------------
# 2. NVMe RAID0 array → /mnt/bench
# ---------------------------------------------------------------------------
ensure_bench_mount() {
    local device=""
    local raid_uuid=""
    local filesystem_size=""
    local -a nvme_disks=()
    local -a stale_md_disks=()

    sudo mkdir -p "$BENCH_MOUNT"

    if mountpoint -q "$BENCH_MOUNT" 2>/dev/null; then
        sudo chown "$(whoami):$(id -gn)" "$BENCH_MOUNT"
        filesystem_size="$(df -h "$BENCH_MOUNT" | awk 'NR==2 {print $2}')"
        BENCH_STORAGE_LABEL="NVMe RAID at $BENCH_MOUNT (${filesystem_size} total)"
        log "$BENCH_STORAGE_LABEL already mounted"
        return 0
    fi

    if [[ -e "$RAID_DEV" ]]; then
        log "Mounting existing RAID device $RAID_DEV at $BENCH_MOUNT"
        sudo mount -o noatime,discard "$RAID_DEV" "$BENCH_MOUNT" || true
    fi

    if ! mountpoint -q "$BENCH_MOUNT" 2>/dev/null; then
        sudo mdadm --assemble --scan >/dev/null 2>&1 || true
        if [[ -e "$RAID_DEV" ]]; then
            log "Assembled existing RAID device $RAID_DEV"
            sudo mount -o noatime,discard "$RAID_DEV" "$BENCH_MOUNT"
        fi
    fi

    if ! mountpoint -q "$BENCH_MOUNT" 2>/dev/null; then
        for device in /dev/nvme*n1; do
            [[ -e "$device" ]] || continue
            if ls "${device}p"* >/dev/null 2>&1; then
                continue
            fi
            if sudo mdadm --examine "$device" >/dev/null 2>&1; then
                stale_md_disks+=("$device")
                continue
            fi
            nvme_disks+=("$device")
        done

        if [[ ${#nvme_disks[@]} -eq 0 ]]; then
            if [[ ${#stale_md_disks[@]} -gt 0 ]]; then
                die "Found NVMe devices with existing md metadata (${stale_md_disks[*]}) but could not assemble $RAID_DEV"
            fi
            log "No unmounted NVMe disks found; using the existing filesystem at $BENCH_MOUNT"
            sudo chown "$(whoami):$(id -gn)" "$BENCH_MOUNT"
            BENCH_STORAGE_LABEL="OS disk fallback at $BENCH_MOUNT"
            return 0
        fi

        log "Creating RAID0 across ${#nvme_disks[@]} NVMe disks: ${nvme_disks[*]}"
        sudo mdadm --create "$RAID_DEV" \
            --level=0 \
            --raid-devices=${#nvme_disks[@]} \
            "${nvme_disks[@]}" \
            --force --run
        sudo mkfs.xfs -f "$RAID_DEV"
        sudo mount -o noatime,discard "$RAID_DEV" "$BENCH_MOUNT"
    fi

    sudo chown "$(whoami):$(id -gn)" "$BENCH_MOUNT"
    raid_uuid="$(sudo blkid -s UUID -o value "$RAID_DEV" 2>/dev/null || true)"
    if [[ -n "$raid_uuid" ]]; then
        sudo mkdir -p /etc/mdadm
        sudo mdadm --detail --scan | sudo tee /etc/mdadm/mdadm.conf >/dev/null
        if ! grep -qs "UUID=$raid_uuid[[:space:]]\+$BENCH_MOUNT[[:space:]]" /etc/fstab; then
            printf 'UUID=%s %s xfs defaults,noatime,discard,nofail 0 2\n' "$raid_uuid" "$BENCH_MOUNT" | sudo tee -a /etc/fstab >/dev/null
        fi
    fi

    filesystem_size="$(df -h "$BENCH_MOUNT" | awk 'NR==2 {print $2}')"
    BENCH_STORAGE_LABEL="NVMe RAID at $BENCH_MOUNT (${filesystem_size} total)"
    log "$BENCH_STORAGE_LABEL"
}

ensure_bench_mount

# Export for downstream scripts
export BENCH_MOUNT

# ---------------------------------------------------------------------------
# 3. Clone repo (if running standalone via curl | bash)
# ---------------------------------------------------------------------------
ensure_checkout() {
    local current_root=""
    local preferred_root=""

    current_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd 2>/dev/null || true)"
    if mountpoint -q "$BENCH_MOUNT" 2>/dev/null && [[ -w "$BENCH_MOUNT" ]]; then
        preferred_root="$BENCH_MOUNT/FerrisSearch"
    else
        preferred_root="$HOME/FerrisSearch"
    fi

    if [[ -n "$current_root" && -f "$current_root/Cargo.toml" && "$current_root" == "$preferred_root" ]]; then
        ROOT_DIR="$current_root"
        return 0
    fi

    mkdir -p "$(dirname "$preferred_root")"
    if [[ -d "$preferred_root/.git" ]]; then
        log "Benchmark checkout already present at $preferred_root, pulling latest..."
        (cd "$preferred_root" && git pull --ff-only)
    else
        log "Cloning FerrisSearch repo into $preferred_root..."
        git clone --depth 1 "$REPO_URL" "$preferred_root"
    fi

    if [[ -n "$current_root" && -f "$current_root/Cargo.toml" && "$current_root" != "$preferred_root" ]]; then
        log "Using $preferred_root instead of $current_root so build artifacts and benchmark data stay on fast storage"
    fi

    ROOT_DIR="$preferred_root"
}

ensure_checkout

log "Working directory: $ROOT_DIR"

# ---------------------------------------------------------------------------
# 4. Rust toolchain
# ---------------------------------------------------------------------------
if ! command -v rustup &>/dev/null; then
    log "Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    log "Rust already installed: $(rustc --version)"
    source "$HOME/.cargo/env" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 5. Python venv
# ---------------------------------------------------------------------------
VENV="$ROOT_DIR/.venv"

if [[ ! -x "$VENV/bin/python" ]]; then
    log "Creating Python venv at $VENV"
    python3 -m venv "$VENV"
fi

log "Installing Python packages..."
"$VENV/bin/pip" install --upgrade pip
"$VENV/bin/pip" install pyarrow opensearch-py

# ---------------------------------------------------------------------------
# 6. Kernel tuning for high-throughput I/O
# ---------------------------------------------------------------------------
log "Applying kernel tuning..."

# Increase max open files
sudo bash -c 'cat > /etc/security/limits.d/ferrissearch.conf <<EOF
* soft nofile 1048576
* hard nofile 1048576
EOF'

# Persist benchmark-oriented kernel settings idempotently
sudo bash -c 'cat > /etc/sysctl.d/99-ferrissearch.conf <<EOF
vm.max_map_count=262144
vm.swappiness=1
vm.dirty_ratio=40
vm.dirty_background_ratio=10
fs.file-max=2097152
net.core.somaxconn=65535
net.ipv4.tcp_max_syn_backlog=65535
EOF'

# Apply
sudo sysctl -p /etc/sysctl.d/99-ferrissearch.conf >/dev/null

# Set ulimit for current session
ulimit -n 1048576 2>/dev/null || ulimit -n 65535 2>/dev/null || true

# ---------------------------------------------------------------------------
# 7. Build release binary
# ---------------------------------------------------------------------------
log "Building FerrisSearch release binary (target-cpu=native)..."
cd "$ROOT_DIR"
RUSTFLAGS="-C target-cpu=native" cargo build --release --locked

log "Build complete: $(ls -lh target/release/ferrissearch | awk '{print $5}')"

# ---------------------------------------------------------------------------
# 8. Build verification
# ---------------------------------------------------------------------------
log "Verifying build artifacts..."
[[ -x "$ROOT_DIR/target/release/ferrissearch" ]] || die "release binary missing after build"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
CORES=$(nproc)
RAM_GB=$(awk '/MemTotal/ {printf "%.0f", $2/1024/1024}' /proc/meminfo)

printf '\n'
printf '%s\n' '============================================================='
printf '%s\n' '  FerrisSearch Azure Benchmark Setup Complete'
printf '%s\n' '============================================================='
printf '  Machine:    %s cores, %s GB RAM\n' "$CORES" "$RAM_GB"
printf '  Storage:    %s\n' "$BENCH_STORAGE_LABEL"
printf '  Repo:       %s\n' "$ROOT_DIR"
printf '  Rust:       %s\n' "$(rustc --version)"
printf '%s\n' '  Binary:     target/release/ferrissearch'
printf '  Python:     %s\n' "$VENV/bin/python"
printf '\n'
printf '%s\n' '  Next step — load 243M NYC taxi rows:'
printf '\n'
printf '    cd "%s"\n' "$ROOT_DIR"
printf '%s\n' '    DATASET_DIR="$PWD/test_workload_data/nyc_taxis_243m"'
printf '%s\n' '    CLUSTER_DATA_DIR="$PWD/data/nyc_taxis_243m_cluster"'
printf '%s\n' '    LOG_DIR="$PWD/logs/nyc_taxis_243m"'
printf '%s\n' '    # benchmark defaults: async translog (5s), refresh_interval_ms=60000, flush_threshold_bytes=4GiB'
printf '%s\n' '    bash scripts/load_nyc_taxis.sh \'
printf '%s\n' '      --start-year 2022 \'
printf '%s\n' '      --end-year 2024 \'
printf '%s\n' '      --shards 24 \'
printf '%s\n' '      --workers 24 \'
printf '%s\n' '      --batch-size 25000 \'
printf '%s\n' '      --parquet-batch 200000 \'
printf '%s\n' '      --http-base-port 19200 \'
printf '%s\n' '      --transport-base-port 19300 \'
printf '%s\n' '      --dataset-dir "$DATASET_DIR" \'
printf '%s\n' '      --cluster-data-dir "$CLUSTER_DATA_DIR" \'
printf '%s\n' '      --log-dir "$LOG_DIR"'
printf '\n'
printf '%s\n' '    Note: with no --target-rows, the loader ingests all rows available in the selected date range.'
printf '\n'
printf '%s\n' '  Or customize:'
printf '\n'
printf '    cd "%s"\n' "$ROOT_DIR"
printf '%s\n' '    DATASET_DIR="$PWD/test_workload_data/nyc_taxis_243m"'
printf '%s\n' '    CLUSTER_DATA_DIR="$PWD/data/nyc_taxis_243m_cluster"'
printf '%s\n' '    bash scripts/load_nyc_taxis.sh \'
printf '%s\n' '      --start-year 2022 \'
printf '%s\n' '      --end-year 2024 \'
printf '%s\n' '      --shards 24 \'
printf '%s\n' '      --workers 24 \'
printf '%s\n' '      --batch-size 25000 \'
printf '%s\n' '      --cluster-data-dir "$CLUSTER_DATA_DIR" \'
printf '%s\n' '      --dataset-dir "$DATASET_DIR"'
printf '%s\n' '============================================================='
