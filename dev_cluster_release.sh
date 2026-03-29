#!/usr/bin/env bash

# dev_cluster_release.sh — builds with native CPU optimizations and runs release nodes.
#
# Usage:
#   ./dev_cluster_release.sh <node_id>     # Start a single node
#   ./dev_cluster_release.sh --nodes 3     # Start a 3-node cluster (background, Ctrl+C stops all)

set -e

build_release() {
    if [ ! -f ./target/release/ferrissearch ] || [ "$1" = "force" ]; then
        echo "Building release binary with target-cpu=native..."
        RUSTFLAGS="-C target-cpu=native" cargo build --release
    fi
}

start_single_node() {
    local NODE_ID=$1
    local OFFSET=$((NODE_ID - 1))

    [[ "$NODE_ID" = "1" ]] && build_release force || build_release

    export FERRISSEARCH_NODE_NAME="node-$NODE_ID"
    export FERRISSEARCH_HTTP_PORT=$((9200 + OFFSET))
    export FERRISSEARCH_TRANSPORT_PORT=$((9300 + OFFSET))
    export FERRISSEARCH_DATA_DIR="./data/node-$NODE_ID"
    export FERRISSEARCH_RAFT_NODE_ID=$NODE_ID
    export FERRISSEARCH_SEED_HOSTS="127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302"

    echo "Starting FerrisSearch Node $NODE_ID (RELEASE, Raft ID $NODE_ID)..."
    echo "HTTP Port: $FERRISSEARCH_HTTP_PORT | Transport Port: $FERRISSEARCH_TRANSPORT_PORT"

    ./target/release/ferrissearch
}

start_cluster() {
    local NUM_NODES=$1
    local PIDS=()

    build_release force

    # Build seed hosts for all nodes
    local SEEDS=""
    for i in $(seq 1 "$NUM_NODES"); do
        [[ -n "$SEEDS" ]] && SEEDS="$SEEDS,"
        SEEDS="${SEEDS}127.0.0.1:$((9300 + i - 1))"
    done

    trap 'echo; echo "Stopping all nodes..."; kill "${PIDS[@]}" 2>/dev/null; wait; echo "All nodes stopped."; exit 0' INT TERM

    for i in $(seq 1 "$NUM_NODES"); do
        local OFFSET=$((i - 1))
        FERRISSEARCH_NODE_NAME="node-$i" \
        FERRISSEARCH_HTTP_PORT=$((9200 + OFFSET)) \
        FERRISSEARCH_TRANSPORT_PORT=$((9300 + OFFSET)) \
        FERRISSEARCH_DATA_DIR="./data/node-$i" \
        FERRISSEARCH_RAFT_NODE_ID=$i \
        FERRISSEARCH_SEED_HOSTS="$SEEDS" \
        ./target/release/ferrissearch &
        PIDS+=($!)
        echo "Node $i started (PID ${PIDS[-1]}, Raft ID $i) — HTTP $((9200 + OFFSET)) | Transport $((9300 + OFFSET))"

        # Stagger: let node 1 bootstrap before starting the rest
        if [[ $i -eq 1 ]]; then
            echo "Waiting for node 1 to bootstrap..."
            sleep 3
        else
            sleep 1
        fi
    done

    echo
    echo "$NUM_NODES-node cluster running (RELEASE). Press Ctrl+C to stop all nodes."
    wait
}

if [[ "$1" == "--nodes" ]]; then
    if [[ -z "$2" ]] || ! [[ "$2" =~ ^[0-9]+$ ]]; then
        echo "Usage: ./dev_cluster_release.sh --nodes <count>"
        exit 1
    fi
    start_cluster "$2"
elif [[ -n "$1" ]] && [[ "$1" =~ ^[0-9]+$ ]]; then
    start_single_node "$1"
else
    echo "Usage: ./dev_cluster_release.sh <node_id>       # Start a single node"
    echo "       ./dev_cluster_release.sh --nodes <count>  # Start a multi-node cluster"
    echo
    echo "Examples:"
    echo "  ./dev_cluster_release.sh 1          # Start node 1 on port 9200"
    echo "  ./dev_cluster_release.sh --nodes 3  # Start 3-node cluster"
    exit 1
fi
