#!/usr/bin/env bash

# dev_cluster_release.sh — builds with native CPU optimizations and runs a release node

if [ -z "$1" ]; then
    echo "Usage: ./dev_cluster_release.sh <node_id>"
    exit 1
fi

NODE_ID=$1
OFFSET=$((NODE_ID - 1))

# Build with native CPU optimizations if binary is stale or missing
if [ "$NODE_ID" = "1" ] || [ ! -f ./target/release/ferrissearch ]; then
    echo "Building release binary with target-cpu=native..."
    RUSTFLAGS="-C target-cpu=native" cargo build --release
fi

export FERRISSEARCH_NODE_NAME="node-$NODE_ID"
export FERRISSEARCH_HTTP_PORT=$((9200 + OFFSET))
export FERRISSEARCH_TRANSPORT_PORT=$((9300 + OFFSET))
export FERRISSEARCH_DATA_DIR="./data/node-$NODE_ID"
export FERRISSEARCH_RAFT_NODE_ID=$NODE_ID
export FERRISSEARCH_SEED_HOSTS="127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302"

echo "Starting FerrisSearch Node $NODE_ID (RELEASE)..."
echo "HTTP Port: $FERRISSEARCH_HTTP_PORT | Transport Port: $FERRISSEARCH_TRANSPORT_PORT"

./target/release/ferrissearch
