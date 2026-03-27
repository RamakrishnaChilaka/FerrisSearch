#!/usr/bin/env bash

# dev_cluster.sh
# A quick helper script to spin up isolated local nodes for FerrisSearch testing

if [ -z "$1" ]; then
    echo "Usage: ./dev_cluster.sh <node_id> [port_offset]"
    echo "Example: ./dev_cluster.sh 1"
    echo "         ./dev_cluster.sh 2"
    exit 1
fi

NODE_ID=$1
OFFSET=$((NODE_ID - 1))

# By default, Node 1 gets 9200/9300. Node 2 gets 9201/9301, etc.
HTTP_PORT=$((9200 + OFFSET))
TRANSPORT_PORT=$((9300 + OFFSET))

export FERRISSEARCH_NODE_NAME="node-$NODE_ID"
export FERRISSEARCH_HTTP_PORT=$HTTP_PORT
export FERRISSEARCH_TRANSPORT_PORT=$TRANSPORT_PORT
export FERRISSEARCH_DATA_DIR="./data/node-$NODE_ID"
export FERRISSEARCH_RAFT_NODE_ID=$NODE_ID
export FERRISSEARCH_SEED_HOSTS="127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302"

echo "Starting FerrisSearch Node $NODE_ID..."
echo "HTTP Port: $HTTP_PORT | Transport Port: $TRANSPORT_PORT"

cargo run
