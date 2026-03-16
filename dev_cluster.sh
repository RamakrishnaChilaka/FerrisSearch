#!/usr/bin/env bash

# dev_cluster.sh
# A quick helper script to spin up isolated local nodes for ROpenSearch testing

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

export ROPENSEARCH_NODE_NAME="node-$NODE_ID"
export ROPENSEARCH_HTTP_PORT=$HTTP_PORT
export ROPENSEARCH_TRANSPORT_PORT=$TRANSPORT_PORT
export ROPENSEARCH_DATA_DIR="./data/node-$NODE_ID"
export ROPENSEARCH_RAFT_NODE_ID=$NODE_ID

echo "Starting ROpenSearch Node $NODE_ID..."
echo "HTTP Port: $HTTP_PORT | Transport Port: $TRANSPORT_PORT"

cargo run
