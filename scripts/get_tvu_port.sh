#!/bin/bash

set -eu
HOST=${1:-"http://localhost:8899"}
# check jq exists
if [ ! -x "$(command -v jq)" ]; then
    echo "'jq' not found"
    exit 1
fi

# check curl exists
if [ ! -x "$(command -v curl)" ]; then
    echo "'curl' not found"
    exit 1
fi

IDENTITY_PUBKEY=$(curl "$HOST" -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1, "method":"getIdentity"}' | jq -r .result.identity )
TPU_PORT=$(curl "$HOST" -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}' | jq -r ".result | map(select(.pubkey == \"$IDENTITY_PUBKEY\")) | .[0].tpu" )

echo TVU Port to use with Shredstream: "$TPU_PORT"