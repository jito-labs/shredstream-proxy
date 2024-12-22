#!/bin/bash
set -eu

LEDGER_DIR=${LEDGER_DIR:-"/solana/ledger"}

# fetch and print port using solana tooling
get_tvu_solana() {
  echo "Getting shred listen port using solana cli with \$LEDGER_DIR=$LEDGER_DIR"
  
  # get solana cli version
  SOLANA_VERSION=$(solana --version)

  # check the solana cli version
  if [[ $SOLANA_VERSION == solana-cli\ 2.* ]]; then
    # use agave-validator for solana cli version 2.x
    echo "Using agave-validator for solana cli version 2.x"
    agave-validator --ledger "$LEDGER_DIR" contact-info | grep "TVU:" | cut -d ':' -f 3
  elif [[ $SOLANA_VERSION == solana-cli\ 1.* ]]; then
    # use solana-validator for solana cli version 1.x
    echo "Using solana-validator for solana cli version 1.x"
    solana-validator --ledger "$LEDGER_DIR" contact-info | grep "TVU:" | cut -d ':' -f 3
  else
    # unsupported solana cli version
    echo "Unsupported solana cli version: $SOLANA_VERSION"
  fi
}

# fetch port using curl. not guaranteed to be accurate as we assume it uses the default port allocation order
get_tvu_curl() {
  HOST=${HOST:-"http://localhost:8899"}
  echo "Getting shred listen port from \$HOST=$HOST using curl"
  IDENTITY_PUBKEY=$(curl --show-error --silent "$HOST" -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","id":1, "method":"getIdentity"}' | jq -r .result.identity)
  GOSSIP_SOCKETADDR=$(curl --show-error --silent "$HOST" -X POST -H "Content-Type: application/json" -d '{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}' | jq -r ".result | map(select(.pubkey == \"$IDENTITY_PUBKEY\")) | .[0].gossip")
  GOSSIP_PORT=$(echo "$GOSSIP_SOCKETADDR" | cut -d ':' -f 2)

  # offset by 2: https://github.com/jito-foundation/jito-solana/blob/efc5f1af5442fbd6645b2debcacd555c7c4b955b/gossip/src/cluster_info.rs#L2942
  echo $(("$GOSSIP_PORT" + 1))
}

# check solana cli and ledger directory exists
if [[ -x "$(command -v solana)" && -d $LEDGER_DIR ]]; then
  get_tvu_solana
  exit 0
fi

# exit if jq not exists
if [[ ! -x "$(command -v jq)" ]]; then
  echo "'jq' not found"
  exit 1
fi

# exit if curl not exists
if [[ ! -x "$(command -v curl)" ]]; then
  echo "'curl' not found"
  exit 1
fi

get_tvu_curl
