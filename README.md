# Shredstream Proxy

Connects to Jito infrastructure, providing a fast path to shreds

## Example:
```bash
git submodule update --init --recursive

cargo run --bin jito-shredstream-proxy -- \
    --auth-addr https://ny.mainnet.block-engine.jito.wtf \
    --shredstream-addr https://ny.mainnet.block-engine.jito.wtf \
    --auth-keypair /home/eric/dev/jito-creds/aprdev.json \
    --desired-regions ny,la \
    --dst-sockets 127.0.0.1:4000,127.0.0.1:5000
```

## Connecting
Please check out https://jito-labs.gitbook.io/mev/systems/connecting for the most up-to-date information on block engines.

## Disclaimer
Use this at your own risk.
