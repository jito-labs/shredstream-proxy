# Shredstream Proxy

Connects to Jito infrastructure, providing a fast path to shreds

## Example:
```bash
git submodule update --init --recursive

# example: receiving shreds from dallas and amsterdam, only directly connecting to dallas
cargo run --bin jito-shredstream-proxy -- \
    --block-engine-url https://dallas.mainnet.block-engine.jito.wtf \
    --auth-keypair my_keypair.json \
    --desired-regions dallas,amsterdam \
    --dst-sockets 127.0.0.1:9900,127.0.0.1:9901
```

## Connecting
Please check out https://jito-labs.gitbook.io/mev/systems/connecting for the most up-to-date information on block engines.

## Disclaimer
Use this at your own risk.
