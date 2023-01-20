# Shredstream Proxy

Connects to Jito infrastructure, providing a fast path to shreds

## Usage

### Docker
```shell
docker run --rm \
-e RUST_LOG=info \
-e BLOCK_ENGINE_URL=https://nyc.testnet.block-engine.jito.wtf \
-e AUTH_KEYPAIR=my_keypair.json \
-e DESIRED_REGIONS=dallas,amsterdam \
-e DEST_SOCKETS=127.0.0.1:9900,127.0.0.1:9901 \
-v $(pwd)/my_keypair.json:/app/my_keypair.json \
jitolabs/shredstream-proxy
```

### Local
```bash
git submodule update --init --recursive

# example: receiving shreds from dallas and amsterdam, only directly connecting to dallas
RUST_LOG=info cargo run --bin jito-shredstream-proxy -- \
    --block-engine-url https://nyc.testnet.block-engine.jito.wtf \
    --auth-keypair my_keypair.json \
    --desired-regions dallas,amsterdam \
    --dest-sockets 127.0.0.1:9900,127.0.0.1:9901
```

## Connecting
Please check out https://jito-labs.gitbook.io/mev/systems/connecting for the most up-to-date information on block engines.

## Disclaimer
Use this at your own risk.