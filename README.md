# Jito Shredstream Proxy

Connects to Jito infrastructure, providing a fast path to shreds.

## Usage
1. Submit your auth pubkey to be allowed
2. Ensure your firewall is open. Default listen port for shreds is 10000/udp. NAT connections currently not supported.
3. Run via docker or natively
    - These examples: receive shreds from `dallas` and `nyc`, only directly connecting to `dallas` region

### Docker

View logs with `docker logs -f jito-shredstream-proxy`

#### Host Networking
This exposes all ports, bypassing Docker NAT. Recommended setup.
```shell
docker run -d \
--name jito-shredstream-proxy \
--rm \
--env RUST_LOG=info \
--env BLOCK_ENGINE_URL=https://nyc.testnet.block-engine.jito.wtf \
--env AUTH_KEYPAIR=my_keypair.json \
--env DESIRED_REGIONS=dallas,nyc \
--env DEST_IP_PORTS=127.0.0.1:9900,127.0.0.1:9901 \
--network host \
-v $(pwd)/my_keypair.json:/app/my_keypair.json \
jitolabs/jito-shredstream-proxy
```

#### Bridge Networking
For use in places where `host` networking is not available. Requires manually exposing each destination.
Use `172.17.0.1` for any shred listeners on localhost, and normal ip for all other endpoints.
This IP may differ for your system, confirm with `ip -brief a show dev docker0`.
```shell
docker run -d \
--name jito-shredstream-proxy \
--rm \
--env RUST_LOG=info \
--env BLOCK_ENGINE_URL=https://nyc.testnet.block-engine.jito.wtf \
--env AUTH_KEYPAIR=my_keypair.json \
--env DESIRED_REGIONS=dallas,nyc \
--env SRC_BIND_PORT=10000 \
--env DEST_IP_PORTS=172.17.0.1:9900,172.17.0.1:9901 \
--network bridge \
-p 10000:10000/udp \
-v $(pwd)/my_keypair.json:/app/my_keypair.json \
jitolabs/jito-shredstream-proxy
```

### Native
```bash
git clone https://github.com/jito-labs/shredstream-proxy.git
git submodule update --init --recursive

RUST_LOG=info cargo run --bin jito-shredstream-proxy -- \
    --block-engine-url https://nyc.testnet.block-engine.jito.wtf \
    --auth-keypair my_keypair.json \
    --desired-regions dallas,nyc \
    --dest-ip-ports 127.0.0.1:9900,127.0.0.1:9901
```

## Connecting
Please check out https://jito-labs.gitbook.io/mev/systems/connecting for the most up-to-date information on block engines.

## Disclaimer
Use this at your own risk.