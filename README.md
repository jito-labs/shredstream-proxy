# Shredstream Proxy

Connects to Jito infrastructure, providing a fast path to shreds

## Usage
Default listen port for shreds is 10000/udp (ensure your firewall is open).

### Docker
Host
```shell

docker run -d \
--name jito-shredstream-proxy \
--rm \
-e RUST_LOG=info \
-e BLOCK_ENGINE_URL=https://nyc.testnet.block-engine.jito.wtf \
-e AUTH_KEYPAIR=my_keypair.json \
-e DESIRED_REGIONS=dallas,nyc \
-e DEST_SOCKETS=127.0.0.1:9900,127.0.0.1:9901 \
--network host \
-v $(pwd)/my_keypair.json:/app/my_keypair.json \
jitolabs/jito-shredstream-proxy
```

Bridge
```shell
docker run -d \
--name jito-shredstream-proxy \
--rm \
-e RUST_LOG=info \
-e BLOCK_ENGINE_URL=https://nyc.testnet.block-engine.jito.wtf \
-e AUTH_KEYPAIR=my_keypair.json \
-e DESIRED_REGIONS=dallas,nyc \
-e SRC_BIND_PORT=10000 \
-e DEST_SOCKETS=172.17.0.1:9900,172.17.0.1:9901 \
--network bridge \
-p 10000:10000/udp \
-v $(pwd)/my_keypair.json:/app/my_keypair.json \
jitolabs/jito-shredstream-proxy
```
Use `172.17.0.1` as IP for any local listeners, regular ip for all other endpoints.
This may differ for your system. Confirm docker local IP with `ip -brief a show dev docker0`, 
View logs with `docker logs -f jito-shredstream-proxy`

### Local
```bash
git submodule update --init --recursive

# example: receiving shreds from dallas and nyc, only directly connecting to dallas
RUST_LOG=info cargo run --bin jito-shredstream-proxy -- \
    --block-engine-url https://nyc.testnet.block-engine.jito.wtf \
    --auth-keypair my_keypair.json \
    --desired-regions dallas,nyc \
    --dest-sockets 127.0.0.1:9900,127.0.0.1:9901
```

## Connecting
Please check out https://jito-labs.gitbook.io/mev/systems/connecting for the most up-to-date information on block engines.

## Disclaimer
Use this at your own risk.