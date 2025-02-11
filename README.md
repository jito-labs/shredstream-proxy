# Jito Shredstream Proxy

ShredStream provides the lowest latency to shreds from leaders on Solana. 

See more at https://docs.jito.wtf/lowlatencytxnfeed/

## Disclaimer
Use this at your own risk.

RUST_LOG=info cargo run --release --bin jito-shredstream-proxy -- shredstream \
    --block-engine-url "https://mainnet.block-engine.jito.wtf" \
    --auth-keypair "key.json" \
    --desired-regions "amsterdam" \
    --dest-ip-ports "127.0.0.1:8001,10.0.0.1:8001"

RUST_LOG=info cargo run --release --bin jito-shredstream-proxy -- shredstream \
--block-engine-url https://mainnet.block-engine.jito.wtf \
--auth-keypair key.json \
--desired-regions amsterdam \
--dest-ip-ports 127.0.0.1:8001,10.0.0.1:8001 \
--dest-deshredded-ip-port 127.0.0.1:8002,10.0.0.1:8002 