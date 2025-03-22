# syntax=docker/dockerfile:1.4.0
FROM --platform=linux/amd64 rust:1.84-slim-bullseye as builder

RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl-dev protobuf-compiler pkg-config libudev-dev zlib1g-dev llvm clang cmake make libprotobuf-dev g++
RUN rustup component add rustfmt && update-ca-certificates

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

# with buildkit, you need to copy the binary to the main folder
# w/o buildkit, you can remove the cp
RUN --mount=type=cache,mode=0777,target=/home/root/app/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    cargo build --release && cp target/release/jito-* ./

################################################################################
FROM --platform=linux/amd64 debian:bullseye-slim as base_image
RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl1.1 && rm -rf /var/lib/apt/lists/*

################################################################################
FROM base_image as shredstream_proxy
ENV APP="jito-shredstream-proxy"

WORKDIR /app
# with buildkit, the binary is placed in the git root folder
# w/o buildkit, the binary will be in target/release
COPY --from=builder /home/root/app/${APP} ./
ENTRYPOINT ["/app/jito-shredstream-proxy"]
