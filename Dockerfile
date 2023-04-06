# syntax=docker/dockerfile:1.4.0
FROM rust:1.66-slim-bullseye as builder

RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl-dev protobuf-compiler pkg-config
RUN rustup component add rustfmt && update-ca-certificates

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

RUN --mount=type=cache,mode=0777,target=/home/root/app/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    RUSTFLAGS="-C target-cpu=native" cargo build --release && cp target/release/jito-* ./

################################################################################
FROM debian:bullseye-slim as base_image
RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl1.1 && rm -rf /var/lib/apt/lists/*

################################################################################
FROM base_image as shredstream_proxy
ENV APP="jito-shredstream-proxy"

WORKDIR /app
COPY --from=builder /home/root/app/${APP} ./
ENTRYPOINT ["/app/jito-shredstream-proxy"]

