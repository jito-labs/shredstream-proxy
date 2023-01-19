# syntax=docker/dockerfile:1.4.0
FROM rust:1.64.0-slim-buster as builder

RUN apt-get update && apt-get install -y libudev-dev clang pkg-config libssl-dev build-essential cmake protobuf-compiler ca-certificates libssl1.1
RUN rustup component add rustfmt
RUN update-ca-certificates

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

RUN --mount=type=cache,mode=0777,target=/home/root/app/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    RUSTFLAGS="-C target-cpu=native" cargo build --release && cp target/release/jito-* ./

FROM debian:buster-slim as base_image
RUN apt-get update && apt-get install -y ca-certificates libssl1.1


FROM base_image as shredstream_proxy
ENV APP="jito-shredstream-proxy"

WORKDIR /app
COPY --from=builder /home/root/app/${APP} ./
ENTRYPOINT ["/app/jito-shredstream-proxy"]

