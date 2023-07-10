# syntax=docker/dockerfile:1.4.0
FROM --platform=linux/amd64 rust:1.66-slim-bullseye as builder

RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl-dev protobuf-compiler pkg-config
RUN rustup component add rustfmt && update-ca-certificates

ENV HOME=/home/root
WORKDIR $HOME/app
COPY . .

RUN cargo build --release

################################################################################
FROM --platform=linux/amd64 debian:bullseye-slim as base_image
RUN apt-get -qq update && apt-get install -qq -y ca-certificates libssl1.1 && rm -rf /var/lib/apt/lists/*

################################################################################
FROM base_image as shredstream_proxy
ENV APP="jito-shredstream-proxy"

WORKDIR /app
COPY --from=builder /home/root/app/target/release/${APP} ./
ENTRYPOINT ["/app/jito-shredstream-proxy"]
