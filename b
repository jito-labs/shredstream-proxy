#!/usr/bin/env bash
set -eux

# Some container vars
TAG=$(git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty)
ORG="jitolabs"

DOCKER_BUILDKIT=1 \
  docker build -t "jitolabs/shredstream-proxy:${TAG}" .

docker run "jitolabs/shredstream-proxy:${TAG}"
