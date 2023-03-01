#!/usr/bin/env bash
set -eux

# Some container vars
TAG=$(git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty)
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/jito-shredstream-proxy:${TAG}" .

docker run "$ORG/jito-shredstream-proxy:${TAG}"
