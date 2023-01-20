#!/usr/bin/env bash
set -ex

# Some container vars
TAG=${USER}-dev
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/shredstream-proxy:${TAG}" .

docker push "${ORG}/jito-searcher-canary:${TAG}"
