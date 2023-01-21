#!/usr/bin/env bash
set -ex

# Some container vars
TAG=${USER}-dev
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/jito-shredstream-proxy:${TAG}" .

docker push "${ORG}/jito-shredstream-proxy:${TAG}"
