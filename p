#!/usr/bin/env bash
set -eux

# Some container vars
TAG=${TAG:-${USER}-dev} # READ tag in from env var, defaulting to ${USER}-dev
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/jito-shredstream-proxy:${TAG}" .

docker push "${ORG}/jito-shredstream-proxy:${TAG}"
