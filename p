#!/usr/bin/env bash
set -eux

# Some container vars
TAG=${TAG:-${USER}-dev} # READ tag in from env var, defaulting to ${USER}-dev
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/jito-shredstream-proxy:${TAG}" .

docker push "${ORG}/jito-shredstream-proxy:${TAG}"

# deploy
#VERSION=v0.2.0
#git tag $VERSION -f; git push --tags -f
#docker tag jitolabs/jito-shredstream-proxy:$VERSION jitolabs/shredstream-proxy:latest
#docker push jitolabs/shredstream-proxy:latest