#!/usr/bin/env bash
set -eux

# Some container vars
TAG=${TAG:-${USER}-dev} # READ tag in from env var, defaulting to ${USER}-dev
ORG="jitolabs"

DOCKER_BUILDKIT=1 docker build -t "$ORG/jito-shredstream-proxy:${TAG}" .

docker push "${ORG}/jito-shredstream-proxy:${TAG}"

# push image
#VERSION=v0.2.0
#docker build -t jitolabs/jito-shredstream-proxy:$VERSION .
#docker push jitolabs/jito-shredstream-proxy:$VERSION

# deploy
#VERSION=v0.2.0
#git tag $VERSION -f; git push --tags -f
#docker tag jitolabs/jito-shredstream-proxy:$VERSION jitolabs/jito-shredstream-proxy:latest
#docker push jitolabs/jito-shredstream-proxy:latest