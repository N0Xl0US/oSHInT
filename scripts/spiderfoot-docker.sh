#!/usr/bin/env bash
set -euo pipefail

IMAGE="${SPIDERFOOT_DOCKER_IMAGE:-ctdc/spiderfoot:latest}"
PLATFORM="${SPIDERFOOT_DOCKER_PLATFORM:-linux/amd64}"
VOLUME="${SPIDERFOOT_DOCKER_VOLUME:-spiderfoot_data}"

exec docker run --rm --platform "$PLATFORM" -v "$VOLUME:/root/.spiderfoot" "$IMAGE" sf.py "$@"
