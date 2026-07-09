#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure the network exists
if ! docker network inspect ai-effect-services &>/dev/null; then
    echo "Creating ai-effect-services network..."
    docker network create ai-effect-services
fi

IMAGE_NAME="orchestrator-tests"

echo "Building test image..."
docker build -f Dockerfile.test -t "$IMAGE_NAME" .

echo "Running tests..."
docker run --rm \
    --network ai-effect-services \
    -v /var/run/docker.sock:/var/run/docker.sock \
    "$IMAGE_NAME" "$@"