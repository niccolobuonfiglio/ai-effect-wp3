#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_NAME="orchestrator-tests"

echo "Building test image..."
docker build -f Dockerfile.test -t "$IMAGE_NAME" .

echo "Running integration tests..."
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock "$IMAGE_NAME" tests/integration/ -v --cov=src "$@"
