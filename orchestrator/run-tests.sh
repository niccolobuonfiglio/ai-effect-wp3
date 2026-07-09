#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE_NAME="orchestrator-tests"

echo "Building test image..."
docker build -f Dockerfile.test -t "$IMAGE_NAME" .

echo "Running tests..."
docker run --rm "$IMAGE_NAME" "$@"
