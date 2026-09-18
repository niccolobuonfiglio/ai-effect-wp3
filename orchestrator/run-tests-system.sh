#!/bin/bash
# System tests: start the orchestrator and the file-based pipeline, then run
# tests/system against the live containers. The stacks are left running.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
USE_CASE="$REPO_ROOT/use-cases/file_based_energy_pipeline"
cd "$SCRIPT_DIR"

if ! docker network inspect ai-effect-services &>/dev/null; then
    echo "Creating ai-effect-services network..."
    docker network create ai-effect-services
fi

echo "Starting orchestrator (current code)..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d --build

echo "Starting file-based pipeline..."
docker compose -f "$USE_CASE/docker-compose.yml" up -d --build

API="${ORCHESTRATOR_URL:-http://localhost:18000}"
echo "Waiting for $API/health..."
for _ in $(seq 1 30); do
    curl -sf "$API/health" >/dev/null && break
    sleep 1
done

PYTHON=python3
[ -x "$SCRIPT_DIR/.venv/bin/python" ] && PYTHON="$SCRIPT_DIR/.venv/bin/python"

echo "Running system tests..."
RUN_SYSTEM_TESTS=1 PYTHONPATH="$SCRIPT_DIR/src" "$PYTHON" -m pytest tests/system -v "$@"
