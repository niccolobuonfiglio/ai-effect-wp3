#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Running integration tests on host (requires Docker)..."

export PYTHONPATH="$SCRIPT_DIR/src:$PYTHONPATH"
python -m pytest tests/integration/ -v "$@"
