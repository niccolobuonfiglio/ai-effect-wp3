#!/bin/bash
# Micro-benchmark of the Redis transactions (tests/benchmark/redis_transactions.py).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON=python3
[ -x "$SCRIPT_DIR/.venv/bin/python" ] && PYTHON="$SCRIPT_DIR/.venv/bin/python"

PYTHONPATH="$SCRIPT_DIR/src" "$PYTHON" -W ignore::DeprecationWarning -m tests.benchmark.redis_transactions "$@"
