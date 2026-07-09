#!/bin/bash
cd "$(dirname "$0")/.."
docker compose up --build -d
echo "Orchestrator running at http://localhost:18000"
echo "Redis available at localhost:16379"
echo ""
echo "To view logs: ./scripts/logs.sh"
