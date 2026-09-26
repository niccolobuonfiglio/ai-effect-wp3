#!/bin/bash
set -e

cd "$(dirname "$0")"

API_URL="${API_URL:-http://localhost:18000}"

# Read generated blueprint and dockerinfo
BLUEPRINT=$(cat export/blueprint.json)
DOCKERINFO=$(cat export/dockerinfo.json)

# Combine into workflow submission payload
PAYLOAD=$(jq -n --argjson blueprint "$BLUEPRINT" --argjson dockerinfo "$DOCKERINFO" \
  '{"blueprint": $blueprint, "dockerinfo": $dockerinfo}')

echo "Submitting Germany node workflow to orchestrator at $API_URL..."
# Authentication, when enabled: the services' key travels with the workflow
# (the orchestrator sends it to every service), the orchestrator's own key as a
# bearer token.
if [ -n "${SERVICE_API_KEY:-}" ]; then
    PAYLOAD=$(echo "$PAYLOAD" | jq --arg k "$SERVICE_API_KEY" '. + {services_api_key: $k}')
fi
AUTH=()
if [ -n "${ORCHESTRATOR_API_KEY:-}" ]; then
    AUTH=(-H "Authorization: Bearer $ORCHESTRATOR_API_KEY")
fi

RESPONSE=$(curl -s -X POST "$API_URL/workflows" \
  -H "Content-Type: application/json" "${AUTH[@]}" \
  -d "$PAYLOAD")

WORKFLOW_ID=$(echo "$RESPONSE" | jq -r .workflow_id)

if [ "$WORKFLOW_ID" = "null" ] || [ -z "$WORKFLOW_ID" ]; then
    echo "Failed to submit workflow:"
    echo "$RESPONSE" | jq .
    exit 1
fi

echo "Workflow submitted!"
echo "Workflow ID: $WORKFLOW_ID"
echo ""

# Problems that did not stop the submission (e.g. BLUEPRINT_TYPE_CHECK=warn)
WARNINGS=$(echo "$RESPONSE" | jq -r '.warnings[]?')
if [ -n "$WARNINGS" ]; then
    echo "Warnings:"
    echo "$WARNINGS" | sed 's/^/  - /'
    echo ""
fi
echo "Workers will automatically process this workflow."
echo ""
echo "To check status:"
echo "  curl $API_URL/workflows/$WORKFLOW_ID | jq ."
echo ""
echo "To watch worker logs:"
echo "  cd ../../orchestrator && docker compose logs -f worker"
