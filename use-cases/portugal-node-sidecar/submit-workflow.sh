#!/bin/bash
set -e

cd "$(dirname "$0")"

API_URL="${API_URL:-http://localhost:18000}"

# Read blueprint and dockerinfo
BLUEPRINT=$(cat blueprint.json)
DOCKERINFO=$(cat dockerinfo.json)

# Initial inputs for the first node (LoadData)
INPUTS_JSON='{"file_path": "/app/real_data.csv", "max_rows": 1000, "rename_columns": {"datetime": "timestamp"}}'
INPUTS_B64=$(echo -n "$INPUTS_JSON" | base64 -w 0)

# Combine into workflow submission payload
PAYLOAD=$(jq -n --argjson blueprint "$BLUEPRINT" --argjson dockerinfo "$DOCKERINFO" --arg inputs_b64 "$INPUTS_B64" \
  '{"blueprint": $blueprint, "dockerinfo": $dockerinfo, "inputs": [{"protocol": "inline", "uri": $inputs_b64, "format": "json"}]}')

echo "Submitting Portugal node workflow (sidecar) to orchestrator at $API_URL..."
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
echo "Pipeline: LoadData -> ApplyFeatures -> TrainModel -> GenerateData"
echo ""
echo "To check status:"
echo "  curl $API_URL/workflows/$WORKFLOW_ID | jq ."
echo ""
echo "To watch worker logs:"
echo "  cd ../../orchestrator && docker compose logs -f worker"
