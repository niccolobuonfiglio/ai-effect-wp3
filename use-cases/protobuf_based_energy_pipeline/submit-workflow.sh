#!/bin/bash
set -e

API_URL="${API_URL:-http://localhost:18000}"

# Service names (can be read from dockerinfo.json or set here)
SERVICES=("pb-input-provider" "pb-data-generator" "pb-data-analyzer" "pb-report-generator")

echo "Submitting workflow to orchestrator at $API_URL..."

# Build blueprint dynamically
BLUEPRINT=$(cat <<-END
{
  "name": "Protobuf Based Energy Pipeline",
  "pipeline_id": "protobuf-energy",
  "creation_date": "$(date +%Y-%m-%d)",
  "type": "pipeline-topology/v2",
  "version": "2.0",
  "nodes": [
    {
      "container_name": "${SERVICES[0]}",
      "proto_uri": "input_provider.proto",
      "image": "input-provider:latest",
      "node_type": "MLModel",
      "operation_signature_list": [{
        "operation_signature": {
          "operation_name": "GetConfiguration",
          "input_message_name": "GetConfigurationRequest",
          "output_message_name": "GetConfigurationResponse"
        },
        "connected_to": [{
          "container_name": "${SERVICES[1]}",
          "operation_signature": {
            "operation_name": "GenerateData"
          }
        }]
      }]
    },
    {
      "container_name": "${SERVICES[1]}",
      "proto_uri": "data_generator.proto",
      "image": "data-generator:latest",
      "node_type": "MLModel",
      "operation_signature_list": [{
        "operation_signature": {
          "operation_name": "GenerateData",
          "input_message_name": "GenerateDataRequest",
          "output_message_name": "GenerateDataResponse"
        },
        "connected_to": [{
          "container_name": "${SERVICES[2]}",
          "operation_signature": {
            "operation_name": "AnalyzeData"
          }
        }]
      }]
    },
    {
      "container_name": "${SERVICES[2]}",
      "proto_uri": "data_analyzer.proto",
      "image": "data-analyzer:latest",
      "node_type": "MLModel",
      "operation_signature_list": [{
        "operation_signature": {
          "operation_name": "AnalyzeData",
          "input_message_name": "AnalyzeDataRequest",
          "output_message_name": "AnalyzeDataResponse"
        },
        "connected_to": [{
          "container_name": "${SERVICES[3]}",
          "operation_signature": {
            "operation_name": "GenerateReport"
          }
        }]
      }]
    },
    {
      "container_name": "${SERVICES[3]}",
      "proto_uri": "report_generator.proto",
      "image": "report-generator:latest",
      "node_type": "MLModel",
      "operation_signature_list": [{
        "operation_signature": {
          "operation_name": "GenerateReport",
          "input_message_name": "GenerateReportRequest",
          "output_message_name": "GenerateReportResponse"
        },
        "connected_to": []
      }]
    }
  ]
}
END
)

# Build dockerinfo dynamically
DOCKERINFO=$(cat <<-END
{
  "docker_info_list": [
    {"container_name": "${SERVICES[0]}", "ip_address": "${SERVICES[0]}", "port": "8080"},
    {"container_name": "${SERVICES[1]}", "ip_address": "${SERVICES[1]}", "port": "8080"},
    {"container_name": "${SERVICES[2]}", "ip_address": "${SERVICES[2]}", "port": "8080"},
    {"container_name": "${SERVICES[3]}", "ip_address": "${SERVICES[3]}", "port": "8080"}
  ]
}
END
)

# Submit workflow
RESPONSE=$(curl -s -X POST "$API_URL/workflows" \
  -H "Content-Type: application/json" \
  -d "{
    \"blueprint\": $BLUEPRINT,
    \"dockerinfo\": $DOCKERINFO,
    \"inputs\": [
      {
        \"protocol\": \"inline\",
        \"uri\": \"eyJudW1fcmVjb3JkcyI6IDEwLCAib3V0cHV0X2Zvcm1hdCI6ICJjc3YifQ==\",
        \"format\": \"json\"
      }
    ]
  }")

WORKFLOW_ID=$(echo "$RESPONSE" | jq -r .workflow_id)

if [ "$WORKFLOW_ID" = "null" ] || [ -z "$WORKFLOW_ID" ]; then
    echo "Failed to submit workflow:"
    echo "$RESPONSE" | jq .
    exit 1
fi

echo "Workflow submitted!"
echo "Workflow ID: $WORKFLOW_ID"
echo ""
echo "Data flow (via gRPC):"
echo "  pb-input-provider --[grpc]--> pb-data-generator --[grpc]--> pb-data-analyzer --[grpc]--> pb-report-generator"
echo ""
echo "To check status:"
echo "  curl -s $API_URL/workflows/$WORKFLOW_ID | jq ."
echo "To check individual tasks:"
echo "  curl -s $API_URL/workflows/$WORKFLOW_ID/tasks | jq '.tasks[] | {node_key, status, error}'"
echo ""
echo "To watch worker logs:"
echo "  cd ../../orchestrator && docker compose logs -f worker"
