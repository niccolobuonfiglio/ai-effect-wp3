# Sequential Service Template

For operations that complete quickly (< 30 seconds). Single-threaded, no task state management.

## Running with Orchestrator

### 1. Start Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### 2. Start Your Service

```bash
cd use-cases/templates/file_based/sequential
pip install -r requirements.txt
PORT=8080 python service.py
```

### 3. Start Orchestrator API

```bash
cd orchestrator
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Start Worker

```bash
cd orchestrator
python -m worker --workflow-id <workflow_id>
```

### 5. Submit Workflow

```bash
curl -X POST http://localhost:8000/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "blueprint": {
      "name": "My Pipeline",
      "pipeline_id": "my-pipeline",
      "creation_date": "2025-01-01",
      "type": "pipeline-topology/v2",
      "version": "2.0",
      "nodes": [{
        "container_name": "my-service",
        "proto_uri": "service.proto",
        "image": "my-service:latest",
        "node_type": "MLModel",
        "operation_signature_list": [{
          "operation_signature": {
            "operation_name": "ProcessData",
            "input_message_name": "Input",
            "output_message_name": "Output"
          },
          "connected_to": []
        }]
      }]
    },
    "dockerinfo": {
      "docker_info_list": [{
        "container_name": "my-service",
        "ip_address": "localhost",
        "port": "8080"
      }]
    }
  }'
```

The `operation_name` in blueprint must match your `execute_<MethodName>` function.

## Architecture

```
service.py          handler.py
    |                   |
    | execute_*     create_app()
    | methods           |
    |                   |
    +--------->---------+
              |
         FastAPI app
              |
    POST /control/execute
              |
         {status: "complete", output: {...}}
```

## Quick Start

```bash
pip install -r requirements.txt
python service.py
```

## Adding Methods

Edit `service.py` and add `execute_<MethodName>` functions:

```python
from handler import DataReference, ExecuteRequest, ExecuteResponse

def execute_MyMethod(request: ExecuteRequest) -> ExecuteResponse:
    # Your processing logic
    result = process(request.inputs)

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="s3",
            uri=f"s3://bucket/{request.task_id}.json",
            format="json",
        ),
    )
```

The method name must match the operation name in the blueprint.

## Request Data

```python
def execute_AnalyzeData(request: ExecuteRequest) -> ExecuteResponse:
    # Inputs from previous tasks
    for ref in request.inputs:
        data = fetch(ref["protocol"], ref["uri"])

    # Parameters from blueprint
    threshold = request.parameters.get("threshold", 0.5)

    # Task identifiers
    workflow_id = request.workflow_id
    task_id = request.task_id
```

## Docker

```bash
docker build -t my-service .
docker run -p 8080:8080 my-service
```

## When to Use

- Data transformations
- Format conversions
- Quick API calls
- Lightweight inference
- Any operation completing in < 30 seconds
