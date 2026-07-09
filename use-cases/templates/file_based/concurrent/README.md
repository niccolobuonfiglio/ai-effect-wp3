# Concurrent Service Template

For long-running operations (minutes to hours). Multithreaded with progress tracking.

## Running with Orchestrator

### 1. Start Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### 2. Start Your Service

```bash
cd use-cases/templates/concurrent
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
            "operation_name": "LongProcess",
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

### 6. Monitor Progress

```bash
# Get workflow status
curl http://localhost:8000/workflows/<workflow_id>

# Get task details
curl http://localhost:8000/workflows/<workflow_id>/tasks
```

## Architecture

```
service.py              handler.py
    |                       |
    | execute_*         TaskManager (thread-safe)
    | methods               |
    |                   create_app()
    +--------->-------------+
              |
         FastAPI app
              |
    POST /control/execute  -->  {status: "running", task_id: "..."}
    GET  /control/status   -->  {status: "running", progress: 50}
    GET  /control/output   -->  {output: {...}}
```

## Quick Start

```bash
pip install -r requirements.txt
python service.py
```

## Adding Methods

Edit `service.py`. For quick operations:

```python
def execute_QuickTask(request: ExecuteRequest) -> ExecuteResponse:
    result = process(request.inputs)
    return ExecuteResponse(
        status="complete",
        output=DataReference(protocol="s3", uri="...", format="json"),
    )
```

For long-running operations:

```python
def execute_LongTask(request: ExecuteRequest) -> ExecuteResponse:
    # Use orchestrator's task_id (no need to generate our own)
    task_manager.register_task(request.task_id, request)
    run_in_background(request.task_id, _my_worker, request)
    return ExecuteResponse(status="running", task_id=request.task_id)


def _my_worker(task_id: str, request: ExecuteRequest, manager: TaskManager) -> None:
    try:
        for i in range(10):
            # Do work...
            manager.update_progress(task_id, i * 10)

        manager.complete_task(task_id, {
            "protocol": "s3",
            "uri": f"s3://bucket/{task_id}.json",
            "format": "json",
        })
    except Exception as e:
        manager.fail_task(task_id, str(e))
```

## TaskManager API

```python
# Register task for tracking (uses orchestrator's task_id)
task_manager.register_task(request.task_id, request)

# Update progress (0-100)
task_manager.update_progress(task_id, 50)

# Complete with output
task_manager.complete_task(task_id, {"protocol": "s3", "uri": "...", "format": "json"})

# Fail with error
task_manager.fail_task(task_id, "Something went wrong")
```

## Thread Safety

The `TaskManager` uses a lock internally. Multiple background threads can safely:
- Update progress
- Complete or fail tasks

The orchestrator polls `/control/status` until complete, then fetches `/control/output`.

## Docker

```bash
docker build -t my-service .
docker run -p 8080:8080 my-service
```

## When to Use

- ML model training
- Large batch processing
- Long-running simulations
- Multi-step pipelines
- Any operation taking > 30 seconds
