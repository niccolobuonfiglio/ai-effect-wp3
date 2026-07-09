# Concurrent Protobuf Service Template

For long-running operations (> 30 seconds) with direct gRPC data exchange between services.
Supports multiple simultaneous tasks with progress tracking.

## Architecture

```
Orchestrator                     Services
────────────                     ────────
     │
     │ HTTP /control/execute
     ├─────────────────────────► Service A (long-running)
     │◄───────────────────────── status="running", task_id
     │
     │ HTTP /control/status      (poll until complete)
     ├─────────────────────────►
     │◄───────────────────────── progress=50%
     │
     │ HTTP /control/output      (when complete)
     ├─────────────────────────►
     │◄───────────────────────── {protocol:"grpc", uri:"...", format:"ProcessData"}
     │
     │ HTTP /control/execute     Service B
     ├─────────────────────────► (calls A.ProcessData via gRPC)
```

- **Orchestrator** controls execution ORDER via HTTP, polls for completion
- **Services** exchange DATA directly via gRPC (calling actual methods)
- **format** field contains the gRPC method name to call

## Quick Start

```bash
pip install -r requirements.txt
python service.py
```

## How It Works

1. Service exposes two interfaces:
   - **HTTP** (port 8080): Control interface for orchestrator
   - **gRPC** (port 50051): Data interface for service-to-service communication

2. For long-running tasks:
   - Orchestrator triggers execution via HTTP
   - Service returns `status="running"` with `task_id`
   - Background thread processes, updates progress
   - Orchestrator polls `/control/status/{task_id}`
   - When complete, orchestrator fetches output via `/control/output/{task_id}`
   - Output contains gRPC endpoint + method name

3. Downstream services call the method directly via gRPC

## Adding Methods

Edit `service.py`:

```python
def execute_TrainModel(request: ExecuteRequest) -> ExecuteResponse:
    """Long-running training operation."""
    task_manager.register_task(request.task_id, request)
    run_in_background(request.task_id, _train_worker, request)

    return ExecuteResponse(
        status="running",
        task_id=request.task_id,
    )


def _train_worker(task_id: str, request: ExecuteRequest, manager: TaskManager) -> None:
    try:
        # 1. Call upstream method via gRPC
        for inp in request.inputs:
            if inp.get("protocol") == "grpc":
                upstream_uri = inp["uri"]
                method_name = inp["format"]
                config = call_upstream(upstream_uri, method_name)

        # 2. Process with progress updates
        for epoch in range(100):
            train_one_epoch()
            manager.update_progress(task_id, epoch + 1)

        # 3. Complete with gRPC endpoint reference
        manager.complete_task(
            task_id,
            {
                "protocol": "grpc",
                "uri": f"{grpc_host}:{grpc_port}",
                "format": "TrainModel",  # Method for downstream to call
            },
        )

    except Exception as e:
        manager.fail_task(task_id, str(e))
```

## Proto Files

Define your service methods:

```protobuf
service MyService {
  rpc TrainModel(TrainRequest) returns (TrainResponse);
  rpc GetPrediction(PredictRequest) returns (PredictResponse);
}
```

Each method can be called directly by downstream services.

## Task States

| Status | Meaning |
|--------|---------|
| `running` | Task in progress, poll `/control/status/{task_id}` |
| `complete` | Task done, get output from `/control/output/{task_id}` |
| `failed` | Task failed, check `error` field |

## Docker

```bash
docker build -t my-service .
docker run -p 8080:8080 -p 50051:50051 my-service
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 8080 | HTTP control interface port |
| `GRPC_PORT` | 50051 | gRPC data interface port |
| `GRPC_HOST` | my-service | Hostname for gRPC endpoint references |

## When to Use

- Model training
- Batch processing
- Large data analysis
- Any operation > 30 seconds
- Operations needing progress tracking
