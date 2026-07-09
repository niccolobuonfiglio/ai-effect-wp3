# Sequential Protobuf Service Template

For operations that complete quickly (< 30 seconds) with direct gRPC data exchange between services.

## Architecture

```
Orchestrator                     Services
────────────                     ────────
     │
     │ HTTP /control/execute
     ├─────────────────────────► Service A
     │                              │
     │                              │ returns gRPC endpoint
     │                              │ format = "GetConfiguration"
     │                              ▼
     │ HTTP /control/execute     Service B
     ├─────────────────────────► (calls A.GetConfiguration via gRPC)
     │                              │
     │                              │ returns gRPC endpoint
     │                              │ format = "GenerateData"
     │                              ▼
     │ HTTP /control/execute     Service C
     └─────────────────────────► (calls B.GenerateData via gRPC)
```

- **Orchestrator** controls execution ORDER via HTTP
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

2. When orchestrator triggers execution:
   - Service calls upstream method via gRPC (e.g., `stub.GetConfiguration()`)
   - Processes the data
   - Returns gRPC endpoint + method name for downstream

3. Downstream services call the method directly via gRPC

## Adding Methods

Edit `service.py`:

```python
def execute_MyMethod(request: ExecuteRequest) -> ExecuteResponse:
    # 1. Call upstream method via gRPC
    for inp in request.inputs:
        if inp.get("protocol") == "grpc":
            upstream_uri = inp["uri"]
            method_name = inp["format"]  # e.g., "GetConfiguration"
            upstream_data = call_upstream(upstream_uri, method_name)

    # 2. Process data
    result = process(upstream_data)

    # 3. Return gRPC endpoint with your method name
    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="grpc",
            uri=f"{grpc_host}:{grpc_port}",
            format="MyMethod",  # Method for downstream to call
        ),
    )
```

## Proto Files

Define your service methods:

```protobuf
service MyService {
  rpc ProcessData(ProcessRequest) returns (ProcessResponse);
  rpc AnalyzeData(AnalyzeRequest) returns (AnalyzeResponse);
}
```

Each method can be called directly by downstream services.

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

- Services that need structured protobuf message schemas
- Direct service-to-service data transfer (no serialization overhead)
- Operations completing in < 30 seconds
