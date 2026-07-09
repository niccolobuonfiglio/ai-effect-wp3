"""Service implementation - HTTP control + gRPC data exchange for long-running tasks.

This template shows how to:
1. Expose HTTP control interface for orchestrator
2. Run gRPC server for data exchange with other services
3. Handle long-running tasks with progress tracking
4. Call upstream services via gRPC (direct method calls)

Each method should be named execute_<MethodName> where MethodName
matches the operation name in the blueprint.
"""

import logging
import os
import sys
import time
from concurrent import futures

import grpc

from handler import (
    DataReference,
    ExecuteRequest,
    ExecuteResponse,
    TaskManager,
    run,
    run_in_background,
    task_manager,
)

# Import your generated protobuf classes
# import my_service_pb2
# import my_service_pb2_grpc
# import upstream_service_pb2
# import upstream_service_pb2_grpc

logger = logging.getLogger(__name__)


# --- gRPC Server (for downstream services to call your methods) ---

# class MyServiceServicer(my_service_pb2_grpc.MyServiceServicer):
#     """gRPC servicer that handles requests from downstream services."""
#
#     def ProcessData(self, request, context):
#         """Process data. Called directly by downstream services."""
#         return _process_data(request.input_value)


def start_grpc_server():
    """Start gRPC server in background for data exchange."""
    grpc_port = os.environ.get("GRPC_PORT", "50051")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Register your servicer:
    # my_service_pb2_grpc.add_MyServiceServicer_to_server(
    #     MyServiceServicer(), server
    # )

    server.add_insecure_port(f"[::]:{grpc_port}")
    server.start()
    logger.info(f"gRPC server started on port {grpc_port}")
    return server


# --- gRPC Client (to call upstream service methods directly) ---

def fetch_from_upstream(grpc_uri: str, method_name: str):
    """Call a method on upstream service via gRPC.

    Args:
        grpc_uri: The gRPC endpoint (e.g., "upstream-service:50051")
        method_name: The method to call (e.g., "GetConfiguration")

    Returns:
        The protobuf response from upstream service.
    """
    logger.info(f"Calling {method_name} on {grpc_uri}")

    channel = grpc.insecure_channel(grpc_uri)
    # stub = upstream_service_pb2_grpc.UpstreamServiceStub(channel)

    try:
        # Call the method directly:
        # method = getattr(stub, method_name)
        # response = method(upstream_service_pb2.SomeRequest())
        # return response
        pass
    finally:
        channel.close()


# --- HTTP Control Interface (for orchestrator) ---

def execute_QuickProcess(request: ExecuteRequest) -> ExecuteResponse:
    """Quick operation that completes immediately and returns gRPC endpoint."""

    # 1. Fetch input from upstream via gRPC (if inputs provided)
    for inp in request.inputs:
        if inp.get("protocol") == "grpc":
            upstream_uri = inp.get("uri")
            method_name = inp.get("format")  # format contains the method name
            try:
                upstream_data = fetch_from_upstream(upstream_uri, method_name)
                # Use upstream_data for processing...
            except grpc.RpcError as e:
                logger.error(f"Failed to fetch from upstream: {e}")
                return ExecuteResponse(status="failed", error=str(e))

    # 2. Process the data
    # result = process(upstream_data)

    # 3. Return gRPC endpoint reference
    grpc_host = os.environ.get("GRPC_HOST", "my-service")
    grpc_port = os.environ.get("GRPC_PORT", "50051")

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="grpc",
            uri=f"{grpc_host}:{grpc_port}",
            format="QuickProcess",  # Method name for downstream to call
        ),
    )


def execute_LongProcess(request: ExecuteRequest) -> ExecuteResponse:
    """Long-running operation with progress tracking.

    Registers task, processes in background, returns gRPC endpoint when done.
    """
    task_manager.register_task(request.task_id, request)
    run_in_background(request.task_id, _process_long_running, request)

    return ExecuteResponse(
        status="running",
        task_id=request.task_id,
    )


def _process_long_running(
    task_id: str,
    request: ExecuteRequest,
    manager: TaskManager,
) -> None:
    """Background worker for long-running task.

    Args:
        task_id: Task ID for progress updates.
        request: Original request with inputs/parameters.
        manager: TaskManager for updating progress.
    """
    try:
        # 1. Fetch input from upstream via gRPC (if inputs provided)
        upstream_data = None
        for inp in request.inputs:
            if inp.get("protocol") == "grpc":
                upstream_uri = inp.get("uri")
                method_name = inp.get("format")  # format contains the method name
                upstream_data = fetch_from_upstream(upstream_uri, method_name)
                break

        # 2. Process with progress updates
        for progress in range(0, 101, 20):
            time.sleep(1)  # Replace with actual work
            manager.update_progress(task_id, progress)

        # 3. Complete task with gRPC endpoint reference
        grpc_host = os.environ.get("GRPC_HOST", "my-service")
        grpc_port = os.environ.get("GRPC_PORT", "50051")

        manager.complete_task(
            task_id,
            {
                "protocol": "grpc",
                "uri": f"{grpc_host}:{grpc_port}",
                "format": "LongProcess",  # Method name for downstream to call
            },
        )

    except Exception as e:
        manager.fail_task(task_id, str(e))


if __name__ == "__main__":
    # Start gRPC server in background (for data exchange)
    grpc_server = start_grpc_server()

    # Run HTTP control interface (blocking, for orchestrator)
    run(sys.modules[__name__])
