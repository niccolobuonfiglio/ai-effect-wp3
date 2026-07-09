"""Service implementation - HTTP control + gRPC data exchange.

This template shows how to:
1. Expose HTTP control interface for orchestrator
2. Run gRPC server for data exchange with other services
3. Call upstream services via gRPC (direct method calls)
4. Provide gRPC methods for downstream services to call

Each method should be named execute_<MethodName> where MethodName
matches the operation name in the blueprint.
"""

import logging
import os
import sys
from concurrent import futures

import grpc

from handler import DataReference, ExecuteRequest, ExecuteResponse, run

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

def execute_ProcessData(request: ExecuteRequest) -> ExecuteResponse:
    """HTTP handler: Fetch data via gRPC, process it, return gRPC endpoint.

    The orchestrator calls this via HTTP. This method:
    1. Fetches input data from upstream service via gRPC (direct method call)
    2. Processes the data
    3. Returns a gRPC endpoint reference for downstream to call our method
    """

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

    # 3. Return gRPC endpoint reference (downstream will call our method)
    grpc_host = os.environ.get("GRPC_HOST", "my-service")
    grpc_port = os.environ.get("GRPC_PORT", "50051")

    return ExecuteResponse(
        status="complete",
        output=DataReference(
            protocol="grpc",
            uri=f"{grpc_host}:{grpc_port}",
            format="ProcessData",  # Method name for downstream to call
        ),
    )


if __name__ == "__main__":
    # Start gRPC server in background (for data exchange)
    grpc_server = start_grpc_server()

    # Run HTTP control interface (blocking, for orchestrator)
    run(sys.modules[__name__])
