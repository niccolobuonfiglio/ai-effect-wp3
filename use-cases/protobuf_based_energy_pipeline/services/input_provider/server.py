import grpc
from concurrent import futures
import logging
import os
import sys

# Add proto directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'proto'))

# Compile proto files
import grpc_tools.protoc
proto_file = os.path.join(os.path.dirname(__file__), 'proto', 'input_provider.proto')
grpc_tools.protoc.main([
    'grpc_tools.protoc',
    f'-I{os.path.dirname(proto_file)}',
    f'--python_out={os.path.dirname(proto_file)}',
    f'--grpc_python_out={os.path.dirname(proto_file)}',
    proto_file
])

import proto.input_provider_pb2 as input_provider_pb2
import proto.input_provider_pb2_grpc as input_provider_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InputProviderService(input_provider_pb2_grpc.InputProviderServicer):
    """Service that provides initial configuration for the pipeline."""

    def GetConfiguration(self, request, context):
        """Return predefined configuration for the pipeline."""
        try:
            # Predefined configuration
            num_records = 10

            logger.info(f"GetConfiguration called - returning num_records={num_records}")

            return input_provider_pb2.GetConfigurationResponse(
                success=True,
                message=f"Configuration provided: {num_records} records",
                num_records=num_records
            )

        except Exception as e:
            logger.error(f"Failed to provide configuration: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to provide configuration: {str(e)}")
            return input_provider_pb2.GetConfigurationResponse(
                success=False,
                message=f"Error: {str(e)}"
            )


def serve():
    port = os.environ.get('GRPC_PORT', '50051')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    input_provider_pb2_grpc.add_InputProviderServicer_to_server(
        InputProviderService(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Input Provider service listening on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
