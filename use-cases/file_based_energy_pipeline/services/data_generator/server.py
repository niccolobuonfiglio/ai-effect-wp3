import os
import logging
import grpc
import pandas as pd
import signal
import sys
from concurrent import futures
from pathlib import Path

# Import generated proto files
import data_generator_pb2
import data_generator_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataGeneratorService(data_generator_pb2_grpc.DataGeneratorServiceServicer):
    """Generates synthetic energy data and saves to CSV"""
    
    def GenerateData(self, request, context):
        """Generate synthetic energy data based on request parameters"""
        logger.info(f"GenerateData called: {request.num_records} records, format: {request.output_format}")
        
        try:
            # Generate synthetic energy data
            data = {
                'timestamp': [f"2025-01-01T00:00:{i:02d}Z" for i in range(request.num_records)],
                'household_id': [f"HH-{i%3}" for i in range(request.num_records)],
                'power_consumption': [120.5 + i for i in range(request.num_records)],
                'voltage': [230.0] * request.num_records,
                'current': [5.1 + (i*0.1) for i in range(request.num_records)]
            }
            
            # Create output directory and save data
            output_path = Path("data/raw_energy.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            df = pd.DataFrame(data)
            df.to_csv(output_path, index=False)
            
            message = f"Generated {len(data['timestamp'])} records to {output_path}"
            logger.info(message)
            
            # Return protobuf response
            response = data_generator_pb2.GenerateDataResponse()
            response.success = True
            response.message = message
            response.records_generated = len(data['timestamp'])
            response.output_file_path = str(output_path)

            return response
            
        except Exception as e:
            error_msg = f"Failed to generate data: {str(e)}"
            logger.error(error_msg)
            
            # Return error response
            response = data_generator_pb2.GenerateDataResponse()
            response.success = False
            response.message = error_msg
            response.records_generated = 0
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return response


def serve(port: int = 50051):
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the service to the server
    data_generator_pb2_grpc.add_DataGeneratorServiceServicer_to_server(
        DataGeneratorService(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Data Generator service listening on port {port}")
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully")
        server.stop(5)
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down server")
        server.stop(0)


if __name__ == "__main__":
    # Use GRPC_PORT environment variable or default to 50051
    port = int(os.environ.get('GRPC_PORT', 50051))
    serve(port)