import os
import logging
import grpc
import signal
import sys
import random
from datetime import datetime, timedelta
from concurrent import futures

import common_pb2
import data_generator_pb2
import data_generator_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataGeneratorService(data_generator_pb2_grpc.DataGeneratorServiceServicer):
    """Generates synthetic energy data and returns it as protobuf messages"""

    def GenerateData(self, request, context):
        """Generate synthetic energy data and return as protobuf"""
        logger.info(f"GenerateData called: {request.num_records} records requested")

        try:
            response = data_generator_pb2.GenerateDataResponse()

            base_time = datetime.now()
            households = ["HH-001", "HH-002", "HH-003", "HH-004", "HH-005"]

            for i in range(request.num_records):
                record = common_pb2.EnergyRecord()

                timestamp = base_time + timedelta(minutes=i)
                record.timestamp = timestamp.isoformat() + "Z"
                record.household_id = households[i % len(households)]

                base_consumption = 120.0 + (i % 10) * 5
                record.power_consumption = base_consumption + random.uniform(-10, 10)
                record.voltage = 230.0 + random.uniform(-5, 5)
                record.current = record.power_consumption / record.voltage

                response.records.append(record)

            response.success = True
            response.message = f"Successfully generated {len(response.records)} energy records"
            logger.info(response.message)

            return response

        except Exception as e:
            logger.error(f"Error generating data: {e}")
            response = data_generator_pb2.GenerateDataResponse()
            response.success = False
            response.message = f"Error: {str(e)}"
            return response


def serve():
    """Start the gRPC server"""
    port = os.getenv('GRPC_PORT', '50051')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_generator_pb2_grpc.add_DataGeneratorServiceServicer_to_server(
        DataGeneratorService(), server
    )
    server.add_insecure_port(f'[::]:{port}')

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        server.stop(0)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start()
    logger.info(f"Data Generator Service started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()