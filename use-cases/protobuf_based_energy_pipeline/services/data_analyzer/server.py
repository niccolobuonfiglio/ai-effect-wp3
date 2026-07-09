import os
import logging
import grpc
import signal
import sys
from concurrent import futures
from statistics import mean, stdev

import common_pb2
import data_analyzer_pb2
import data_analyzer_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataAnalyzerService(data_analyzer_pb2_grpc.DataAnalyzerServiceServicer):
    """Analyzes energy data received as protobuf messages"""

    def AnalyzeData(self, request, context):
        """Analyze energy data from protobuf request"""
        logger.info(f"AnalyzeData called: {len(request.records)} records, threshold: {request.anomaly_threshold}")

        try:
            response = data_analyzer_pb2.AnalyzeDataResponse()

            if not request.records:
                response.success = False
                response.message = "No records provided for analysis"
                return response

            power_consumptions = [r.power_consumption for r in request.records]
            mean_consumption = mean(power_consumptions)
            std_consumption = stdev(power_consumptions) if len(power_consumptions) > 1 else 0

            efficiency_scores = []
            anomaly_count = 0

            for record in request.records:
                analyzed = common_pb2.AnalyzedRecord()

                analyzed.original.CopyFrom(record)

                expected_current = record.power_consumption / record.voltage
                actual_current = record.current
                efficiency = 1.0 - abs(expected_current - actual_current) / max(expected_current, 0.001)
                analyzed.efficiency_score = max(0, min(1.0, efficiency))
                efficiency_scores.append(analyzed.efficiency_score)

                deviation = abs(record.power_consumption - mean_consumption)
                if std_consumption > 0:
                    z_score = deviation / std_consumption
                    if z_score > request.anomaly_threshold:
                        analyzed.is_anomaly = True
                        analyzed.anomaly_reason = f"Power consumption deviation: z-score={z_score:.2f}"
                        anomaly_count += 1

                if record.voltage < 220 or record.voltage > 240:
                    analyzed.is_anomaly = True
                    analyzed.anomaly_reason = f"Voltage out of range: {record.voltage:.1f}V"
                    anomaly_count += 1

                response.analyzed_records.append(analyzed)

            response.total_records = len(request.records)
            response.anomalies_detected = anomaly_count
            response.average_efficiency = mean(efficiency_scores) if efficiency_scores else 0
            response.success = True
            response.message = f"Analyzed {response.total_records} records, found {response.anomalies_detected} anomalies"

            logger.info(response.message)
            return response

        except Exception as e:
            logger.error(f"Error analyzing data: {e}")
            response = data_analyzer_pb2.AnalyzeDataResponse()
            response.success = False
            response.message = f"Error: {str(e)}"
            return response


def serve():
    """Start the gRPC server"""
    port = os.getenv('GRPC_PORT', '50051')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_analyzer_pb2_grpc.add_DataAnalyzerServiceServicer_to_server(
        DataAnalyzerService(), server
    )
    server.add_insecure_port(f'[::]:{port}')

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        server.stop(0)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start()
    logger.info(f"Data Analyzer Service started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()