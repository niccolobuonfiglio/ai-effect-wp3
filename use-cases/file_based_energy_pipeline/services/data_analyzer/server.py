import os
import logging
import grpc
import pandas as pd
import signal
import sys
from concurrent import futures
from pathlib import Path

# Import generated proto files
import data_analyzer_pb2
import data_analyzer_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataAnalyzerService(data_analyzer_pb2_grpc.DataAnalyzerServiceServicer):
    """Analyzes energy data and detects anomalies"""
    
    def AnalyzeData(self, request, context):
        """Analyze energy data for anomalies and efficiency"""
        logger.info(f"AnalyzeData called: input={request.output_file_path}, threshold={request.anomaly_threshold}")

        try:
            # Read input CSV file
            input_path = Path(request.output_file_path)
            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {request.output_file_path}")
            
            df = pd.read_csv(input_path)
            
            # Perform analysis
            analyzed_data = []
            anomaly_count = 0
            total_efficiency = 0
            
            for _, row in df.iterrows():
                power = float(row['power_consumption'])
                voltage = float(row['voltage'])
                current = float(row['current'])
                
                # Calculate efficiency
                efficiency = power / (voltage * current) if (voltage * current) != 0 else 0
                total_efficiency += efficiency
                
                # Detect anomalies
                anomaly = efficiency < request.anomaly_threshold or efficiency > 0.95
                if anomaly:
                    anomaly_count += 1
                
                status = "anomaly" if anomaly else "normal"
                
                analyzed_data.append({
                    'timestamp': row['timestamp'],
                    'household_id': row['household_id'],
                    'power': power,
                    'efficiency': efficiency,
                    'status': status,
                    'anomaly_detected': anomaly
                })
            
            # Save analyzed data
            output_path = Path("data/analyzed_energy.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            analyzed_df = pd.DataFrame(analyzed_data)
            analyzed_df.to_csv(output_path, index=False)
            
            avg_efficiency = total_efficiency / len(analyzed_data) if analyzed_data else 0
            
            message = f"Analyzed {len(analyzed_data)} records, saved to {output_path}"
            logger.info(message)
            
            # Return protobuf response
            response = data_analyzer_pb2.AnalyzeDataResponse()
            response.success = True
            response.message = message
            response.total_records = len(analyzed_data)
            response.anomalies_detected = anomaly_count
            response.average_efficiency = avg_efficiency
            response.output_file_path = str(output_path)

            return response
            
        except Exception as e:
            error_msg = f"Failed to analyze data: {str(e)}"
            logger.error(error_msg)
            
            # Return error response
            response = data_analyzer_pb2.AnalyzeDataResponse()
            response.success = False
            response.message = error_msg
            response.total_records = 0
            response.anomalies_detected = 0
            response.average_efficiency = 0.0
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return response


def serve(port: int = 50051):
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the service to the server
    data_analyzer_pb2_grpc.add_DataAnalyzerServiceServicer_to_server(
        DataAnalyzerService(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Data Analyzer service listening on port {port}")
    
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