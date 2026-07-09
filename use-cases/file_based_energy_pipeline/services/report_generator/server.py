import os
import logging
import grpc
import pandas as pd
import signal
import sys
from concurrent import futures
from pathlib import Path

# Import generated proto files
import report_generator_pb2
import report_generator_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ReportGeneratorService(report_generator_pb2_grpc.ReportGeneratorServiceServicer):
    """Generates summary reports from analyzed data"""
    
    def GenerateReport(self, request, context):
        """Generate summary report from analyzed data"""
        logger.info(f"GenerateReport called: input={request.output_file_path}, format={request.report_format}")

        try:
            # Read analyzed data
            input_path = Path(request.output_file_path)
            if not input_path.exists():
                raise FileNotFoundError(f"Input file not found: {request.output_file_path}")
            
            df = pd.read_csv(input_path)
            
            # Generate summary statistics
            total_records = len(df)
            anomaly_count = df['anomaly_detected'].sum()
            avg_efficiency = df['efficiency'].mean()
            max_power = df['power'].max()
            min_power = df['power'].min()
            
            # Create summary report
            report_data = {
                'metric': [
                    'Total Records',
                    'Anomalies Detected', 
                    'Average Efficiency',
                    'Max Power (W)',
                    'Min Power (W)',
                    'Anomaly Rate (%)'
                ],
                'value': [
                    total_records,
                    int(anomaly_count),
                    round(avg_efficiency, 3),
                    max_power,
                    min_power,
                    round((anomaly_count / total_records) * 100, 2)
                ]
            }
            
            # Save report
            output_path = Path("data/energy_report.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            report_df = pd.DataFrame(report_data)
            report_df.to_csv(output_path, index=False)
            
            message = f"Generated summary report with {len(report_data['metric'])} metrics, saved to {output_path}"
            summary = f"Processed {total_records} records, found {int(anomaly_count)} anomalies"
            
            logger.info(message)
            
            # Return protobuf response
            response = report_generator_pb2.GenerateReportResponse()
            response.success = True
            response.message = message
            response.report_path = str(output_path)
            response.report_summary = summary
            
            return response
            
        except Exception as e:
            error_msg = f"Failed to generate report: {str(e)}"
            logger.error(error_msg)
            
            # Return error response
            response = report_generator_pb2.GenerateReportResponse()
            response.success = False
            response.message = error_msg
            response.report_path = ""
            response.report_summary = ""
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return response


def serve(port: int = 50051):
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the service to the server
    report_generator_pb2_grpc.add_ReportGeneratorServiceServicer_to_server(
        ReportGeneratorService(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Report Generator service listening on port {port}")
    
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