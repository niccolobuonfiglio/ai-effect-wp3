#!/usr/bin/env python3

import grpc
import sys
import time
import logging

# Import generated proto files (they're in the same directory in the container)
import common_pb2
import data_generator_pb2
import data_generator_pb2_grpc
import data_analyzer_pb2
import data_analyzer_pb2_grpc
import report_generator_pb2
import report_generator_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_pipeline():
    """Test the complete protobuf-based energy pipeline"""

    # Service endpoints
    generator_host = 'data_generator:50051'
    analyzer_host = 'data_analyzer:50051'
    report_host = 'report_generator:50051'

    try:
        # Step 1: Generate energy data
        logger.info("=" * 60)
        logger.info("STEP 1: Generating energy data...")
        logger.info("=" * 60)

        with grpc.insecure_channel(generator_host) as channel:
            stub = data_generator_pb2_grpc.DataGeneratorServiceStub(channel)

            request = data_generator_pb2.GenerateDataRequest(num_records=20)
            response = stub.GenerateData(request)

            if response.success:
                logger.info(f"Data generation successful: {response.message}")
                logger.info(f"   Generated {len(response.records)} records")

                # Display sample records
                if response.records:
                    logger.info("   Sample records:")
                    for i, record in enumerate(response.records[:3]):
                        logger.info(f"     Record {i+1}: Household={record.household_id}, "
                                  f"Power={record.power_consumption:.1f}W, "
                                  f"Voltage={record.voltage:.1f}V")

                energy_records = response.records
            else:
                logger.error(f"Data generation failed: {response.message}")
                return False

        time.sleep(1)

        # Step 2: Analyze the generated data
        logger.info("=" * 60)
        logger.info("STEP 2: Analyzing energy data...")
        logger.info("=" * 60)

        with grpc.insecure_channel(analyzer_host) as channel:
            stub = data_analyzer_pb2_grpc.DataAnalyzerServiceStub(channel)

            # Create analyzer request with the generated records
            request = data_analyzer_pb2.AnalyzeDataRequest()
            request.records.extend(energy_records)
            request.anomaly_threshold = 2.0

            response = stub.AnalyzeData(request)

            if response.success:
                logger.info(f"Analysis successful: {response.message}")
                logger.info(f"   Total records: {response.total_records}")
                logger.info(f"   Anomalies detected: {response.anomalies_detected}")
                logger.info(f"   Average efficiency: {response.average_efficiency:.2%}")

                # Display anomalies if any
                anomalies = [r for r in response.analyzed_records if r.is_anomaly]
                if anomalies:
                    logger.info("   Detected anomalies:")
                    for i, anomaly in enumerate(anomalies[:3]):
                        logger.info(f"     {anomaly.original.household_id}: {anomaly.anomaly_reason}")

                analyzed_records = response.analyzed_records
                total_records = response.total_records
                anomalies_detected = response.anomalies_detected
                average_efficiency = response.average_efficiency
            else:
                logger.error(f"Analysis failed: {response.message}")
                return False

        time.sleep(1)

        # Step 3: Generate report from analyzed data
        logger.info("=" * 60)
        logger.info("STEP 3: Generating report...")
        logger.info("=" * 60)

        with grpc.insecure_channel(report_host) as channel:
            stub = report_generator_pb2_grpc.ReportGeneratorServiceStub(channel)

            # Create report request with analyzed records
            request = report_generator_pb2.GenerateReportRequest()
            request.analyzed_records.extend(analyzed_records)

            request.total_records = total_records
            request.anomalies_detected = anomalies_detected
            request.average_efficiency = average_efficiency

            response = stub.GenerateReport(request)

            if response.success:
                logger.info(f"Report generation successful: {response.message}")
                logger.info(f"   Summary: {response.summary}")

                # Display report sections
                logger.info("\n" + "=" * 60)
                logger.info("GENERATED REPORT")
                logger.info("=" * 60)

                for section in response.sections:
                    logger.info(f"\n{section.title}")
                    logger.info("-" * len(section.title))
                    for content in section.content:
                        logger.info(f"   - {content}")
            else:
                logger.error(f"Report generation failed: {response.message}")
                return False

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE TEST COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info("The protobuf-based pipeline successfully:")
        logger.info("1. Generated energy data as protobuf messages")
        logger.info("2. Analyzed the data directly from protobuf (no files!)")
        logger.info("3. Generated a comprehensive report from protobuf data")
        logger.info("=" * 60)

        return True

    except grpc.RpcError as e:
        logger.error(f"gRPC error: {e.code()}: {e.details()}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


if __name__ == "__main__":
    # Wait for services to be ready
    logger.info("Waiting for services to start...")
    time.sleep(5)

    # Run the test
    success = test_pipeline()

    # Exit with appropriate code
    sys.exit(0 if success else 1)