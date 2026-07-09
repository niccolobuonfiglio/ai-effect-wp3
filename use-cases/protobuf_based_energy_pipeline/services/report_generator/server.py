import os
import logging
import grpc
import signal
import sys
from concurrent import futures
from collections import defaultdict

import common_pb2
import report_generator_pb2
import report_generator_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ReportGeneratorService(report_generator_pb2_grpc.ReportGeneratorServiceServicer):
    """Generates reports from analyzed energy data received as protobuf messages"""

    def GenerateReport(self, request, context):
        """Generate report from analyzed energy data"""
        logger.info(f"GenerateReport called: {request.total_records} records, {request.anomalies_detected} anomalies")

        try:
            response = report_generator_pb2.GenerateReportResponse()

            summary_section = report_generator_pb2.ReportSection()
            summary_section.title = "Executive Summary"
            summary_section.content.extend([
                f"Total Records Analyzed: {request.total_records}",
                f"Anomalies Detected: {request.anomalies_detected}",
                f"Average Efficiency Score: {request.average_efficiency:.2%}",
                f"Anomaly Rate: {(request.anomalies_detected/max(request.total_records, 1)):.2%}"
            ])
            response.sections.append(summary_section)

            household_stats = defaultdict(lambda: {
                'count': 0,
                'total_consumption': 0,
                'anomalies': 0,
                'efficiency_sum': 0
            })

            anomaly_types = defaultdict(int)
            efficiency_buckets = {'high': 0, 'medium': 0, 'low': 0}

            for record in request.analyzed_records:
                household_id = record.original.household_id
                stats = household_stats[household_id]
                stats['count'] += 1
                stats['total_consumption'] += record.original.power_consumption
                stats['efficiency_sum'] += record.efficiency_score

                if record.is_anomaly:
                    stats['anomalies'] += 1
                    if "Voltage" in record.anomaly_reason:
                        anomaly_types["Voltage Issues"] += 1
                    elif "Power consumption" in record.anomaly_reason:
                        anomaly_types["Consumption Deviation"] += 1
                    else:
                        anomaly_types["Other"] += 1

                if record.efficiency_score >= 0.9:
                    efficiency_buckets['high'] += 1
                elif record.efficiency_score >= 0.7:
                    efficiency_buckets['medium'] += 1
                else:
                    efficiency_buckets['low'] += 1

            household_section = report_generator_pb2.ReportSection()
            household_section.title = "Household Analysis"
            for household_id, stats in household_stats.items():
                avg_consumption = stats['total_consumption'] / max(stats['count'], 1)
                avg_efficiency = stats['efficiency_sum'] / max(stats['count'], 1)
                household_section.content.append(
                    f"{household_id}: {stats['count']} records, "
                    f"Avg Consumption: {avg_consumption:.1f}W, "
                    f"Anomalies: {stats['anomalies']}, "
                    f"Avg Efficiency: {avg_efficiency:.2%}"
                )
            response.sections.append(household_section)

            if anomaly_types:
                anomaly_section = report_generator_pb2.ReportSection()
                anomaly_section.title = "Anomaly Breakdown"
                for anomaly_type, count in anomaly_types.items():
                    percentage = (count / max(request.anomalies_detected, 1)) * 100
                    anomaly_section.content.append(f"{anomaly_type}: {count} ({percentage:.1f}%)")
                response.sections.append(anomaly_section)

            efficiency_section = report_generator_pb2.ReportSection()
            efficiency_section.title = "Efficiency Distribution"
            total_analyzed = max(request.total_records, 1)
            efficiency_section.content.extend([
                f"High Efficiency (≥90%): {efficiency_buckets['high']} ({efficiency_buckets['high']/total_analyzed:.1%})",
                f"Medium Efficiency (70-90%): {efficiency_buckets['medium']} ({efficiency_buckets['medium']/total_analyzed:.1%})",
                f"Low Efficiency (<70%): {efficiency_buckets['low']} ({efficiency_buckets['low']/total_analyzed:.1%})"
            ])
            response.sections.append(efficiency_section)

            recommendations_section = report_generator_pb2.ReportSection()
            recommendations_section.title = "Recommendations"
            recommendations = []

            if request.anomalies_detected > request.total_records * 0.1:
                recommendations.append("High anomaly rate detected. Investigate system stability.")

            if request.average_efficiency < 0.8:
                recommendations.append("Overall efficiency below 80%. Consider maintenance or upgrades.")

            if anomaly_types.get("Voltage Issues", 0) > 0:
                recommendations.append("Voltage irregularities detected. Check power supply stability.")

            if not recommendations:
                recommendations.append("System operating within normal parameters.")

            recommendations_section.content.extend(recommendations)
            response.sections.append(recommendations_section)

            response.summary = (
                f"Report generated successfully for {request.total_records} records. "
                f"Key findings: {request.anomalies_detected} anomalies ({(request.anomalies_detected/max(request.total_records, 1)):.1%}), "
                f"average efficiency {request.average_efficiency:.1%}."
            )

            response.success = True
            response.message = "Report generated successfully"

            logger.info(f"Report generated with {len(response.sections)} sections")
            return response

        except Exception as e:
            logger.error(f"Error generating report: {e}")
            response = report_generator_pb2.GenerateReportResponse()
            response.success = False
            response.message = f"Error: {str(e)}"
            return response


def serve():
    """Start the gRPC server"""
    port = os.getenv('GRPC_PORT', '50051')
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    report_generator_pb2_grpc.add_ReportGeneratorServiceServicer_to_server(
        ReportGeneratorService(), server
    )
    server.add_insecure_port(f'[::]:{port}')

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        server.stop(0)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start()
    logger.info(f"Report Generator Service started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()