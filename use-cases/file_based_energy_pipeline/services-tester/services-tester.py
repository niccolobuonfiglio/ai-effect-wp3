#!/usr/bin/env python3
"""
Test client that calls actual gRPC services to test the complete pipeline
"""

import grpc
import os
import time

# Import generated protobuf files
import data_generator_pb2
import data_generator_pb2_grpc
import data_analyzer_pb2
import data_analyzer_pb2_grpc
import report_generator_pb2
import report_generator_pb2_grpc


def test_connectivity():
    """Test if services are reachable"""
    print("=== Testing Service Connectivity ===")
    services = [
        ("Data Generator", "data-generator:50051"),
        ("Data Analyzer", "data-analyzer:50052"), 
        ("Report Generator", "report-generator:50053")
    ]
    
    for name, address in services:
        try:
            print(f"Connecting to {name} at {address}...")
            with grpc.insecure_channel(address) as channel:
                # Test if channel is ready
                grpc.channel_ready_future(channel).result(timeout=5)
                print(f"SUCCESS: {name} is reachable")
        except Exception as e:
            print(f"FAILED: {name} failed: {e}")
            return False
    
    return True


def call_data_generator():
    """Call the Data Generator service"""
    print("\n1. Calling Data Generator service...")
    
    try:
        with grpc.insecure_channel('data-generator:50051') as channel:
            stub = data_generator_pb2_grpc.DataGeneratorServiceStub(channel)
            
            # Create request
            request = data_generator_pb2.GenerateDataRequest()
            request.num_records = 10
            request.output_format = "csv"
            
            print("   - Requesting synthetic energy data generation")
            print(f"   - Parameters: {request.num_records} records, {request.output_format} format")
            
            # Make the gRPC call
            response = stub.GenerateData(request)
            
            if response.success:
                print(f"   SUCCESS: {response.message}")
                print(f"   Generated {response.records_generated} records")
                return True
            else:
                print(f"   FAILED: {response.message}")
                return False
                
    except Exception as e:
        print(f"   FAILED: Data Generator service call failed: {e}")
        return False


def call_data_analyzer(input_file):
    """Call the Data Analyzer service"""
    print("\n2. Calling Data Analyzer service...")
    
    try:
        with grpc.insecure_channel('data-analyzer:50052') as channel:
            stub = data_analyzer_pb2_grpc.DataAnalyzerServiceStub(channel)
            
            # Create request  
            request = data_analyzer_pb2.AnalyzeDataRequest()
            request.input_file_path = input_file
            request.anomaly_threshold = 0.1
            
            print(f"   - Reading input: {request.input_file_path}")
            print(f"   - Anomaly threshold: {request.anomaly_threshold}")
            print("   - Analyzing for anomalies and calculating efficiency")
            
            # Make the gRPC call
            response = stub.AnalyzeData(request)
            
            if response.success:
                print(f"   SUCCESS: {response.message}")
                print(f"   Total records processed: {response.total_records}")
                print(f"   Anomalies detected: {response.anomalies_detected}")
                print(f"   Average efficiency: {response.average_efficiency:.3f}")
                return True
            else:
                print(f"   FAILED: {response.message}")
                return False
                
    except Exception as e:
        print(f"   FAILED: Data Analyzer service call failed: {e}")
        return False


def call_report_generator(input_file):
    """Call the Report Generator service"""
    print("\n3. Calling Report Generator service...")
    
    try:
        with grpc.insecure_channel('report-generator:50053') as channel:
            stub = report_generator_pb2_grpc.ReportGeneratorServiceStub(channel)
            
            # Create request
            request = report_generator_pb2.GenerateReportRequest()
            request.analyzed_data_path = input_file
            request.report_format = "csv"
            
            print(f"   - Reading analyzed data: {request.analyzed_data_path}")
            print(f"   - Report format: {request.report_format}")
            print("   - Generating summary report")
            
            # Make the gRPC call
            response = stub.GenerateReport(request)
            
            if response.success:
                print(f"   SUCCESS: {response.message}")
                if response.report_path:
                    print(f"   Report saved to: {response.report_path}")
                if response.report_summary:
                    print(f"   Summary: {response.report_summary}")
                return True
            else:
                print(f"   FAILED: {response.message}")
                return False
                
    except Exception as e:
        print(f"   FAILED: Report Generator service call failed: {e}")
        return False


def show_results():
    """Show the generated files"""
    print("\n=== Workflow Results ===")
    
    files_to_check = [
        "data/raw_energy.csv",
        "data/analyzed_energy.csv", 
        "data/energy_report.csv"
    ]
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"FILE: {file_path} ({size} bytes)")
            
            # Show first few lines of each file
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                    print(f"      Preview: {lines[0].strip()}")
                    if len(lines) > 1:
                        print(f"               {lines[1].strip()}")
            except Exception as e:
                print(f"      Error reading file: {e}")
        else:
            print(f"MISSING: {file_path} not found")


def main():
    print("AI-Effect Energy Processing Pipeline Test Client")
    print("=" * 50)
    
    # Test connectivity first
    if not test_connectivity():
        print("\nERROR: Some services are not reachable. Exiting.")
        return 1
    
    print("\nSUCCESS: All services are reachable!")
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Execute the actual workflow by calling gRPC services
    print("\n=== Executing Real Workflow via gRPC ===")
    
    # Step 1: Generate data
    if not call_data_generator():
        print("\nERROR: Data generation failed")
        return 1
    
    # Step 2: Analyze data (assumes generator created data/raw_energy.csv)
    if not call_data_analyzer("data/raw_energy.csv"):
        print("\nERROR: Data analysis failed")
        return 1
        
    # Step 3: Generate report (assumes analyzer created data/analyzed_energy.csv)
    if not call_report_generator("data/analyzed_energy.csv"):
        print("\nERROR: Report generation failed")
        return 1
    
    # Show results
    print("\nSUCCESS: Complete workflow executed successfully!")
    show_results()
    
    print("\n=== Summary ===")
    print("SUCCESS: Data Generator - Called gRPC service to generate synthetic energy data")
    print("SUCCESS: Data Analyzer - Called gRPC service to analyze data for anomalies") 
    print("SUCCESS: Report Generator - Called gRPC service to create summary report")
    print("\nReal workflow completed successfully! Check the data/ directory for outputs.")
    
    return 0


if __name__ == "__main__":
    exit(main())