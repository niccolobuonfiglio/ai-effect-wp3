FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY services-tester/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared proto and all service proto files
COPY shared/proto/common.proto .
COPY services/data_generator/proto/data_generator.proto .
COPY services/data_analyzer/proto/data_analyzer.proto .
COPY services/report_generator/proto/report_generator.proto .

# Compile all proto files in the same directory
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. common.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. data_generator.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. data_analyzer.proto
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. report_generator.proto

# Copy test script
COPY services-tester/test_pipeline.py .

CMD ["python", "test_pipeline.py"]