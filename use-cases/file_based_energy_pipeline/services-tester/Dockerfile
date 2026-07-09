FROM python:3.11-slim

WORKDIR /app

COPY services-tester/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services-tester/services-tester.py .

# Copy all proto files from the services using relative paths
COPY services/data_generator/proto ./proto/data_generator/
COPY services/data_analyzer/proto ./proto/data_analyzer/
COPY services/report_generator/proto ./proto/report_generator/

# Generate all protobuf files
RUN python -m grpc_tools.protoc \
    --python_out=. \
    --grpc_python_out=. \
    -I./proto/data_generator \
    proto/data_generator/data_generator.proto

RUN python -m grpc_tools.protoc \
    --python_out=. \
    --grpc_python_out=. \
    -I./proto/data_analyzer \
    proto/data_analyzer/data_analyzer.proto

RUN python -m grpc_tools.protoc \
    --python_out=. \
    --grpc_python_out=. \
    -I./proto/report_generator \
    proto/report_generator/report_generator.proto

CMD ["python", "services-tester.py"]