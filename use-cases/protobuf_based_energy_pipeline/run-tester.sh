#!/bin/bash

echo "Starting Protobuf-Based Energy Pipeline Test..."
echo "================================================"

# Build and run all services with the tester
docker compose -f docker-compose.services-tester.yml up --build --abort-on-container-exit

# Check the exit code of the tester container
EXIT_CODE=$(docker inspect protobuf_pipeline_tester --format='{{.State.ExitCode}}')

# Clean up
docker compose -f docker-compose.services-tester.yml down

if [ "$EXIT_CODE" = "0" ]; then
    echo ""
    echo "Test completed successfully!"
    exit 0
else
    echo ""
    echo "Test failed with exit code: $EXIT_CODE"
    exit 1
fi