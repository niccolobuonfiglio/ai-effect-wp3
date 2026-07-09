#!/bin/bash
set -e

echo "Building Docker images for protobuf_based_energy_pipeline..."

# Build all services using docker compose
docker compose build

echo ""
echo "Tagging images with :latest..."

# Tag images with :latest for export compatibility
docker tag protobuf_based_energy_pipeline-data_generator protobuf_based_energy_pipeline-data_generator:latest
docker tag protobuf_based_energy_pipeline-data_analyzer protobuf_based_energy_pipeline-data_analyzer:latest
docker tag protobuf_based_energy_pipeline-report_generator protobuf_based_energy_pipeline-report_generator:latest

echo ""
echo "Successfully built and tagged all images:"
echo "  - protobuf_based_energy_pipeline-data_generator:latest"
echo "  - protobuf_based_energy_pipeline-data_analyzer:latest"
echo "  - protobuf_based_energy_pipeline-report_generator:latest"
echo ""
echo "Images are ready for:"
echo "  1. Local development: docker compose up"
echo "  2. Platform export generation: python scripts/onboarding-export-generator.py"
