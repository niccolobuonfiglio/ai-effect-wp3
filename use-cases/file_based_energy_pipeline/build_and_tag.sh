#!/bin/bash
set -e

echo "Building Docker images for file_based_energy_pipeline..."

# Build all services using docker compose
docker compose build

echo ""
echo "Tagging images with :latest..."

# Tag images with :latest for export compatibility
docker tag file_based_energy_pipeline-input-provider file_based_energy_pipeline-input-provider:latest
docker tag file_based_energy_pipeline-data-generator file_based_energy_pipeline-data-generator:latest
docker tag file_based_energy_pipeline-data-analyzer file_based_energy_pipeline-data-analyzer:latest
docker tag file_based_energy_pipeline-report-generator file_based_energy_pipeline-report-generator:latest

echo ""
echo "Successfully built and tagged all images:"
echo "  - file_based_energy_pipeline-input-provider:latest"
echo "  - file_based_energy_pipeline-data-generator:latest"
echo "  - file_based_energy_pipeline-data-analyzer:latest"
echo "  - file_based_energy_pipeline-report-generator:latest"
echo ""
echo "Images are ready for:"
echo "  1. Local development: docker compose up"
echo "  2. Platform export generation: python scripts/onboarding-export-generator.py"
