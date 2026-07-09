#!/bin/bash

echo "Running services tester..."
echo "Note: Make sure the main pipeline is running first (./start.sh)"
echo ""

# Run the tester and remove container when done
docker compose -f docker-compose.services-tester.yml up --build

echo ""
echo "Tester completed. Check the logs above for results."