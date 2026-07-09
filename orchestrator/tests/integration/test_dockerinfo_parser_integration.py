"""Integration tests for DockerInfoParser."""

import json
import tempfile
from pathlib import Path

import pytest

from services.blueprint_parser import BlueprintParser
from services.dockerinfo_parser import DockerInfoParser, ServiceEndpoint


def create_test_dockerinfo() -> dict:
    """Create a test dockerinfo similar to real AI-Effect format."""
    return {
        "docker_info_list": [
            {
                "container_name": "data_generator1",
                "ip_address": "data_generator1",
                "port": "50051",
            },
            {
                "container_name": "data_analyzer1",
                "ip_address": "data_analyzer1",
                "port": "50052",
            },
            {
                "container_name": "report_generator1",
                "ip_address": "report_generator1",
                "port": "50053",
            },
        ]
    }


def create_test_blueprint() -> dict:
    """Create a test blueprint matching the dockerinfo."""
    return {
        "name": "Test Energy Pipeline",
        "pipeline_id": "test-pipeline-123",
        "creation_date": "2025-01-01 12:00:00",
        "type": "pipeline-topology/v2",
        "version": "2.0",
        "nodes": [
            {
                "container_name": "data_generator1",
                "proto_uri": "microservice/data_generator1.proto",
                "image": "energy-pipeline-data-generator:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "GenerateData",
                            "input_message_name": "GenerateDataRequest",
                            "output_message_name": "GenerateDataResponse",
                        },
                        "connected_to": [
                            {
                                "container_name": "data_analyzer1",
                                "operation_signature": {"operation_name": "AnalyzeData"},
                            }
                        ],
                    }
                ],
            },
            {
                "container_name": "data_analyzer1",
                "proto_uri": "microservice/data_analyzer1.proto",
                "image": "energy-pipeline-data-analyzer:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "AnalyzeData",
                            "input_message_name": "AnalyzeDataRequest",
                            "output_message_name": "AnalyzeDataResponse",
                        },
                        "connected_to": [
                            {
                                "container_name": "report_generator1",
                                "operation_signature": {
                                    "operation_name": "GenerateReport"
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "container_name": "report_generator1",
                "proto_uri": "microservice/report_generator1.proto",
                "image": "energy-pipeline-report-generator:latest",
                "node_type": "MLModel",
                "operation_signature_list": [
                    {
                        "operation_signature": {
                            "operation_name": "GenerateReport",
                            "input_message_name": "GenerateReportRequest",
                            "output_message_name": "GenerateReportResponse",
                        },
                        "connected_to": [],
                    }
                ],
            },
        ],
    }


@pytest.fixture
def dockerinfo_parser():
    return DockerInfoParser()


@pytest.fixture
def blueprint_parser():
    return BlueprintParser()


class TestParseRealDockerinfo:
    """Tests for parsing real dockerinfo files."""

    def test_parse_test_dockerinfo(self, dockerinfo_parser):
        """Parse test dockerinfo matching AI-Effect format."""
        data = create_test_dockerinfo()
        endpoints = dockerinfo_parser.parse_json(data)

        assert len(endpoints) == 3
        assert "data_generator1" in endpoints
        assert "data_analyzer1" in endpoints
        assert "report_generator1" in endpoints

        # Verify endpoint details
        gen = endpoints["data_generator1"]
        assert isinstance(gen, ServiceEndpoint)
        assert gen.address == "data_generator1"
        assert gen.port == 50051

    def test_parse_dockerinfo_from_file(self, dockerinfo_parser):
        """Parse dockerinfo from temporary file."""
        data = create_test_dockerinfo()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(data, f)
            temp_path = f.name

        try:
            endpoints = dockerinfo_parser.parse_file(temp_path)
            assert len(endpoints) == 3
            assert endpoints["data_analyzer1"].port == 50052
        finally:
            Path(temp_path).unlink()


class TestBlueprintDockerinfoAlignment:
    """Tests for verifying dockerinfo aligns with blueprint."""

    def test_endpoints_match_blueprint_containers(
        self, dockerinfo_parser, blueprint_parser
    ):
        """All blueprint containers have matching dockerinfo entries."""
        dockerinfo_data = create_test_dockerinfo()
        blueprint_data = create_test_blueprint()

        endpoints = dockerinfo_parser.parse_json(dockerinfo_data)
        graph = blueprint_parser.parse_json(blueprint_data)

        # Extract container names from blueprint nodes
        blueprint_containers = set()
        for node in graph.all_nodes.values():
            container_name = node.key.split(":")[0]
            blueprint_containers.add(container_name)

        # Verify all blueprint containers have endpoints
        for container_name in blueprint_containers:
            assert container_name in endpoints, (
                f"Container {container_name} missing from dockerinfo"
            )

    def test_can_build_service_addresses(
        self, dockerinfo_parser, blueprint_parser
    ):
        """Can construct service addresses from combined data."""
        dockerinfo_data = create_test_dockerinfo()
        blueprint_data = create_test_blueprint()

        endpoints = dockerinfo_parser.parse_json(dockerinfo_data)
        graph = blueprint_parser.parse_json(blueprint_data)

        # Build address map for each node
        addresses = {}
        for node_key, node in graph.all_nodes.items():
            container_name = node_key.split(":")[0]
            endpoint = endpoints[container_name]
            addresses[node_key] = f"{endpoint.address}:{endpoint.port}"

        assert addresses["data_generator1:GenerateData"] == "data_generator1:50051"
        assert addresses["data_analyzer1:AnalyzeData"] == "data_analyzer1:50052"
        assert addresses["report_generator1:GenerateReport"] == "report_generator1:50053"
