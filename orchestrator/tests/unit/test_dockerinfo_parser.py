"""Unit tests for DockerInfoParser."""

import json
import tempfile
from pathlib import Path

import pytest

from services.dockerinfo_parser import (
    DockerInfoParseError,
    DockerInfoParser,
    ServiceEndpoint,
)


def create_minimal_dockerinfo() -> dict:
    """Create minimal valid dockerinfo."""
    return {
        "docker_info_list": [
            {
                "container_name": "service-a",
                "ip_address": "service-a",
                "port": "50051",
            }
        ]
    }


def create_multi_service_dockerinfo() -> dict:
    """Create dockerinfo with multiple services."""
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


@pytest.fixture
def parser():
    return DockerInfoParser()


class TestServiceEndpoint:
    """Tests for ServiceEndpoint model."""

    def test_create_endpoint(self):
        """Create valid endpoint."""
        endpoint = ServiceEndpoint(address="localhost", port=50051)
        assert endpoint.address == "localhost"
        assert endpoint.port == 50051

    def test_endpoint_is_frozen(self):
        """Endpoint is immutable."""
        endpoint = ServiceEndpoint(address="localhost", port=50051)
        with pytest.raises(Exception):
            endpoint.address = "other"

    def test_empty_address_raises(self):
        """Empty address raises error."""
        with pytest.raises(ValueError, match="address is required"):
            ServiceEndpoint(address="", port=50051)

    def test_invalid_port_zero_raises(self):
        """Port 0 raises error."""
        with pytest.raises(ValueError, match="port must be between"):
            ServiceEndpoint(address="localhost", port=0)

    def test_invalid_port_negative_raises(self):
        """Negative port raises error."""
        with pytest.raises(ValueError, match="port must be between"):
            ServiceEndpoint(address="localhost", port=-1)

    def test_invalid_port_too_high_raises(self):
        """Port > 65535 raises error."""
        with pytest.raises(ValueError, match="port must be between"):
            ServiceEndpoint(address="localhost", port=65536)


class TestParseJson:
    """Tests for parse_json method."""

    def test_parse_minimal_dockerinfo(self, parser):
        """Parse minimal valid dockerinfo."""
        data = create_minimal_dockerinfo()
        endpoints = parser.parse_json(data)

        assert len(endpoints) == 1
        assert "service-a" in endpoints
        assert endpoints["service-a"].address == "service-a"
        assert endpoints["service-a"].port == 50051

    def test_parse_multiple_services(self, parser):
        """Parse dockerinfo with multiple services."""
        data = create_multi_service_dockerinfo()
        endpoints = parser.parse_json(data)

        assert len(endpoints) == 3
        assert "data_generator1" in endpoints
        assert "data_analyzer1" in endpoints
        assert "report_generator1" in endpoints

    def test_returns_service_endpoints(self, parser):
        """Returns dict of ServiceEndpoint objects."""
        data = create_minimal_dockerinfo()
        endpoints = parser.parse_json(data)

        endpoint = endpoints["service-a"]
        assert isinstance(endpoint, ServiceEndpoint)
        assert endpoint.address == "service-a"
        assert endpoint.port == 50051

    def test_port_converted_to_int(self, parser):
        """Port string is converted to integer."""
        data = create_minimal_dockerinfo()
        endpoints = parser.parse_json(data)

        assert isinstance(endpoints["service-a"].port, int)

    def test_none_data_raises(self, parser):
        """Parse None raises ValueError."""
        with pytest.raises(ValueError, match="data is required"):
            parser.parse_json(None)


class TestValidation:
    """Tests for validation errors."""

    def test_missing_docker_info_list_raises(self, parser):
        """Missing docker_info_list raises error."""
        data = {}

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_empty_docker_info_list_raises(self, parser):
        """Empty docker_info_list raises error."""
        data = {"docker_info_list": []}

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_missing_container_name_raises(self, parser):
        """Missing container_name raises error."""
        data = {
            "docker_info_list": [
                {
                    "ip_address": "service-a",
                    "port": "50051",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_missing_ip_address_raises(self, parser):
        """Missing ip_address raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "port": "50051",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_missing_port_raises(self, parser):
        """Missing port raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "service-a",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_empty_container_name_raises(self, parser):
        """Empty container_name raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "",
                    "ip_address": "service-a",
                    "port": "50051",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_empty_ip_address_raises(self, parser):
        """Empty ip_address raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "",
                    "port": "50051",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_empty_port_raises(self, parser):
        """Empty port raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "service-a",
                    "port": "",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid dockerinfo structure"):
            parser.parse_json(data)

    def test_invalid_port_non_numeric_raises(self, parser):
        """Non-numeric port raises error."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "service-a",
                    "port": "abc",
                }
            ]
        }

        with pytest.raises(DockerInfoParseError, match="Invalid port"):
            parser.parse_json(data)


class TestParseFile:
    """Tests for parse_file method."""

    def test_parse_file(self, parser):
        """Parse dockerinfo from file."""
        data = create_minimal_dockerinfo()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(data, f)
            temp_path = f.name

        try:
            endpoints = parser.parse_file(temp_path)
            assert len(endpoints) == 1
            assert "service-a" in endpoints
        finally:
            Path(temp_path).unlink()

    def test_parse_file_not_found_raises(self, parser):
        """Parse non-existent file raises error."""
        with pytest.raises(FileNotFoundError, match="DockerInfo file not found"):
            parser.parse_file("/nonexistent/path.json")

    def test_parse_file_empty_path_raises(self, parser):
        """Parse empty path raises error."""
        with pytest.raises(ValueError, match="path is required"):
            parser.parse_file("")

    def test_parse_invalid_json_raises(self, parser):
        """Parse invalid JSON raises error."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            f.write("not valid json {")
            temp_path = f.name

        try:
            with pytest.raises(DockerInfoParseError, match="Invalid JSON"):
                parser.parse_file(temp_path)
        finally:
            Path(temp_path).unlink()


class TestEndpointMapping:
    """Tests for endpoint mapping correctness."""

    def test_different_ports_per_service(self, parser):
        """Each service can have different port."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "host-a",
                    "port": "8080",
                },
                {
                    "container_name": "service-b",
                    "ip_address": "host-b",
                    "port": "9090",
                },
            ]
        }

        endpoints = parser.parse_json(data)

        assert endpoints["service-a"].port == 8080
        assert endpoints["service-b"].port == 9090

    def test_different_addresses_per_service(self, parser):
        """Each service can have different address."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "10.0.0.1",
                    "port": "50051",
                },
                {
                    "container_name": "service-b",
                    "ip_address": "10.0.0.2",
                    "port": "50051",
                },
            ]
        }

        endpoints = parser.parse_json(data)

        assert endpoints["service-a"].address == "10.0.0.1"
        assert endpoints["service-b"].address == "10.0.0.2"

    def test_duplicate_container_name_last_wins(self, parser):
        """Duplicate container names use last entry."""
        data = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "first",
                    "port": "8080",
                },
                {
                    "container_name": "service-a",
                    "ip_address": "second",
                    "port": "9090",
                },
            ]
        }

        endpoints = parser.parse_json(data)

        assert len(endpoints) == 1
        assert endpoints["service-a"].address == "second"
        assert endpoints["service-a"].port == 9090
