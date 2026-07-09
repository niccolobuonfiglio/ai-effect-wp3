"""Integration tests for main entry point."""

import os
from unittest.mock import patch

import pytest
import redis
from fastapi.testclient import TestClient
from testcontainers.redis import RedisContainer

from main import create_app, get_redis_client


@pytest.fixture(scope="module")
def redis_container():
    """Start Redis container for tests."""
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture
def redis_url(redis_container):
    """Get Redis URL from container."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}"


@pytest.fixture
def redis_client(redis_url):
    """Create Redis client connected to container."""
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    yield client
    client.flushall()


class TestGetRedisClientIntegration:
    """Integration tests for get_redis_client."""

    def test_connects_to_redis(self, redis_url):
        """Should successfully connect to Redis."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            client = get_redis_client()
            client.ping()  # Should not raise

    def test_can_set_and_get(self, redis_url):
        """Should be able to set and get values."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            client = get_redis_client()
            client.set("test-key", "test-value")
            assert client.get("test-key") == "test-value"
            client.delete("test-key")


class TestCreateAppIntegration:
    """Integration tests for create_app."""

    def test_creates_working_app(self, redis_url):
        """Should create app that responds to health check."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            app = create_app()
            client = TestClient(app)

            response = client.get("/health")

            assert response.status_code == 200
            assert response.json()["status"] == "ok"

    def test_app_can_accept_workflow(self, redis_url):
        """Should accept workflow submission."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            app = create_app()
            client = TestClient(app)

            blueprint = {
                "name": "Test Pipeline",
                "pipeline_id": "test",
                "creation_date": "2025-01-01",
                "type": "pipeline-topology/v2",
                "version": "2.0",
                "nodes": [
                    {
                        "container_name": "service-a",
                        "proto_uri": "test.proto",
                        "image": "test:latest",
                        "node_type": "MLModel",
                        "operation_signature_list": [
                            {
                                "operation_signature": {
                                    "operation_name": "Process",
                                    "input_message_name": "Input",
                                    "output_message_name": "Output",
                                },
                                "connected_to": [],
                            }
                        ],
                    }
                ],
            }
            dockerinfo = {
                "docker_info_list": [
                    {
                        "container_name": "service-a",
                        "ip_address": "localhost",
                        "port": "8080",
                    }
                ]
            }

            response = client.post(
                "/workflows",
                json={"blueprint": blueprint, "dockerinfo": dockerinfo},
            )

            assert response.status_code == 200
            data = response.json()
            assert "workflow_id" in data
            assert data["status"] == "running"

    def test_workflow_status_retrievable(self, redis_url):
        """Should retrieve workflow status after submission."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            app = create_app()
            client = TestClient(app)

            blueprint = {
                "name": "Test Pipeline",
                "pipeline_id": "test",
                "creation_date": "2025-01-01",
                "type": "pipeline-topology/v2",
                "version": "2.0",
                "nodes": [
                    {
                        "container_name": "service-a",
                        "proto_uri": "test.proto",
                        "image": "test:latest",
                        "node_type": "MLModel",
                        "operation_signature_list": [
                            {
                                "operation_signature": {
                                    "operation_name": "Process",
                                    "input_message_name": "Input",
                                    "output_message_name": "Output",
                                },
                                "connected_to": [],
                            }
                        ],
                    }
                ],
            }
            dockerinfo = {
                "docker_info_list": [
                    {
                        "container_name": "service-a",
                        "ip_address": "localhost",
                        "port": "8080",
                    }
                ]
            }

            # Submit workflow
            submit_response = client.post(
                "/workflows",
                json={"blueprint": blueprint, "dockerinfo": dockerinfo},
            )
            workflow_id = submit_response.json()["workflow_id"]

            # Get status
            status_response = client.get(f"/workflows/{workflow_id}")

            assert status_response.status_code == 200
            assert status_response.json()["workflow_id"] == workflow_id
            assert status_response.json()["status"] == "running"
