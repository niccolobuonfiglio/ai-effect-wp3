"""Integration tests for worker CLI."""

import json
import os
from unittest.mock import patch

import pytest
import redis
from testcontainers.redis import RedisContainer

from services.dockerinfo_parser import ServiceEndpoint
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine
from worker_cli import get_redis_client, load_endpoints


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


class TestLoadEndpointsIntegration:
    """Integration tests for load_endpoints."""

    def test_loads_endpoints_from_redis(self, redis_url, redis_client):
        """Should load endpoints stored by API."""
        # Store endpoints like API does
        workflow_id = "wf-test-123"
        endpoints_key = f"endpoints:{workflow_id}"

        endpoint_a = ServiceEndpoint(
            container_name="service-a",
            address="localhost",
            port=8080,
        )
        endpoint_b = ServiceEndpoint(
            container_name="service-b",
            address="localhost",
            port=8081,
        )

        redis_client.hset(
            endpoints_key,
            mapping={
                "service-a": endpoint_a.model_dump_json(),
                "service-b": endpoint_b.model_dump_json(),
            },
        )

        # Load using worker function
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            worker_redis = get_redis_client()
            endpoints = load_endpoints(worker_redis, workflow_id)

        assert len(endpoints) == 2
        assert endpoints["service-a"].address == "localhost"
        assert endpoints["service-a"].port == 8080
        assert endpoints["service-b"].address == "localhost"
        assert endpoints["service-b"].port == 8081

    def test_raises_when_workflow_not_found(self, redis_url):
        """Should raise when workflow has no endpoints."""
        with patch.dict(os.environ, {"REDIS_URL": redis_url}):
            client = get_redis_client()

            with pytest.raises(ValueError, match="No endpoints found"):
                load_endpoints(client, "wf-nonexistent")


class TestWorkerWithEngineIntegration:
    """Integration tests for worker with real engine."""

    def test_worker_processes_queued_task(self, redis_url, redis_client):
        """Should process task that was enqueued by engine."""
        from unittest.mock import MagicMock

        from models.data_reference import DataReference, Format, Protocol
        from services.blueprint_parser import BlueprintParser
        from services.control_client import ControlClient, ExecuteResponse
        from services.worker import Worker

        # Setup engine
        state_store = RedisStateStore(redis_client)
        task_queue = RedisTaskQueue(redis_client)
        engine = WorkflowEngine(state_store, task_queue, redis_client)

        # Create simple graph using BlueprintParser
        blueprint = {
            "name": "Test",
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
        parser = BlueprintParser()
        graph = parser.parse_json(blueprint)

        # Initialize and start workflow
        workflow_id = "wf-worker-test"
        engine.initialize_workflow(workflow_id, graph)
        engine.start_workflow(workflow_id)

        # Store endpoints
        endpoint = ServiceEndpoint(
            container_name="service-a",
            address="localhost",
            port=8080,
        )
        redis_client.hset(
            f"endpoints:{workflow_id}",
            mapping={"service-a": endpoint.model_dump_json()},
        )

        # Mock control client
        mock_client = MagicMock(spec=ControlClient)
        mock_client.execute.return_value = ExecuteResponse(
            status="complete",
            output=DataReference(
                protocol=Protocol.S3,
                uri="s3://bucket/output.json",
                format=Format.JSON,
            ),
        )

        # Create worker with mock client
        endpoints = {"service-a": endpoint}
        worker = Worker(engine, mock_client, endpoints)

        # Process task
        processed = worker.process_task(workflow_id, timeout=1)

        assert processed is True
        mock_client.execute.assert_called_once()

        # Verify workflow completed
        assert engine.is_workflow_complete(workflow_id) is True
        status = engine.get_workflow_status(workflow_id)
        assert status.status.value == "completed"
