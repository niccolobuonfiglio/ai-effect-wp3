"""Integration tests for REST API."""

import time
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from testcontainers.redis import RedisContainer

from api.app import OrchestratorAPI
from services.blueprint_parser import BlueprintParser
from services.dockerinfo_parser import DockerInfoParser
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine


@pytest.fixture(scope="module")
def redis_container():
    """Start Redis container for tests."""
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture
def redis_client(redis_container):
    """Create Redis client."""
    import redis

    client = redis.Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=True,
    )
    yield client
    client.flushall()


@pytest.fixture
def state_store(redis_client):
    """Create state store."""
    return RedisStateStore(redis_client)


@pytest.fixture
def task_queue(redis_client):
    """Create task queue."""
    return RedisTaskQueue(redis_client)


@pytest.fixture
def engine(state_store, task_queue, redis_client):
    """Create workflow engine."""
    return WorkflowEngine(state_store, task_queue, redis_client)


@pytest.fixture
def blueprint_parser():
    """Create blueprint parser."""
    return BlueprintParser()


@pytest.fixture
def dockerinfo_parser():
    """Create dockerinfo parser."""
    return DockerInfoParser()


@pytest.fixture
def api(engine, blueprint_parser, dockerinfo_parser, redis_client):
    """Create API instance."""
    return OrchestratorAPI(engine, blueprint_parser, dockerinfo_parser, redis_client)


@pytest.fixture
def client(api):
    """Create test client."""
    app = api.create_app()
    return TestClient(app)


@pytest.fixture
def valid_blueprint():
    """Valid blueprint for testing."""
    return {
        "name": "Test Pipeline",
        "pipeline_id": "test-123",
        "creation_date": "2025-01-01",
        "type": "pipeline-topology/v2",
        "version": "2.0",
        "nodes": [
            {
                "container_name": "service-a",
                "proto_uri": "service-a.proto",
                "image": "service-a:latest",
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


@pytest.fixture
def valid_dockerinfo():
    """Valid dockerinfo for testing."""
    return {
        "docker_info_list": [
            {
                "container_name": "service-a",
                "ip_address": "localhost",
                "port": "50051",
            }
        ]
    }


class TestWorkflowLifecycle:
    """Integration tests for workflow lifecycle through API."""

    def test_submit_and_get_workflow(
        self, client, valid_blueprint, valid_dockerinfo
    ):
        """Submit workflow and get its status."""
        # Submit workflow
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 200
        data = response.json()
        workflow_id = data["workflow_id"]
        assert workflow_id.startswith("wf-")
        assert data["status"] == "running"

        # Get workflow status
        response = client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        status_data = response.json()
        assert status_data["workflow_id"] == workflow_id
        assert status_data["status"] == "running"

    def test_submit_and_get_tasks(
        self, client, valid_blueprint, valid_dockerinfo
    ):
        """Submit workflow and get its tasks."""
        # Submit workflow
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )
        workflow_id = response.json()["workflow_id"]

        # Get tasks
        response = client.get(f"/workflows/{workflow_id}/tasks")
        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == workflow_id
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["node_key"] == "service-a:Process"
        assert data["tasks"][0]["status"] == "pending"

    def test_submit_and_get_single_task(
        self, client, valid_blueprint, valid_dockerinfo
    ):
        """Submit workflow and get a single task."""
        # Submit workflow
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )
        workflow_id = response.json()["workflow_id"]

        # Get tasks to find task_id
        response = client.get(f"/workflows/{workflow_id}/tasks")
        task_id = response.json()["tasks"][0]["task_id"]

        # Get single task
        response = client.get(f"/workflows/{workflow_id}/tasks/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == task_id
        assert data["node_key"] == "service-a:Process"

    def test_submit_and_delete_workflow(
        self, client, valid_blueprint, valid_dockerinfo, redis_client
    ):
        """Submit workflow and delete it."""
        # Submit workflow
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )
        workflow_id = response.json()["workflow_id"]

        # Verify workflow exists
        response = client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200

        # Delete workflow
        response = client.delete(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "deleted"

        # Verify workflow no longer exists
        response = client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 404

    def test_multi_node_workflow(self, client, valid_dockerinfo):
        """Submit workflow with multiple nodes."""
        blueprint = {
            "name": "Multi-Node Pipeline",
            "pipeline_id": "multi-123",
            "creation_date": "2025-01-01",
            "type": "pipeline-topology/v2",
            "version": "2.0",
            "nodes": [
                {
                    "container_name": "service-a",
                    "proto_uri": "service-a.proto",
                    "image": "service-a:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "Process",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [
                                {
                                    "container_name": "service-b",
                                    "operation_signature": {
                                        "operation_name": "Analyze",
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "service-b",
                    "proto_uri": "service-b.proto",
                    "image": "service-b:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "Analyze",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [],
                        }
                    ],
                },
            ],
        }

        dockerinfo = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "localhost",
                    "port": "50051",
                },
                {
                    "container_name": "service-b",
                    "ip_address": "localhost",
                    "port": "50052",
                },
            ]
        }

        # Submit workflow
        response = client.post(
            "/workflows",
            json={"blueprint": blueprint, "dockerinfo": dockerinfo},
        )
        assert response.status_code == 200
        workflow_id = response.json()["workflow_id"]

        # Get tasks
        response = client.get(f"/workflows/{workflow_id}/tasks")
        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 2

        # Verify node keys
        node_keys = {task["node_key"] for task in data["tasks"]}
        assert "service-a:Process" in node_keys
        assert "service-b:Analyze" in node_keys


class TestErrorHandling:
    """Integration tests for error handling."""

    def test_invalid_blueprint_structure(self, client, valid_dockerinfo):
        """Invalid blueprint structure returns 400."""
        invalid_blueprint = {"invalid": "structure"}

        response = client.post(
            "/workflows",
            json={"blueprint": invalid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 400
        assert "Invalid blueprint" in response.json()["detail"]

    def test_invalid_dockerinfo_structure(self, client, valid_blueprint):
        """Invalid dockerinfo structure returns 400."""
        invalid_dockerinfo = {"invalid": "structure"}

        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": invalid_dockerinfo},
        )

        assert response.status_code == 400
        assert "Invalid dockerinfo" in response.json()["detail"]

    def test_get_nonexistent_workflow(self, client):
        """Getting nonexistent workflow returns 404."""
        response = client.get("/workflows/wf-nonexistent")
        assert response.status_code == 404

    def test_get_tasks_nonexistent_workflow(self, client):
        """Getting tasks for nonexistent workflow returns 404."""
        response = client.get("/workflows/wf-nonexistent/tasks")
        assert response.status_code == 404

    def test_get_nonexistent_task(self, client, valid_blueprint, valid_dockerinfo):
        """Getting nonexistent task returns 404."""
        # Create a workflow first
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )
        workflow_id = response.json()["workflow_id"]

        # Try to get nonexistent task
        response = client.get(f"/workflows/{workflow_id}/tasks/task-nonexistent")
        assert response.status_code == 404

    def test_delete_nonexistent_workflow(self, client):
        """Deleting nonexistent workflow returns 404."""
        response = client.delete("/workflows/wf-nonexistent")
        assert response.status_code == 404


class TestHealthCheck:
    """Integration tests for health check."""

    def test_health_check(self, client):
        """Health check returns ok."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
