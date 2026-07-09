"""End-to-end tests for complete workflow execution.

Tests the full flow: API -> WorkflowEngine -> Worker -> Service -> Completion
"""

import threading
import time

import pytest
import redis
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel
from testcontainers.redis import RedisContainer

from api.app import OrchestratorAPI
from services.blueprint_parser import BlueprintParser
from services.control_client import ControlClient
from services.dockerinfo_parser import DockerInfoParser
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.worker import Worker
from services.workflow_engine import WorkflowEngine


# Inline test service (sequential pattern)


class ExecuteRequest(BaseModel):
    method: str
    workflow_id: str
    task_id: str
    inputs: list[dict] = []
    parameters: dict = {}


class DataRef(BaseModel):
    protocol: str
    uri: str
    format: str


def create_test_service() -> FastAPI:
    """Create a simple test service implementing control interface."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> dict:
        if request.method == "ProcessData":
            return {
                "status": "complete",
                "output": {
                    "protocol": "s3",
                    "uri": f"s3://bucket/output/{request.task_id}.json",
                    "format": "json",
                },
            }
        raise HTTPException(status_code=400, detail=f"Unknown method: {request.method}")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app


class ServiceRunner:
    """Runs test service in background thread."""

    def __init__(self, host: str = "127.0.0.1", port: int = 19080):
        self.host = host
        self.port = port
        self._server = None
        self._thread = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        app = create_test_service()
        config = uvicorn.Config(app, host=self.host, port=self.port, log_level="error")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        self._wait_for_ready()

    def _wait_for_ready(self, timeout: float = 5.0) -> None:
        import httpx

        start = time.time()
        while time.time() - start < timeout:
            try:
                with httpx.Client() as client:
                    client.get(f"{self.base_url}/health")
                    return
            except httpx.ConnectError:
                time.sleep(0.1)
        raise RuntimeError("Test service did not start")

    def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=2.0)


@pytest.fixture(scope="module")
def redis_container():
    """Start Redis container."""
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture(scope="module")
def test_service():
    """Start test service."""
    service = ServiceRunner(port=19080)
    service.start()
    yield service
    service.stop()


@pytest.fixture
def redis_client(redis_container):
    """Create Redis client."""
    client = redis.Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=True,
    )
    yield client
    client.flushall()


@pytest.fixture
def orchestrator_components(redis_client):
    """Create all orchestrator components."""
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    engine = WorkflowEngine(state_store, task_queue, redis_client)
    blueprint_parser = BlueprintParser()
    dockerinfo_parser = DockerInfoParser()

    return {
        "state_store": state_store,
        "task_queue": task_queue,
        "engine": engine,
        "blueprint_parser": blueprint_parser,
        "dockerinfo_parser": dockerinfo_parser,
        "redis_client": redis_client,
    }


@pytest.fixture
def api_client(orchestrator_components, redis_client):
    """Create API test client."""
    api = OrchestratorAPI(
        orchestrator_components["engine"],
        orchestrator_components["blueprint_parser"],
        orchestrator_components["dockerinfo_parser"],
        redis_client,
    )
    app = api.create_app()
    return TestClient(app)


class TestSingleNodeWorkflow:
    """E2E test for single node workflow."""

    def test_complete_workflow(
        self, api_client, orchestrator_components, test_service, redis_client
    ):
        """Submit workflow, run worker, verify completion."""
        # Blueprint with single node
        blueprint = {
            "name": "E2E Test Pipeline",
            "pipeline_id": "e2e-test",
            "creation_date": "2025-01-01",
            "type": "pipeline-topology/v2",
            "version": "2.0",
            "nodes": [
                {
                    "container_name": "test-service",
                    "proto_uri": "test.proto",
                    "image": "test:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "ProcessData",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [],
                        }
                    ],
                }
            ],
        }

        # Dockerinfo pointing to test service
        dockerinfo = {
            "docker_info_list": [
                {
                    "container_name": "test-service",
                    "ip_address": "127.0.0.1",
                    "port": "19080",
                }
            ]
        }

        # Submit workflow via API
        response = api_client.post(
            "/workflows",
            json={"blueprint": blueprint, "dockerinfo": dockerinfo},
        )
        assert response.status_code == 200
        workflow_id = response.json()["workflow_id"]

        # Parse endpoints for worker
        dockerinfo_parser = orchestrator_components["dockerinfo_parser"]
        endpoints = dockerinfo_parser.parse_json(dockerinfo)

        # Create worker
        engine = orchestrator_components["engine"]
        client = ControlClient()
        worker = Worker(engine, client, endpoints)

        # Process task
        processed = worker.process_task(workflow_id, timeout=1)
        assert processed is True

        # Verify workflow completed
        response = api_client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "completed"

        # Verify task completed
        response = api_client.get(f"/workflows/{workflow_id}/tasks")
        assert response.status_code == 200
        tasks = response.json()["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["status"] == "completed"


class TestMultiNodeWorkflow:
    """E2E test for multi-node workflow."""

    def test_chained_workflow(
        self, api_client, orchestrator_components, test_service, redis_client
    ):
        """Submit workflow with two chained nodes, verify both complete."""
        blueprint = {
            "name": "Chained Pipeline",
            "pipeline_id": "chain-test",
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
                                "operation_name": "ProcessData",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [
                                {
                                    "container_name": "service-b",
                                    "operation_signature": {
                                        "operation_name": "ProcessData",
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "service-b",
                    "proto_uri": "test.proto",
                    "image": "test:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "ProcessData",
                                "input_message_name": "Input",
                                "output_message_name": "Output",
                            },
                            "connected_to": [],
                        }
                    ],
                },
            ],
        }

        # Both services point to same test service
        dockerinfo = {
            "docker_info_list": [
                {
                    "container_name": "service-a",
                    "ip_address": "127.0.0.1",
                    "port": "19080",
                },
                {
                    "container_name": "service-b",
                    "ip_address": "127.0.0.1",
                    "port": "19080",
                },
            ]
        }

        # Submit workflow
        response = api_client.post(
            "/workflows",
            json={"blueprint": blueprint, "dockerinfo": dockerinfo},
        )
        assert response.status_code == 200
        workflow_id = response.json()["workflow_id"]

        # Create worker
        endpoints = orchestrator_components["dockerinfo_parser"].parse_json(dockerinfo)
        engine = orchestrator_components["engine"]
        worker = Worker(engine, ControlClient(), endpoints)

        # Run worker until complete
        worker.run(workflow_id, timeout=1)

        # Verify workflow completed
        response = api_client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "completed"


class TestWorkflowFailure:
    """E2E test for workflow failure handling."""

    def test_unknown_method_fails_workflow(
        self, api_client, orchestrator_components, test_service, redis_client
    ):
        """Workflow with unknown method should fail."""
        blueprint = {
            "name": "Failing Pipeline",
            "pipeline_id": "fail-test",
            "creation_date": "2025-01-01",
            "type": "pipeline-topology/v2",
            "version": "2.0",
            "nodes": [
                {
                    "container_name": "test-service",
                    "proto_uri": "test.proto",
                    "image": "test:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "UnknownMethod",
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
                    "container_name": "test-service",
                    "ip_address": "127.0.0.1",
                    "port": "19080",
                }
            ]
        }

        # Submit workflow
        response = api_client.post(
            "/workflows",
            json={"blueprint": blueprint, "dockerinfo": dockerinfo},
        )
        assert response.status_code == 200
        workflow_id = response.json()["workflow_id"]

        # Create worker and process
        endpoints = orchestrator_components["dockerinfo_parser"].parse_json(dockerinfo)
        engine = orchestrator_components["engine"]
        worker = Worker(engine, ControlClient(), endpoints)
        worker.process_task(workflow_id, timeout=1)

        # Verify workflow failed
        response = api_client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "failed"
