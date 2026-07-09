"""E2E test for file_based_energy_pipeline with migrated services."""

import base64
import json
import os
import sys
import threading
import time

import pytest
import redis
import uvicorn
from fastapi import FastAPI
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


# Inline service implementations for testing


class DataReference(BaseModel):
    protocol: str
    uri: str
    format: str


class ExecuteRequest(BaseModel):
    method: str
    workflow_id: str
    task_id: str
    inputs: list[dict] = []
    parameters: dict = {}


class ExecuteResponse(BaseModel):
    status: str
    output: DataReference | None = None
    error: str | None = None


def create_input_provider() -> FastAPI:
    """Create input_provider service."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> ExecuteResponse:
        if request.method != "GetConfiguration":
            return ExecuteResponse(status="failed", error=f"Unknown: {request.method}")

        config = {"num_records": 10, "output_format": "csv"}
        config_b64 = base64.b64encode(json.dumps(config).encode()).decode()

        return ExecuteResponse(
            status="complete",
            output=DataReference(protocol="inline", uri=config_b64, format="json"),
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


def create_data_generator() -> FastAPI:
    """Create data_generator service."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> ExecuteResponse:
        if request.method != "GenerateData":
            return ExecuteResponse(status="failed", error=f"Unknown: {request.method}")

        # Decode input
        config = {}
        for inp in request.inputs:
            if inp.get("protocol") == "inline":
                config = json.loads(base64.b64decode(inp["uri"]).decode())

        num_records = config.get("num_records", 10)

        # Simulate file generation (don't actually write)
        output_path = f"/tmp/raw_energy_{request.task_id}.csv"

        return ExecuteResponse(
            status="complete",
            output=DataReference(protocol="file", uri=output_path, format="csv"),
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


def create_data_analyzer() -> FastAPI:
    """Create data_analyzer service."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> ExecuteResponse:
        if request.method != "AnalyzeData":
            return ExecuteResponse(status="failed", error=f"Unknown: {request.method}")

        # Simulate analysis
        output_path = f"/tmp/analyzed_energy_{request.task_id}.csv"

        return ExecuteResponse(
            status="complete",
            output=DataReference(protocol="file", uri=output_path, format="csv"),
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


def create_report_generator() -> FastAPI:
    """Create report_generator service."""
    app = FastAPI()

    @app.post("/control/execute")
    def execute(request: ExecuteRequest) -> ExecuteResponse:
        if request.method != "GenerateReport":
            return ExecuteResponse(status="failed", error=f"Unknown: {request.method}")

        # Simulate report generation
        output_path = f"/tmp/energy_report_{request.task_id}.csv"

        return ExecuteResponse(
            status="complete",
            output=DataReference(protocol="file", uri=output_path, format="csv"),
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


class ServiceRunner:
    """Runs a service in background thread."""

    def __init__(self, app: FastAPI, host: str = "127.0.0.1", port: int = 8080):
        self.app = app
        self.host = host
        self.port = port
        self._server = None
        self._thread = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def start(self) -> None:
        config = uvicorn.Config(
            self.app, host=self.host, port=self.port, log_level="error"
        )
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
        raise RuntimeError(f"Service did not start on port {self.port}")

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
def services():
    """Start all pipeline services."""
    runners = {
        "input_provider1": ServiceRunner(create_input_provider(), port=19081),
        "data_generator1": ServiceRunner(create_data_generator(), port=19082),
        "data_analyzer1": ServiceRunner(create_data_analyzer(), port=19083),
        "report_generator1": ServiceRunner(create_report_generator(), port=19084),
    }

    for runner in runners.values():
        runner.start()

    yield runners

    for runner in runners.values():
        runner.stop()


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
    """Create orchestrator components."""
    state_store = RedisStateStore(redis_client)
    task_queue = RedisTaskQueue(redis_client)
    engine = WorkflowEngine(state_store, task_queue, redis_client)
    blueprint_parser = BlueprintParser()
    dockerinfo_parser = DockerInfoParser()

    return {
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


class TestEnergyPipelineE2E:
    """E2E tests for the energy pipeline."""

    def test_complete_pipeline(
        self, api_client, orchestrator_components, services, redis_client
    ):
        """Test full pipeline: input -> generate -> analyze -> report."""
        # Blueprint matching the file_based_energy_pipeline structure
        blueprint = {
            "name": "File Based Energy Pipeline",
            "pipeline_id": "test-pipeline",
            "creation_date": "2025-01-01",
            "type": "pipeline-topology/v2",
            "version": "2.0",
            "nodes": [
                {
                    "container_name": "input_provider1",
                    "proto_uri": "input_provider.proto",
                    "image": "input-provider:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "GetConfiguration",
                                "input_message_name": "Request",
                                "output_message_name": "Response",
                            },
                            "connected_to": [
                                {
                                    "container_name": "data_generator1",
                                    "operation_signature": {
                                        "operation_name": "GenerateData",
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "data_generator1",
                    "proto_uri": "data_generator.proto",
                    "image": "data-generator:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "GenerateData",
                                "input_message_name": "Request",
                                "output_message_name": "Response",
                            },
                            "connected_to": [
                                {
                                    "container_name": "data_analyzer1",
                                    "operation_signature": {
                                        "operation_name": "AnalyzeData",
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "data_analyzer1",
                    "proto_uri": "data_analyzer.proto",
                    "image": "data-analyzer:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "AnalyzeData",
                                "input_message_name": "Request",
                                "output_message_name": "Response",
                            },
                            "connected_to": [
                                {
                                    "container_name": "report_generator1",
                                    "operation_signature": {
                                        "operation_name": "GenerateReport",
                                    },
                                }
                            ],
                        }
                    ],
                },
                {
                    "container_name": "report_generator1",
                    "proto_uri": "report_generator.proto",
                    "image": "report-generator:latest",
                    "node_type": "MLModel",
                    "operation_signature_list": [
                        {
                            "operation_signature": {
                                "operation_name": "GenerateReport",
                                "input_message_name": "Request",
                                "output_message_name": "Response",
                            },
                            "connected_to": [],
                        }
                    ],
                },
            ],
        }

        # Dockerinfo pointing to test services
        dockerinfo = {
            "docker_info_list": [
                {
                    "container_name": "input_provider1",
                    "ip_address": "127.0.0.1",
                    "port": "19081",
                },
                {
                    "container_name": "data_generator1",
                    "ip_address": "127.0.0.1",
                    "port": "19082",
                },
                {
                    "container_name": "data_analyzer1",
                    "ip_address": "127.0.0.1",
                    "port": "19083",
                },
                {
                    "container_name": "report_generator1",
                    "ip_address": "127.0.0.1",
                    "port": "19084",
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

        # Parse endpoints for worker
        endpoints = orchestrator_components["dockerinfo_parser"].parse_json(dockerinfo)

        # Create and run worker
        engine = orchestrator_components["engine"]
        worker = Worker(engine, ControlClient(), endpoints)
        worker.run(workflow_id, timeout=1)

        # Verify workflow completed
        response = api_client.get(f"/workflows/{workflow_id}")
        assert response.status_code == 200
        workflow_data = response.json()

        # Debug: print task statuses
        response = api_client.get(f"/workflows/{workflow_id}/tasks")
        tasks = response.json()["tasks"]
        for task in tasks:
            print(f"Task {task['node_key']}: {task['status']}, error: {task.get('error')}")

        assert workflow_data["status"] == "completed", f"Workflow error: {workflow_data.get('error')}"

        # Verify all 4 tasks completed
        assert len(tasks) == 4
        for task in tasks:
            assert task["status"] == "completed", f"Task {task['node_key']} error: {task.get('error')}"
