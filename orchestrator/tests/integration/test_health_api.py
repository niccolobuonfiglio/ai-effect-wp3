"""Integration tests for the service health API.
"""

import os

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.health_routes import create_health_router
from services.circuit_breaker import CircuitBreaker
from services.dockerinfo_parser import ServiceEndpoint
from services.service_registry import ServiceRegistry

BASE = "/orchestrator/health/services"

DATABROKER = ServiceEndpoint(address="databroker", port=8080)
PREDICTOR = ServiceEndpoint(address="predictor", port=8080)


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def breaker(redis_client):
    return CircuitBreaker(redis_client, failure_threshold=3, reset_timeout=60.0)


@pytest.fixture
def registry(redis_client, breaker):
    return ServiceRegistry(redis_client, breaker)


@pytest.fixture
def client(registry):
    app = FastAPI()
    app.include_router(create_health_router(registry))
    return TestClient(app)


def kill(breaker, name, base_url, error="connection refused", times=3):
    """Drive a service past the failure threshold."""
    for _ in range(times):
        breaker.record_failure(name, base_url, error)


class TestRouterConstruction:
    def test_requires_registry(self):
        with pytest.raises(ValueError, match="registry is required"):
            create_health_router(None)

    def test_routes_are_mounted_under_prefix(self, registry):
        app = FastAPI()
        app.include_router(create_health_router(registry))
        paths = set(TestClient(app).get("/openapi.json").json()["paths"])

        assert BASE in paths
        assert f"{BASE}/{{service_name}}" in paths
        assert f"{BASE}/{{service_name}}/recover" in paths


class TestListEndpoint:
    def test_empty_registry(self, client):
        response = client.get(BASE)

        assert response.status_code == 200
        body = response.json()
        assert body["overall_status"] == "HEALTHY"
        assert body["total"] == 0
        assert body["services"] == []

    def test_lists_registered_services(self, client, registry):
        registry.register_all(
            {"databroker": DATABROKER, "predictor": PREDICTOR}, workflow_id="wf-1"
        )
        body = client.get(BASE).json()

        assert body["total"] == 2
        assert body["healthy"] == 2
        assert [s["service_name"] for s in body["services"]] == [
            "databroker",
            "predictor",
        ]

    def test_service_payload_shape(self, client, registry):
        registry.register("databroker", DATABROKER)
        service = client.get(BASE).json()["services"][0]

        assert set(service) == {
            "service_name",
            "base_url",
            "status",
            "circuit_state",
            "failure_count",
            "last_failure",
            "last_error",
            "last_success",
            "opened_at",
            "retry_after",
            "manually_recovered_at",
        }
        assert service["base_url"] == "http://databroker:8080"

    def test_degraded_service_reported(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        breaker.record_failure("databroker", "http://databroker:8080", "flaky")

        body = client.get(BASE).json()
        assert body["overall_status"] == "DEGRADED"
        assert body["degraded"] == 1
        assert body["services"][0]["failure_count"] == 1

    def test_down_service_reported_with_details(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080", error="upstream timeout")

        body = client.get(BASE).json()
        service = body["services"][0]

        assert body["overall_status"] == "UNHEALTHY"
        assert body["unhealthy"] == 1
        assert service["status"] == "UNHEALTHY"
        assert service["circuit_state"] == "open"
        assert service["last_error"] == "upstream timeout"
        assert service["last_failure"] is not None
        assert service["opened_at"] is not None
        assert service["retry_after"] is not None

    def test_mixed_fleet_counts(self, client, registry, breaker):
        registry.register_all(
            {
                "ok": ServiceEndpoint(address="ok", port=8080),
                "warn": ServiceEndpoint(address="warn", port=8080),
                "bad": ServiceEndpoint(address="bad", port=8080),
            }
        )
        breaker.record_failure("warn", "http://warn:8080", "flaky")
        kill(breaker, "bad", "http://bad:8080")

        body = client.get(BASE).json()
        assert (body["healthy"], body["degraded"], body["unhealthy"]) == (1, 1, 1)
        assert body["overall_status"] == "UNHEALTHY"
        assert body["total"] == 3


class TestDetailEndpoint:
    def test_returns_single_service(self, client, registry):
        registry.register("databroker", DATABROKER)
        response = client.get(f"{BASE}/databroker")

        assert response.status_code == 200
        assert response.json()["service_name"] == "databroker"
        assert response.json()["status"] == "HEALTHY"

    def test_unknown_service_404(self, client):
        response = client.get(f"{BASE}/ghost")

        assert response.status_code == 404
        assert "ghost" in response.json()["detail"]

    def test_down_service_detail(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080", error="refused")

        body = client.get(f"{BASE}/databroker").json()
        assert body["status"] == "UNHEALTHY"
        assert body["failure_count"] == 3
        assert body["last_error"] == "refused"

    def test_detail_for_service_only_known_to_breaker(self, client, breaker):
        breaker.record_failure("stray", "http://stray:8080", "boom")

        assert client.get(f"{BASE}/stray").status_code == 200


class TestEndpointDetailRoute:
    def test_returns_registration_details(self, client, registry):
        registry.register("databroker", DATABROKER, workflow_id="wf-1")
        body = client.get(f"{BASE}/databroker/endpoint").json()

        assert body["address"] == "databroker"
        assert body["port"] == 8080
        assert body["base_url"] == "http://databroker:8080"
        assert body["workflow_ids"] == ["wf-1"]
        assert body["first_seen"] and body["last_seen"]

    def test_unknown_service_404(self, client):
        assert client.get(f"{BASE}/ghost/endpoint").status_code == 404

    def test_service_exercised_but_never_registered_404(self, client, breaker):
        """Health exists, but there is no endpoint record to return."""
        breaker.record_failure("stray", "http://stray:8080", "boom")

        assert client.get(f"{BASE}/stray/endpoint").status_code == 404


class TestRecoverEndpoint:
    def test_recovers_downed_service(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")
        assert breaker.allow("databroker") is False

        response = client.post(f"{BASE}/databroker/recover")

        assert response.status_code == 200
        assert response.json()["service_name"] == "databroker"
        assert response.json()["status"] == "HEALTHY"
        assert "reset" in response.json()["message"]
        assert breaker.allow("databroker") is True

    def test_status_flips_to_healthy(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")
        assert client.get(BASE).json()["overall_status"] == "UNHEALTHY"

        client.post(f"{BASE}/databroker/recover")

        body = client.get(BASE).json()
        assert body["overall_status"] == "HEALTHY"
        assert body["unhealthy"] == 0
        assert body["services"][0]["failure_count"] == 0
        assert body["services"][0]["manually_recovered_at"] is not None

    def test_unknown_service_404(self, client):
        response = client.post(f"{BASE}/ghost/recover")

        assert response.status_code == 404
        assert "ghost" in response.json()["detail"]

    def test_recover_healthy_service_is_noop(self, client, registry):
        registry.register("databroker", DATABROKER)
        response = client.post(f"{BASE}/databroker/recover")

        assert response.status_code == 200
        assert client.get(f"{BASE}/databroker").json()["status"] == "HEALTHY"

    def test_recovery_is_idempotent(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")

        assert client.post(f"{BASE}/databroker/recover").status_code == 200
        assert client.post(f"{BASE}/databroker/recover").status_code == 200
        assert client.get(f"{BASE}/databroker").json()["status"] == "HEALTHY"

    def test_recovered_service_can_fail_again(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")
        client.post(f"{BASE}/databroker/recover")

        kill(breaker, "databroker", "http://databroker:8080", error="down again")

        body = client.get(f"{BASE}/databroker").json()
        assert body["status"] == "UNHEALTHY"
        assert body["last_error"] == "down again"

    def test_get_not_allowed_on_recover(self, client, registry):
        registry.register("databroker", DATABROKER)
        assert client.get(f"{BASE}/databroker/recover").status_code == 405


class TestOperatorWorkflow:
    """The full detect -> inspect -> fix -> resume loop."""

    def test_end_to_end_recovery(self, client, registry, breaker):
        registry.register_all({"databroker": DATABROKER, "predictor": PREDICTOR})

        assert client.get(BASE).json()["overall_status"] == "HEALTHY"

        kill(breaker, "predictor", "http://predictor:8080", error="conn refused")

        dash = client.get(BASE).json()
        assert dash["overall_status"] == "UNHEALTHY"
        down = [s for s in dash["services"] if s["status"] == "UNHEALTHY"]
        assert [s["service_name"] for s in down] == ["predictor"]

        detail = client.get(f"{BASE}/predictor").json()
        assert detail["last_error"] == "conn refused"

        endpoint = client.get(f"{BASE}/predictor/endpoint").json()
        assert endpoint["base_url"] == "http://predictor:8080"

        assert client.post(f"{BASE}/predictor/recover").status_code == 200

        assert client.get(BASE).json()["overall_status"] == "HEALTHY"
        assert breaker.allow("predictor") is True

    def test_recovery_does_not_affect_other_services(self, client, registry, breaker):
        registry.register_all({"databroker": DATABROKER, "predictor": PREDICTOR})
        kill(breaker, "databroker", "http://databroker:8080")
        kill(breaker, "predictor", "http://predictor:8080")

        client.post(f"{BASE}/predictor/recover")

        body = client.get(BASE).json()
        statuses = {s["service_name"]: s["status"] for s in body["services"]}
        assert statuses == {"databroker": "UNHEALTHY", "predictor": "HEALTHY"}
        assert body["overall_status"] == "UNHEALTHY"


class TestSharedStateAcrossProcesses:
    """API and workers are separate processes sharing Redis."""

    def test_worker_failure_surfaces_on_api(self, client, registry, redis_client):
        registry.register("databroker", DATABROKER)

        # A worker replica with its own breaker instance sees the failures.
        worker_breaker = CircuitBreaker(redis_client, failure_threshold=3)
        kill(worker_breaker, "databroker", "http://databroker:8080", error="worker saw this")

        body = client.get(f"{BASE}/databroker").json()
        assert body["status"] == "UNHEALTHY"
        assert body["last_error"] == "worker saw this"

    def test_api_recovery_unblocks_worker(self, client, registry, redis_client):
        registry.register("databroker", DATABROKER)
        worker_breaker = CircuitBreaker(redis_client, failure_threshold=3)
        kill(worker_breaker, "databroker", "http://databroker:8080")
        assert worker_breaker.allow("databroker") is False

        client.post(f"{BASE}/databroker/recover")

        assert worker_breaker.allow("databroker") is True

    def test_service_registered_by_another_replica_is_visible(
        self, client, redis_client, breaker
    ):
        other_replica = ServiceRegistry(redis_client, breaker)
        other_replica.register("predictor", PREDICTOR, workflow_id="wf-9")

        body = client.get(BASE).json()
        assert [s["service_name"] for s in body["services"]] == ["predictor"]


class TestAuthDependency:
    """Routes accept injected dependencies, e.g. the orchestrator API key."""

    @pytest.fixture
    def secured_client(self, registry, monkeypatch):
        monkeypatch.setenv("ORCHESTRATOR_API_KEY", "s3cret")
        from api.app import _verify_orchestrator_key
        from fastapi import Depends

        app = FastAPI()
        app.include_router(
            create_health_router(
                registry, dependencies=[Depends(_verify_orchestrator_key)]
            )
        )
        return TestClient(app)

    def test_deregister_rejects_missing_credentials(self, secured_client, registry):
        registry.register("databroker", DATABROKER)

        assert secured_client.delete(f"{BASE}/databroker").status_code == 401

    def test_rejects_missing_credentials(self, secured_client, registry):
        registry.register("databroker", DATABROKER)
        assert secured_client.get(BASE).status_code == 401

    def test_rejects_wrong_key(self, secured_client):
        response = secured_client.get(
            BASE, headers={"Authorization": "Bearer wrong"}
        )
        assert response.status_code == 401

    def test_accepts_valid_key(self, secured_client, registry):
        registry.register("databroker", DATABROKER)
        response = secured_client.get(
            BASE, headers={"Authorization": "Bearer s3cret"}
        )

        assert response.status_code == 200
        assert response.json()["total"] == 1

    def test_recover_requires_key(self, secured_client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")

        assert secured_client.post(f"{BASE}/databroker/recover").status_code == 401
        assert (
            secured_client.post(
                f"{BASE}/databroker/recover",
                headers={"Authorization": "Bearer s3cret"},
            ).status_code
            == 200
        )


class TestOpenApiContract:
    def test_endpoints_documented(self, client):
        schema = client.get("/openapi.json").json()

        assert BASE in schema["paths"]
        assert "get" in schema["paths"][BASE]
        assert "post" in schema["paths"][f"{BASE}/{{service_name}}/recover"]

    def test_404_documented_on_detail(self, client):
        schema = client.get("/openapi.json").json()
        responses = schema["paths"][f"{BASE}/{{service_name}}"]["get"]["responses"]

        assert "404" in responses


def chain_graph(*names):
    """Linear execution graph over the given container names."""
    from models.graph import ExecutionGraph, GraphNode
    from models.node import Node, OperationSignature, OperationSignatureList

    graph = ExecutionGraph()
    previous = None
    for name in names:
        signature = OperationSignatureList(
            operation_signature=OperationSignature(
                operation_name="Process",
                input_message_name="Input",
                output_message_name="Output",
            )
        )
        node = GraphNode(
            node=Node(
                container_name=name,
                proto_uri=f"{name}.proto",
                image=f"{name}:latest",
                node_type="microservice",
                operation_signature_list=[signature],
            ),
            operation=signature,
        )
        graph.add_node(node)
        if previous is None:
            graph.start_nodes.append(node)
        else:
            previous.next_nodes.append(node)
            node.dependencies.append(previous)
        previous = node
    return graph


class TestRecoveryResumesWorkflows:
    """Manual recovery re-queues tasks blocked on the service."""

    @pytest.fixture
    def breaker(self, redis_client):
        # Open on the first failure, like the docker-compose worker config.
        return CircuitBreaker(redis_client, failure_threshold=1, reset_timeout=60.0)

    @pytest.fixture
    def engine(self, redis_client):
        from services.state_store import RedisStateStore
        from services.task_queue import RedisTaskQueue
        from services.workflow_engine import WorkflowEngine

        return WorkflowEngine(
            RedisStateStore(redis_client), RedisTaskQueue(redis_client), redis_client
        )

    @pytest.fixture
    def engine_client(self, redis_client, breaker, engine):
        registry = ServiceRegistry(redis_client, breaker, engine)
        app = FastAPI()
        app.include_router(create_health_router(registry))
        return TestClient(app), registry

    def start_workflow(self, engine, registry, workflow_id="wf-resume"):
        engine.initialize_workflow(workflow_id, chain_graph("service-a", "service-b", "service-c"))
        engine.start_workflow(workflow_id)
        endpoints = {
            name: ServiceEndpoint(address=name, port=8080)
            for name in ("service-a", "service-b", "service-c")
        }
        registry.register_all(endpoints, workflow_id=workflow_id)
        return workflow_id, endpoints

    def make_worker(self, engine, breaker, endpoints, client):
        from services.worker import Worker

        return Worker(
            engine,
            client,
            endpoints,
            poll_interval=0.01,
            breaker=breaker,
            blocked_retry_delay=3600,
        )

    def test_recover_resumes_blocked_workflow(self, engine_client, engine, breaker):
        from unittest.mock import MagicMock

        from models.state import TaskStatus, WorkflowStatus
        from services.control_client import (
            ControlClient,
            ControlClientError,
            ExecuteResponse,
        )

        client, registry = engine_client
        wf, endpoints = self.start_workflow(engine, registry)
        control = MagicMock(spec=ControlClient)
        worker = self.make_worker(engine, breaker, endpoints, control)

        # service-b is down: its task is blocked, the workflow stays running.
        def execute(base_url, **kwargs):
            if "service-b" in base_url:
                raise ControlClientError("Connection failed: refused", transient=True)
            return ExecuteResponse(status="complete")

        control.execute.side_effect = execute
        assert worker.process_task(wf) is True  # service-a completes
        assert worker.process_task(wf) is True  # service-b blocks
        assert worker.process_task(wf) is False  # nothing claimable

        workflow = engine.get_workflow_status(wf)
        assert workflow.status == WorkflowStatus.RUNNING
        blocked = [t for t in engine.get_all_tasks(wf) if t.status == TaskStatus.BLOCKED]
        assert len(blocked) == 1
        assert client.get(f"{BASE}/service-b").json()["status"] == "UNHEALTHY"

        # Operator fixes service-b and recovers it.
        control.execute.side_effect = None
        control.execute.return_value = ExecuteResponse(status="complete")
        resp = client.post(f"{BASE}/service-b/recover")

        assert resp.status_code == 200
        assert resp.json()["resumed_tasks"] == 1
        assert client.get(f"{BASE}/service-b").json()["status"] == "HEALTHY"

        # The same workflow finishes without being resubmitted.
        worker.run(wf, timeout=0)
        assert engine.get_workflow_status(wf).status == WorkflowStatus.COMPLETED

    def test_recover_without_blocked_tasks_resumes_nothing(self, engine_client, engine):
        client, registry = engine_client
        self.start_workflow(engine, registry)

        body = client.post(f"{BASE}/service-a/recover").json()

        assert body["resumed_tasks"] == 0
        assert body["status"] == "HEALTHY"

    def test_recover_skips_deleted_workflow(self, engine_client, engine, redis_client):
        from services.state_store import RedisStateStore

        client, registry = engine_client
        wf, _ = self.start_workflow(engine, registry)
        RedisStateStore(redis_client).delete_workflow(wf)

        resp = client.post(f"{BASE}/service-a/recover")

        assert resp.status_code == 200
        assert resp.json()["resumed_tasks"] == 0

    def test_registry_without_engine_still_recovers(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")

        body = client.post(f"{BASE}/databroker/recover").json()

        assert body["status"] == "HEALTHY"
        assert body["resumed_tasks"] == 0


class TestDeregisterService:
    """Services of use cases that no longer run can be removed from the list."""

    def test_removes_the_service(self, client, registry, breaker):
        registry.register("databroker", DATABROKER)
        kill(breaker, "databroker", "http://databroker:8080")

        response = client.delete(f"{BASE}/databroker")

        assert response.status_code == 200
        assert response.json()["removed"] is True
        assert client.get(f"{BASE}/databroker").status_code == 404
        assert client.get(BASE).json()["total"] == 0

    def test_unknown_service_404(self, client):
        assert client.delete(f"{BASE}/never-seen").status_code == 404

    def test_service_known_only_to_the_breaker(self, client, breaker):
        breaker.record_failure("stray", "http://stray:8080", "boom")

        assert client.delete(f"{BASE}/stray").status_code == 200
        assert client.get(f"{BASE}/stray").status_code == 404

    def test_registering_again_brings_it_back(self, client, registry):
        registry.register("databroker", DATABROKER)
        client.delete(f"{BASE}/databroker")

        registry.register("databroker", DATABROKER)

        assert client.get(f"{BASE}/databroker").json()["status"] == "HEALTHY"
