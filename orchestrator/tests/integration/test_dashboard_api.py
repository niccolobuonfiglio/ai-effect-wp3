"""Integration tests for the dashboard routes and page.

The whole API app, with the real blueprint/dockerinfo parsers, engine, breaker
and registry on fakeredis, and a temporary logs directory.
"""

import json
import time

import fakeredis
import pytest
from fastapi.testclient import TestClient

from api.app import OrchestratorAPI
from services.blueprint_parser import BlueprintParser
from services.circuit_breaker import CircuitBreaker
from services.dockerinfo_parser import DockerInfoParser
from services.log_reader import LogReader
from services.service_registry import ServiceRegistry
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_catalog import WorkflowCatalog
from services.workflow_engine import WorkflowEngine


def blueprint(name="Demo pipeline"):
    def node(container, operation, next_container=None, next_operation=None):
        return {
            "container_name": container,
            "proto_uri": f"{container}.proto",
            "image": f"{container}:latest",
            "node_type": "MLModel",
            "operation_signature_list": [{
                "operation_signature": {
                    "operation_name": operation,
                    "input_message_name": "In",
                    "output_message_name": "Out",
                },
                "connected_to": (
                    [{"container_name": next_container,
                      "operation_signature": {"operation_name": next_operation}}]
                    if next_container else []
                ),
            }],
        }

    return {
        "name": name,
        "pipeline_id": "demo",
        "creation_date": "2026-01-01",
        "type": "pipeline-topology/v2",
        "version": "2.0",
        "nodes": [node("svc_a", "Produce", "svc_b", "Consume"), node("svc_b", "Consume")],
    }


DOCKERINFO = {"docker_info_list": [
    {"container_name": "svc_a", "ip_address": "svc-a", "port": "8080"},
    # extra fields such as webui_url are accepted and ignored
    {"container_name": "svc_b", "ip_address": "svc-b", "port": "8080", "webui_url": "http://x:1"},
]}


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def engine(redis_client):
    return WorkflowEngine(RedisStateStore(redis_client), RedisTaskQueue(redis_client), redis_client)


@pytest.fixture
def breaker(redis_client):
    return CircuitBreaker(redis_client, failure_threshold=1)


@pytest.fixture
def log_dir(tmp_path):
    return tmp_path


@pytest.fixture
def client(redis_client, engine, breaker, log_dir, monkeypatch):
    monkeypatch.delenv("ORCHESTRATOR_API_KEY", raising=False)
    api = OrchestratorAPI(
        engine, BlueprintParser(), DockerInfoParser(), redis_client,
        breaker=breaker,
        registry=ServiceRegistry(redis_client, breaker, engine),
        log_reader=LogReader(log_dir),
    )
    return TestClient(api.create_app())


def submit(client, name="Demo pipeline") -> str:
    response = client.post("/workflows", json={"blueprint": blueprint(name), "dockerinfo": DOCKERINFO})
    assert response.status_code == 200, response.text
    return response.json()["workflow_id"]


class TestListWorkflows:
    def test_empty(self, client):
        assert client.get("/workflows").json() == {"workflows": [], "total": 0}

    def test_submitted_workflow_is_listed(self, client):
        wf = submit(client)

        body = client.get("/workflows").json()

        assert body["total"] == 1
        entry = body["workflows"][0]
        assert entry["workflow_id"] == wf
        assert entry["name"] == "Demo pipeline"
        assert entry["status"] == "running"

    def test_newest_first(self, client):
        first = submit(client, "first")
        time.sleep(0.01)
        second = submit(client, "second")

        ids = [w["workflow_id"] for w in client.get("/workflows").json()["workflows"]]

        assert ids == [second, first]

    def test_delete_removes_from_list(self, client):
        wf = submit(client)
        client.delete(f"/workflows/{wf}")

        assert client.get("/workflows").json()["total"] == 0

    def test_workflow_with_lost_state_is_dropped(self, client, redis_client):
        wf = submit(client)
        RedisStateStore(redis_client).delete_workflow(wf)   # e.g. Redis partially reset

        assert client.get("/workflows").json()["total"] == 0
        assert WorkflowCatalog(redis_client).ids() == []


class TestGraph:
    def test_nodes_edges_and_initial_status(self, client):
        wf = submit(client)

        graph = client.get(f"/workflows/{wf}/graph").json()

        assert graph["name"] == "Demo pipeline"
        statuses = {n["key"]: n["status"] for n in graph["nodes"]}
        assert statuses == {"svc_a:Produce": "pending", "svc_b:Consume": "pending"}
        assert graph["edges"] == [{"source": "svc_a:Produce", "target": "svc_b:Consume"}]
        assert all(n["service_status"] == "HEALTHY" for n in graph["nodes"])

    def test_blocked_task_and_unhealthy_service(self, client, engine, breaker):
        wf = submit(client)
        task = engine.claim_task(wf, timeout=0)
        engine.defer_task(wf, task.task_id, "svc_a unavailable", delay=60)
        breaker.record_failure("svc_a", "http://svc-a:8080", "Connection refused")

        nodes = {n["key"]: n for n in client.get(f"/workflows/{wf}/graph").json()["nodes"]}

        assert nodes["svc_a:Produce"]["status"] == "blocked"
        assert nodes["svc_a:Produce"]["service_status"] == "UNHEALTHY"
        assert nodes["svc_a:Produce"]["circuit_state"] == "open"
        assert nodes["svc_b:Consume"]["service_status"] == "HEALTHY"

    def test_unknown_workflow(self, client):
        assert client.get("/workflows/wf-nope/graph").status_code == 404

    def test_workflow_without_stored_blueprint(self, client, redis_client):
        wf = submit(client)
        WorkflowCatalog(redis_client).forget(wf)

        response = client.get(f"/workflows/{wf}/graph")

        assert response.status_code == 404
        assert "Blueprint" in response.json()["detail"]


class TestLogsAndAlerts:
    def test_workflow_logs(self, client, log_dir):
        wf = submit(client)
        (log_dir / "worker-w1.log").write_text(
            f"2026-09-18 23:23:03,000 - WARNING - services.worker - Task t of workflow {wf} blocked\n"
            "2026-09-18 23:23:04,000 - INFO - other - unrelated\n"
        )

        text = client.get(f"/workflows/{wf}/logs").text

        assert wf in text and "unrelated" not in text

    def test_fallback_header_when_nothing_matches(self, client, log_dir):
        wf = submit(client)
        (log_dir / "orchestrator.log").write_text("2026-09-18 23:23:04,000 - INFO - x - unrelated\n")

        text = client.get(f"/workflows/{wf}/logs").text

        assert text.startswith("(no lines mention this workflow")

    def test_logs_for_unknown_workflow(self, client):
        assert client.get("/workflows/wf-nope/logs").status_code == 404

    def test_lines_parameter_is_bounded(self, client):
        wf = submit(client)
        assert client.get(f"/workflows/{wf}/logs?lines=0").status_code == 422
        assert client.get(f"/workflows/{wf}/logs?lines=5000").status_code == 422

    def test_missing_log_directory(self, redis_client, engine, breaker, tmp_path, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_API_KEY", raising=False)
        api = OrchestratorAPI(
            engine, BlueprintParser(), DockerInfoParser(), redis_client,
            breaker=breaker, log_reader=LogReader(tmp_path / "absent"),
        )
        client = TestClient(api.create_app())
        wf = submit(client)

        assert "not found" in client.get(f"/workflows/{wf}/logs").text

    def test_alerts(self, client, log_dir):
        (log_dir / "alerts-worker-w1.jsonl").write_text(
            json.dumps({"timestamp": "2026-09-18T23:23:09+00:00", "alert": "SERVICE_DOWN",
                        "service_name": "svc_a"}) + "\n"
        )

        body = client.get("/orchestrator/alerts").json()

        assert body["total"] == 1
        assert body["alerts"][0]["alert"] == "SERVICE_DOWN"


class TestPageAndAuth:
    def test_dashboard_page_is_served(self, client):
        response = client.get("/ui/")

        assert response.status_code == 200
        assert "<title>Orchestrator Dashboard</title>" in response.text

    def test_every_data_route_requires_the_key(self, client, monkeypatch):
        wf = submit(client)
        monkeypatch.setenv("ORCHESTRATOR_API_KEY", "s3cret")
        routes = ["/workflows", f"/workflows/{wf}/graph", f"/workflows/{wf}/logs", "/orchestrator/alerts"]

        for route in routes:
            assert client.get(route).status_code == 401, route
            ok = client.get(route, headers={"Authorization": "Bearer s3cret"})
            assert ok.status_code == 200, route

    def test_page_handles_a_missing_key(self, client):
        """The page shows a locked state on 401 and applies the key as typed."""
        page = client.get("/ui/").text

        assert "API key required" in page
        assert "showLocked" in page
        assert "addEventListener('input'" in page

    def test_page_itself_needs_no_key(self, client, monkeypatch):
        monkeypatch.setenv("ORCHESTRATOR_API_KEY", "s3cret")
        assert client.get("/ui/").status_code == 200
