"""Integration tests for the resume and retry API.

Exercises the mounted workflow router end to end through the FastAPI stack:
real routing, status codes and response models, against a Redis-backed engine
with a real execution graph.
"""

import fakeredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.workflow_routes import create_workflow_router
from models.graph import ExecutionGraph, GraphNode
from models.node import Node, OperationSignature, OperationSignatureList
from models.state import TaskStatus, WorkflowStatus
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue
from services.workflow_engine import WorkflowEngine

WF = "wf-resume-1"


def graph_node(name: str) -> GraphNode:
    node = Node(
        container_name=name,
        proto_uri=f"{name}.proto",
        image=f"{name}:latest",
        node_type="microservice",
        operation_signature_list=[
            OperationSignatureList(
                operation_signature=OperationSignature(
                    operation_name="Process",
                    input_message_name="Input",
                    output_message_name="Output",
                )
            )
        ],
    )
    return GraphNode(node=node, operation=node.operation_signature_list[0])


def chain_graph() -> ExecutionGraph:
    """A -> B."""
    graph = ExecutionGraph()
    node_a, node_b = graph_node("service-a"), graph_node("service-b")
    node_a.next_nodes.append(node_b)
    node_b.dependencies.append(node_a)
    graph.add_node(node_a)
    graph.add_node(node_b)
    graph.start_nodes.append(node_a)
    return graph


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def engine(redis_client):
    return WorkflowEngine(
        RedisStateStore(redis_client), RedisTaskQueue(redis_client), redis_client
    )


@pytest.fixture
def client(engine):
    app = FastAPI()
    app.include_router(create_workflow_router(engine))
    return TestClient(app)


@pytest.fixture
def failed_workflow(engine):
    """A workflow whose first task completed and whose second one failed."""
    engine.initialize_workflow(WF, chain_graph())
    engine.start_workflow(WF)
    task_a = engine.claim_task(WF, timeout=1)
    engine.complete_task(WF, task_a.task_id)
    task_b = engine.claim_task(WF, timeout=1)
    engine.fail_task(WF, task_b.task_id, "service exploded")
    return task_a, task_b


class TestRouterConstruction:
    def test_requires_engine(self):
        with pytest.raises(ValueError, match="engine is required"):
            create_workflow_router(None)

    def test_routes_are_mounted(self, engine):
        app = FastAPI()
        app.include_router(create_workflow_router(engine))
        paths = set(TestClient(app).get("/openapi.json").json()["paths"])

        assert "/workflows/{workflow_id}/resume" in paths
        assert "/workflows/{workflow_id}/tasks/{task_id}/retry" in paths
        assert "/workflows/{workflow_id}/verify" in paths


class TestResume:
    def test_resume_requeues_failed_tasks(self, client, engine, failed_workflow):
        task_a, task_b = failed_workflow

        response = client.post(f"/workflows/{WF}/resume")

        assert response.status_code == 200
        body = response.json()
        assert body["workflow_id"] == WF
        assert body["status"] == "running"
        assert body["retried_tasks"] == [task_b.task_id]
        assert "1 task" in body["message"]

    def test_resumed_workflow_continues(self, client, engine, failed_workflow):
        task_a, task_b = failed_workflow
        client.post(f"/workflows/{WF}/resume")

        claimed = engine.claim_task(WF, timeout=1)
        assert claimed.task_id == task_b.task_id
        engine.complete_task(WF, claimed.task_id)
        assert engine.get_workflow_status(WF).status == WorkflowStatus.COMPLETED

    def test_completed_tasks_are_not_rerun(self, client, engine, failed_workflow):
        task_a, _ = failed_workflow
        before = engine.get_all_tasks(WF)
        client.post(f"/workflows/{WF}/resume")

        after = {task.task_id: task for task in engine.get_all_tasks(WF)}
        original = {task.task_id: task for task in before}[task_a.task_id]
        assert after[task_a.task_id].status == TaskStatus.COMPLETED
        assert after[task_a.task_id].updated_at == original.updated_at

    def test_unknown_workflow_is_404(self, client):
        response = client.post("/workflows/wf-missing/resume")
        assert response.status_code == 404
        assert "wf-missing" in response.json()["detail"]

    def test_running_workflow_is_409(self, client, engine):
        engine.initialize_workflow(WF, chain_graph())
        engine.start_workflow(WF)

        response = client.post(f"/workflows/{WF}/resume")

        assert response.status_code == 409
        assert "only failed workflows" in response.json()["detail"]


class TestRetry:
    def test_retry_requeues_one_task(self, client, engine, failed_workflow):
        _, task_b = failed_workflow

        response = client.post(f"/workflows/{WF}/tasks/{task_b.task_id}/retry")

        assert response.status_code == 200
        body = response.json()
        assert body["task_id"] == task_b.task_id
        assert body["node_key"] == "service-b:Process"
        assert body["status"] == "pending"

        claimed = engine.claim_task(WF, timeout=1)
        assert claimed.task_id == task_b.task_id
        assert claimed.error is None

    def test_unknown_task_is_404(self, client, engine, failed_workflow):
        response = client.post(f"/workflows/{WF}/tasks/task-missing/retry")
        assert response.status_code == 404

    def test_unknown_workflow_is_404(self, client):
        response = client.post("/workflows/wf-missing/tasks/task-1/retry")
        assert response.status_code == 404

    def test_completed_task_is_409(self, client, engine, failed_workflow):
        task_a, _ = failed_workflow

        response = client.post(f"/workflows/{WF}/tasks/{task_a.task_id}/retry")

        assert response.status_code == 409
        assert "only failed tasks" in response.json()["detail"]


class TestAuthDependency:
    """The router accepts the orchestrator API-key dependency."""

    @pytest.fixture
    def secured_client(self, engine, monkeypatch):
        monkeypatch.setenv("ORCHESTRATOR_API_KEY", "s3cret")
        from api.app import _verify_orchestrator_key
        from fastapi import Depends

        app = FastAPI()
        app.include_router(
            create_workflow_router(engine, dependencies=[Depends(_verify_orchestrator_key)])
        )
        return TestClient(app)

    def test_resume_rejects_missing_credentials(self, secured_client):
        assert secured_client.post(f"/workflows/{WF}/resume").status_code == 401

    def test_verify_rejects_missing_credentials(self, secured_client):
        assert secured_client.get(f"/workflows/{WF}/verify").status_code == 401

    def test_retry_rejects_missing_credentials(self, secured_client):
        assert (
            secured_client.post(f"/workflows/{WF}/tasks/task-1/retry").status_code == 401
        )

    def test_accepts_valid_key(self, secured_client, failed_workflow):
        response = secured_client.post(
            f"/workflows/{WF}/resume", headers={"Authorization": "Bearer s3cret"}
        )
        assert response.status_code == 200


class TestVerify:
    """GET /workflows/{id}/verify (docs/VERIFICATION.md)."""

    def test_a_healthy_workflow_is_verified(self, client, engine):
        engine.initialize_workflow(WF, chain_graph())
        engine.start_workflow(WF)
        while (task := engine.claim_task(WF)) is not None:
            engine.complete_task(WF, task.task_id)

        body = client.get(f"/workflows/{WF}/verify").json()

        assert body["workflow_id"] == WF
        assert body["status"] == "completed"
        assert body["verified"] is True
        assert [c["name"] for c in body["checks"]] == [
            "status", "placement", "dependencies", "handover", "inputs",
            "bookkeeping", "history", "cleanup",
        ]
        assert all(c["passed"] and c["problems"] == [] for c in body["checks"])
        assert body["history"] == {
            "claims": 2, "reclaims": 0, "deferrals": 0, "retries": 0, "clean": True,
        }

    def test_a_failed_workflow_can_still_be_consistent(self, client, failed_workflow):
        body = client.get(f"/workflows/{WF}/verify").json()

        assert body["status"] == "failed"
        assert body["verified"] is True
        assert body["history"]["clean"] is True

    def test_damage_is_reported(self, client, engine, redis_client, failed_workflow):
        engine.task_queue.enqueue_task(WF, failed_workflow[1].task_id)

        body = client.get(f"/workflows/{WF}/verify").json()

        assert body["verified"] is False
        (placement,) = [c for c in body["checks"] if not c["passed"]]
        assert placement["name"] == "placement"
        assert "is failed but is queued 1 time(s)" in placement["problems"][0]

    def test_unknown_workflow(self, client):
        response = client.get("/workflows/wf-missing/verify")

        assert response.status_code == 404
        assert response.json()["detail"] == "Workflow not found"
