"""Integration tests for RedisStateStore with real Redis."""

import pytest
from redis import Redis
from testcontainers.redis import RedisContainer

from models.data_reference import DataReference, Format, Protocol
from models.state import TaskStatus, WorkflowStatus
from services.state_store import RedisStateStore


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as container:
        yield container


@pytest.fixture
def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
        decode_responses=False,
    )
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def state_store(redis_client):
    return RedisStateStore(redis_client)


class TestWorkflowIntegration:
    """Integration tests for workflow operations."""

    def test_workflow_lifecycle(self, state_store):
        state = state_store.create_workflow("wf-int-1")
        assert state.status == WorkflowStatus.PENDING

        state = state_store.update_workflow_status("wf-int-1", WorkflowStatus.RUNNING)
        assert state.status == WorkflowStatus.RUNNING

        state = state_store.update_workflow_status("wf-int-1", WorkflowStatus.COMPLETED)
        assert state.status == WorkflowStatus.COMPLETED

        retrieved = state_store.get_workflow("wf-int-1")
        assert retrieved.status == WorkflowStatus.COMPLETED

    def test_workflow_failure(self, state_store):
        state_store.create_workflow("wf-fail")
        state = state_store.update_workflow_status(
            "wf-fail", WorkflowStatus.FAILED, error="Integration test error"
        )
        assert state.error == "Integration test error"


class TestTaskIntegration:
    """Integration tests for task operations."""

    def test_task_lifecycle(self, state_store):
        state_store.create_workflow("wf-task")

        input_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/input.json",
            format=Format.JSON,
        )
        state = state_store.create_task("wf-task", "t1", "node:op", [input_ref])
        assert state.status == TaskStatus.PENDING
        assert len(state.input_refs) == 1

        state = state_store.update_task_status("wf-task", "t1", TaskStatus.RUNNING)
        assert state.status == TaskStatus.RUNNING

        output_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/output.json",
            format=Format.JSON,
        )
        state = state_store.update_task_status(
            "wf-task", "t1", TaskStatus.COMPLETED, output_refs=[output_ref]
        )
        assert state.status == TaskStatus.COMPLETED
        assert len(state.output_refs) == 1


class TestMultipleWorkflows:
    """Integration tests for multiple workflows."""

    def test_multiple_workflows_isolation(self, state_store):
        state_store.create_workflow("wf-a")
        state_store.create_workflow("wf-b")

        state_store.create_task("wf-a", "task-1", "node:op")
        state_store.create_task("wf-b", "task-1", "node:op")

        tasks_a = state_store.get_workflow_tasks("wf-a")
        tasks_b = state_store.get_workflow_tasks("wf-b")

        assert len(tasks_a) == 1
        assert len(tasks_b) == 1
        assert tasks_a[0].workflow_id == "wf-a"
        assert tasks_b[0].workflow_id == "wf-b"

    def test_delete_workflow_isolation(self, state_store):
        state_store.create_workflow("wf-del-a")
        state_store.create_workflow("wf-del-b")

        state_store.create_task("wf-del-a", "task-1", "node:op")
        state_store.create_task("wf-del-b", "task-1", "node:op")

        state_store.delete_workflow("wf-del-a")

        tasks_b = state_store.get_workflow_tasks("wf-del-b")
        assert len(tasks_b) == 1


class TestDataPersistence:
    """Integration tests for data persistence."""

    def test_data_survives_reconnect(self, redis_container):
        client1 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        store1 = RedisStateStore(client1)
        store1.create_workflow("wf-persist")
        store1.create_task("wf-persist", "task-1", "node:op")
        client1.close()

        client2 = Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            decode_responses=False,
        )
        store2 = RedisStateStore(client2)
        workflow = store2.get_workflow("wf-persist")
        assert workflow.workflow_id == "wf-persist"

        tasks = store2.get_workflow_tasks("wf-persist")
        assert len(tasks) == 1
        client2.close()
