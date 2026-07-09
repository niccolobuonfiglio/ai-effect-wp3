"""Unit tests for RedisStateStore."""

import pytest
import fakeredis

from models.data_reference import DataReference, Format, Protocol
from models.state import TaskStatus, WorkflowStatus
from services.state_store import (
    RedisStateStore,
    TaskNotFoundError,
    WorkflowNotFoundError,
)


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=False)


@pytest.fixture
def state_store(redis_client):
    return RedisStateStore(redis_client)


class TestRedisStateStoreInit:
    """Tests for RedisStateStore initialization."""

    def test_init_with_redis_client(self, redis_client):
        store = RedisStateStore(redis_client)
        assert store is not None

    def test_init_without_client_raises(self):
        with pytest.raises(ValueError, match="redis_client is required"):
            RedisStateStore(None)


class TestWorkflowOperations:
    """Tests for workflow CRUD operations."""

    def test_create_workflow(self, state_store):
        state = state_store.create_workflow("wf-1")
        assert state.workflow_id == "wf-1"
        assert state.status == WorkflowStatus.PENDING
        assert state.error is None

    def test_create_workflow_empty_id_raises(self, state_store):
        with pytest.raises(ValueError, match="workflow_id is required"):
            state_store.create_workflow("")

    def test_create_duplicate_workflow_raises(self, state_store):
        state_store.create_workflow("wf-1")
        with pytest.raises(ValueError, match="already exists"):
            state_store.create_workflow("wf-1")

    def test_get_workflow(self, state_store):
        state_store.create_workflow("wf-1")
        state = state_store.get_workflow("wf-1")
        assert state.workflow_id == "wf-1"
        assert state.status == WorkflowStatus.PENDING

    def test_get_workflow_empty_id_raises(self, state_store):
        with pytest.raises(ValueError, match="workflow_id is required"):
            state_store.get_workflow("")

    def test_get_workflow_not_found_raises(self, state_store):
        with pytest.raises(WorkflowNotFoundError) as exc:
            state_store.get_workflow("nonexistent")
        assert exc.value.workflow_id == "nonexistent"

    def test_update_workflow_status_to_running(self, state_store):
        state_store.create_workflow("wf-1")
        state = state_store.update_workflow_status("wf-1", WorkflowStatus.RUNNING)
        assert state.status == WorkflowStatus.RUNNING

    def test_update_workflow_status_to_completed(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.update_workflow_status("wf-1", WorkflowStatus.RUNNING)
        state = state_store.update_workflow_status("wf-1", WorkflowStatus.COMPLETED)
        assert state.status == WorkflowStatus.COMPLETED

    def test_update_workflow_status_to_failed_with_error(self, state_store):
        state_store.create_workflow("wf-1")
        state = state_store.update_workflow_status(
            "wf-1", WorkflowStatus.FAILED, error="Connection timeout"
        )
        assert state.status == WorkflowStatus.FAILED
        assert state.error == "Connection timeout"

    def test_update_workflow_status_not_found_raises(self, state_store):
        with pytest.raises(WorkflowNotFoundError):
            state_store.update_workflow_status("nonexistent", WorkflowStatus.RUNNING)

    def test_update_workflow_updates_timestamp(self, state_store):
        state_store.create_workflow("wf-1")
        initial = state_store.get_workflow("wf-1")
        updated = state_store.update_workflow_status("wf-1", WorkflowStatus.RUNNING)
        assert updated.updated_at >= initial.updated_at

    def test_delete_workflow(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.delete_workflow("wf-1")
        with pytest.raises(WorkflowNotFoundError):
            state_store.get_workflow("wf-1")


class TestTaskOperations:
    """Tests for task CRUD operations."""

    def test_create_task(self, state_store):
        state_store.create_workflow("wf-1")
        state = state_store.create_task("wf-1", "task-1", "node:op")
        assert state.task_id == "task-1"
        assert state.workflow_id == "wf-1"
        assert state.node_key == "node:op"
        assert state.status == TaskStatus.PENDING

    def test_create_task_empty_workflow_id_raises(self, state_store):
        with pytest.raises(ValueError, match="workflow_id is required"):
            state_store.create_task("", "task-1", "node:op")

    def test_create_task_empty_task_id_raises(self, state_store):
        state_store.create_workflow("wf-1")
        with pytest.raises(ValueError, match="task_id is required"):
            state_store.create_task("wf-1", "", "node:op")

    def test_create_task_empty_node_key_raises(self, state_store):
        state_store.create_workflow("wf-1")
        with pytest.raises(ValueError, match="node_key is required"):
            state_store.create_task("wf-1", "task-1", "")

    def test_create_task_workflow_not_found_raises(self, state_store):
        with pytest.raises(WorkflowNotFoundError):
            state_store.create_task("nonexistent", "task-1", "node:op")

    def test_create_duplicate_task_raises(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        with pytest.raises(ValueError, match="already exists"):
            state_store.create_task("wf-1", "task-1", "node:op")

    def test_create_task_with_input_refs(self, state_store):
        state_store.create_workflow("wf-1")
        input_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/input.json",
            format=Format.JSON,
        )
        state = state_store.create_task("wf-1", "task-1", "node:op", [input_ref])
        assert len(state.input_refs) == 1
        assert state.input_refs[0].uri == "s3://bucket/input.json"

    def test_get_task(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        state = state_store.get_task("wf-1", "task-1")
        assert state.task_id == "task-1"

    def test_get_task_not_found_raises(self, state_store):
        state_store.create_workflow("wf-1")
        with pytest.raises(TaskNotFoundError) as exc:
            state_store.get_task("wf-1", "nonexistent")
        assert exc.value.workflow_id == "wf-1"
        assert exc.value.task_id == "nonexistent"

    def test_update_task_status_to_running(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        state = state_store.update_task_status("wf-1", "task-1", TaskStatus.RUNNING)
        assert state.status == TaskStatus.RUNNING

    def test_update_task_status_to_completed_with_output(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        state_store.update_task_status("wf-1", "task-1", TaskStatus.RUNNING)
        output_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/output.json",
            format=Format.JSON,
        )
        state = state_store.update_task_status(
            "wf-1", "task-1", TaskStatus.COMPLETED, output_refs=[output_ref]
        )
        assert state.status == TaskStatus.COMPLETED
        assert len(state.output_refs) == 1
        assert state.output_refs[0].uri == "s3://bucket/output.json"

    def test_update_task_status_to_failed_with_error(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        state = state_store.update_task_status(
            "wf-1", "task-1", TaskStatus.FAILED, error="Service unavailable"
        )
        assert state.status == TaskStatus.FAILED
        assert state.error == "Service unavailable"

    def test_update_task_not_found_raises(self, state_store):
        state_store.create_workflow("wf-1")
        with pytest.raises(TaskNotFoundError):
            state_store.update_task_status("wf-1", "nonexistent", TaskStatus.RUNNING)

    def test_get_workflow_tasks_empty(self, state_store):
        state_store.create_workflow("wf-1")
        tasks = state_store.get_workflow_tasks("wf-1")
        assert tasks == []

    def test_get_workflow_tasks(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node1:op")
        state_store.create_task("wf-1", "task-2", "node2:op")
        tasks = state_store.get_workflow_tasks("wf-1")
        assert len(tasks) == 2
        task_ids = {t.task_id for t in tasks}
        assert task_ids == {"task-1", "task-2"}

    def test_get_workflow_tasks_workflow_not_found_raises(self, state_store):
        with pytest.raises(WorkflowNotFoundError):
            state_store.get_workflow_tasks("nonexistent")


class TestDeleteWorkflow:
    """Tests for workflow deletion."""

    def test_delete_workflow_removes_tasks(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        state_store.create_task("wf-1", "task-2", "node:op")
        state_store.delete_workflow("wf-1")
        with pytest.raises(WorkflowNotFoundError):
            state_store.get_workflow("wf-1")

    def test_delete_nonexistent_workflow_silent(self, state_store):
        state_store.delete_workflow("nonexistent")


class TestDataReferencePersistence:
    """Tests for DataReference serialization in state."""

    def test_input_refs_roundtrip(self, state_store):
        state_store.create_workflow("wf-1")
        input_ref = DataReference(
            protocol=Protocol.HTTPS,
            uri="https://api.example.com/data",
            format=Format.JSON,
            size_bytes=1024,
            checksum="sha256:abc123",
            metadata={"version": "1.0"},
        )
        state_store.create_task("wf-1", "task-1", "node:op", [input_ref])
        retrieved = state_store.get_task("wf-1", "task-1")
        assert retrieved.input_refs[0] == input_ref

    def test_output_refs_roundtrip(self, state_store):
        state_store.create_workflow("wf-1")
        state_store.create_task("wf-1", "task-1", "node:op")
        output_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/result.parquet",
            format=Format.PARQUET,
        )
        state_store.update_task_status(
            "wf-1", "task-1", TaskStatus.COMPLETED, output_refs=[output_ref]
        )
        retrieved = state_store.get_task("wf-1", "task-1")
        assert retrieved.output_refs[0] == output_ref
