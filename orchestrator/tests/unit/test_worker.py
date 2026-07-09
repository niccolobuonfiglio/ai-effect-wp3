"""Unit tests for Worker."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from models.data_reference import DataReference, Format, Protocol
from models.state import TaskState, TaskStatus
from services.control_client import (
    ControlClient,
    ControlClientError,
    ExecuteResponse,
    OutputResponse,
    StatusResponse,
)
from services.dockerinfo_parser import ServiceEndpoint
from services.worker import Worker, WorkerError


def create_task(
    task_id: str = "task-123",
    node_key: str = "service-a:ProcessData",
    input_refs: list | None = None,
) -> TaskState:
    """Create a TaskState for testing."""
    now = datetime.now()
    return TaskState(
        task_id=task_id,
        workflow_id="wf-123",
        node_key=node_key,
        status=TaskStatus.RUNNING,
        created_at=now,
        updated_at=now,
        input_refs=input_refs or [],
    )


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_client():
    return MagicMock(spec=ControlClient)


@pytest.fixture
def endpoints():
    return {
        "service-a": ServiceEndpoint(address="service-a", port=50051),
        "service-b": ServiceEndpoint(address="service-b", port=50052),
    }


@pytest.fixture
def worker(mock_engine, mock_client, endpoints):
    return Worker(mock_engine, mock_client, endpoints, poll_interval=0.01)


@pytest.fixture
def sample_task():
    return create_task()


@pytest.fixture
def sample_output():
    return DataReference(
        protocol=Protocol.S3,
        uri="s3://bucket/output.json",
        format=Format.JSON,
    )


class TestWorkerInit:
    """Tests for Worker initialization."""

    def test_init_with_valid_args(self, mock_engine, mock_client, endpoints):
        """Create worker with valid arguments."""
        worker = Worker(mock_engine, mock_client, endpoints)
        assert worker._engine == mock_engine
        assert worker._client == mock_client
        assert worker._endpoints == endpoints

    def test_init_with_poll_interval(self, mock_engine, mock_client, endpoints):
        """Create worker with custom poll interval."""
        worker = Worker(mock_engine, mock_client, endpoints, poll_interval=10.0)
        assert worker._poll_interval == 10.0

    def test_init_none_engine_raises(self, mock_client, endpoints):
        """None engine raises error."""
        with pytest.raises(ValueError, match="engine is required"):
            Worker(None, mock_client, endpoints)

    def test_init_none_client_raises(self, mock_engine, endpoints):
        """None client raises error."""
        with pytest.raises(ValueError, match="client is required"):
            Worker(mock_engine, None, endpoints)

    def test_init_none_endpoints_raises(self, mock_engine, mock_client):
        """None endpoints raises error."""
        with pytest.raises(ValueError, match="endpoints is required"):
            Worker(mock_engine, mock_client, None)

    def test_init_zero_poll_interval_raises(self, mock_engine, mock_client, endpoints):
        """Zero poll interval raises error."""
        with pytest.raises(ValueError, match="poll_interval must be positive"):
            Worker(mock_engine, mock_client, endpoints, poll_interval=0)

    def test_init_negative_poll_interval_raises(
        self, mock_engine, mock_client, endpoints
    ):
        """Negative poll interval raises error."""
        with pytest.raises(ValueError, match="poll_interval must be positive"):
            Worker(mock_engine, mock_client, endpoints, poll_interval=-1)


class TestProcessTaskQuickCompletion:
    """Tests for quick task completion."""

    def test_process_task_quick_completion(
        self, worker, mock_engine, mock_client, sample_task, sample_output
    ):
        """Process task that completes immediately."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.claim_task.assert_called_once_with("wf-123", 0)
        mock_client.execute.assert_called_once()
        mock_engine.complete_task.assert_called_once_with(
            "wf-123", "task-123", [sample_output]
        )

    def test_process_task_quick_no_output(
        self, worker, mock_engine, mock_client, sample_task
    ):
        """Process task that completes with no output."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(status="complete")

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.complete_task.assert_called_once_with("wf-123", "task-123", [])


class TestProcessTaskLongRunning:
    """Tests for long-running task completion."""

    def test_process_task_long_running(
        self, worker, mock_engine, mock_client, sample_task, sample_output
    ):
        """Process task that requires polling."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="running", task_id="svc-task-456"
        )
        mock_client.get_status.side_effect = [
            StatusResponse(status="running", progress=50),
            StatusResponse(status="complete"),
        ]
        mock_client.get_output.return_value = OutputResponse(output=sample_output)

        result = worker.process_task("wf-123")

        assert result is True
        assert mock_client.get_status.call_count == 2
        mock_client.get_output.assert_called_once()
        mock_engine.complete_task.assert_called_once_with(
            "wf-123", "task-123", [sample_output]
        )

    def test_process_task_long_running_fails_during_poll(
        self, worker, mock_engine, mock_client, sample_task
    ):
        """Long-running task fails during polling."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="running", task_id="svc-task-456"
        )
        mock_client.get_status.return_value = StatusResponse(
            status="failed", error="Out of memory"
        )

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once()
        args = mock_engine.fail_task.call_args[0]
        assert "Out of memory" in args[2]


class TestProcessTaskFailed:
    """Tests for failed task handling."""

    def test_process_task_service_returns_failed(
        self, worker, mock_engine, mock_client, sample_task
    ):
        """Service returns failed status."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="failed", error="Invalid input"
        )

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once_with(
            "wf-123", "task-123", "Invalid input"
        )
        mock_engine.complete_task.assert_not_called()

    def test_process_task_connection_error(
        self, worker, mock_engine, mock_client, sample_task
    ):
        """Connection error fails task."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.side_effect = ControlClientError("Connection refused")

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once()
        args = mock_engine.fail_task.call_args[0]
        assert "Connection refused" in args[2]


class TestProcessTaskNoTask:
    """Tests for empty queue."""

    def test_process_task_no_task(self, worker, mock_engine):
        """Queue empty returns False."""
        mock_engine.claim_task.return_value = None

        result = worker.process_task("wf-123")

        assert result is False
        mock_engine.complete_task.assert_not_called()
        mock_engine.fail_task.assert_not_called()


class TestProcessTaskWithInputs:
    """Tests for task with input references."""

    def test_process_task_with_inputs(
        self, worker, mock_engine, mock_client, sample_output
    ):
        """Task inputs are passed to service."""
        input_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/input.csv",
            format=Format.CSV,
        )
        task = create_task(input_refs=[input_ref])
        mock_engine.claim_task.return_value = task
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        worker.process_task("wf-123")

        call_args = mock_client.execute.call_args
        assert call_args.kwargs["inputs"] == [input_ref]


class TestProcessTaskEndpointNotFound:
    """Tests for missing endpoint."""

    def test_process_task_endpoint_not_found(self, worker, mock_engine):
        """Unknown container fails task."""
        task = create_task(node_key="unknown-service:DoSomething")
        mock_engine.claim_task.return_value = task

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once()
        args = mock_engine.fail_task.call_args[0]
        assert "Endpoint not found" in args[2]


class TestProcessTaskInvalidNodeKey:
    """Tests for invalid node_key format."""

    def test_process_task_invalid_node_key_no_colon(self, worker, mock_engine):
        """Node key without colon fails task."""
        task = create_task(node_key="invalid-node-key")
        mock_engine.claim_task.return_value = task

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once()
        args = mock_engine.fail_task.call_args[0]
        assert "Invalid node_key" in args[2]

    def test_process_task_invalid_node_key_empty_parts(self, worker, mock_engine):
        """Node key with empty parts fails task."""
        task = create_task(node_key=":EmptyContainer")
        mock_engine.claim_task.return_value = task

        result = worker.process_task("wf-123")

        assert result is True
        mock_engine.fail_task.assert_called_once()


class TestProcessTaskValidation:
    """Tests for input validation."""

    def test_process_task_empty_workflow_id_raises(self, worker):
        """Empty workflow_id raises error."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            worker.process_task("")

    def test_process_task_whitespace_workflow_id_raises(self, worker):
        """Whitespace workflow_id raises error."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            worker.process_task("   ")


class TestRun:
    """Tests for run loop."""

    def test_run_processes_all_tasks(
        self, worker, mock_engine, mock_client, sample_output
    ):
        """Run processes tasks until complete."""
        task1 = create_task(task_id="task-1", node_key="service-a:Op1")
        task2 = create_task(task_id="task-2", node_key="service-a:Op2")

        mock_engine.claim_task.side_effect = [task1, task2]
        mock_engine.is_workflow_complete.side_effect = [False, False, True]
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        worker.run("wf-123")

        # Loop: check complete (False) -> process task1 -> check complete (False) -> process task2 -> check complete (True) -> exit
        assert mock_engine.claim_task.call_count == 2
        assert mock_engine.complete_task.call_count == 2

    def test_run_stops_on_workflow_complete(self, worker, mock_engine):
        """Run exits when workflow complete."""
        mock_engine.is_workflow_complete.return_value = True

        worker.run("wf-123")

        mock_engine.claim_task.assert_not_called()

    def test_run_empty_workflow_id_raises(self, worker):
        """Empty workflow_id raises error."""
        with pytest.raises(ValueError, match="workflow_id is required"):
            worker.run("")


class TestServiceCall:
    """Tests for correct service call parameters."""

    def test_correct_base_url_constructed(
        self, worker, mock_engine, mock_client, sample_task, sample_output
    ):
        """Base URL is constructed from endpoint."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        worker.process_task("wf-123")

        call_args = mock_client.execute.call_args
        assert call_args.kwargs["base_url"] == "http://service-a:50051"

    def test_correct_method_extracted(
        self, worker, mock_engine, mock_client, sample_task, sample_output
    ):
        """Method is extracted from node_key."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        worker.process_task("wf-123")

        call_args = mock_client.execute.call_args
        assert call_args.kwargs["method"] == "ProcessData"

    def test_workflow_and_task_id_passed(
        self, worker, mock_engine, mock_client, sample_task, sample_output
    ):
        """Workflow and task IDs are passed to service."""
        mock_engine.claim_task.return_value = sample_task
        mock_client.execute.return_value = ExecuteResponse(
            status="complete", output=sample_output
        )

        worker.process_task("wf-123")

        call_args = mock_client.execute.call_args
        assert call_args.kwargs["workflow_id"] == "wf-123"
        assert call_args.kwargs["task_id"] == "task-123"
