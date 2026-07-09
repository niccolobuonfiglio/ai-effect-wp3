"""Integration tests for ControlClient with real HTTP service."""

import pytest

from models.data_reference import DataReference, Format, Protocol
from services.control_client import ControlClient, ControlClientError

# Import test service runner
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "fixtures"))
from test_service import ControlServiceRunner


@pytest.fixture(scope="module")
def test_service():
    """Start test service for module."""
    runner = ControlServiceRunner(port=18081)
    runner.start()
    yield runner
    runner.stop()


@pytest.fixture
def client():
    return ControlClient(timeout=5.0)


@pytest.fixture
def base_url(test_service):
    return test_service.base_url


@pytest.fixture(autouse=True)
def reset_service(test_service):
    """Reset service state before each test."""
    test_service.reset()


class TestExecuteRealService:
    """Tests for execute with real HTTP service."""

    def test_execute_quick_operation(self, client, base_url):
        """Execute quick operation returns complete immediately."""
        response = client.execute(
            base_url=base_url,
            method="_test_quick",
            workflow_id="wf-123",
            task_id="task-456",
        )

        assert response.status == "complete"
        assert response.output is not None
        assert response.output.protocol == Protocol.S3
        assert "task-456" in response.output.uri

    def test_execute_long_running_operation(self, client, base_url):
        """Execute long-running operation returns running."""
        response = client.execute(
            base_url=base_url,
            method="_test_long_running",
            workflow_id="wf-123",
            task_id="task-456",
        )

        assert response.status == "running"
        assert response.task_id is not None
        assert response.task_id.startswith("svc-")

    def test_execute_failing_operation(self, client, base_url):
        """Execute failing operation returns failed."""
        response = client.execute(
            base_url=base_url,
            method="_test_failing",
            workflow_id="wf-123",
            task_id="task-456",
        )

        assert response.status == "failed"
        assert response.error is not None

    def test_execute_with_inputs(self, client, base_url):
        """Execute with inputs sends data correctly."""
        input_ref = DataReference(
            protocol=Protocol.S3,
            uri="s3://bucket/input.csv",
            format=Format.CSV,
        )

        response = client.execute(
            base_url=base_url,
            method="_test_quick",
            workflow_id="wf-123",
            task_id="task-456",
            inputs=[input_ref],
        )

        assert response.status == "complete"


class TestLongRunningTaskFlow:
    """Tests for long-running task execution flow."""

    def test_long_running_flow_execute_poll_output(self, client, base_url):
        """Complete flow: execute -> poll status -> get output."""
        # Start long-running task
        exec_response = client.execute(
            base_url=base_url,
            method="_test_long_running",
            workflow_id="wf-123",
            task_id="task-789",
        )

        assert exec_response.status == "running"
        service_task_id = exec_response.task_id

        # Poll status until complete
        status_response = client.get_status(base_url, service_task_id)
        assert status_response.status in ["running", "complete"]
        assert status_response.progress is not None

        # Poll again to complete
        status_response = client.get_status(base_url, service_task_id)
        assert status_response.status == "complete"

        # Get output
        output_response = client.get_output(base_url, service_task_id)
        assert output_response.output is not None
        assert output_response.output.protocol == Protocol.S3


class TestErrorHandling:
    """Tests for error handling with real service."""

    def test_status_not_found(self, client, base_url):
        """Status for unknown task raises error."""
        with pytest.raises(ControlClientError, match="HTTP 404"):
            client.get_status(base_url, "nonexistent-task")

    def test_output_not_found(self, client, base_url):
        """Output for unknown task raises error."""
        with pytest.raises(ControlClientError, match="HTTP 404"):
            client.get_output(base_url, "nonexistent-task")

    def test_connection_refused(self, client):
        """Connection to unavailable service raises error."""
        with pytest.raises(ControlClientError, match="Connection failed"):
            client.execute(
                base_url="http://localhost:19999",
                method="SyncTask",
                workflow_id="wf-123",
                task_id="task-456",
            )
