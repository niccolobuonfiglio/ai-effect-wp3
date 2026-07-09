"""Unit tests for REST API."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.app import OrchestratorAPI
from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus
from services.blueprint_parser import BlueprintParseError
from services.dockerinfo_parser import DockerInfoParseError
from services.state_store import WorkflowNotFoundError, TaskNotFoundError


def create_mock_workflow(
    workflow_id: str = "wf-123",
    status: WorkflowStatus = WorkflowStatus.RUNNING,
) -> WorkflowState:
    """Create mock WorkflowState."""
    now = datetime.now()
    return WorkflowState(
        workflow_id=workflow_id,
        status=status,
        created_at=now,
        updated_at=now,
    )


def create_mock_task(
    task_id: str = "task-123",
    workflow_id: str = "wf-123",
    node_key: str = "service-a:ProcessData",
    status: TaskStatus = TaskStatus.PENDING,
) -> TaskState:
    """Create mock TaskState."""
    now = datetime.now()
    return TaskState(
        task_id=task_id,
        workflow_id=workflow_id,
        node_key=node_key,
        status=status,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_blueprint_parser():
    return MagicMock()


@pytest.fixture
def mock_dockerinfo_parser():
    return MagicMock()


@pytest.fixture
def mock_redis():
    return MagicMock()


@pytest.fixture
def api(mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis):
    return OrchestratorAPI(
        mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis
    )


@pytest.fixture
def client(api):
    app = api.create_app()
    return TestClient(app)


@pytest.fixture
def valid_blueprint():
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
    return {
        "docker_info_list": [
            {
                "container_name": "service-a",
                "ip_address": "service-a",
                "port": "50051",
            }
        ]
    }


class TestOrchestratorAPIInit:
    """Tests for OrchestratorAPI initialization."""

    def test_init_valid(
        self, mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis
    ):
        """Create API with valid args."""
        api = OrchestratorAPI(
            mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis
        )
        assert api._engine == mock_engine

    def test_init_none_engine_raises(
        self, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis
    ):
        """None engine raises error."""
        with pytest.raises(ValueError, match="engine is required"):
            OrchestratorAPI(
                None, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis
            )

    def test_init_none_blueprint_parser_raises(
        self, mock_engine, mock_dockerinfo_parser, mock_redis
    ):
        """None blueprint_parser raises error."""
        with pytest.raises(ValueError, match="blueprint_parser is required"):
            OrchestratorAPI(mock_engine, None, mock_dockerinfo_parser, mock_redis)

    def test_init_none_dockerinfo_parser_raises(
        self, mock_engine, mock_blueprint_parser, mock_redis
    ):
        """None dockerinfo_parser raises error."""
        with pytest.raises(ValueError, match="dockerinfo_parser is required"):
            OrchestratorAPI(mock_engine, mock_blueprint_parser, None, mock_redis)

    def test_init_none_redis_raises(
        self, mock_engine, mock_blueprint_parser, mock_dockerinfo_parser
    ):
        """None redis_client raises error."""
        with pytest.raises(ValueError, match="redis_client is required"):
            OrchestratorAPI(
                mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, None
            )


class TestSubmitWorkflow:
    """Tests for POST /workflows."""

    def test_submit_workflow_success(
        self,
        client,
        mock_engine,
        mock_blueprint_parser,
        mock_dockerinfo_parser,
        valid_blueprint,
        valid_dockerinfo,
    ):
        """Submit valid workflow returns workflow_id."""
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.return_value = {}

        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 200
        data = response.json()
        assert "workflow_id" in data
        assert data["workflow_id"].startswith("wf-")
        assert data["status"] == "running"
        mock_engine.initialize_workflow.assert_called_once()
        mock_engine.start_workflow.assert_called_once()

    def test_submit_workflow_with_inputs(
        self,
        client,
        mock_engine,
        mock_blueprint_parser,
        mock_dockerinfo_parser,
        valid_blueprint,
        valid_dockerinfo,
    ):
        """Submit workflow with initial inputs passes them to engine."""
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.return_value = {}

        inputs = [
            {"protocol": "grpc", "uri": "upstream:50051", "format": "GetConfiguration"}
        ]

        response = client.post(
            "/workflows",
            json={
                "blueprint": valid_blueprint,
                "dockerinfo": valid_dockerinfo,
                "inputs": inputs,
            },
        )

        assert response.status_code == 200
        # Verify start_workflow was called with initial_inputs
        call_args = mock_engine.start_workflow.call_args
        assert call_args is not None
        # Check the initial_inputs argument
        initial_inputs = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("initial_inputs")
        assert initial_inputs is not None
        assert len(initial_inputs) == 1
        assert initial_inputs[0].uri == "upstream:50051"

    def test_submit_workflow_with_invalid_inputs(
        self,
        client,
        mock_blueprint_parser,
        mock_dockerinfo_parser,
        valid_blueprint,
        valid_dockerinfo,
    ):
        """Submit workflow with invalid inputs returns 400."""
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.return_value = {}

        # Invalid input: missing required 'uri' field
        inputs = [{"protocol": "grpc", "format": "GetConfiguration"}]

        response = client.post(
            "/workflows",
            json={
                "blueprint": valid_blueprint,
                "dockerinfo": valid_dockerinfo,
                "inputs": inputs,
            },
        )

        assert response.status_code == 400
        assert "Invalid inputs" in response.json()["detail"]

    def test_submit_workflow_without_inputs(
        self,
        client,
        mock_engine,
        mock_blueprint_parser,
        mock_dockerinfo_parser,
        valid_blueprint,
        valid_dockerinfo,
    ):
        """Submit workflow without inputs passes None to engine."""
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.return_value = {}

        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 200
        # Verify start_workflow was called with None for initial_inputs
        call_args = mock_engine.start_workflow.call_args
        assert call_args is not None
        initial_inputs = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("initial_inputs")
        assert initial_inputs is None

    def test_submit_workflow_invalid_blueprint(
        self, client, mock_blueprint_parser, valid_blueprint, valid_dockerinfo
    ):
        """Invalid blueprint returns 400."""
        mock_blueprint_parser.parse_json.side_effect = BlueprintParseError(
            "Invalid structure"
        )

        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 400
        assert "Invalid blueprint" in response.json()["detail"]

    def test_submit_workflow_invalid_dockerinfo(
        self, client, mock_blueprint_parser, mock_dockerinfo_parser, valid_blueprint, valid_dockerinfo
    ):
        """Invalid dockerinfo returns 400."""
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.side_effect = DockerInfoParseError(
            "Invalid structure"
        )

        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 400
        assert "Invalid dockerinfo" in response.json()["detail"]

    def test_submit_workflow_empty_blueprint(self, client, valid_dockerinfo):
        """Empty blueprint returns 422."""
        response = client.post(
            "/workflows",
            json={"blueprint": {}, "dockerinfo": valid_dockerinfo},
        )

        assert response.status_code == 422

    def test_submit_workflow_empty_dockerinfo(self, client, valid_blueprint):
        """Empty dockerinfo returns 422."""
        response = client.post(
            "/workflows",
            json={"blueprint": valid_blueprint, "dockerinfo": {}},
        )

        assert response.status_code == 422


class TestGetWorkflowStatus:
    """Tests for GET /workflows/{workflow_id}."""

    def test_get_workflow_status_success(self, client, mock_engine):
        """Get status returns workflow state."""
        mock_engine.get_workflow_status.return_value = create_mock_workflow()

        response = client.get("/workflows/wf-123")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "wf-123"
        assert data["status"] == "running"

    def test_get_workflow_status_not_found(self, client, mock_engine):
        """Unknown workflow returns 404."""
        mock_engine.get_workflow_status.side_effect = WorkflowNotFoundError(
            "Workflow not found"
        )

        response = client.get("/workflows/wf-unknown")

        assert response.status_code == 404
        assert "Workflow not found" in response.json()["detail"]


class TestGetWorkflowTasks:
    """Tests for GET /workflows/{workflow_id}/tasks."""

    def test_get_workflow_tasks_success(self, client, mock_engine):
        """Get tasks returns task list."""
        mock_engine.get_workflow_status.return_value = create_mock_workflow()
        mock_engine.get_all_tasks.return_value = [
            create_mock_task(task_id="task-1"),
            create_mock_task(task_id="task-2"),
        ]

        response = client.get("/workflows/wf-123/tasks")

        assert response.status_code == 200
        data = response.json()
        assert data["workflow_id"] == "wf-123"
        assert len(data["tasks"]) == 2

    def test_get_workflow_tasks_not_found(self, client, mock_engine):
        """Unknown workflow returns 404."""
        mock_engine.get_workflow_status.side_effect = WorkflowNotFoundError(
            "Workflow not found"
        )

        response = client.get("/workflows/wf-unknown/tasks")

        assert response.status_code == 404


class TestGetTaskStatus:
    """Tests for GET /workflows/{workflow_id}/tasks/{task_id}."""

    def test_get_task_status_success(self, client, mock_engine):
        """Get task returns task state."""
        mock_engine._state_store.get_task.return_value = create_mock_task()

        response = client.get("/workflows/wf-123/tasks/task-123")

        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == "task-123"
        assert data["node_key"] == "service-a:ProcessData"

    def test_get_task_status_not_found(self, client, mock_engine):
        """Unknown task returns 404."""
        mock_engine._state_store.get_task.side_effect = TaskNotFoundError(
            "wf-123", "task-unknown"
        )

        response = client.get("/workflows/wf-123/tasks/task-unknown")

        assert response.status_code == 404


class TestDeleteWorkflow:
    """Tests for DELETE /workflows/{workflow_id}."""

    def test_delete_workflow_success(self, client, mock_engine):
        """Delete workflow returns success."""
        mock_engine.get_workflow_status.return_value = create_mock_workflow()

        response = client.delete("/workflows/wf-123")

        assert response.status_code == 200
        assert response.json()["status"] == "deleted"
        mock_engine._state_store.delete_workflow.assert_called_once_with("wf-123")

    def test_delete_workflow_not_found(self, client, mock_engine):
        """Unknown workflow returns 404."""
        mock_engine.get_workflow_status.side_effect = WorkflowNotFoundError(
            "Workflow not found"
        )

        response = client.delete("/workflows/wf-unknown")

        assert response.status_code == 404


class TestHealthCheck:
    """Tests for GET /health."""

    def test_health_check(self, client):
        """Health check returns ok."""
        response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "ok"
