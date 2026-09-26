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


class TestDataPolicyOnSubmission:
    """Inline payloads are capped at submission time."""

    def submit(self, client, blueprint, dockerinfo, inputs):
        return client.post(
            "/workflows",
            json={"blueprint": blueprint, "dockerinfo": dockerinfo, "inputs": inputs},
        )

    @pytest.fixture
    def parsers(self, mock_blueprint_parser, mock_dockerinfo_parser):
        mock_blueprint_parser.parse_json.return_value = MagicMock()
        mock_dockerinfo_parser.parse_json.return_value = {}

    def inline_input(self, size):
        import base64

        return {
            "protocol": "inline",
            "uri": base64.b64encode(b"x" * size).decode(),
            "format": "csv",
        }

    def test_small_inline_input_accepted(
        self, client, parsers, valid_blueprint, valid_dockerinfo
    ):
        response = self.submit(
            client, valid_blueprint, valid_dockerinfo, [self.inline_input(64)]
        )

        assert response.status_code == 200

    def test_oversized_inline_input_rejected(
        self,
        mock_engine,
        mock_blueprint_parser,
        mock_dockerinfo_parser,
        mock_redis,
        parsers,
        valid_blueprint,
        valid_dockerinfo,
    ):
        from services.privacy import DataPolicy

        api = OrchestratorAPI(
            mock_engine,
            mock_blueprint_parser,
            mock_dockerinfo_parser,
            mock_redis,
            policy=DataPolicy(max_inline_bytes=32),
        )
        client = TestClient(api.create_app())

        response = self.submit(
            client, valid_blueprint, valid_dockerinfo, [self.inline_input(500)]
        )

        assert response.status_code == 400
        assert "exceeds the limit" in response.json()["detail"]
        mock_engine.start_workflow.assert_not_called()


class TestInlineMasking:
    """Inline payloads are not echoed back through the API."""

    @pytest.fixture
    def task_with_inline(self):
        from models.data_reference import DataReference, Format

        task = create_mock_task(status=TaskStatus.COMPLETED)
        return task.model_copy(
            update={
                "input_refs": [
                    DataReference(protocol="file", uri="/shared/in.csv", format="csv")
                ],
                "output_refs": [
                    DataReference.from_inline_data(b"secret,payload\n" * 4, Format.CSV)
                ],
            }
        )

    def test_task_list_masks_inline_output(self, client, mock_engine, task_with_inline):
        mock_engine.get_workflow_status.return_value = create_mock_workflow()
        mock_engine.get_all_tasks.return_value = [task_with_inline]

        body = client.get("/workflows/wf-123/tasks").json()
        refs = body["tasks"][0]["output_refs"]

        assert refs[0]["uri"] == "<inline data, 60 bytes>"
        assert "secret" not in response_text(body)

    def test_task_list_keeps_locations(self, client, mock_engine, task_with_inline):
        mock_engine.get_workflow_status.return_value = create_mock_workflow()
        mock_engine.get_all_tasks.return_value = [task_with_inline]

        body = client.get("/workflows/wf-123/tasks").json()

        assert body["tasks"][0]["input_refs"][0]["uri"] == "/shared/in.csv"

    def test_single_task_masks_inline_output(
        self, client, mock_engine, task_with_inline
    ):
        mock_engine._state_store.get_task.return_value = task_with_inline

        body = client.get("/workflows/wf-123/tasks/task-123").json()

        assert body["output_refs"][0]["uri"].startswith("<inline data,")


def response_text(body) -> str:
    import json as _json

    return _json.dumps(body)


class TestServicesKeyStorage:
    """The per-workflow services key is encrypted before it reaches Redis."""

    def submit_with_key(self, client, blueprint, dockerinfo, key="super-secret-key"):
        return client.post(
            "/workflows",
            json={
                "blueprint": blueprint,
                "dockerinfo": dockerinfo,
                "services_api_key": key,
            },
        )

    def build(self, mock_engine, bp, di, redis, **kwargs):
        bp.parse_json.return_value = MagicMock()
        di.parse_json.return_value = {}
        api = OrchestratorAPI(mock_engine, bp, di, redis, **kwargs)
        return TestClient(api.create_app())

    def stored_value(self, mock_redis):
        calls = [
            c for c in mock_redis.set.call_args_list
            if str(c[0][0]).startswith("services_key:")
        ]
        assert len(calls) == 1
        return calls[0][0][1]

    def test_key_encrypted_when_secret_configured(
        self, mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis,
        valid_blueprint, valid_dockerinfo,
    ):
        from services.secrets import PREFIX, SecretBox

        box = SecretBox("a-passphrase")
        client = self.build(
            mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis,
            secrets=box,
        )

        self.submit_with_key(client, valid_blueprint, valid_dockerinfo)
        stored = self.stored_value(mock_redis)

        assert stored.startswith(PREFIX)
        assert "super-secret-key" not in stored
        assert box.decrypt(stored) == "super-secret-key"

    def test_key_stored_plainly_without_secret(
        self, mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis,
        valid_blueprint, valid_dockerinfo,
    ):
        from services.secrets import SecretBox

        client = self.build(
            mock_engine, mock_blueprint_parser, mock_dockerinfo_parser, mock_redis,
            secrets=SecretBox(None),
        )

        self.submit_with_key(client, valid_blueprint, valid_dockerinfo)

        assert self.stored_value(mock_redis) == "super-secret-key"


class TestOrchestratorKeyComparison:
    """The orchestrator key check (now constant-time) still decides correctly."""

    @pytest.fixture(autouse=True)
    def key(self, monkeypatch, mock_engine):
        monkeypatch.setenv("ORCHESTRATOR_API_KEY", "s3cret-key")
        mock_engine.get_workflow_status.return_value = create_mock_workflow()

    def test_correct_key_accepted(self, client):
        response = client.get(
            "/workflows/wf-123", headers={"Authorization": "Bearer s3cret-key"}
        )
        assert response.status_code == 200

    @pytest.mark.parametrize("token", ["s3cret-kez", "s3cret", "s3cret-key-and-more", ""])
    def test_other_keys_refused(self, client, token):
        response = client.get(
            "/workflows/wf-123", headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 401

    def test_missing_key_refused(self, client):
        assert client.get("/workflows/wf-123").status_code == 401


class TestServiceAccessCheck:
    """At submission the services are asked whether they accept the key."""

    URL = "http://service-a:50051/control/execute"

    def build(self, mock_engine, redis, **kwargs):
        from services.dockerinfo_parser import DockerInfoParser

        bp = MagicMock()
        bp.parse_json.return_value = MagicMock()
        api = OrchestratorAPI(mock_engine, bp, DockerInfoParser(), redis, **kwargs)
        return TestClient(api.create_app())

    def submit(self, client, blueprint, dockerinfo, key=None):
        body = {"blueprint": blueprint, "dockerinfo": dockerinfo}
        if key:
            body["services_api_key"] = key
        return client.post("/workflows", json=body)

    def test_refused_key_rejects_submission_before_storing(
        self, httpx_mock, mock_engine, mock_redis, valid_blueprint, valid_dockerinfo
    ):
        httpx_mock.add_response(url=self.URL, method="POST", status_code=401)
        client = self.build(mock_engine, mock_redis)

        response = self.submit(client, valid_blueprint, valid_dockerinfo, key="wrong")

        assert response.status_code == 400
        assert "service-a" in response.json()["detail"]
        assert "not accepted" in response.json()["detail"]
        assert httpx_mock.get_request().headers["Authorization"] == "Bearer wrong"
        mock_engine.initialize_workflow.assert_not_called()
        mock_redis.set.assert_not_called()

    def test_missing_key_is_named_as_the_cause(
        self, httpx_mock, mock_engine, mock_redis, valid_blueprint, valid_dockerinfo
    ):
        httpx_mock.add_response(url=self.URL, method="POST", status_code=403)
        client = self.build(mock_engine, mock_redis)

        response = self.submit(client, valid_blueprint, valid_dockerinfo)

        assert response.status_code == 400
        assert "no services_api_key was given" in response.json()["detail"]
        assert "Authorization" not in httpx_mock.get_request().headers

    def test_accepted_key_submits(
        self, httpx_mock, mock_engine, mock_redis, valid_blueprint, valid_dockerinfo
    ):
        httpx_mock.add_response(url=self.URL, method="POST", status_code=422)
        client = self.build(mock_engine, mock_redis)

        response = self.submit(client, valid_blueprint, valid_dockerinfo, key="right")

        assert response.status_code == 200
        mock_engine.start_workflow.assert_called_once()

    def test_unreachable_service_does_not_block_submission(
        self, httpx_mock, mock_engine, mock_redis, valid_blueprint, valid_dockerinfo
    ):
        import httpx

        httpx_mock.add_exception(httpx.ConnectError("refused"), url=self.URL)
        client = self.build(mock_engine, mock_redis)

        response = self.submit(client, valid_blueprint, valid_dockerinfo)

        assert response.status_code == 200

    def test_check_can_be_turned_off(
        self, httpx_mock, mock_engine, mock_redis, valid_blueprint, valid_dockerinfo
    ):
        client = self.build(mock_engine, mock_redis, service_auth_check=False)

        response = self.submit(client, valid_blueprint, valid_dockerinfo)

        assert response.status_code == 200
        assert httpx_mock.get_requests() == []

    def test_check_off_via_environment(
        self, monkeypatch, mock_engine, mock_redis
    ):
        monkeypatch.setenv("SERVICE_AUTH_CHECK", "false")
        api = OrchestratorAPI(mock_engine, MagicMock(), MagicMock(), mock_redis)
        assert api._service_auth_check is False


class TestDeleteRemovesServicesKey:
    def test_delete_removes_services_key(self, client, mock_engine, mock_redis):
        mock_engine.get_workflow_status.return_value = create_mock_workflow()

        client.delete("/workflows/wf-123")

        deleted = [c[0][0] for c in mock_redis.delete.call_args_list]
        assert "services_key:wf-123" in deleted
