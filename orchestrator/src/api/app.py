"""FastAPI REST API for orchestration platform."""

import hmac
import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from redis import Redis

_bearer = HTTPBearer(auto_error=False)


def _verify_orchestrator_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> None:
    api_key = os.environ.get("ORCHESTRATOR_API_KEY")
    if not api_key:
        return
    if not credentials or not hmac.compare_digest(
        credentials.credentials.encode("utf-8"), api_key.encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

from api.dashboard_routes import create_dashboard_router
from api.health_routes import create_health_router
from api.workflow_routes import create_workflow_router
from api.models import (
    DataReferenceResponse,
    ErrorResponse,
    HealthResponse,
    TaskHistoryResponse,
    TaskListResponse,
    TaskStatusResponse,
    WorkflowHistoryResponse,
    WorkflowStatusResponse,
    WorkflowSubmitRequest,
    WorkflowSubmitResponse,
)
from models.data_reference import DataReference
from models.state import HISTORY_COUNTERS, TaskState, history_totals
from services.blueprint_parser import (
    BlueprintParseError,
    BlueprintParser,
    message_type_mismatches,
)
from services.circuit_breaker import CircuitBreaker
from services.control_client import AUTH_REFUSED, ControlClient, parse_tls_verify
from services.dockerinfo_parser import DockerInfoParseError, DockerInfoParser
from services.integrity import IntegrityError, verify_all
from services.log_reader import LogReader
from services.privacy import DataPolicy, DataPolicyError
from services.secrets import SecretBox, services_key_name
from services.service_registry import ServiceRegistry
from services.state_store import WorkflowNotFoundError, TaskNotFoundError
from services.workflow_catalog import WorkflowCatalog
from services.workflow_engine import WorkflowEngine

UI_DIR = Path(__file__).resolve().parent.parent / "ui"

SERVICE_AUTH_CHECK_TIMEOUT = 3.0

TYPE_CHECK_MODES = ("strict", "warn", "off")

logger = logging.getLogger(__name__)


class OrchestratorAPI:
    """REST API for orchestration platform."""

    def __init__(
        self,
        engine: WorkflowEngine,
        blueprint_parser: BlueprintParser,
        dockerinfo_parser: DockerInfoParser,
        redis_client: Redis,
        breaker: CircuitBreaker | None = None,
        registry: ServiceRegistry | None = None,
        policy: DataPolicy | None = None,
        secrets: SecretBox | None = None,
        catalog: WorkflowCatalog | None = None,
        log_reader: LogReader | None = None,
        service_auth_check: bool | None = None,
        type_check: str | None = None,
    ):
        """Initialize API with dependencies.
        """
        if engine is None:
            raise ValueError("engine is required")
        if blueprint_parser is None:
            raise ValueError("blueprint_parser is required")
        if dockerinfo_parser is None:
            raise ValueError("dockerinfo_parser is required")
        if redis_client is None:
            raise ValueError("redis_client is required")

        self._engine = engine
        self._blueprint_parser = blueprint_parser
        self._dockerinfo_parser = dockerinfo_parser
        self._redis = redis_client
        self._breaker = breaker or CircuitBreaker(redis_client)
        self._registry = registry or ServiceRegistry(
            redis_client, self._breaker, engine
        )
        self._policy = policy or DataPolicy.from_env()
        self._secrets = secrets or SecretBox.from_env()
        self._catalog = catalog or WorkflowCatalog(redis_client)
        self._log_reader = log_reader or LogReader()
        if service_auth_check is None:
            service_auth_check = os.environ.get(
                "SERVICE_AUTH_CHECK", "true"
            ).strip().lower() not in ("0", "false", "no", "off")
        self._service_auth_check = service_auth_check
        if type_check is None:
            type_check = os.environ.get("BLUEPRINT_TYPE_CHECK", "warn")
        type_check = type_check.strip().lower()
        if type_check not in TYPE_CHECK_MODES:
            raise ValueError(
                f"type_check must be one of: {', '.join(TYPE_CHECK_MODES)}"
            )
        self._type_check = type_check

    def _check_service_access(
        self, endpoints: dict, services_api_key: str | None
    ) -> None:
        """Refuse a workflow whose services would reject its credentials.
        """
        names = list(endpoints or {})
        if not self._service_auth_check or not names:
            return

        client = ControlClient(
            timeout=SERVICE_AUTH_CHECK_TIMEOUT,
            api_key=services_api_key,
            verify=parse_tls_verify(os.environ.get("SERVICE_TLS_VERIFY")),
        )
        with ThreadPoolExecutor(max_workers=min(8, len(names))) as pool:
            codes = pool.map(
                lambda name: client.check_access(endpoints[name].base_url), names
            )
            refused = sorted(
                name for name, code in zip(names, codes) if code in AUTH_REFUSED
            )

        if refused:
            reason = (
                "the services_api_key was not accepted"
                if services_api_key
                else "no services_api_key was given"
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Services refused the orchestrator's credentials "
                    f"({reason}): {', '.join(refused)}"
                ),
            )

    def _reference_response(self, reference: DataReference) -> "DataReferenceResponse":
        """Build an API view of a reference, masking inline payloads."""
        return DataReferenceResponse(
            protocol=reference.protocol.value,
            uri=self._policy.masked_uri(reference),
            format=(
                reference.format
                if isinstance(reference.format, str)
                else reference.format.value
            ),
            checksum=reference.checksum,
            size_bytes=reference.size_bytes,
            metadata=reference.metadata,
        )

    def _task_response(self, task: TaskState) -> TaskStatusResponse:
        """Build the API view of a task, masking inline payloads."""
        return TaskStatusResponse(
            task_id=task.task_id,
            node_key=task.node_key,
            status=task.status.value,
            created_at=task.created_at,
            updated_at=task.updated_at,
            error=task.error,
            input_refs=[self._reference_response(r) for r in task.input_refs],
            output_refs=[self._reference_response(r) for r in task.output_refs],
            history=TaskHistoryResponse(
                **{counter: getattr(task, counter) for counter in HISTORY_COUNTERS}
            ),
        )

    def _check_message_types(self, graph) -> list[str]:
        """Apply the type check mode; return the warnings to report.
        """
        if self._type_check == "off":
            return []
        mismatches = message_type_mismatches(graph)
        if mismatches and self._type_check == "strict":
            raise HTTPException(
                status_code=400,
                detail=f"Incompatible connections: {'; '.join(mismatches)}",
            )
        for mismatch in mismatches:
            logger.warning(
                "Blueprint connection with mismatching message types: %s",
                mismatch,
                extra={"event": "blueprint_type_mismatch"},
            )
        return [f"Incompatible connection: {m}" for m in mismatches]

    def _parse_inputs(self, raw: list[dict]) -> list[DataReference] | None:
        """Validate the workflow's initial inputs or raise HTTPException(400)."""
        if not raw:
            return None
        try:
            inputs = [DataReference(**inp) for inp in raw]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid inputs: {e}")
        try:
            self._policy.check_all(inputs)
            verify_all(inputs)
        except (DataPolicyError, IntegrityError) as e:
            raise HTTPException(status_code=400, detail=f"Invalid inputs: {e}")
        return inputs

    def create_app(self) -> FastAPI:
        """Create FastAPI application."""
        app = FastAPI(
            title="Orchestrator API",
            description="REST API for AI-Effect orchestration platform",
            version="1.0.0",
        )

        @app.post(
            "/workflows",
            response_model=WorkflowSubmitResponse,
            responses={400: {"model": ErrorResponse}},
            dependencies=[Depends(_verify_orchestrator_key)],
        )
        def submit_workflow(request: WorkflowSubmitRequest) -> WorkflowSubmitResponse:
            """Submit a new workflow."""
            # Parse blueprint
            try:
                graph = self._blueprint_parser.parse_json(request.blueprint)
            except BlueprintParseError as e:
                raise HTTPException(status_code=400, detail=f"Invalid blueprint: {e}")

            warnings = self._check_message_types(graph)

            # Parse dockerinfo
            try:
                endpoints = self._dockerinfo_parser.parse_json(request.dockerinfo)
            except DockerInfoParseError as e:
                raise HTTPException(status_code=400, detail=f"Invalid dockerinfo: {e}")

            # Initial inputs for start nodes: policy and integrity
            initial_inputs = self._parse_inputs(request.inputs)

            self._check_service_access(endpoints, request.services_api_key)

            # Generate workflow ID
            workflow_id = f"wf-{uuid.uuid4().hex[:12]}"

            if request.services_api_key:
                self._redis.set(
                    services_key_name(workflow_id),
                    self._secrets.encrypt(request.services_api_key),
                )

            # Store endpoints for worker lookup
            endpoints_key = f"endpoints:{workflow_id}"
            endpoints_data = {
                name: endpoint.model_dump_json()
                for name, endpoint in endpoints.items()
            }
            if endpoints_data:
                self._redis.hset(endpoints_key, mapping=endpoints_data)

            self._registry.register_all(endpoints, workflow_id=workflow_id)

            # Initialize and start workflow
            self._engine.initialize_workflow(workflow_id, graph)
            self._catalog.register(workflow_id, request.blueprint)
            self._engine.start_workflow(workflow_id, initial_inputs)

            return WorkflowSubmitResponse(
                workflow_id=workflow_id, status="running", warnings=warnings
            )

        @app.get(
            "/workflows/{workflow_id}",
            response_model=WorkflowStatusResponse,
            responses={404: {"model": ErrorResponse}},
            dependencies=[Depends(_verify_orchestrator_key)],
        )
        def get_workflow_status(workflow_id: str) -> WorkflowStatusResponse:
            """Get workflow status."""
            try:
                state = self._engine.get_workflow_status(workflow_id)
            except WorkflowNotFoundError:
                raise HTTPException(status_code=404, detail="Workflow not found")

            return WorkflowStatusResponse(
                workflow_id=state.workflow_id,
                status=state.status.value,
                created_at=state.created_at,
                updated_at=state.updated_at,
                error=state.error,
            )

        @app.get(
            "/workflows/{workflow_id}/tasks",
            response_model=TaskListResponse,
            responses={404: {"model": ErrorResponse}},
            dependencies=[Depends(_verify_orchestrator_key)],
        )
        def get_workflow_tasks(workflow_id: str) -> TaskListResponse:
            """Get all tasks for a workflow."""
            # Check workflow exists
            try:
                self._engine.get_workflow_status(workflow_id)
            except WorkflowNotFoundError:
                raise HTTPException(status_code=404, detail="Workflow not found")

            # Get all tasks
            tasks = self._engine.get_all_tasks(workflow_id)
            return TaskListResponse(
                workflow_id=workflow_id,
                tasks=[self._task_response(task) for task in tasks],
                history=WorkflowHistoryResponse(**history_totals(tasks)),
            )

        @app.get(
            "/workflows/{workflow_id}/tasks/{task_id}",
            response_model=TaskStatusResponse,
            responses={404: {"model": ErrorResponse}},
            dependencies=[Depends(_verify_orchestrator_key)],
        )
        def get_task_status(workflow_id: str, task_id: str) -> TaskStatusResponse:
            """Get task status."""
            try:
                task = self._engine._state_store.get_task(workflow_id, task_id)
            except (WorkflowNotFoundError, TaskNotFoundError):
                raise HTTPException(status_code=404, detail="Task not found")

            return self._task_response(task)

        @app.delete(
            "/workflows/{workflow_id}",
            responses={404: {"model": ErrorResponse}},
            dependencies=[Depends(_verify_orchestrator_key)],
        )
        def delete_workflow(workflow_id: str) -> dict:
            """Delete a workflow."""
            try:
                self._engine.get_workflow_status(workflow_id)
            except WorkflowNotFoundError:
                raise HTTPException(status_code=404, detail="Workflow not found")

            # Delete workflow state
            self._engine._state_store.delete_workflow(workflow_id)

            self._redis.delete(f"endpoints:{workflow_id}")
            self._redis.delete(services_key_name(workflow_id))

            self._catalog.forget(workflow_id)

            # Clear queue
            self._engine._task_queue.clear_queue(workflow_id)

            return {"status": "deleted"}

        @app.get("/health", response_model=HealthResponse)
        def health_check() -> HealthResponse:
            """Health check endpoint."""
            return HealthResponse(status="ok")

        app.include_router(
            create_health_router(
                self._registry,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        app.include_router(
            create_workflow_router(
                self._engine,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        app.include_router(
            create_dashboard_router(
                self._engine,
                self._catalog,
                self._registry,
                log_reader=self._log_reader,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        if UI_DIR.is_dir():
            app.mount("/ui", StaticFiles(directory=str(UI_DIR), html=True), name="ui")

        return app
