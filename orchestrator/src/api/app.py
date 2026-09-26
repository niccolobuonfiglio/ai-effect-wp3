"""FastAPI REST API for orchestration platform."""

import hmac
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
    # Constant-time comparison, so response timing does not reveal the key
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
    TaskListResponse,
    TaskStatusResponse,
    WorkflowStatusResponse,
    WorkflowSubmitRequest,
    WorkflowSubmitResponse,
)
from models.data_reference import DataReference
from services.blueprint_parser import BlueprintParseError, BlueprintParser
from services.circuit_breaker import CircuitBreaker
from services.control_client import AUTH_REFUSED, ControlClient, parse_tls_verify
from services.dockerinfo_parser import DockerInfoParseError, DockerInfoParser
from services.log_reader import LogReader
from services.privacy import DataPolicy, DataPolicyError
from services.secrets import SecretBox, services_key_name
from services.service_registry import ServiceRegistry
from services.state_store import WorkflowNotFoundError, TaskNotFoundError
from services.workflow_catalog import WorkflowCatalog
from services.workflow_engine import WorkflowEngine

# The dashboard page served at /ui (a single static HTML file).
UI_DIR = Path(__file__).resolve().parent.parent / "ui"

# Seconds to wait for each service when checking credentials at submission
SERVICE_AUTH_CHECK_TIMEOUT = 3.0


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
    ):
        """Initialize API with dependencies.

        ``service_auth_check`` asks every service, when a workflow is
        submitted, whether it accepts the workflow's services API key. None
        reads SERVICE_AUTH_CHECK (default on).
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

    def _check_service_access(
        self, endpoints: dict, services_api_key: str | None
    ) -> None:
        """Refuse a workflow whose services would reject its credentials.

        Without this the mistake only shows when the first task runs, as a
        failed workflow. Services that cannot be reached are let through: an
        outage is the circuit breaker's business and may be over by the time
        their task runs.
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
            metadata=reference.metadata,
        )

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

            # Parse dockerinfo
            try:
                endpoints = self._dockerinfo_parser.parse_json(request.dockerinfo)
            except DockerInfoParseError as e:
                raise HTTPException(status_code=400, detail=f"Invalid dockerinfo: {e}")

            # Before anything is stored: would the services accept the key?
            self._check_service_access(endpoints, request.services_api_key)

            # Generate workflow ID
            workflow_id = f"wf-{uuid.uuid4().hex[:12]}"

            # Store services API key for worker to use when calling services
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

            # Register services so they show up in the health dashboard
            # before any task has run against them.
            self._registry.register_all(endpoints, workflow_id=workflow_id)

            # Parse initial inputs for start nodes
            initial_inputs: list[DataReference] | None = None
            if request.inputs:
                try:
                    initial_inputs = [DataReference(**inp) for inp in request.inputs]
                except Exception as e:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid inputs: {e}"
                    )
                try:
                    self._policy.check_all(initial_inputs)
                except DataPolicyError as e:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid inputs: {e}"
                    )

            # Initialize and start workflow
            self._engine.initialize_workflow(workflow_id, graph)
            # Record it for the dashboard: the list view and, since the engine
            # keeps no blueprint, the graph view.
            self._catalog.register(workflow_id, request.blueprint)
            self._engine.start_workflow(workflow_id, initial_inputs)

            return WorkflowSubmitResponse(workflow_id=workflow_id, status="running")

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
            task_responses = [
                TaskStatusResponse(
                    task_id=task.task_id,
                    node_key=task.node_key,
                    status=task.status.value,
                    created_at=task.created_at,
                    updated_at=task.updated_at,
                    error=task.error,
                    input_refs=[
                        self._reference_response(r)
                        for r in task.input_refs
                    ],
                    output_refs=[
                        self._reference_response(r)
                        for r in task.output_refs
                    ],
                )
                for task in tasks
            ]

            return TaskListResponse(workflow_id=workflow_id, tasks=task_responses)

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

            return TaskStatusResponse(
                task_id=task.task_id,
                node_key=task.node_key,
                status=task.status.value,
                created_at=task.created_at,
                updated_at=task.updated_at,
                error=task.error,
                input_refs=[
                    self._reference_response(r)
                    for r in task.input_refs
                ],
                output_refs=[
                    self._reference_response(r)
                    for r in task.output_refs
                ],
            )

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

            # Delete endpoints and the services API key
            self._redis.delete(f"endpoints:{workflow_id}")
            self._redis.delete(services_key_name(workflow_id))

            # Remove it from the dashboard's catalog
            self._catalog.forget(workflow_id)

            # Clear queue
            self._engine._task_queue.clear_queue(workflow_id)

            return {"status": "deleted"}

        @app.get("/health", response_model=HealthResponse)
        def health_check() -> HealthResponse:
            """Health check endpoint."""
            return HealthResponse(status="ok")

        # Service health awareness & manual recovery routes
        app.include_router(
            create_health_router(
                self._registry,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        # Resume a failed workflow / retry a failed task
        app.include_router(
            create_workflow_router(
                self._engine,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        # Dashboard data routes (list, graph, logs, alerts)
        app.include_router(
            create_dashboard_router(
                self._engine,
                self._catalog,
                self._registry,
                log_reader=self._log_reader,
                dependencies=[Depends(_verify_orchestrator_key)],
            )
        )

        # Dashboard page. It holds no data itself; every call it makes goes
        # through the routes above and needs the API key when one is set.
        if UI_DIR.is_dir():
            app.mount("/ui", StaticFiles(directory=str(UI_DIR), html=True), name="ui")

        return app
