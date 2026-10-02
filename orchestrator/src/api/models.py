"""Request and response models for REST API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class WorkflowSubmitRequest(BaseModel):
    """Request to submit a workflow."""

    model_config = ConfigDict(extra="forbid")

    blueprint: dict
    dockerinfo: dict
    inputs: list[dict] = []  # Initial DataReference objects for start nodes
    services_api_key: str | None = None  # Optional bearer token sent to services

    @field_validator("blueprint")
    @classmethod
    def blueprint_not_empty(cls, v: dict) -> dict:
        if not v:
            raise ValueError("blueprint is required")
        return v

    @field_validator("dockerinfo")
    @classmethod
    def dockerinfo_not_empty(cls, v: dict) -> dict:
        if not v:
            raise ValueError("dockerinfo is required")
        return v


class WorkflowSubmitResponse(BaseModel):
    """Response from workflow submission."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    status: str
    # Problems that did not stop the submission, e.g. blueprint connections
    # whose message types do not match (BLUEPRINT_TYPE_CHECK=warn)
    warnings: list[str] = []


class WorkflowStatusResponse(BaseModel):
    """Response for workflow status."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    status: str
    created_at: datetime
    updated_at: datetime
    error: str | None = None


class DataReferenceResponse(BaseModel):
    """Slim representation of a DataReference for API responses."""

    model_config = ConfigDict(frozen=True)

    protocol: str
    uri: str
    format: str
    checksum: str | None = None
    size_bytes: int | None = None
    metadata: dict[str, Any] = {}


class TaskHistoryResponse(BaseModel):
    """How a task got where it is (docs/SCHEDULING.md section 12)."""

    model_config = ConfigDict(frozen=True)

    claims: int = 0
    reclaims: int = 0
    deferrals: int = 0
    retries: int = 0


class TaskStatusResponse(BaseModel):
    """Response for task status."""

    model_config = ConfigDict(frozen=True)

    task_id: str
    node_key: str
    status: str
    created_at: datetime
    updated_at: datetime
    error: str | None = None
    input_refs: list[DataReferenceResponse] = []
    output_refs: list[DataReferenceResponse] = []
    history: TaskHistoryResponse = TaskHistoryResponse()


class WorkflowHistoryResponse(TaskHistoryResponse):
    """The tasks' histories added up.

    ``clean`` means no task was claimed more than once, so nothing was
    deferred, taken back from a worker or retried. ``reclaims`` counts the
    times a service may have done a task's work twice (execution is
    at-least-once).
    """

    clean: bool = True


class TaskListResponse(BaseModel):
    """Response for task list."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    tasks: list[TaskStatusResponse]
    history: WorkflowHistoryResponse = WorkflowHistoryResponse()


class ErrorResponse(BaseModel):
    """Error response."""

    model_config = ConfigDict(frozen=True)

    detail: str


class HealthResponse(BaseModel):
    """Health check response."""

    model_config = ConfigDict(frozen=True)

    status: str


class ServiceHealthResponse(BaseModel):
    """Health of a single registered service."""

    model_config = ConfigDict(frozen=True)

    service_name: str
    base_url: str
    status: str  # HEALTHY | DEGRADED | UNHEALTHY
    circuit_state: str  # closed | half_open | open
    failure_count: int
    last_failure: str | None = None
    last_error: str | None = None
    last_success: str | None = None
    opened_at: str | None = None
    retry_after: str | None = None
    manually_recovered_at: str | None = None


class ServicesHealthResponse(BaseModel):
    """Aggregate health of all registered services."""

    model_config = ConfigDict(frozen=True)

    overall_status: str  # HEALTHY | DEGRADED | UNHEALTHY
    total: int
    healthy: int
    degraded: int
    unhealthy: int
    services: list[ServiceHealthResponse] = []


class RecoveryResponse(BaseModel):
    """Response from a manual service recovery."""

    model_config = ConfigDict(frozen=True)

    service_name: str
    status: str
    message: str
    resumed_tasks: int = 0


class DeregisterResponse(BaseModel):
    """Response from removing a service from the registry."""

    model_config = ConfigDict(frozen=True)

    service_name: str
    removed: bool
    message: str


class RegisteredServiceResponse(BaseModel):
    """Registration details for a known service."""

    model_config = ConfigDict(frozen=True)

    service_name: str
    address: str
    port: int
    base_url: str
    first_seen: str
    last_seen: str
    workflow_ids: list[str] = []


class WorkflowSummaryResponse(BaseModel):
    """One entry of the workflow list shown by the dashboard."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    name: str | None = None
    status: str
    created_at: datetime
    updated_at: datetime
    error: str | None = None


class WorkflowListResponse(BaseModel):
    """All known workflows, newest first."""

    model_config = ConfigDict(frozen=True)

    workflows: list[WorkflowSummaryResponse] = []
    total: int


class GraphNodeResponse(BaseModel):
    """A blueprint node with its task status and its service's health."""

    model_config = ConfigDict(frozen=True)

    key: str  # container:operation, the task's node_key
    container_name: str
    operation: str | None = None
    node_type: str | None = None
    status: str  # pending | running | blocked | completed | failed
    service_status: str | None = None  # HEALTHY | DEGRADED | UNHEALTHY
    circuit_state: str | None = None


class GraphEdgeResponse(BaseModel):
    """A connection between two nodes."""

    model_config = ConfigDict(frozen=True)

    source: str
    target: str


class WorkflowGraphResponse(BaseModel):
    """A workflow's graph with live status, for drawing."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    name: str | None = None
    status: str
    nodes: list[GraphNodeResponse] = []
    edges: list[GraphEdgeResponse] = []


class AlertListResponse(BaseModel):
    """Recent alerts from all orchestrator processes, newest first."""

    model_config = ConfigDict(frozen=True)

    alerts: list[dict[str, Any]] = []
    total: int


class VerificationCheckResponse(BaseModel):
    """One property checked on a workflow (docs/VERIFICATION.md)."""

    model_config = ConfigDict(frozen=True)

    name: str
    passed: bool
    problems: list[str] = []


class WorkflowVerificationResponse(BaseModel):
    """Whether a workflow's state in Redis has the properties the engine promises."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    status: str
    verified: bool
    checked_at: datetime
    checks: list[VerificationCheckResponse]
    history: WorkflowHistoryResponse


class WorkflowResumeResponse(BaseModel):
    """Result of resuming a failed workflow."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    status: str
    retried_tasks: list[str] = []
    message: str


class TaskRetryResponse(BaseModel):
    """Result of retrying a single failed task."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    task_id: str
    node_key: str
    status: str
    message: str
