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
    metadata: dict[str, Any] = {}


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


class TaskListResponse(BaseModel):
    """Response for task list."""

    model_config = ConfigDict(frozen=True)

    workflow_id: str
    tasks: list[TaskStatusResponse]


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
