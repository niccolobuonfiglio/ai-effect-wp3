"""State models for workflow and task tracking."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from models.data_reference import DataReference


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkflowState(BaseModel):
    """Persistent state of a workflow."""

    workflow_id: str
    status: WorkflowStatus
    created_at: datetime
    updated_at: datetime
    error: str | None = None


class TaskState(BaseModel):
    """Persistent state of a task."""

    task_id: str
    workflow_id: str
    node_key: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    input_refs: list[DataReference] = []
    output_refs: list[DataReference] = []
    error: str | None = None
