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
    # Waiting for an unhealthy service.
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


HISTORY_COUNTERS = ("claims", "reclaims", "deferrals", "retries")


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
    claims: int = 0  
    reclaims: int = 0  
    deferrals: int = 0  
    retries: int = 0  


def history_totals(tasks) -> dict:
    """The tasks' histories added up, and whether the workflow ran clean.
    """
    tasks = list(tasks)
    totals = {c: sum(getattr(t, c) for t in tasks) for c in HISTORY_COUNTERS}
    totals["clean"] = all(t.claims <= 1 for t in tasks) and not (
        totals["reclaims"] or totals["deferrals"] or totals["retries"]
    )
    return totals
