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
    # Waiting for an unhealthy service; re-queued automatically after a delay
    # or immediately when the service is manually recovered.
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


# A task's history (TaskState), shown per task and added up per workflow
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
    # How the task got where it is (docs/SCHEDULING.md section 12). Counted
    # in the same transaction as the transition, and never reset.
    claims: int = 0  # times a worker took it
    reclaims: int = 0  # times it was taken back from a worker whose lease expired
    deferrals: int = 0  # times it was parked because its service was unavailable
    retries: int = 0  # times an operator re-queued it after it failed


def history_totals(tasks) -> dict:
    """The tasks' histories added up, and whether the workflow ran clean.

    Clean means one claim per task and nothing else: nothing was deferred,
    taken back from a worker or retried.
    """
    tasks = list(tasks)
    totals = {c: sum(getattr(t, c) for t in tasks) for c in HISTORY_COUNTERS}
    totals["clean"] = all(t.claims <= 1 for t in tasks) and not (
        totals["reclaims"] or totals["deferrals"] or totals["retries"]
    )
    return totals
