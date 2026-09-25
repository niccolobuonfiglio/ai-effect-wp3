"""Routes for restarting work that failed.

Exposed as a factory returning an ``APIRouter``, like the health routes, so
``OrchestratorAPI`` mounts them with the same API-key dependency.

A failed workflow used to be final: the only way forward was to submit it
again, re-running everything that had already succeeded. These two routes put
the failed tasks back on the queue instead, leaving completed tasks and their
outputs untouched.

Endpoints:
    POST /workflows/{workflow_id}/resume
    POST /workflows/{workflow_id}/tasks/{task_id}/retry
"""

import logging

from fastapi import APIRouter, HTTPException

from api.models import ErrorResponse, TaskRetryResponse, WorkflowResumeResponse
from services.state_store import TaskNotFoundError, WorkflowNotFoundError
from services.workflow_engine import NotFailedError, WorkflowEngine

logger = logging.getLogger(__name__)


def create_workflow_router(
    engine: WorkflowEngine,
    dependencies: list | None = None,
) -> APIRouter:
    """Build the resume/retry router bound to an engine.

    Args:
        engine: Workflow engine owning task and workflow state.
        dependencies: FastAPI dependencies applied to every route - pass the
            orchestrator API-key verifier here.
    """
    if engine is None:
        raise ValueError("engine is required")

    router = APIRouter(
        prefix="/workflows",
        tags=["workflows"],
        dependencies=list(dependencies or []),
    )

    @router.post(
        "/{workflow_id}/resume",
        response_model=WorkflowResumeResponse,
        summary="Re-queue the failed tasks of a failed workflow",
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    def resume_workflow(workflow_id: str) -> WorkflowResumeResponse:
        """Continue a failed workflow where it stopped.

        Completed tasks keep their outputs and are not run again, so this is
        the counterpart to resubmitting the whole workflow after fixing
        whatever made a task fail.
        """
        try:
            retried = engine.resume_workflow(workflow_id)
        except WorkflowNotFoundError:
            raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")
        except NotFailedError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

        logger.info(
            f"Workflow {workflow_id} resumed, {len(retried)} task(s) re-queued",
            extra={
                "event": "workflow_resumed",
                "workflow_id": workflow_id,
                "retried_tasks": retried,
            },
        )
        return WorkflowResumeResponse(
            workflow_id=workflow_id,
            status="running",
            retried_tasks=retried,
            message=f"{len(retried)} task(s) re-queued",
        )

    @router.post(
        "/{workflow_id}/tasks/{task_id}/retry",
        response_model=TaskRetryResponse,
        summary="Re-queue one failed task",
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    def retry_task(workflow_id: str, task_id: str) -> TaskRetryResponse:
        """Retry a single failed task, leaving the rest of the workflow alone."""
        try:
            task = engine.retry_task(workflow_id, task_id)
        except (WorkflowNotFoundError, TaskNotFoundError):
            raise HTTPException(
                status_code=404, detail=f"Task not found: {workflow_id}/{task_id}"
            )
        except NotFailedError as exc:
            raise HTTPException(status_code=409, detail=str(exc))

        logger.info(
            f"Task {task_id} of workflow {workflow_id} re-queued",
            extra={
                "event": "task_retried",
                "workflow_id": workflow_id,
                "task_id": task_id,
            },
        )
        return TaskRetryResponse(
            workflow_id=workflow_id,
            task_id=task_id,
            node_key=task.node_key,
            status=task.status.value,
            message="Task re-queued",
        )

    return router
