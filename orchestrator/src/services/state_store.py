"""Redis-based state store for workflow and task management."""

from datetime import datetime, timezone

from redis import Redis

from models.data_reference import DataReference
from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus


class WorkflowNotFoundError(Exception):
    """Raised when workflow is not found."""

    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        super().__init__(f"Workflow not found: {workflow_id}")


class TaskNotFoundError(Exception):
    """Raised when task is not found."""

    def __init__(self, workflow_id: str, task_id: str):
        self.workflow_id = workflow_id
        self.task_id = task_id
        super().__init__(f"Task not found: {workflow_id}/{task_id}")


class RedisStateStore:
    """Manages workflow and task state in Redis."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    def _workflow_key(self, workflow_id: str) -> str:
        return f"workflow:{workflow_id}"

    def _task_key(self, workflow_id: str, task_id: str) -> str:
        return f"task:{workflow_id}:{task_id}"

    def _workflow_tasks_key(self, workflow_id: str) -> str:
        return f"workflow:{workflow_id}:tasks"

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    def create_workflow(self, workflow_id: str) -> WorkflowState:
        """Create a new workflow in pending state."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        key = self._workflow_key(workflow_id)
        if self._redis.exists(key):
            raise ValueError(f"Workflow already exists: {workflow_id}")

        now = self._utc_now()
        state = WorkflowState(
            workflow_id=workflow_id,
            status=WorkflowStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        self._redis.set(key, state.model_dump_json())
        return state

    def get_workflow(self, workflow_id: str) -> WorkflowState:
        """Get workflow state by ID."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        key = self._workflow_key(workflow_id)
        data = self._redis.get(key)
        if data is None:
            raise WorkflowNotFoundError(workflow_id)

        return WorkflowState.model_validate_json(data)

    def update_workflow_status(
        self,
        workflow_id: str,
        status: WorkflowStatus,
        error: str | None = None,
    ) -> WorkflowState:
        """Update workflow status."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        state = self.get_workflow(workflow_id)
        updated = state.model_copy(
            update={
                "status": status,
                "updated_at": self._utc_now(),
                "error": error,
            }
        )

        key = self._workflow_key(workflow_id)
        self._redis.set(key, updated.model_dump_json())
        return updated

    def create_task(
        self,
        workflow_id: str,
        task_id: str,
        node_key: str,
        input_refs: list[DataReference] | None = None,
    ) -> TaskState:
        """Create a new task in pending state."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not node_key:
            raise ValueError("node_key is required")

        # Verify workflow exists
        self.get_workflow(workflow_id)

        key = self._task_key(workflow_id, task_id)
        if self._redis.exists(key):
            raise ValueError(f"Task already exists: {workflow_id}/{task_id}")

        now = self._utc_now()
        state = TaskState(
            task_id=task_id,
            workflow_id=workflow_id,
            node_key=node_key,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
            input_refs=input_refs or [],
        )

        self._redis.set(key, state.model_dump_json())
        self._redis.sadd(self._workflow_tasks_key(workflow_id), task_id)
        return state

    def get_task(self, workflow_id: str, task_id: str) -> TaskState:
        """Get task state by ID."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        key = self._task_key(workflow_id, task_id)
        data = self._redis.get(key)
        if data is None:
            raise TaskNotFoundError(workflow_id, task_id)

        return TaskState.model_validate_json(data)

    def update_task_status(
        self,
        workflow_id: str,
        task_id: str,
        status: TaskStatus,
        output_refs: list[DataReference] | None = None,
        error: str | None = None,
    ) -> TaskState:
        """Update task status and optionally set output refs."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        state = self.get_task(workflow_id, task_id)
        update_data: dict = {
            "status": status,
            "updated_at": self._utc_now(),
        }
        if output_refs is not None:
            update_data["output_refs"] = output_refs
        if error is not None:
            update_data["error"] = error

        updated = state.model_copy(update=update_data)
        key = self._task_key(workflow_id, task_id)
        self._redis.set(key, updated.model_dump_json())
        return updated

    def get_workflow_tasks(self, workflow_id: str) -> list[TaskState]:
        """Get all tasks for a workflow."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        # Verify workflow exists
        self.get_workflow(workflow_id)

        task_ids = self._redis.smembers(self._workflow_tasks_key(workflow_id))
        tasks = []
        for task_id in task_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")
            tasks.append(self.get_task(workflow_id, task_id))

        return sorted(tasks, key=lambda t: t.created_at)

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete workflow and all its tasks."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        # Delete all tasks
        task_ids = self._redis.smembers(self._workflow_tasks_key(workflow_id))
        for task_id in task_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")
            self._redis.delete(self._task_key(workflow_id, task_id))

        # Delete task set and workflow
        self._redis.delete(self._workflow_tasks_key(workflow_id))
        self._redis.delete(self._workflow_key(workflow_id))
