"""Redis-based state store for workflow and task management.

Every multi-key change is applied with ``MULTI``/``EXEC``, and every
read-modify-write of a state document is guarded by ``WATCH``, so a crash
cannot leave a change half-written and two writers cannot overwrite each
other's update. Methods that read or write take an optional ``client``: the
engine passes the pipeline of its own transaction, so its reads are watched and
its writes are applied together with those of the task queue.
"""

import json
from datetime import datetime, timezone

from redis import Redis

from models.data_reference import DataReference
from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus

# Ids of the workflows in RUNNING status, kept in step with the status itself
# so workers find them without scanning the keyspace.
RUNNING_WORKFLOWS_KEY = "workflows:running"


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


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class RedisStateStore:
    """Manages workflow and task state in Redis."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    def workflow_key(self, workflow_id: str) -> str:
        return f"workflow:{workflow_id}"

    def task_key(self, workflow_id: str, task_id: str) -> str:
        return f"task:{workflow_id}:{task_id}"

    def workflow_tasks_key(self, workflow_id: str) -> str:
        return f"workflow:{workflow_id}:tasks"

    def _utc_now(self) -> datetime:
        return datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # building and writing state documents
    # ------------------------------------------------------------------
    def new_workflow(self, workflow_id: str) -> WorkflowState:
        """A workflow in pending state, not yet stored."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        now = self._utc_now()
        return WorkflowState(
            workflow_id=workflow_id,
            status=WorkflowStatus.PENDING,
            created_at=now,
            updated_at=now,
        )

    def new_task(
        self,
        workflow_id: str,
        task_id: str,
        node_key: str,
        input_refs: list[DataReference] | None = None,
    ) -> TaskState:
        """A task in pending state, not yet stored."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not node_key:
            raise ValueError("node_key is required")
        now = self._utc_now()
        return TaskState(
            task_id=task_id,
            workflow_id=workflow_id,
            node_key=node_key,
            status=TaskStatus.PENDING,
            created_at=now,
            updated_at=now,
            input_refs=input_refs or [],
        )

    def with_workflow_status(
        self, state: WorkflowState, status: WorkflowStatus, error: str | None = None
    ) -> WorkflowState:
        """``state`` with a new status; the error is replaced (None clears it)."""
        return state.model_copy(
            update={"status": status, "updated_at": self._utc_now(), "error": error}
        )

    def with_task_status(
        self,
        state: TaskState,
        status: TaskStatus,
        input_refs: list[DataReference] | None = None,
        output_refs: list[DataReference] | None = None,
        error: str | None = None,
        clear_error: bool = False,
    ) -> TaskState:
        """``state`` with a new status and optionally new refs or error.

        ``clear_error`` resets a previously stored error (ignored if ``error``
        is given).
        """
        update_data: dict = {"status": status, "updated_at": self._utc_now()}
        if input_refs is not None:
            update_data["input_refs"] = input_refs
        if output_refs is not None:
            update_data["output_refs"] = output_refs
        if error is not None:
            update_data["error"] = error
        elif clear_error:
            update_data["error"] = None
        return state.model_copy(update=update_data)

    def write_workflow(self, state: WorkflowState, client=None) -> None:
        """Store a workflow document and keep the running index in step.

        Pass a pipeline in ``MULTI`` mode as ``client`` so both writes are
        applied together.
        """
        r = client if client is not None else self._redis.pipeline(transaction=True)
        r.set(self.workflow_key(state.workflow_id), state.model_dump_json())
        if state.status == WorkflowStatus.RUNNING:
            r.sadd(RUNNING_WORKFLOWS_KEY, state.workflow_id)
        else:
            r.srem(RUNNING_WORKFLOWS_KEY, state.workflow_id)
        if client is None:
            r.execute()

    def write_task(self, state: TaskState, client=None) -> None:
        """Store a task document and list it under its workflow."""
        r = client if client is not None else self._redis.pipeline(transaction=True)
        r.set(self.task_key(state.workflow_id, state.task_id), state.model_dump_json())
        r.sadd(self.workflow_tasks_key(state.workflow_id), state.task_id)
        if client is None:
            r.execute()

    # ------------------------------------------------------------------
    # workflows
    # ------------------------------------------------------------------
    def create_workflow(self, workflow_id: str) -> WorkflowState:
        """Create a new workflow in pending state."""
        state = self.new_workflow(workflow_id)
        key = self.workflow_key(workflow_id)

        def create(pipe):
            if pipe.exists(key):
                raise ValueError(f"Workflow already exists: {workflow_id}")
            pipe.multi()
            self.write_workflow(state, pipe)

        self._redis.transaction(create, key)
        return state

    def get_workflow(self, workflow_id: str, client=None) -> WorkflowState:
        """Get workflow state by ID."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        r = client if client is not None else self._redis
        data = r.get(self.workflow_key(workflow_id))
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

        def update(pipe):
            updated = self.with_workflow_status(
                self.get_workflow(workflow_id, client=pipe), status, error
            )
            pipe.multi()
            self.write_workflow(updated, pipe)
            return updated

        return self._redis.transaction(
            update, self.workflow_key(workflow_id), value_from_callable=True
        )

    def running_workflow_ids(self) -> list[str]:
        """Ids of the workflows currently in RUNNING status."""
        return sorted(_decode(w) for w in self._redis.smembers(RUNNING_WORKFLOWS_KEY))

    def rebuild_running_index(self) -> int:
        """Add running workflows missing from the index.

        Needed once for data written before the index existed; returns the
        number of running workflows found. Each workflow is added in a
        transaction that watches its document, so one that finishes during the
        scan is not added, and workflows changing status meanwhile keep the
        index entries their own writes gave them.
        """
        found = 0
        for key in self._redis.scan_iter(match="workflow:*", count=100):
            key = _decode(key)
            # Skip task sets and other workflow sub-keys
            if key.count(":") > 1:
                continue
            workflow_id = key.split(":", 1)[1]

            def add_if_running(pipe):
                try:
                    status = json.loads(pipe.get(key) or "").get("status")
                except (ValueError, AttributeError):
                    return False
                if status != WorkflowStatus.RUNNING.value:
                    return False
                pipe.multi()
                pipe.sadd(RUNNING_WORKFLOWS_KEY, workflow_id)
                return True

            if self._redis.transaction(add_if_running, key, value_from_callable=True):
                found += 1
        return found

    # ------------------------------------------------------------------
    # tasks
    # ------------------------------------------------------------------
    def create_task(
        self,
        workflow_id: str,
        task_id: str,
        node_key: str,
        input_refs: list[DataReference] | None = None,
    ) -> TaskState:
        """Create a new task in pending state."""
        state = self.new_task(workflow_id, task_id, node_key, input_refs)
        key = self.task_key(workflow_id, task_id)

        def create(pipe):
            # Verify workflow exists
            self.get_workflow(workflow_id, client=pipe)
            if pipe.exists(key):
                raise ValueError(f"Task already exists: {workflow_id}/{task_id}")
            pipe.multi()
            self.write_task(state, pipe)

        self._redis.transaction(create, self.workflow_key(workflow_id), key)
        return state

    def get_task(self, workflow_id: str, task_id: str, client=None) -> TaskState:
        """Get task state by ID."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        r = client if client is not None else self._redis
        data = r.get(self.task_key(workflow_id, task_id))
        if data is None:
            raise TaskNotFoundError(workflow_id, task_id)

        return TaskState.model_validate_json(data)

    def update_task_status(
        self,
        workflow_id: str,
        task_id: str,
        status: TaskStatus,
        input_refs: list[DataReference] | None = None,
        output_refs: list[DataReference] | None = None,
        error: str | None = None,
        clear_error: bool = False,
    ) -> TaskState:
        """Update task status and optionally set input or output refs.

        ``clear_error`` resets a previously stored error (ignored if ``error``
        is given).
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        def update(pipe):
            updated = self.with_task_status(
                self.get_task(workflow_id, task_id, client=pipe),
                status,
                input_refs=input_refs,
                output_refs=output_refs,
                error=error,
                clear_error=clear_error,
            )
            pipe.multi()
            self.write_task(updated, pipe)
            return updated

        return self._redis.transaction(
            update, self.task_key(workflow_id, task_id), value_from_callable=True
        )

    def task_ids(self, workflow_id: str, client=None) -> list[str]:
        """Ids of all tasks of a workflow (unordered)."""
        r = client if client is not None else self._redis
        return [_decode(t) for t in r.smembers(self.workflow_tasks_key(workflow_id))]

    def get_workflow_tasks(self, workflow_id: str, client=None) -> list[TaskState]:
        """Get all tasks for a workflow."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        # Verify workflow exists
        self.get_workflow(workflow_id, client=client)

        tasks = [
            self.get_task(workflow_id, task_id, client=client)
            for task_id in self.task_ids(workflow_id, client=client)
        ]
        return sorted(tasks, key=lambda t: t.created_at)

    def delete_workflow(self, workflow_id: str) -> None:
        """Delete workflow and all its tasks."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        tasks_key = self.workflow_tasks_key(workflow_id)

        def delete(pipe):
            keys = [self.task_key(workflow_id, t) for t in self.task_ids(workflow_id, pipe)]
            pipe.multi()
            pipe.delete(*keys, tasks_key, self.workflow_key(workflow_id))
            pipe.srem(RUNNING_WORKFLOWS_KEY, workflow_id)

        self._redis.transaction(delete, tasks_key)
