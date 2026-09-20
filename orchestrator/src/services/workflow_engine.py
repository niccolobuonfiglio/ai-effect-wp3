"""Workflow engine for orchestrating task execution."""

import hashlib
import time

from redis import Redis

from models.data_reference import DataReference
from models.graph import ExecutionGraph, GraphNode
from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus
from services.privacy import sanitize_error
from services.state_store import RedisStateStore
from services.task_queue import RedisTaskQueue


class WorkflowEngine:
    """Orchestrates workflow execution using state store and task queue."""

    def __init__(
        self,
        state_store: RedisStateStore,
        task_queue: RedisTaskQueue,
        redis_client: Redis,
    ):
        if state_store is None:
            raise ValueError("state_store is required")
        if task_queue is None:
            raise ValueError("task_queue is required")
        if redis_client is None:
            raise ValueError("redis_client is required")

        self._state_store = state_store
        self._task_queue = task_queue
        self._redis = redis_client

    def _deps_key(self, workflow_id: str, task_id: str) -> str:
        return f"deps:{workflow_id}:{task_id}"

    def _dependents_key(self, workflow_id: str, task_id: str) -> str:
        return f"dependents:{workflow_id}:{task_id}"

    def _graph_key(self, workflow_id: str) -> str:
        return f"graph:{workflow_id}"

    def _task_id_from_node_key(self, node_key: str) -> str:
        hash_val = hashlib.sha256(node_key.encode()).hexdigest()[:8]
        return f"task_{hash_val}"

    def initialize_workflow(
        self,
        workflow_id: str,
        graph: ExecutionGraph,
    ) -> WorkflowState:
        """Initialize workflow with tasks from execution graph."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if graph is None:
            raise ValueError("graph is required")
        if not graph.all_nodes:
            raise ValueError("graph must have at least one node")

        workflow = self._state_store.create_workflow(workflow_id)

        # Create tasks and track node_key -> task_id mapping
        node_to_task: dict[str, str] = {}
        for node_key, graph_node in graph.all_nodes.items():
            task_id = self._task_id_from_node_key(node_key)
            node_to_task[node_key] = task_id
            self._state_store.create_task(workflow_id, task_id, node_key)
            self._redis.hset(self._graph_key(workflow_id), node_key, task_id)

        # Set up dependency tracking
        for node_key, graph_node in graph.all_nodes.items():
            task_id = node_to_task[node_key]

            # Track dependencies (tasks this task waits for)
            for dep in graph_node.dependencies:
                dep_task_id = node_to_task[dep.key]
                self._redis.sadd(self._deps_key(workflow_id, task_id), dep_task_id)

            # Track dependents (tasks waiting for this task)
            for next_node in graph_node.next_nodes:
                next_task_id = node_to_task[next_node.key]
                self._redis.sadd(
                    self._dependents_key(workflow_id, task_id), next_task_id
                )

        return workflow

    def start_workflow(
        self,
        workflow_id: str,
        initial_inputs: list[DataReference] | None = None,
    ) -> None:
        """Start workflow execution by enqueueing initial tasks.

        Args:
            workflow_id: The workflow ID.
            initial_inputs: Optional list of DataReferences to pass to start nodes.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        self._state_store.update_workflow_status(workflow_id, WorkflowStatus.RUNNING)

        # Find tasks with no dependencies and enqueue them
        task_ids = self._redis.hvals(self._graph_key(workflow_id))
        for task_id in task_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")

            deps_count = self._redis.scard(self._deps_key(workflow_id, task_id))
            if deps_count == 0:
                # Set initial inputs for start nodes
                if initial_inputs:
                    self._append_input_refs(workflow_id, task_id, initial_inputs)
                self._task_queue.enqueue_task(workflow_id, task_id)

    def claim_task(self, workflow_id: str, timeout: int = 0) -> TaskState | None:
        """Claim next available task for execution."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        task_id = self._task_queue.dequeue_task(workflow_id, timeout)
        if task_id is None:
            return None

        return self._state_store.update_task_status(
            workflow_id, task_id, TaskStatus.RUNNING
        )

    def complete_task(
        self,
        workflow_id: str,
        task_id: str,
        output_refs: list[DataReference] | None = None,
    ) -> TaskState:
        """Mark task as completed and enqueue ready dependents."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        task = self._state_store.update_task_status(
            workflow_id, task_id, TaskStatus.COMPLETED, output_refs=output_refs
        )
        self._task_queue.reset_attempts(workflow_id, task_id)

        # Remove this task from dependents' dependency sets
        dependent_ids = self._redis.smembers(
            self._dependents_key(workflow_id, task_id)
        )
        for dep_id in dependent_ids:
            if isinstance(dep_id, bytes):
                dep_id = dep_id.decode("utf-8")

            # Pass output refs to dependent task as input
            if output_refs:
                self._append_input_refs(workflow_id, dep_id, output_refs)

            self._redis.srem(self._deps_key(workflow_id, dep_id), task_id)

            # If dependent has no more dependencies, enqueue it
            remaining = self._redis.scard(self._deps_key(workflow_id, dep_id))
            if remaining == 0:
                self._task_queue.enqueue_task(workflow_id, dep_id)

        # Check if workflow is complete
        if self._all_tasks_completed(workflow_id):
            self._state_store.update_workflow_status(
                workflow_id, WorkflowStatus.COMPLETED
            )

        return task

    def _append_input_refs(
        self,
        workflow_id: str,
        task_id: str,
        refs: list[DataReference],
    ) -> None:
        """Append input refs to a pending task."""
        task = self._state_store.get_task(workflow_id, task_id)
        updated_refs = list(task.input_refs) + list(refs)
        # Update task with new input refs
        updated = task.model_copy(update={"input_refs": updated_refs})
        key = f"task:{workflow_id}:{task_id}"
        self._redis.set(key, updated.model_dump_json())

    def fail_task(self, workflow_id: str, task_id: str, error: str) -> TaskState:
        """Mark task as failed and fail the workflow."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not error:
            raise ValueError("error is required")

        error = sanitize_error(error)
        task = self._state_store.update_task_status(
            workflow_id, task_id, TaskStatus.FAILED, error=error
        )
        self._task_queue.reset_attempts(workflow_id, task_id)

        self._state_store.update_workflow_status(
            workflow_id, WorkflowStatus.FAILED, error=f"Task {task_id} failed: {error}"
        )

        return task

    def defer_task(
        self, workflow_id: str, task_id: str, reason: str, delay: float
    ) -> TaskState:
        """Mark a task BLOCKED and re-queue it after ``delay`` seconds.

        Unlike ``fail_task`` the workflow keeps running, so it can continue once
        the blocking service is healthy again.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not reason:
            raise ValueError("reason is required")

        task = self._state_store.update_task_status(
            workflow_id, task_id, TaskStatus.BLOCKED, error=sanitize_error(reason)
        )
        self._task_queue.schedule_task(workflow_id, task_id, delay)
        return task

    def record_blocked_attempt(self, workflow_id: str, task_id: str) -> int:
        """Count a deferral of this task and return the count (1 for the first).

        The worker uses it to grow the retry delay; the count is cleared when
        the task completes or fails.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        return self._task_queue.increment_attempts(workflow_id, task_id)

    def blocked_age(self, workflow_id: str, task_id: str) -> float:
        """Seconds since this task was first blocked (0 the first time)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        return max(0.0, time.time() - self._task_queue.mark_blocked(workflow_id, task_id))

    def release_due_tasks(self, workflow_id: str) -> int:
        """Re-queue blocked tasks whose retry delay has elapsed."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        released = self._task_queue.release_due_tasks(
            workflow_id, on_release=lambda t: self._mark_released(workflow_id, t)
        )
        return len(released)

    def resume_blocked_tasks(self, workflow_id: str, service_name: str) -> int:
        """Immediately re-queue tasks of a running workflow blocked on a service.

        Returns the number of tasks re-queued.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not service_name:
            raise ValueError("service_name is required")

        workflow = self._state_store.get_workflow(workflow_id)
        if workflow.status != WorkflowStatus.RUNNING:
            return 0

        prefix = f"{service_name}:"
        matching = [
            task_id
            for task_id in self._task_queue.delayed_tasks(workflow_id)
            if self._state_store.get_task(workflow_id, task_id).node_key.startswith(prefix)
        ]
        released = self._task_queue.release_tasks(
            workflow_id, matching, on_release=lambda t: self._mark_released(workflow_id, t)
        )
        return len(released)

    def _mark_released(self, workflow_id: str, task_id: str) -> None:
        self._state_store.update_task_status(
            workflow_id, task_id, TaskStatus.PENDING, clear_error=True
        )

    def get_workflow_status(self, workflow_id: str) -> WorkflowState:
        """Get current workflow state."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        return self._state_store.get_workflow(workflow_id)

    def is_workflow_complete(self, workflow_id: str) -> bool:
        """Check if workflow has completed (success or failure)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        state = self._state_store.get_workflow(workflow_id)
        return state.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED)

    def get_all_tasks(self, workflow_id: str) -> list[TaskState]:
        """Get all tasks for a workflow."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        return self._state_store.get_workflow_tasks(workflow_id)

    def _all_tasks_completed(self, workflow_id: str) -> bool:
        """Check if all tasks in workflow are completed."""
        tasks = self._state_store.get_workflow_tasks(workflow_id)
        return all(t.status == TaskStatus.COMPLETED for t in tasks)
