"""Workflow engine for orchestrating task execution.
"""

import hashlib
import json
import logging
import time

from redis import Redis

from models.data_reference import DataReference
from models.graph import ExecutionGraph, GraphNode
from models.state import TaskState, TaskStatus, WorkflowState, WorkflowStatus
from services.privacy import sanitize_error
from services.secrets import services_key_name
from services.state_store import RedisStateStore, TaskNotFoundError
from services.task_queue import DEFAULT_LEASE_SECONDS, RedisTaskQueue, Writer

logger = logging.getLogger(__name__)

INITIAL_INPUTS = "initial"
SOURCE_NODE_KEY = "source_node"
CLAIM_POLL_SECONDS = 0.1


def _decode(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _counted(task: TaskState, counter: str) -> TaskState:
    """``task`` with one more of ``counter`` (claims, reclaims, deferrals, retries)."""
    return task.model_copy(update={counter: getattr(task, counter) + 1})


class NotFailedError(Exception):
    """Raised when retrying something that has not failed."""


class WorkflowEngine:
    """Orchestrates workflow execution using state store and task queue."""

    def __init__(
        self,
        state_store: RedisStateStore,
        task_queue: RedisTaskQueue,
        redis_client: Redis,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
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
        self._lease_seconds = lease_seconds

    @property
    def state_store(self) -> RedisStateStore:
        return self._state_store

    @property
    def task_queue(self) -> RedisTaskQueue:
        return self._task_queue

    @property
    def redis(self) -> Redis:
        return self._redis

    def deps_key(self, workflow_id: str, task_id: str) -> str:
        return f"deps:{workflow_id}:{task_id}"

    def dependents_key(self, workflow_id: str, task_id: str) -> str:
        return f"dependents:{workflow_id}:{task_id}"

    def graph_key(self, workflow_id: str) -> str:
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

        store = self._state_store
        workflow = store.new_workflow(workflow_id)
        tasks = {
            node_key: store.new_task(
                workflow_id, self._task_id_from_node_key(node_key), node_key
            )
            for node_key in graph.all_nodes
        }

        def initialize(pipe):
            if pipe.exists(store.workflow_key(workflow_id)):
                raise ValueError(f"Workflow already exists: {workflow_id}")
            pipe.multi()
            store.write_workflow(workflow, pipe)
            for node_key, graph_node in graph.all_nodes.items():
                task_id = tasks[node_key].task_id
                store.write_task(tasks[node_key], pipe)
                pipe.hset(self.graph_key(workflow_id), node_key, task_id)

                for dep in graph_node.dependencies:
                    pipe.sadd(
                        self.deps_key(workflow_id, task_id), tasks[dep.key].task_id
                    )

                for next_node in graph_node.next_nodes:
                    pipe.sadd(
                        self.dependents_key(workflow_id, task_id),
                        tasks[next_node.key].task_id,
                    )

        self._redis.transaction(initialize, store.workflow_key(workflow_id))
        return workflow

    def start_workflow(
        self,
        workflow_id: str,
        initial_inputs: list[DataReference] | None = None,
    ) -> None:
        """Start workflow execution by enqueueing initial tasks.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        store, queue = self._state_store, self._task_queue
        payload = (
            self._inputs_payload(INITIAL_INPUTS, initial_inputs)
            if initial_inputs
            else None
        )

        def start(pipe):
            workflow = store.get_workflow(workflow_id, client=pipe)
            # Tasks with no dependencies
            starts = [
                task_id
                for task_id in map(_decode, pipe.hvals(self.graph_key(workflow_id)))
                if pipe.scard(self.deps_key(workflow_id, task_id)) == 0
            ]
            if starts:
                pipe.watch(*(queue.queued_key(workflow_id, t) for t in starts))
            fresh = [t for t in starts if not queue.is_enqueued(workflow_id, t, pipe)]

            pipe.multi()
            store.write_workflow(
                store.with_workflow_status(workflow, WorkflowStatus.RUNNING), pipe
            )
            for task_id in starts:
                # Set initial inputs for start nodes
                if payload is not None:
                    queue.add_inputs(
                        workflow_id, task_id, INITIAL_INPUTS, payload, client=pipe
                    )
                if task_id in fresh:
                    queue.enqueue_once(workflow_id, task_id, client=pipe)

        self._redis.transaction(start, store.workflow_key(workflow_id))

    def claim_task(self, workflow_id: str, timeout: float = 0) -> TaskState | None:
        """Claim next available task for execution.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        deadline = time.monotonic() + timeout
        while True:
            task = self._claim_once(workflow_id)
            remaining = deadline - time.monotonic()
            if task is not None or remaining <= 0:
                return task
            time.sleep(min(CLAIM_POLL_SECONDS, remaining))

    def _claim_once(self, workflow_id: str) -> TaskState | None:
        store = self._state_store
        claimed: TaskState | None = None

        def mark_running(pipe, task_id: str) -> Writer:
            nonlocal claimed
            pipe.watch(
                store.task_key(workflow_id, task_id),
                self._task_queue.inputs_key(workflow_id, task_id),
            )
            claimed = _counted(
                store.with_task_status(
                    store.get_task(workflow_id, task_id, client=pipe),
                    TaskStatus.RUNNING,
                    input_refs=self._collect_inputs(workflow_id, task_id, client=pipe),
                ),
                "claims",
            )
            return lambda p: store.write_task(claimed, p)

        try:
            task_id = self._task_queue.claim(
                workflow_id, self._lease_seconds, on_claim=mark_running
            )
        except TaskNotFoundError as e:
            logger.warning(
                "Dropping queued task %s of workflow %s: no task state",
                e.task_id,
                workflow_id,
            )
            self._task_queue.discard(workflow_id, e.task_id)
            return None
        return claimed if task_id is not None else None

    def renew_lease(self, workflow_id: str, task_id: str) -> None:
        """Extend the lease of a task that is still being worked on."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._task_queue.lease(workflow_id, task_id, self._lease_seconds)

    def complete_task(
        self,
        workflow_id: str,
        task_id: str,
        output_refs: list[DataReference] | None = None,
    ) -> TaskState:
        """Mark task as completed and enqueue ready dependents.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        store, queue = self._state_store, self._task_queue

        def complete(pipe):
            task = store.get_task(workflow_id, task_id, client=pipe)
            dependents = [
                _decode(d)
                for d in pipe.smembers(self.dependents_key(workflow_id, task_id))
            ]
            others = [t for t in store.task_ids(workflow_id, client=pipe) if t != task_id]
            watched = [store.task_key(workflow_id, t) for t in others]
            for dep_id in dependents:
                watched.append(self.deps_key(workflow_id, dep_id))
                watched.append(queue.queued_key(workflow_id, dep_id))
            if watched:
                pipe.watch(*watched)

            ready = [
                dep_id
                for dep_id in dependents
                if {_decode(d) for d in pipe.smembers(self.deps_key(workflow_id, dep_id))}
                <= {task_id}
                and not queue.is_enqueued(workflow_id, dep_id, pipe)
            ]
            workflow_done = all(
                store.get_task(workflow_id, t, client=pipe).status == TaskStatus.COMPLETED
                for t in others
            )
            workflow = store.get_workflow(workflow_id, client=pipe) if workflow_done else None

            completed = store.with_task_status(
                task, TaskStatus.COMPLETED, output_refs=output_refs
            )
            payload = (
                self._inputs_payload(task.node_key, output_refs) if output_refs else None
            )

            pipe.multi()
            store.write_task(completed, pipe)
            queue.reset_attempts(workflow_id, task_id, client=pipe)
            queue.release_lease(workflow_id, task_id, client=pipe)
            queue.unschedule(workflow_id, task_id, client=pipe)
            for dep_id in dependents:
                if payload is not None:
                    queue.add_inputs(workflow_id, dep_id, task_id, payload, client=pipe)
                pipe.srem(self.deps_key(workflow_id, dep_id), task_id)
            for dep_id in ready:
                queue.enqueue_once(workflow_id, dep_id, client=pipe)

            if workflow is not None:
                store.write_workflow(
                    store.with_workflow_status(workflow, WorkflowStatus.COMPLETED), pipe
                )
                pipe.delete(services_key_name(workflow_id))
            return completed

        return self._redis.transaction(
            complete,
            store.task_key(workflow_id, task_id),
            self.dependents_key(workflow_id, task_id),
            store.workflow_tasks_key(workflow_id),
            value_from_callable=True,
        )

    def _inputs_payload(self, source_node: str, refs: list[DataReference]) -> str:
        """Serialize refs handed to a dependent, each tagged with its producer.
        """
        return json.dumps([
            ref.model_dump(mode="json")
            | {"metadata": {**ref.metadata, SOURCE_NODE_KEY: source_node}}
            for ref in refs
        ])

    def _collect_inputs(
        self, workflow_id: str, task_id: str, client=None
    ) -> list[DataReference]:
        """The inputs stored for a task, in producer order."""
        refs: list[DataReference] = []
        for payload in self._task_queue.inputs(workflow_id, task_id, client=client):
            refs.extend(DataReference(**item) for item in json.loads(payload))
        return refs

    def fail_task(self, workflow_id: str, task_id: str, error: str) -> TaskState:
        """Mark task as failed and fail the workflow."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not error:
            raise ValueError("error is required")

        error = sanitize_error(error)
        store, queue = self._state_store, self._task_queue

        def fail(pipe):
            failed = store.with_task_status(
                store.get_task(workflow_id, task_id, client=pipe),
                TaskStatus.FAILED,
                error=error,
            )
            workflow = store.get_workflow(workflow_id, client=pipe)
            pipe.multi()
            store.write_task(failed, pipe)
            queue.reset_attempts(workflow_id, task_id, client=pipe)
            queue.release_lease(workflow_id, task_id, client=pipe)
            queue.unschedule(workflow_id, task_id, client=pipe)
            store.write_workflow(
                store.with_workflow_status(
                    workflow, WorkflowStatus.FAILED, error=f"Task {task_id} failed: {error}"
                ),
                pipe,
            )
            return failed

        return self._redis.transaction(
            fail,
            store.task_key(workflow_id, task_id),
            store.workflow_key(workflow_id),
            value_from_callable=True,
        )

    def defer_task(
        self, workflow_id: str, task_id: str, reason: str, delay: float
    ) -> TaskState:
        """Mark a task BLOCKED and re-queue it after delay seconds.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if not reason:
            raise ValueError("reason is required")

        store, queue = self._state_store, self._task_queue

        def defer(pipe):
            blocked = _counted(
                store.with_task_status(
                    store.get_task(workflow_id, task_id, client=pipe),
                    TaskStatus.BLOCKED,
                    error=sanitize_error(reason),
                ),
                "deferrals",
            )
            pipe.multi()
            store.write_task(blocked, pipe)
            queue.release_lease(workflow_id, task_id, client=pipe)
            queue.schedule_task(workflow_id, task_id, delay, client=pipe)
            return blocked

        return self._redis.transaction(
            defer, store.task_key(workflow_id, task_id), value_from_callable=True
        )

    def record_blocked_attempt(self, workflow_id: str, task_id: str) -> int:
        """Count a deferral of this task and return the count (1 for the first).
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
        """Re-queue tasks that are due to run again.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        released = self._task_queue.release_due_tasks(
            workflow_id, on_release=self._back_to_pending(workflow_id)
        )
        reclaimed = self._task_queue.reclaim_expired(
            workflow_id,
            on_reclaim=self._back_to_pending(workflow_id, counter="reclaims"),
        )
        for task_id in reclaimed:
            logger.warning(
                "Task %s of workflow %s lost its worker; re-queued it",
                task_id,
                workflow_id,
                extra={
                    "event": "task_reclaimed",
                    "workflow_id": workflow_id,
                    "task_id": task_id,
                },
            )
        return len(released) + len(reclaimed)

    def _back_to_pending(self, workflow_id: str, counter: str | None = None):
        """Queue hook that resets a task to PENDING as it is re-queued.
        """
        store = self._state_store

        def hook(pipe, task_id: str) -> Writer:
            pipe.watch(store.task_key(workflow_id, task_id))
            pending = store.with_task_status(
                store.get_task(workflow_id, task_id, client=pipe),
                TaskStatus.PENDING,
                clear_error=True,
            )
            if counter is not None:
                pending = _counted(pending, counter)
            return lambda p: store.write_task(pending, p)

        return hook

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
            workflow_id, matching, on_release=self._back_to_pending(workflow_id)
        )
        return len(released)

    def retry_task(self, workflow_id: str, task_id: str) -> TaskState:
        """Put a failed task back on the queue, keeping everything else.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        store = self._state_store

        def retry(pipe):
            task = store.get_task(workflow_id, task_id, client=pipe)
            if task.status != TaskStatus.FAILED:
                raise NotFailedError(
                    f"Task {task_id} is {task.status.value}, only failed tasks can be retried"
                )
            workflow = store.get_workflow(workflow_id, client=pipe)
            pending, requeue = self._prepare_requeue(pipe, workflow_id, task)
            pipe.multi()
            requeue(pipe)
            # A workflow set running again has no error
            store.write_workflow(
                store.with_workflow_status(workflow, WorkflowStatus.RUNNING), pipe
            )
            return pending

        return self._redis.transaction(
            retry,
            store.task_key(workflow_id, task_id),
            store.workflow_key(workflow_id),
            value_from_callable=True,
        )

    def resume_workflow(self, workflow_id: str) -> list[str]:
        """Re-queue every failed task of a failed workflow.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        store = self._state_store

        def resume(pipe):
            workflow = store.get_workflow(workflow_id, client=pipe)
            if workflow.status != WorkflowStatus.FAILED:
                raise NotFailedError(
                    f"Workflow {workflow_id} is {workflow.status.value}, "
                    "only failed workflows can be resumed"
                )
            task_ids = store.task_ids(workflow_id, client=pipe)
            if task_ids:
                pipe.watch(*(store.task_key(workflow_id, t) for t in task_ids))
            failed = [
                task
                for task in store.get_workflow_tasks(workflow_id, client=pipe)
                if task.status == TaskStatus.FAILED
            ]
            requeues = [self._prepare_requeue(pipe, workflow_id, t)[1] for t in failed]

            pipe.multi()
            for requeue in requeues:
                requeue(pipe)
            # A workflow set running again has no error
            store.write_workflow(
                store.with_workflow_status(workflow, WorkflowStatus.RUNNING), pipe
            )
            return [task.task_id for task in failed]

        return self._redis.transaction(
            resume,
            store.workflow_key(workflow_id),
            store.workflow_tasks_key(workflow_id),
            value_from_callable=True,
        )

    def _prepare_requeue(
        self, pipe, workflow_id: str, task: TaskState
    ) -> tuple[TaskState, Writer]:
        """Reset one failed task and queue it if its dependencies are met.
        """
        store, queue = self._state_store, self._task_queue
        task_id = task.task_id
        deps_key = self.deps_key(workflow_id, task_id)
        pipe.watch(deps_key)
        ready = pipe.scard(deps_key) == 0
        pending = _counted(
            store.with_task_status(task, TaskStatus.PENDING, clear_error=True), "retries"
        )

        def write(p) -> None:
            store.write_task(pending, p)
            queue.reset_attempts(workflow_id, task_id, client=p)
            queue.clear_enqueue_guard(workflow_id, task_id, client=p)
            if ready:
                queue.enqueue_once(workflow_id, task_id, client=p)

        return pending, write

    def fail_if_stalled(self, workflow_id: str) -> list[str] | None:
        """Fail a running workflow that can no longer make progress.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        store, queue = self._state_store, self._task_queue
        scheduled = (
            queue.queue_key(workflow_id),
            queue.delayed_key(workflow_id),
            queue.processing_key(workflow_id),
        )
        if self._redis.exists(*scheduled):
            return None

        def check(pipe):
            if pipe.exists(*scheduled):
                return None
            workflow = store.get_workflow(workflow_id, client=pipe)
            if workflow.status != WorkflowStatus.RUNNING:
                return None
            task_ids = store.task_ids(workflow_id, client=pipe)
            if task_ids:
                pipe.watch(*(store.task_key(workflow_id, t) for t in task_ids))
            tasks = [store.get_task(workflow_id, t, client=pipe) for t in task_ids]
            if all(t.status == TaskStatus.COMPLETED for t in tasks):
                return None
            failed = sorted(t.task_id for t in tasks if t.status == TaskStatus.FAILED)
            if not failed:
                return []
            pipe.multi()
            store.write_workflow(
                store.with_workflow_status(
                    workflow,
                    WorkflowStatus.FAILED,
                    error=(
                        f"Stalled: nothing left to run while task(s) "
                        f"{', '.join(failed)} failed and were not retried; "
                        f"resume the workflow or retry them"
                    ),
                ),
                pipe,
            )
            return failed

        return self._redis.transaction(
            check,
            store.workflow_key(workflow_id),
            store.workflow_tasks_key(workflow_id),
            *scheduled,
            value_from_callable=True,
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
