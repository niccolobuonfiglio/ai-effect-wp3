"""Redis-based task queue for workflow task distribution."""

import time
from collections.abc import Callable

from redis import Redis


class RedisTaskQueue:
    """BRPOP-based task queue for distributing tasks to workers."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    def _queue_key(self, workflow_id: str) -> str:
        return f"queue:{workflow_id}"

    def _delayed_key(self, workflow_id: str) -> str:
        return f"delayed:{workflow_id}"

    def _attempts_key(self, workflow_id: str) -> str:
        return f"attempts:{workflow_id}"

    def _blocked_since_key(self, workflow_id: str) -> str:
        return f"blocked_since:{workflow_id}"

    def mark_blocked(self, workflow_id: str, task_id: str, now: float | None = None) -> float:
        """Record when a task was *first* blocked; returns that moment.

        Later calls keep the original time, so the age of an outage is measured
        from the first deferral rather than the last one.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        now = time.time() if now is None else now
        key = self._blocked_since_key(workflow_id)
        self._redis.hsetnx(key, task_id, now)
        return float(self._redis.hget(key, task_id))

    def increment_attempts(self, workflow_id: str, task_id: str) -> int:
        """Count one more deferral of a task; returns the new count (1, 2, …)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        return int(self._redis.hincrby(self._attempts_key(workflow_id), task_id, 1))

    def reset_attempts(self, workflow_id: str, task_id: str) -> None:
        """Forget a task's deferral count and start time once it has finished."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        self._redis.hdel(self._attempts_key(workflow_id), task_id)
        self._redis.hdel(self._blocked_since_key(workflow_id), task_id)

    def enqueue_task(self, workflow_id: str, task_id: str) -> None:
        """Add task to workflow queue (FIFO)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        self._redis.lpush(self._queue_key(workflow_id), task_id)

    def dequeue_task(self, workflow_id: str, timeout: int = 0) -> str | None:
        """Remove and return next task from queue.

        Blocks up to ``timeout`` seconds; ``timeout=0`` returns immediately
        (unlike Redis BRPOP, where 0 means block forever).
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if timeout < 0:
            raise ValueError("timeout must be non-negative")

        if timeout == 0:
            task_id = self._redis.rpop(self._queue_key(workflow_id))
        else:
            result = self._redis.brpop(self._queue_key(workflow_id), timeout=timeout)
            task_id = result[1] if result is not None else None
        if task_id is None:
            return None

        if isinstance(task_id, bytes):
            task_id = task_id.decode("utf-8")
        return task_id

    def schedule_task(self, workflow_id: str, task_id: str, delay: float) -> None:
        """Park a task and make it eligible for the queue after ``delay`` seconds.

        Rescheduling an already parked task moves its release time.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")
        if delay < 0:
            raise ValueError("delay must be non-negative")

        self._redis.zadd(self._delayed_key(workflow_id), {task_id: time.time() + delay})

    def release_due_tasks(
        self,
        workflow_id: str,
        now: float | None = None,
        on_release: Callable[[str], None] | None = None,
    ) -> list[str]:
        """Move parked tasks whose delay has elapsed onto the queue."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        now = time.time() if now is None else now
        due = self._redis.zrangebyscore(self._delayed_key(workflow_id), "-inf", now)
        return self.release_tasks(workflow_id, due, on_release)

    def release_tasks(
        self,
        workflow_id: str,
        task_ids: list[str],
        on_release: Callable[[str], None] | None = None,
    ) -> list[str]:
        """Move specific parked tasks onto the queue immediately.

        Only tasks this call removed from the delayed set are enqueued, so
        concurrent callers (API and worker replicas) never enqueue a task twice.
        ``on_release`` runs after a task is claimed but before it is enqueued,
        so state updates cannot race with a worker picking the task up.
        Returns the released task ids.
        """
        if not workflow_id:
            raise ValueError("workflow_id is required")

        released = []
        for task_id in task_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode("utf-8")
            if self._redis.zrem(self._delayed_key(workflow_id), task_id):
                if on_release is not None:
                    on_release(task_id)
                self.enqueue_task(workflow_id, task_id)
                released.append(task_id)
        return released

    def delayed_tasks(self, workflow_id: str) -> list[str]:
        """Parked task ids, earliest release first."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        return [
            t.decode("utf-8") if isinstance(t, bytes) else t
            for t in self._redis.zrange(self._delayed_key(workflow_id), 0, -1)
        ]

    def peek_queue(self, workflow_id: str, count: int = 10) -> list[str]:
        """View tasks in queue without removing them."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if count < 0:
            raise ValueError("count must be non-negative")

        # LRANGE returns from head to tail, we want FIFO order (tail first)
        items = self._redis.lrange(self._queue_key(workflow_id), -count, -1)
        result = []
        for item in reversed(items):
            if isinstance(item, bytes):
                item = item.decode("utf-8")
            result.append(item)
        return result

    def queue_length(self, workflow_id: str) -> int:
        """Get number of tasks in queue."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        return self._redis.llen(self._queue_key(workflow_id))

    def clear_queue(self, workflow_id: str) -> None:
        """Remove all tasks from queue, including parked ones."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        self._redis.delete(
            self._queue_key(workflow_id),
            self._delayed_key(workflow_id),
            self._attempts_key(workflow_id),
            self._blocked_since_key(workflow_id),
        )
