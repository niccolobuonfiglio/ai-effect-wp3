"""Redis-based task queue for workflow task distribution."""

from redis import Redis


class RedisTaskQueue:
    """BRPOP-based task queue for distributing tasks to workers."""

    def __init__(self, redis_client: Redis):
        if redis_client is None:
            raise ValueError("redis_client is required")
        self._redis = redis_client

    def _queue_key(self, workflow_id: str) -> str:
        return f"queue:{workflow_id}"

    def enqueue_task(self, workflow_id: str, task_id: str) -> None:
        """Add task to workflow queue (FIFO)."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if not task_id:
            raise ValueError("task_id is required")

        self._redis.lpush(self._queue_key(workflow_id), task_id)

    def dequeue_task(self, workflow_id: str, timeout: int = 0) -> str | None:
        """Remove and return next task from queue. Blocks up to timeout seconds."""
        if not workflow_id:
            raise ValueError("workflow_id is required")
        if timeout < 0:
            raise ValueError("timeout must be non-negative")

        result = self._redis.brpop(self._queue_key(workflow_id), timeout=timeout)
        if result is None:
            return None

        _, task_id = result
        if isinstance(task_id, bytes):
            task_id = task_id.decode("utf-8")
        return task_id

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
        """Remove all tasks from queue."""
        if not workflow_id:
            raise ValueError("workflow_id is required")

        self._redis.delete(self._queue_key(workflow_id))
